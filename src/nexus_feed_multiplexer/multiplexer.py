"""
Nexus Market Feed Multiplexer
Institutional Cross-Broker Market Data Multiplexer & Arbitrage Engine.
Unifies, normalizes, de-duplicates, and sequences real-time quotes across
IBKR, Alpaca, cTrader, Binance, Bybit, OKX, MT5, and Fallback feeds.
"""

import asyncio
from collections import deque
import logging
import time
from typing import Dict, List, Optional, Callable, Set, Any
import numpy as np

from nexus_feed_multiplexer.models import (
    FeedSource,
    FeedHealth,
    AssetClass,
    UnifiedMarketTick,
    UnifiedCandleBar,
    SymbolMapper
)
from nexus_feed_multiplexer.config import MultiplexerConfig, VenueConfig
from nexus_feed_multiplexer.handlers import (
    BaseFeedHandler,
    IBKRFeedHandler,
    AlpacaFeedHandler,
    CTraderFeedHandler,
    BinanceFeedHandler,
    BybitFeedHandler,
    OKXFeedHandler,
    MT5FeedHandler,
    FallbackExchangeHandler
)

logger = logging.getLogger("NexusMultiplexer.Core")


class FeedMultiplexer:
    """
    Central Price Data Multiplexer.
    Ingests raw streams from multiple brokerages, resolves clashes, eliminates latency
    inconsistencies, applies outlier filters, and emits a standardized unified feed.
    """

    def __init__(self, config: Optional[MultiplexerConfig] = None):
        self.config = config or MultiplexerConfig()
        self.handlers: Dict[FeedSource, BaseFeedHandler] = {}
        self.active_primary_source: FeedSource = FeedSource.IBKR
        self.primary_venues: Dict[str, FeedSource] = dict(self.config.primary_venues)

        # Monotonic Global Sequencer
        self._global_sequence_id: int = 0

        # State Cache
        self._latest_ticks: Dict[str, UnifiedMarketTick] = {}
        self._recent_price_windows: Dict[str, deque] = {}  # Rolling window for outlier detection
        self._subscribers: List[Callable[[UnifiedMarketTick], None]] = []

        # Candle Aggregators (symbol -> timeframe -> List[UnifiedCandleBar])
        self._candles: Dict[str, Dict[str, List[UnifiedCandleBar]]] = {}

        # IBKR Pre-Computation Gate
        self.ibkr_authenticated: bool = False
        self.ibkr_mfa_authenticated: bool = False
        self.ibkr_mfa_method: str = "NONE"

        # Background worker task
        self._running: bool = False
        self._worker_task: Optional[asyncio.Task] = None

        self._init_handlers()

    def _init_handlers(self):
        """Initializes all venue ingestion handlers based on configuration."""
        if self.config.ibkr.enabled:
            self.handlers[FeedSource.IBKR] = IBKRFeedHandler(self.config.ibkr)
        if self.config.alpaca.enabled:
            self.handlers[FeedSource.ALPACA] = AlpacaFeedHandler(self.config.alpaca)
        if self.config.ctrader.enabled:
            self.handlers[FeedSource.CTRADER] = CTraderFeedHandler(self.config.ctrader)
        if self.config.binance.enabled:
            self.handlers[FeedSource.BINANCE] = BinanceFeedHandler(self.config.binance)
        if self.config.bybit.enabled:
            self.handlers[FeedSource.BYBIT] = BybitFeedHandler(self.config.bybit)
        if self.config.okx.enabled:
            self.handlers[FeedSource.OKX] = OKXFeedHandler(self.config.okx)
        if self.config.mt5.enabled:
            self.handlers[FeedSource.MT5] = MT5FeedHandler(self.config.mt5)
        if self.config.fallback.enabled:
            self.handlers[FeedSource.FALLBACK] = FallbackExchangeHandler(self.config.fallback)

        # Register raw tick listener on all handlers
        for handler in self.handlers.values():
            handler.register_tick_callback(self._on_raw_venue_tick)

    async def start(self):
        """Starts all handlers and background multiplexing polling loop."""
        if self._running:
            return
        self._running = True
        logger.info("Starting Nexus Feed Multiplexer across all active venues...")

        # Connect all handlers concurrently
        connect_tasks = [h.connect() for h in self.handlers.values()]
        await asyncio.gather(*connect_tasks, return_exceptions=True)

        self._worker_task = asyncio.create_task(self._multiplexer_poll_loop())
        logger.info(f"Feed Multiplexer online with {len(self.handlers)} registered venue handlers.")

    async def stop(self):
        """Terminates handlers and polling loops."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

        disconnect_tasks = [h.disconnect() for h in self.handlers.values()]
        await asyncio.gather(*disconnect_tasks, return_exceptions=True)
        logger.info("Feed Multiplexer stopped gracefully.")

    def subscribe(self, callback: Callable[[UnifiedMarketTick], None]):
        """Registers a callback to receive every multiplexed unified tick."""
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[UnifiedMarketTick], None]):
        """Unregisters a subscriber callback."""
        if callback in self._subscribers:
            self._subscribers.remove(callback)

    def set_primary_venue(self, source: FeedSource, asset_class: Optional[str] = None):
        """Sets the primary preferred data venue globally or per asset class."""
        if asset_class:
            self.primary_venues[asset_class.lower()] = source
        else:
            self.active_primary_source = source
            for ac in self.primary_venues:
                self.primary_venues[ac] = source
        logger.info(f"Primary venue updated: {source.value} (Asset Class: {asset_class or 'ALL'})")

    def is_computation_allowed(self) -> bool:
        """
        Pre-computation gate:
        When IBKR is the primary active source, model computation is locked
        until the user authenticates with their IBKR credentials and MFA.
        """
        if self.active_primary_source == FeedSource.IBKR:
            return self.ibkr_authenticated
        return True

    def set_ibkr_authenticated(self, authenticated: bool, mfa_method: str = "NONE"):
        """Updates IBKR pre-computation authentication state."""
        self.ibkr_authenticated = authenticated
        self.ibkr_mfa_authenticated = authenticated
        self.ibkr_mfa_method = mfa_method
        if FeedSource.IBKR in self.handlers:
            self.handlers[FeedSource.IBKR].is_connected = True

    # =========================================================================
    # Conflict Resolution, De-duplication & Filtering
    # =========================================================================

    def _on_raw_venue_tick(self, tick: UnifiedMarketTick):
        """Internal callback invoked when any venue handler produces a tick."""
        self._process_and_dispatch_tick(tick)

    def _process_and_dispatch_tick(self, raw_tick: UnifiedMarketTick):
        """
        Validates, sequences, filters outliers, and dispatches unified ticks.
        Prevents clashes and price anomalies.
        """
        symbol = raw_tick.symbol
        asset_class = raw_tick.asset_class.value.lower()
        preferred_venue = self.primary_venues.get(asset_class, self.active_primary_source)

        # 1. Conflict & Priority Resolution
        # If incoming tick is NOT from the preferred venue, only accept it if preferred venue is offline
        if raw_tick.source != preferred_venue:
            pref_handler = self.handlers.get(preferred_venue)
            if pref_handler and pref_handler.is_connected and pref_handler.health_check() == FeedHealth.HEALTHY:
                # Preferred venue is active; ignore secondary venue to prevent price collision
                return

        # 2. Outlier Rejection Guard (Filters bad ticks / flash spikes)
        if symbol not in self._recent_price_windows:
            self._recent_price_windows[symbol] = deque(maxlen=20)

        window = self._recent_price_windows[symbol]
        if len(window) >= 5:
            median_p = float(np.median(list(window)))
            if median_p > 0:
                dev = abs(raw_tick.last - median_p) / median_p
                if dev > self.config.outlier_rejection_pct:
                    logger.warning(
                        f"[OUTLIER_REJECTED] {symbol} tick {raw_tick.last} from {raw_tick.source.value} "
                        f"deviates {dev*100:.1f}% from median {median_p}. Suppressed."
                    )
                    return

        window.append(raw_tick.last)

        # 3. Monotonic Sequencing
        self._global_sequence_id += 1
        raw_tick.sequence_id = self._global_sequence_id

        # 4. Update In-Memory Cache
        self._latest_ticks[symbol] = raw_tick
        self._update_candle_bar(raw_tick)

        # 5. Broadcast to All Downstream Consumers
        for cb in self._subscribers:
            try:
                cb(raw_tick)
            except Exception as ex:
                logger.error(f"Error notifying subscriber of tick {symbol}: {ex}")

    def _update_candle_bar(self, tick: UnifiedMarketTick):
        """Updates real-time rolling OHLCV candles from incoming ticks."""
        symbol = tick.symbol
        if symbol not in self._candles:
            self._candles[symbol] = {"1m": [], "5m": [], "15m": [], "1h": []}

        now_sec = int(time.time())

        for tf, interval in [("1m", 60), ("5m", 300), ("15m", 900), ("1h", 3600)]:
            bucket_ts = (now_sec // interval) * interval
            bars = self._candles[symbol][tf]

            if not bars or bars[-1].timestamp != bucket_ts:
                # Start new bar
                new_bar = UnifiedCandleBar(
                    symbol=symbol,
                    timeframe=tf,
                    open=tick.last,
                    high=tick.last,
                    low=tick.last,
                    close=tick.last,
                    volume=tick.volume,
                    timestamp=bucket_ts,
                    source=tick.source,
                    is_delayed=tick.is_delayed
                )
                bars.append(new_bar)
                if len(bars) > 200:
                    bars.pop(0)
            else:
                # Update existing current bar
                curr = bars[-1]
                curr.high = max(curr.high, tick.last)
                curr.low = min(curr.low, tick.last)
                curr.close = tick.last
                curr.volume += tick.volume

    # =========================================================================
    # Background Polling & Health Heartbeat
    # =========================================================================

    async def _multiplexer_poll_loop(self):
        """Continuous ingestion polling loop across all active venues."""
        while self._running:
            try:
                poll_tasks = []
                for h in self.handlers.values():
                    if h.is_connected:
                        poll_tasks.append(h.poll_ticks())

                if poll_tasks:
                    results = await asyncio.gather(*poll_tasks, return_exceptions=True)
                    for r in results:
                        if isinstance(r, list):
                            for tick in r:
                                self._process_and_dispatch_tick(tick)
            except Exception as e:
                logger.debug(f"Multiplexer cycle error: {e}")

            await asyncio.sleep(1.0)

    # =========================================================================
    # Public Query Interfaces
    # =========================================================================

    def get_latest_tick(self, symbol: str) -> Optional[UnifiedMarketTick]:
        """Returns the most recent verified unified tick for a symbol."""
        canon = SymbolMapper.to_canonical(symbol, FeedSource.FALLBACK)
        return self._latest_ticks.get(canon)

    def get_all_latest_ticks(self) -> List[UnifiedMarketTick]:
        """Returns list of latest ticks for all tracked instruments."""
        return list(self._latest_ticks.values())

    async def get_candles(
        self,
        symbol: str,
        timeframe: str = "5m",
        limit: int = 50
    ) -> List[UnifiedCandleBar]:
        """Returns historical candlestick bars from primary handler or local aggregator."""
        canon = SymbolMapper.to_canonical(symbol, FeedSource.FALLBACK)
        # Check local aggregator first
        if canon in self._candles and timeframe in self._candles[canon]:
            existing = self._candles[canon][timeframe]
            if len(existing) >= limit:
                return existing[-limit:]

        # Query primary venue handler
        handler = self.handlers.get(self.active_primary_source) or self.handlers.get(FeedSource.FALLBACK)
        if handler:
            return await handler.get_historical_candles(canon, timeframe=timeframe, limit=limit)
        return []

    def get_status_telemetry(self) -> Dict[str, Any]:
        """Returns comprehensive real-time status matrix for all venues."""
        venues_status = {}
        for src, h in self.handlers.items():
            venues_status[src.value] = h.get_telemetry()

        return {
            "active_primary_source": self.active_primary_source.value,
            "primary_venues_by_asset": {k: v.value for k, v in self.primary_venues.items()},
            "is_computation_allowed": self.is_computation_allowed(),
            "ibkr_authenticated": self.ibkr_authenticated,
            "ibkr_mfa_authenticated": self.ibkr_mfa_authenticated,
            "ibkr_mfa_method": self.ibkr_mfa_method,
            "total_unified_ticks_processed": self._global_sequence_id,
            "active_instruments_count": len(self._latest_ticks),
            "venues": venues_status
        }
