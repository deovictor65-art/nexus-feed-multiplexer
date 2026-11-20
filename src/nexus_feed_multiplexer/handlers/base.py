"""
Abstract Base Handler for Market Data Ingestion Venues.
Defines standardized async interfaces for streaming and polling price feeds.
"""

from abc import ABC, abstractmethod
import asyncio
import logging
import time
from typing import Dict, List, Optional, Callable
from nexus_feed_multiplexer.models import (
    FeedSource,
    FeedHealth,
    UnifiedMarketTick,
    UnifiedCandleBar,
    SymbolMapper
)
from nexus_feed_multiplexer.config import VenueConfig

logger = logging.getLogger("NexusMultiplexer.Handler")


class BaseFeedHandler(ABC):
    """
    Abstract contract for venue data ingestion handlers.
    Each handler is responsible for:
    - Connecting to venue socket / REST API
    - Ingesting price quotes and normalising to UnifiedMarketTick
    - Maintaining connection state and reporting health
    """

    def __init__(self, source: FeedSource, config: VenueConfig):
        self.source = source
        self.config = config
        self.is_connected: bool = False
        self.last_heartbeat: float = 0.0
        self.last_tick_time: float = 0.0
        self.latency_ms: float = 0.0
        self.error_count: int = 0
        self.subscribed_symbols: List[str] = list(config.symbols)
        self._on_tick_callbacks: List[Callable[[UnifiedMarketTick], None]] = []

    def register_tick_callback(self, callback: Callable[[UnifiedMarketTick], None]) -> None:
        """Subscribes an upstream callback to incoming parsed ticks."""
        self._on_tick_callbacks.append(callback)

    def dispatch_tick(self, tick: UnifiedMarketTick) -> None:
        """Dispatches unified tick to registered downstream consumers."""
        self.last_tick_time = time.time()
        for cb in self._on_tick_callbacks:
            try:
                cb(tick)
            except Exception as ex:
                logger.error(f"[{self.source.value}] Error in tick callback: {ex}")

    @abstractmethod
    async def connect(self) -> bool:
        """Establishes connection to the venue (socket, API session, etc)."""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Gracefully terminates connection."""
        pass

    @abstractmethod
    async def poll_ticks(self) -> List[UnifiedMarketTick]:
        """Polls or drains latest verified market ticks from the venue."""
        pass

    @abstractmethod
    async def get_historical_candles(
        self,
        symbol: str,
        timeframe: str = "5m",
        limit: int = 50
    ) -> List[UnifiedCandleBar]:
        """Retrieves historical OHLCV candlestick bars."""
        pass

    def health_check(self) -> FeedHealth:
        """Determines health status based on connection state and tick recency."""
        if not self.is_connected:
            return FeedHealth.OFFLINE
        
        now = time.time()
        time_since_tick = now - self.last_tick_time
        if time_since_tick > 60.0 and self.last_tick_time > 0:
            return FeedHealth.STALE
        if self.latency_ms > 2000.0 or self.error_count > 5:
            return FeedHealth.DEGRADED
        return FeedHealth.HEALTHY

    def get_telemetry(self) -> Dict[str, Any]:
        """Returns diagnostic telemetry for this venue feed."""
        return {
            "source": self.source.value,
            "connected": self.is_connected,
            "health": self.health_check().value,
            "latency_ms": round(self.latency_ms, 2),
            "subscribed_symbols": self.subscribed_symbols,
            "last_tick_age_sec": round(time.time() - self.last_tick_time, 1) if self.last_tick_time > 0 else None,
            "error_count": self.error_count
        }
