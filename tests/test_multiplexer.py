"""
Unit tests for the central FeedMultiplexer.
Tests conflict resolution, sequencing, outlier filtering, and pub/sub distribution.
"""

import asyncio
import pytest
from nexus_feed_multiplexer.models import (
    FeedSource,
    AssetClass,
    FeedHealth,
    UnifiedMarketTick
)
from nexus_feed_multiplexer.config import MultiplexerConfig, VenueConfig
from nexus_feed_multiplexer.multiplexer import FeedMultiplexer


def test_multiplexer_lifecycle_and_tick_processing():
    async def _test():
        config = MultiplexerConfig()
        mux = FeedMultiplexer(config)

        # 1. Verification of handlers
        assert len(mux.handlers) >= 7
        assert FeedSource.IBKR in mux.handlers
        assert FeedSource.BINANCE in mux.handlers
        assert FeedSource.ALPACA in mux.handlers

        # 2. Start multiplexer
        await mux.start()

        # 3. Subscribe callback
        received_ticks = []
        mux.subscribe(lambda t: received_ticks.append(t))

        # Manually dispatch test ticks
        t1 = UnifiedMarketTick(
            symbol="EURUSD",
            raw_symbol="EURUSD",
            source=FeedSource.IBKR,
            asset_class=AssetClass.FOREX,
            bid=1.0850,
            ask=1.0852,
            last=1.0851,
            volume=100
        )
        mux._process_and_dispatch_tick(t1)

        assert len(received_ticks) == 1
        assert received_ticks[0].symbol == "EURUSD"
        assert received_ticks[0].sequence_id > 0
        assert mux.get_latest_tick("EURUSD") is not None
        assert mux.get_latest_tick("EURUSD").last == 1.0851

        await mux.stop()

    asyncio.run(_test())


def test_multiplexer_conflict_and_priority_resolution():
    async def _test():
        mux = FeedMultiplexer()
        mux.set_primary_venue(FeedSource.IBKR, asset_class="forex")

        received = []
        mux.subscribe(lambda t: received.append(t))

        # Preferred venue tick (IBKR)
        ibkr_tick = UnifiedMarketTick(
            symbol="EURUSD",
            raw_symbol="EURUSD",
            source=FeedSource.IBKR,
            asset_class=AssetClass.FOREX,
            bid=1.0850,
            ask=1.0852,
            last=1.0851
        )
        mux._process_and_dispatch_tick(ibkr_tick)
        assert len(received) == 1
        assert received[-1].source == FeedSource.IBKR

        # Secondary venue tick (cTrader) while IBKR is connected & healthy -> should be filtered out
        mux.handlers[FeedSource.IBKR].is_connected = True
        ctrader_tick = UnifiedMarketTick(
            symbol="EURUSD",
            raw_symbol="EURUSD",
            source=FeedSource.CTRADER,
            asset_class=AssetClass.FOREX,
            bid=1.0860,
            ask=1.0862,
            last=1.0861
        )
        mux._process_and_dispatch_tick(ctrader_tick)
        # Count should still be 1 (conflict resolved; secondary suppressed)
        assert len(received) == 1

        # Now simulate IBKR disconnection -> failover to cTrader
        mux.handlers[FeedSource.IBKR].is_connected = False
        mux._process_and_dispatch_tick(ctrader_tick)
        assert len(received) == 2
        assert received[-1].source == FeedSource.CTRADER

    asyncio.run(_test())


def test_multiplexer_outlier_rejection():
    async def _test():
        mux = FeedMultiplexer()

        # Seed baseline ticks around 1.0850
        for _ in range(6):
            t = UnifiedMarketTick(
                symbol="EURUSD",
                raw_symbol="EURUSD",
                source=FeedSource.IBKR,
                asset_class=AssetClass.FOREX,
                bid=1.0850,
                ask=1.0852,
                last=1.0851
            )
            mux._process_and_dispatch_tick(t)

        initial_seq = mux._global_sequence_id

        # Inject spurious 50% spike outlier
        outlier = UnifiedMarketTick(
            symbol="EURUSD",
            raw_symbol="EURUSD",
            source=FeedSource.IBKR,
            asset_class=AssetClass.FOREX,
            bid=1.6500,
            ask=1.6520,
            last=1.6510
        )
        mux._process_and_dispatch_tick(outlier)

        # Sequence ID must not have advanced because outlier was rejected
        assert mux._global_sequence_id == initial_seq
        assert mux.get_latest_tick("EURUSD").last == 1.0851

    asyncio.run(_test())


def test_multiplexer_candle_aggregator_and_telemetry():
    async def _test():
        mux = FeedMultiplexer()
        tick = UnifiedMarketTick(
            symbol="BTCUSDT",
            raw_symbol="BTCUSDT",
            source=FeedSource.BINANCE,
            asset_class=AssetClass.CRYPTO,
            bid=91800.0,
            ask=91802.0,
            last=91801.0,
            volume=5.0
        )
        mux._process_and_dispatch_tick(tick)

        # Check candles
        candles = await mux.get_candles("BTCUSDT", "5m", 10)
        assert len(candles) >= 1

        # Check telemetry status
        telemetry = mux.get_status_telemetry()
        assert "active_primary_source" in telemetry
        assert "venues" in telemetry
        assert "IBKR" in telemetry["venues"]
        assert "BINANCE" in telemetry["venues"]
        assert "total_unified_ticks_processed" in telemetry

    asyncio.run(_test())
