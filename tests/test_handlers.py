"""
Unit tests for all venue ingestion handlers.
"""

import asyncio
import pytest
from nexus_feed_multiplexer.config import VenueConfig
from nexus_feed_multiplexer.models import FeedSource, FeedHealth
from nexus_feed_multiplexer.handlers import (
    IBKRFeedHandler,
    AlpacaFeedHandler,
    CTraderFeedHandler,
    BinanceFeedHandler,
    BybitFeedHandler,
    OKXFeedHandler,
    MT5FeedHandler,
    FallbackExchangeHandler
)


def test_ibkr_handler():
    async def _test():
        cfg = VenueConfig(enabled=True, symbols=["EURUSD", "ES", "CL"])
        handler = IBKRFeedHandler(cfg)
        connected = await handler.connect()
        assert connected is True
        assert handler.is_connected is True

        ticks = await handler.poll_ticks()
        assert len(ticks) >= 3
        for t in ticks:
            assert t.source == FeedSource.IBKR
            assert t.bid > 0
            assert t.ask > 0
            assert t.last > 0

        candles = await handler.get_historical_candles("EURUSD", "5m", 10)
        assert len(candles) == 10
        await handler.disconnect()

    asyncio.run(_test())


def test_alpaca_handler():
    async def _test():
        cfg = VenueConfig(enabled=True, symbols=["EUR/USD", "BTC/USD"])
        handler = AlpacaFeedHandler(cfg)
        await handler.connect()
        ticks = await handler.poll_ticks()
        assert len(ticks) >= 2
        assert ticks[0].source == FeedSource.ALPACA
        assert ticks[0].symbol == "EURUSD"
        await handler.disconnect()

    asyncio.run(_test())


def test_ctrader_handler():
    async def _test():
        cfg = VenueConfig(enabled=True, symbols=["EURUSD", "US500"])
        handler = CTraderFeedHandler(cfg)
        await handler.connect()
        ticks = await handler.poll_ticks()
        assert len(ticks) >= 2
        assert ticks[0].source == FeedSource.CTRADER
        es_tick = next(t for t in ticks if t.symbol == "ES")
        assert es_tick.raw_symbol == "US500"
        await handler.disconnect()

    asyncio.run(_test())


def test_crypto_handlers_binance_bybit_okx():
    async def _test():
        # Binance
        b_handler = BinanceFeedHandler(VenueConfig(symbols=["BTCUSDT"]))
        await b_handler.connect()
        b_ticks = await b_handler.poll_ticks()
        assert len(b_ticks) >= 1
        assert b_ticks[0].source == FeedSource.BINANCE
        await b_handler.disconnect()

        # Bybit
        by_handler = BybitFeedHandler(VenueConfig(symbols=["BTCUSDT"]))
        await by_handler.connect()
        by_ticks = await by_handler.poll_ticks()
        assert len(by_ticks) >= 1
        assert by_ticks[0].source == FeedSource.BYBIT
        await by_handler.disconnect()

        # OKX
        ok_handler = OKXFeedHandler(VenueConfig(symbols=["BTC-USDT"]))
        await ok_handler.connect()
        ok_ticks = await ok_handler.poll_ticks()
        assert len(ok_ticks) >= 1
        assert ok_ticks[0].source == FeedSource.OKX
        assert ok_ticks[0].symbol == "BTCUSDT"
        await ok_handler.disconnect()

    asyncio.run(_test())


def test_mt5_and_fallback_handlers():
    async def _test():
        mt5 = MT5FeedHandler(VenueConfig(symbols=["EURUSD", "US100"]))
        await mt5.connect()
        mt5_ticks = await mt5.poll_ticks()
        assert len(mt5_ticks) >= 2
        assert mt5_ticks[0].source == FeedSource.MT5
        await mt5.disconnect()

        fallback = FallbackExchangeHandler(VenueConfig(symbols=["EURUSD=X"]))
        await fallback.connect()
        fb_ticks = await fallback.poll_ticks()
        assert len(fb_ticks) >= 1
        assert fb_ticks[0].source == FeedSource.FALLBACK
        await fallback.disconnect()

    asyncio.run(_test())
