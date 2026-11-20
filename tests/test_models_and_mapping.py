"""
Unit tests for data models and cross-venue symbol mapping.
"""

import pytest
from nexus_feed_multiplexer.models import (
    FeedSource,
    AssetClass,
    FeedHealth,
    UnifiedMarketTick,
    UnifiedCandleBar,
    SymbolMapper
)


def test_symbol_mapper_forex():
    assert SymbolMapper.to_canonical("EUR/USD", FeedSource.ALPACA) == "EURUSD"
    assert SymbolMapper.to_canonical("EUR.USD", FeedSource.IBKR) == "EURUSD"
    assert SymbolMapper.to_canonical("EURUSD", FeedSource.CTRADER) == "EURUSD"
    assert SymbolMapper.to_canonical("EURUSD.pro", FeedSource.MT5) == "EURUSD"
    assert SymbolMapper.to_canonical("EURUSD=X", FeedSource.FALLBACK) == "EURUSD"
    assert SymbolMapper.get_asset_class("EURUSD") == AssetClass.FOREX


def test_symbol_mapper_futures_and_cfds():
    assert SymbolMapper.to_canonical("ES", FeedSource.IBKR) == "ES"
    assert SymbolMapper.to_canonical("US500", FeedSource.CTRADER) == "ES"
    assert SymbolMapper.to_canonical("US500", FeedSource.MT5) == "ES"
    assert SymbolMapper.to_canonical("ES=F", FeedSource.FALLBACK) == "ES"
    assert SymbolMapper.get_asset_class("ES") == AssetClass.FUTURES


def test_symbol_mapper_crypto():
    assert SymbolMapper.to_canonical("BTCUSDT", FeedSource.BINANCE) == "BTCUSDT"
    assert SymbolMapper.to_canonical("BTC-USDT", FeedSource.OKX) == "BTCUSDT"
    assert SymbolMapper.to_canonical("BTC/USD", FeedSource.ALPACA) == "BTCUSDT"
    assert SymbolMapper.to_canonical("BTCUSD", FeedSource.MT5) == "BTCUSDT"
    assert SymbolMapper.get_asset_class("BTCUSDT") == AssetClass.CRYPTO


def test_unified_market_tick_calculations():
    tick = UnifiedMarketTick(
        symbol="EURUSD",
        raw_symbol="EUR/USD",
        source=FeedSource.IBKR,
        asset_class=AssetClass.FOREX,
        bid=1.08500,
        ask=1.08520,
        last=1.08510,
        volume=100.0,
        is_delayed=True,
        delay_minutes=15
    )
    assert tick.mid == 1.08510
    assert abs(tick.spread - 0.00020) < 1e-6
    d = tick.to_dict()
    assert d["symbol"] == "EURUSD"
    assert d["source"] == "IBKR"
    assert d["is_delayed"] is True
    assert d["delay_minutes"] == 15
