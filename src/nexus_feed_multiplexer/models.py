"""
Unified Market Data Models & Canonical Symbol Resolution
Institutional schema for cross-broker feed multiplexing.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import time
from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field


class FeedSource(str, Enum):
    IBKR = "IBKR"
    ALPACA = "ALPACA"
    CTRADER = "CTRADER"
    BINANCE = "BINANCE"
    BYBIT = "BYBIT"
    OKX = "OKX"
    MT5 = "MT5"
    FALLBACK = "FALLBACK"


class AssetClass(str, Enum):
    FOREX = "forex"
    EQUITY = "equity"
    FUTURES = "futures"
    OPTIONS = "options"
    COMMODITIES = "commodities"
    CRYPTO = "crypto"


class FeedHealth(str, Enum):
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    OFFLINE = "OFFLINE"


class MarketDataType(str, Enum):
    REAL_TIME = "REAL_TIME"
    DELAYED = "DELAYED"
    FROZEN = "FROZEN"


class OrderBookLevel(BaseModel):
    price: float
    size: float


class UnifiedOrderBookSnapshot(BaseModel):
    symbol: str
    bids: List[OrderBookLevel] = Field(default_factory=list)
    asks: List[OrderBookLevel] = Field(default_factory=list)
    timestamp_ns: int = Field(default_factory=lambda: time.time_ns())
    source: FeedSource = FeedSource.FALLBACK


@dataclass
class UnifiedMarketTick:
    symbol: str                          # Canonical symbol, e.g. "EURUSD", "BTCUSDT", "ES"
    raw_symbol: str                      # Raw venue symbol, e.g. "EUR/USD", "BTC/USDT", "ESZ4"
    source: FeedSource                   # Originating data venue
    asset_class: AssetClass              # Asset category
    bid: float                           # Current best bid
    ask: float                           # Current best ask
    last: float                          # Last executed price (or mid price)
    volume: float = 0.0                  # Tick or session volume
    bid_size: Optional[float] = None     # Size available at bid
    ask_size: Optional[float] = None     # Size available at ask
    timestamp_ns: int = field(default_factory=lambda: time.time_ns())
    timestamp_utc: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    is_delayed: bool = False             # True if regulatory delayed quote
    delay_minutes: int = 0               # Delay magnitude (e.g. 15 for IBKR delayed)
    latency_ms: float = 0.0              # Ingestion latency
    sequence_id: int = 0                 # Monotonic sequence assigned by multiplexer
    feed_health: FeedHealth = FeedHealth.HEALTHY

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return round((self.bid + self.ask) / 2.0, 5 if self.bid < 100 else 2)
        return self.last

    @property
    def spread(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return round(self.ask - self.bid, 5 if self.bid < 100 else 2)
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "raw_symbol": self.raw_symbol,
            "source": self.source.value if isinstance(self.source, FeedSource) else str(self.source),
            "asset_class": self.asset_class.value if isinstance(self.asset_class, AssetClass) else str(self.asset_class),
            "bid": self.bid,
            "ask": self.ask,
            "last": self.last,
            "mid": self.mid,
            "spread": self.spread,
            "volume": self.volume,
            "bid_size": self.bid_size,
            "ask_size": self.ask_size,
            "timestamp_ns": self.timestamp_ns,
            "timestamp_utc": self.timestamp_utc,
            "is_delayed": self.is_delayed,
            "delay_minutes": self.delay_minutes,
            "latency_ms": self.latency_ms,
            "sequence_id": self.sequence_id,
            "feed_health": self.feed_health.value if isinstance(self.feed_health, FeedHealth) else str(self.feed_health)
        }


@dataclass
class UnifiedCandleBar:
    symbol: str
    timeframe: str                       # "1m", "5m", "15m", "1h", "1d"
    open: float
    high: float
    low: float
    close: float
    volume: float
    timestamp: int                       # Epoch timestamp in seconds
    source: FeedSource
    is_delayed: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "timestamp": self.timestamp,
            "source": self.source.value if isinstance(self.source, FeedSource) else str(self.source),
            "is_delayed": self.is_delayed
        }


class SymbolMapper:
    """
    Bi-directional Canonical Symbol Resolver.
    Normalizes venue-specific ticker notations into universal platform symbols.
    """

    # Canonical Symbol -> Asset Class
    ASSET_CLASS_MAP: Dict[str, AssetClass] = {
        "EURUSD": AssetClass.FOREX,
        "GBPUSD": AssetClass.FOREX,
        "USDJPY": AssetClass.FOREX,
        "AUDUSD": AssetClass.FOREX,
        "USDCAD": AssetClass.FOREX,
        "USDCHF": AssetClass.FOREX,
        "EURGBP": AssetClass.FOREX,
        "EURJPY": AssetClass.FOREX,
        "ES": AssetClass.FUTURES,
        "NQ": AssetClass.FUTURES,
        "YM": AssetClass.FUTURES,
        "RTY": AssetClass.FUTURES,
        "CL": AssetClass.COMMODITIES,
        "GC": AssetClass.COMMODITIES,
        "SI": AssetClass.COMMODITIES,
        "NG": AssetClass.COMMODITIES,
        "BTCUSDT": AssetClass.CRYPTO,
        "ETHUSDT": AssetClass.CRYPTO,
        "SOLUSDT": AssetClass.CRYPTO,
        "SPY": AssetClass.EQUITY,
        "QQQ": AssetClass.EQUITY,
        "AAPL": AssetClass.EQUITY,
        "NVDA": AssetClass.EQUITY,
        "MSFT": AssetClass.EQUITY,
    }

    # Venue -> {Raw Ticker: Canonical Symbol}
    VENUE_TO_CANONICAL: Dict[FeedSource, Dict[str, str]] = {
        FeedSource.IBKR: {
            "EUR": "EURUSD",
            "EUR.USD": "EURUSD",
            "GBP.USD": "GBPUSD",
            "USD.JPY": "USDJPY",
            "ES": "ES",
            "NQ": "NQ",
            "CL": "CL",
            "GC": "GC",
            "SPY": "SPY",
            "AAPL": "AAPL",
            "BTC": "BTCUSDT"
        },
        FeedSource.ALPACA: {
            "EUR/USD": "EURUSD",
            "GBP/USD": "GBPUSD",
            "USD/JPY": "USDJPY",
            "BTC/USD": "BTCUSDT",
            "BTCUSD": "BTCUSDT",
            "ETH/USD": "ETHUSDT",
            "ETHUSD": "ETHUSDT",
            "SPY": "SPY",
            "QQQ": "QQQ",
            "AAPL": "AAPL",
            "NVDA": "NVDA"
        },
        FeedSource.CTRADER: {
            "EURUSD": "EURUSD",
            "GBPUSD": "GBPUSD",
            "USDJPY": "USDJPY",
            "AUDUSD": "AUDUSD",
            "US500": "ES",
            "USTech100": "NQ",
            "USCrude": "CL",
            "XAUUSD": "GC",
            "BTCUSD": "BTCUSDT"
        },
        FeedSource.BINANCE: {
            "BTCUSDT": "BTCUSDT",
            "ETHUSDT": "ETHUSDT",
            "SOLUSDT": "SOLUSDT",
            "EURUSDT": "EURUSD",
            "GBPUSDT": "GBPUSD"
        },
        FeedSource.BYBIT: {
            "BTCUSDT": "BTCUSDT",
            "ETHUSDT": "ETHUSDT",
            "SOLUSDT": "SOLUSDT"
        },
        FeedSource.OKX: {
            "BTC-USDT": "BTCUSDT",
            "ETH-USDT": "ETHUSDT",
            "SOL-USDT": "SOLUSDT"
        },
        FeedSource.MT5: {
            "EURUSD": "EURUSD",
            "EURUSD.r": "EURUSD",
            "EURUSD.pro": "EURUSD",
            "GBPUSD": "GBPUSD",
            "USDJPY": "USDJPY",
            "US500": "ES",
            "US100": "NQ",
            "XTIUSD": "CL",
            "XAUUSD": "GC",
            "BTCUSD": "BTCUSDT"
        },
        FeedSource.FALLBACK: {
            "EURUSD=X": "EURUSD",
            "GBPUSD=X": "GBPUSD",
            "USDJPY=X": "USDJPY",
            "ES=F": "ES",
            "NQ=F": "NQ",
            "CL=F": "CL",
            "GC=F": "GC"
        }
    }

    @classmethod
    def to_canonical(cls, raw_symbol: str, source: FeedSource) -> str:
        """Translates a broker/venue ticker to the platform's canonical symbol."""
        cleaned = raw_symbol.strip()
        # Direct lookup
        venue_dict = cls.VENUE_TO_CANONICAL.get(source, {})
        if cleaned in venue_dict:
            return venue_dict[cleaned]
        
        # Strip common punctuation (e.g. "EUR/USD" -> "EURUSD", "BTC-USDT" -> "BTCUSDT")
        stripped = cleaned.replace("/", "").replace("-", "").replace("_", "").replace(".", "")
        if stripped in cls.ASSET_CLASS_MAP:
            return stripped

        for v_sym, canon in venue_dict.items():
            if v_sym.replace("/", "").replace("-", "").upper() == stripped.upper():
                return canon

        return stripped.upper()

    @classmethod
    def get_asset_class(cls, symbol: str) -> AssetClass:
        """Determines asset class from canonical symbol."""
        canon = symbol.replace("/", "").replace("-", "").upper()
        if canon in cls.ASSET_CLASS_MAP:
            return cls.ASSET_CLASS_MAP[canon]
        if "USDT" in canon or "BTC" in canon or "ETH" in canon:
            return AssetClass.CRYPTO
        if canon in ("ES", "NQ", "YM", "RTY", "ZB", "ZN"):
            return AssetClass.FUTURES
        if canon in ("CL", "GC", "SI", "NG", "HO", "RB"):
            return AssetClass.COMMODITIES
        if len(canon) == 6 and any(c in canon for c in ("USD", "EUR", "GBP", "JPY", "CHF", "AUD")):
            return AssetClass.FOREX
        return AssetClass.EQUITY
