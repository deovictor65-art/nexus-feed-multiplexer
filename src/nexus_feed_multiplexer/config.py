"""
Configuration schema and loader for Nexus Feed Multiplexer.
Supports environment variables and YAML configurations.
"""

import os
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from nexus_feed_multiplexer.models import FeedSource, MarketDataType


class VenueConfig(BaseModel):
    enabled: bool = True
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    account_id: Optional[str] = None
    client_id: Optional[int] = None
    market_data_type: str = "DELAYED"     # "REAL_TIME" or "DELAYED"
    symbols: List[str] = Field(default_factory=list)
    poll_interval_sec: float = 1.0


class MultiplexerConfig(BaseModel):
    # Primary venue order per asset class
    primary_venues: Dict[str, FeedSource] = Field(
        default_factory=lambda: {
            "forex": FeedSource.IBKR,
            "futures": FeedSource.IBKR,
            "commodities": FeedSource.IBKR,
            "equity": FeedSource.IBKR,
            "crypto": FeedSource.BINANCE
        }
    )

    # Cross-broker venues
    ibkr: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            host=os.getenv("IBKR_HOST", "127.0.0.1"),
            port=int(os.getenv("IBKR_PORT", "4002")),
            account_id=os.getenv("IBKR_ACCOUNT_ID", "U12345678"),
            client_id=int(os.getenv("IBKR_CLIENT_ID", "99")),
            market_data_type=os.getenv("IBKR_DATA_TYPE", "DELAYED"),
            symbols=["EURUSD", "GBPUSD", "USDJPY", "ES", "NQ", "CL", "BTCUSDT"]
        )
    )

    alpaca: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            api_key=os.getenv("ALPACA_API_KEY", ""),
            api_secret=os.getenv("ALPACA_API_SECRET", ""),
            symbols=["EUR/USD", "BTC/USD", "ETH/USD", "SPY", "AAPL"]
        )
    )

    ctrader: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            account_id=os.getenv("CTRADER_ACCOUNT_ID", "509122"),
            symbols=["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]
        )
    )

    binance: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        )
    )

    bybit: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            symbols=["BTCUSDT", "ETHUSDT"]
        )
    )

    okx: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            symbols=["BTC-USDT", "ETH-USDT"]
        )
    )

    mt5: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            account_id=os.getenv("MT5_LOGIN", "2094182"),
            symbols=["EURUSD", "GBPUSD", "USDJPY", "US500", "US100"]
        )
    )

    fallback: VenueConfig = Field(
        default_factory=lambda: VenueConfig(
            enabled=True,
            symbols=["EURUSD=X", "GBPUSD=X", "USDJPY=X", "ES=F", "CL=F"]
        )
    )

    # Multiplexing & Guardrail Settings
    max_acceptable_latency_ms: float = 2500.0
    outlier_rejection_pct: float = 0.15          # Reject ticks > 15% deviant from window median
    stale_tick_timeout_sec: float = 30.0        # Deem feed STALE if no tick in 30s
    failover_enabled: bool = True
