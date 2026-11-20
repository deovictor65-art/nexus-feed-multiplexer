# Nexus Feed Multiplexer

Institutional Multi-Venue Market Data Feed Multiplexer & Arbitrage Normalizer for High-Frequency and Algorithmic Trading Systems.

## Features

- **Multi-Broker Ingestion**:
  - **Interactive Brokers (IBKR)**: Socket-level ingestion supporting Delayed Type 3 quotes (for unfunded accounts) and Real-Time streaming (Type 1), with MFA/TOTP session gating.
  - **Alpaca Markets**: Equities and crypto feeds via Market Data v2.
  - **Spotware cTrader**: Spot Forex, Index CFD, and Commodity quote streams.
  - **Binance**: 24/7 crypto spot and USD-M perpetual futures streams.
  - **Bybit**: v5 linear perps and spot ticker streams.
  - **OKX**: v5 public market ticker streams.
  - **MetaTrader 5 (MT5)**: Cloud bridge & terminal quote streams.
  - **Fallback Exchange**: Public interbank FX and CME/NYMEX quotes for zero-downtime resilience.

- **Unified Standardized Data Stream**:
  - `UnifiedMarketTick` with nanosecond precision, canon-normalized tickers, bid/ask spreads, volume, latency metrics, and monotonic sequence IDs.
  - `UnifiedCandleBar` with automated multi-timeframe aggregation (1m, 5m, 15m, 1h).
  - Bi-directional `SymbolMapper` converting any venue symbol (e.g., `US500`, `ESZ4`, `ES=F`) to platform canonical symbols (`ES`).

- **Conflict & Clash Prevention**:
  - **Priority Routing**: Designate primary venues globally or per asset class (Forex, Futures, Commodities, Crypto, Equities).
  - **Collision Suppression**: When a primary feed is healthy, duplicate quotes from secondary venues are deduplicated without price thrashing.
  - **Automated Failover**: Transparent failover to secondary or fallback venues when primary feeds experience degradation or disconnects.
  - **Outlier Filtering**: Rolling-median outlier guard rejecting spurious price spikes ($> 15\%$).

- **Pre-Computation Gating & MFA**:
  - Respects IBKR Gateway login gate with 6-digit Authenticator codes, IBKR Mobile push notifications, and automated RFC 6238 TOTP generation.

## Installation

```bash
pip install -e .
```

## Quick Start

```python
import asyncio
from nexus_feed_multiplexer import FeedMultiplexer, FeedSource

async def main():
    mux = FeedMultiplexer()
    await mux.start()

    # Subscribe to unified tick stream
    mux.subscribe(lambda tick: print(f"[{tick.source}] {tick.symbol} Last: {tick.last} (Seq: {tick.sequence_id})"))

    # Set priority venue
    mux.set_primary_venue(FeedSource.IBKR, asset_class="forex")

    await asyncio.sleep(5)
    await mux.stop()

asyncio.run(main())
```

## Running Tests

```bash
pytest tests -v
```
