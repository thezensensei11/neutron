# Neutron Configuration Guide

This guide explains how to configure Neutron to download and store cryptocurrency market data.

## Configuration File (`config.json`)

The configuration file controls:
1.  **Storage**: Where to save the data (Database or Parquet files).
2.  **Tasks**: What data to download (OHLCV, Trades, Order Books, etc.).

### 1. Storage Configuration

Neutron now uses a **Dual Storage Architecture**:
1.  **Parquet**: For OHLCV (Candlestick) data.
2.  **QuestDB**: For Tick and Generic data (Trades, Order Books, etc.).

Both are configured simultaneously in the `storage` block:

```json
"storage": {
    "ohlcv_path": "data/ohlcv",       // Path for Parquet files
    "questdb_host": "localhost",      // QuestDB Host
    "questdb_ilp_port": 9009,         // Influx Line Protocol Port (Ingestion)
    "questdb_pg_port": 8812           // PostgreSQL Wire Protocol Port (Querying)
}
```

**Parquet Directory Structure:**
`{ohlcv_path}/{exchange}/{instrument_type}/{base_asset}/{timeframe}/{date}.parquet`
Example: `data/ohlcv/binance/spot/BTC/1h/2023-01-01.parquet`

---

### 2. Task Configuration

Tasks define what operations the downloader performs. You can define tasks individually or use the `exchanges` block to apply parameters to multiple symbols.

#### Common Parameters
- `start_date` (ISO 8601 string): Start of the data range (e.g., "2023-01-01T00:00:00").
- `end_date` (ISO 8601 string): End of the data range.
- `rewrite` (boolean): If `true`, re-downloads existing data. If `false`, fills gaps (for supported tasks).
- `exchange` (string): Exchange ID (e.g., "binance", "bitstamp").
- `instrument_type` (string): Market type ("spot", "swap", "future").
- `symbol` (string): Trading pair (e.g., "BTC/USDT").

#### Supported Tasks

##### `sync_metadata`
Syncs exchange metadata (markets, symbols, limits) to the database and `exchange_state.json`.
*Scans all exchanges defined in your configuration and syncs them automatically.*

```json
{
    "type": "sync_metadata",
    "params": {}
}
```

##### `backfill_ohlcv`
Downloads OHLCV (Candlestick) data using CCXT.

**Parameters:**
- `timeframe`: Candle size (e.g., "1m", "1h", "1d").
- `start_date`, `end_date`, `symbol`, `exchange`, `instrument_type`.

```json
{
    "type": "backfill_ohlcv",
    "params": {
        "timeframe": "1h",
        "start_date": "2023-01-01T00:00:00"
    },
    "exchanges": {
        "binance": {
            "spot": { "symbols": ["BTC/USDT"] }
        }
    }
}
```

##### `backfill_tick_data`
Downloads individual Trade executions.
*Source: Binance Vision (files) for Binance.*

**Parameters:**
- `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_tick_data",
    "params": {
        "start_date": "2023-01-01T00:00:00",
        "end_date": "2023-01-02T00:00:00"
    },
    "exchanges": {
        "binance": { "spot": { "symbols": ["BTC/USDT"] } }
    }
}
```

##### `backfill_agg_trades`
Downloads Aggregated Trades (compressed trades).
*Source: Binance Vision.*

**Parameters:**
- `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_agg_trades",
    "params": { ... }
}
```

##### `backfill_book_ticker`
Downloads Best Bid/Ask updates (Book Ticker).
*Source: Binance Vision.*

**Parameters:**
- `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_book_ticker",
    "params": { ... }
}
```

##### `backfill_liquidation`
Downloads Liquidation Orders (Futures only).
*Source: Binance Vision.*

**Parameters:**
- `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_liquidation",
    "params": { ... },
    "exchanges": {
        "binance": { "swap": { "symbols": ["BTC/USDT"] } }
    }
}
```

##### `backfill_funding`
Downloads Funding Rate history.
*Source: CCXT.*

**Parameters:**
- `start_date`, `symbol`, `exchange`, `instrument_type`.

```json
{
    "type": "backfill_funding",
    "params": { ... }
}
```

##### `backfill_metrics`
Downloads Market Metrics (Open Interest, Long/Short Ratio).
*Source: Binance Vision.*

**Parameters:**
- `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_metrics",
    "params": { ... }
}
```

##### `backfill_mark_price_klines` / `backfill_index_price_klines` / `backfill_premium_index_klines`
Downloads Advanced Klines for Futures.

**Parameters:**
- `timeframe`, `start_date`, `end_date`, `rewrite`.

```json
{
    "type": "backfill_mark_price_klines",
    "params": {
        "timeframe": "1h",
        "start_date": "2023-01-01T00:00:00"
    }
}
```

---

## 3. Gap Repair Configuration

Neutron includes a dedicated tool for finding and fixing data gaps.

### Usage
The `scripts/fill_gaps.py` script reads your `config.json` to know which exchanges and symbols to check.

```bash
python scripts/fill_gaps.py config.json --mode [smart|zero_fill]
```

### Modes
1.  **`smart` (Default)**:
    *   Identifies gaps in `OHLCV` data.
    *   Attempts to download the missing data from the exchange.
    *   If the exchange returns no data (and the gap is confirmed), it falls back to zero-filling (if configured).
    
2.  **`zero_fill`**:
    *   Forces synthesis of zero-volume candles for the identified gaps.
    *   Useful for known maintenance windows or delisted periods where no data exists.
    *   Synthesized candles have `volume=0` and `is_interpolated=true`.

---

## How to Run

Run the downloader module with your config file:

```bash
uv run python -m neutron.core.downloader config.json
```
