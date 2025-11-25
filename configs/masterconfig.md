# Neutron Master Configuration Guide

This document serves as the definitive reference for configuring Neutron. It details every supported task type, their parameters, and provides a "Master Configuration" example that demonstrates the full capabilities of the system.

## 1. Storage Configuration

Neutron uses a **Hybrid Storage Architecture**:
- **Parquet**: Optimized for OHLCV (Candlestick) data.
- **QuestDB**: Optimized for high-frequency Tick and Generic data.

```json
"storage": {
    "ohlcv_path": "data/ohlcv",       // Local path for Parquet files
    "questdb_host": "localhost",      // QuestDB Host
    "questdb_ilp_port": 9009,         // Influx Line Protocol Port (Ingestion)
    "questdb_pg_port": 8812,          // PostgreSQL Wire Protocol Port (Querying)
    "questdb_username": "admin",      // Optional
    "questdb_password": "quest",      // Optional
    "questdb_database": "qdb"         // Optional
}
```

---

## 2. Task Reference

Tasks define the units of work for the downloader.

### 2.1 Metadata Synchronization
**Type**: `sync_metadata`
**Description**: Fetches and caches market metadata (symbol limits, precision, listing dates) for all exchanges defined in the config.
**Parameters**: None required.

```json
{
    "type": "sync_metadata",
    "params": {}
}
```

### 2.2 OHLCV Backfill
**Type**: `backfill_ohlcv`
**Description**: Downloads historical candlestick data.
**Parameters**:
- `timeframe` (string): Candle size (e.g., "1m", "1h", "1d").
- `start_date` (ISO 8601): Start timestamp.
- `end_date` (ISO 8601): End timestamp (optional, defaults to now).
- `rewrite` (boolean): If true, overwrites existing data.

```json
{
    "type": "backfill_ohlcv",
    "params": {
        "timeframe": "1m",
        "start_date": "2024-01-01T00:00:00",
        "rewrite": false
    },
    "exchanges": {
        "binance": { "spot": { "symbols": ["BTC/USDT"] } }
    }
}
```

### 2.3 Tick Data Backfill
**Type**: `backfill_tick_data`
**Description**: Downloads individual trade executions (tick-by-tick).
**Parameters**:
- `start_date` (ISO 8601)
- `end_date` (ISO 8601)

```json
{
    "type": "backfill_tick_data",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    },
    "exchanges": {
        "binance": { "spot": { "symbols": ["BTC/USDT"] } }
    }
}
```

### 2.4 Aggregated Trades
**Type**: `backfill_agg_trades`
**Description**: Downloads compressed trade data (Binance `aggTrades`).
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_agg_trades",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.5 Funding Rates
**Type**: `backfill_funding`
**Description**: Downloads historical funding rates for perpetual swaps.
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_funding",
    "params": {
        "start_date": "2020-01-01T00:00:00"
    },
    "exchanges": {
        "binance": { "swap": { "symbols": ["BTC/USDT"] } }
    }
}
```

### 2.6 Order Book Ticker
**Type**: `backfill_book_ticker`
**Description**: Downloads best bid/ask updates.
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_book_ticker",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.7 Liquidation Snapshots
**Type**: `backfill_liquidation`
**Description**: Downloads liquidation orders (Futures only).
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_liquidation",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.8 Market Metrics
**Type**: `backfill_metrics`
**Description**: Downloads Open Interest, Long/Short Ratios, etc.
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_metrics",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.9 Order Book Depth
**Type**: `backfill_book_depth`
**Description**: Downloads Order Book snapshots (e.g., depth 5/10/20).
**Parameters**: `start_date`, `end_date`.

```json
{
    "type": "backfill_book_depth",
    "params": {
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.10 Advanced Klines (Mark/Index/Premium)
**Types**: 
- `backfill_mark_price_klines`
- `backfill_index_price_klines`
- `backfill_premium_index_klines`
**Description**: Downloads specialized candlestick data for futures pricing.
**Parameters**: `timeframe`, `start_date`, `end_date`.

```json
{
    "type": "backfill_mark_price_klines",
    "params": {
        "timeframe": "1h",
        "start_date": "2024-01-01T00:00:00"
    }
}
```

### 2.11 QuestDB Loader
**Type**: `load_questdb`
**Description**: Ingests previously downloaded Parquet/CSV data into QuestDB.
**Parameters**:
- `symbol`, `exchange`, `instrument_type`
- `data_type`: The table name (e.g., "aggTrades").
- `start_date`, `end_date`.

```json
{
    "type": "load_questdb",
    "params": {
        "exchange": "binance",
        "symbol": "BTC/USDT",
        "instrument_type": "spot",
        "data_type": "aggTrades",
        "start_date": "2024-01-01T00:00:00",
        "end_date": "2024-01-02T00:00:00"
    }
}
```

### 2.12 OHLCV Aggregator
**Type**: `aggregate_ohlcv`
**Description**: Aggregates 1-minute OHLCV data across all exchanges into a single "global" view.
**Logic**: Price is VWAP, Volume is Sum.
**Parameters**:
- `rewrite` (boolean): If true, re-aggregates existing files.
- `start_date`, `end_date` (optional): Restrict aggregation range.
- `assets` (optional): List of assets to aggregate.
**Note**: If no params provided, it runs in "Aggregate All" mode.

```json
{
    "type": "aggregate_ohlcv",
    "params": {
        "rewrite": false
    }
}
```

### 2.13 Synthetic OHLCV Creation
**Type**: `create_synthetic_ohlcv`
**Description**: Creates a unified "Synthetic" dataset by merging Aggregated Spot and Aggregated Swap data.
**Logic**:
- **Price**: VWAP of Spot and Swap.
- **Volume**: Sum of Spot and Swap.
- **Features**: Adds `twap` and `vwap` columns `(O+H+L+C)/4`.
**Parameters**:
- `rewrite` (boolean): If true, recreates existing files.
**Note**: Runs in Phase 4, strictly after Aggregation.

```json
{
    "type": "create_synthetic_ohlcv",
    "params": {
        "rewrite": false
    }
}
```

---

## 3. Master Configuration Example

This configuration includes **every possible task type** with **every possible parameter**. Use this as a template to copy-paste specific sections.

```json
{
    "storage": {
        "ohlcv_path": "data/ohlcv",
        "questdb_host": "localhost",
        "questdb_ilp_port": 9009,
        "questdb_pg_port": 8812,
        "questdb_username": "admin",
        "questdb_password": "quest",
        "questdb_database": "qdb"
    },
    "max_workers": 10,
    "tasks": [
        {
            "type": "sync_metadata",
            "params": {}
        },
        {
            "type": "backfill_ohlcv",
            "params": {
                "timeframe": "1m",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-02-01T00:00:00",
                "rewrite": false
            },
            "exchanges": {
                "binance": {
                    "spot": { "symbols": ["BTC/USDT", "ETH/USDT"] },
                    "swap": { "symbols": ["BTC/USDT:USDT"] }
                },
                "bybit": {
                    "spot": { "symbols": ["SOL/USDT"] }
                }
            }
        },
        {
            "type": "backfill_tick_data",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00",
                "rewrite": false
            },
            "exchanges": {
                "binance": { "spot": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_agg_trades",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            },
            "exchanges": {
                "binance": { "spot": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_funding",
            "params": {
                "start_date": "2023-01-01T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } },
                "bybit": { "swap": { "symbols": ["ETH/USDT:USDT"] } }
            }
        },
        {
            "type": "backfill_book_ticker",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            },
            "exchanges": {
                "binance": { "spot": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_liquidation",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_metrics",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_book_depth",
            "params": {
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            },
            "exchanges": {
                "binance": { "spot": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_mark_price_klines",
            "params": {
                "timeframe": "1h",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-02-01T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_index_price_klines",
            "params": {
                "timeframe": "1h",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-02-01T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "backfill_premium_index_klines",
            "params": {
                "timeframe": "1h",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-02-01T00:00:00"
            },
            "exchanges": {
                "binance": { "swap": { "symbols": ["BTC/USDT"] } }
            }
        },
        {
            "type": "load_questdb",
            "params": {
                "exchange": "binance",
                "symbol": "BTC/USDT",
                "instrument_type": "spot",
                "data_type": "aggTrades",
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-02T00:00:00"
            }
        },
        {
            "type": "aggregate_ohlcv",
            "params": {
                "rewrite": false
            }
        },
        {
            "type": "create_synthetic_ohlcv",
            "params": {
                "rewrite": false
            }
        }
    ]
}
```
### 2.14 Resample OHLCV (DataProcessor)
**Type**: `resample_ohlcv`
**Description**: Resamples 1-minute synthetic OHLCV data into higher timeframes.
**Logic**:
- **OHLC**: Standard resampling.
- **Volume**: Sum.
- **TWAP**: Mean of 1m TWAPs.
- **VWAP**: Volume-Weighted Mean of 1m VWAPs.
**Parameters**:
- `assets` (list): List of assets to resample (e.g., ["BTC", "ETH"]).
- `timeframes` (list): List of target timeframes (e.g., ["5m", "1h", "1d"]).
- `rewrite` (boolean): If true, overwrites existing files.

```json
{
    "type": "resample_ohlcv",
    "params": {
        "assets": ["BTC"],
        "timeframes": ["5m", "15m", "1h", "4h", "1d"],
        "rewrite": false
    }
}
```

---

## 3. DataProcessor Configuration

The `DataProcessor` uses a separate configuration structure (or section) for post-processing tasks.

```json
{
    "storage": { ... },
    "processor_tasks": [
        {
            "type": "resample_ohlcv",
            "params": {
                "assets": ["BTC"],
                "timeframes": ["5m", "1h"],
                "rewrite": true
            }
        }
    ]
}
```
