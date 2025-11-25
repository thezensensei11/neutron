# Neutron: Data Engine

Neutron is a data ingestion system for cryptocurrency market analysis. It provides an interface to fetch, normalize, aggregate, and store historical data from multiple exchanges.

The system handles rate limits, data gaps, listing date detection, and parallel processing, automating the data engineering required for quantitative research.

---

## Features

### 1. Storage
Neutron uses a hybrid storage approach:
- **Parquet**: Columnar storage for OHLCV (candlestick) data.
- **QuestDB**: Time-series database for high-frequency Tick and Generic data (Trades, Order Books, Funding Rates).

### 2. Exchange Support
A wrapper around `ccxt` normalizes data from:
- **Binance** (Spot & Swap)
- **Bybit** (Spot & Swap)
- **Bitstamp** (Spot)
- **Bitfinex** (Spot)
- **Coinbase** (Spot)
- **Hyperliquid** (Swap)
- **BitMEX** (Swap)

### 3. Aggregation
Neutron includes services for composite data:
- **OHLCV Aggregation**: Combines 1-minute candles from multiple exchanges.
    - **Price**: Volume-Weighted Average Price (VWAP).
    - **Volume**: Sum of volumes.
- **Synthetic Data**: Merges aggregated Spot and Swap markets.
    - **Methodology**: VWAP of aggregated Spot and Swap prices, weighted by volume.

### 4. Data Types
Supported data primitives:
- **OHLCV**: Candlestick data.
- **Trades**: Tick-level execution data.
- **AggTrades**: Aggregated trades.
- **Funding Rates**: Historical funding rates.
- **Liquidation Snapshots**: Liquidation events.
- **Order Book Tickers**: Best bid/ask.
- **Metrics**: Open Interest, Long/Short Ratios.
- **Book Depth**: Order book snapshots.

### 5. Backfill Engine
- **Parallel Execution**: Downloads from multiple exchanges concurrently.
- **Gap Filling**: Detects missing ranges in the state registry and downloads required segments.
- **Listing Date Detection**: Identifies symbol listing dates to minimize API calls.
- **State Persistence**: Tracks downloaded data state.

### 6. Quality Assurance
- **Analytics**: Reports on Volume, Price Range, and Trade Counts.
- **Inspection**: Checks schema and row counts.
- **Gap Detection**: Scans for continuity gaps.
- **Validation**: Validates synthetic data against raw trade data.

---

## Installation

Neutron uses `uv` for dependency management.

### Prerequisites
- Python 3.10+
- QuestDB (for Tick/Generic data)

### Setup
1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-repo/neutron.git
   cd neutron
   ```

2. **Install dependencies:**
   ```bash
   uv sync
   ```

3. **Start QuestDB (Docker):**
   ```bash
   docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
   ```

---

## Configuration

Configuration is defined in JSON files (e.g., `configs/config.json`).

### Example
```json
{
    "storage": {
        "ohlcv_path": "data/ohlcv",
        "aggregated_path": "data/aggregated",
        "questdb": {
            "host": "localhost",
            "ilp_port": 9009,
            "pg_port": 8812
        }
    },
    "tasks": [
        {
            "type": "backfill_ohlcv",
            "params": {
                "timeframe": "1m",
                "start_date": "2024-01-01T00:00:00",
                "rewrite": false
            },
            "exchanges": {
                "binance": { "spot": { "symbols": ["BTC/USDT"] } },
                "bybit": { "spot": { "symbols": ["BTC/USDT"] } }
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

---

## Usage

### Downloader
Run the ingestion process:

```bash
uv run python -m neutron.core.downloader configs/config.json
```

### Quality Check
Generate a data quality report:

```bash
uv run python scripts/data_quality_check.py
```

### Data Access
Access data via `DataCrawler`:

```python
from neutron.core.crawler import DataCrawler

# Initialize
crawler = DataCrawler.from_config('configs/config.json')

# Raw OHLCV
df_ohlcv = crawler.get_ohlcv(
    exchange='binance',
    symbol='BTC/USDT',
    timeframe='1m',
    start_date='2024-01-01'
)

# Aggregated Synthetic
df_synthetic = crawler.aggregated_storage.load_ohlcv(
    exchange='aggregated',
    symbol='BTC',
    timeframe='1m',
    start_date='2024-01-01',
    end_date='2024-01-02',
    instrument_type='synthetic'
)

# Tick Data
df_trades = crawler.get_tick_data(
    exchange='binance',
    symbol='BTC/USDT',
    start_date='2024-01-01'
)
```

---

## Structure

```
neutron/
├── configs/                # Configuration
├── data/                   # Local storage
├── scripts/                # Utilities
├── src/
│   └── neutron/
│       ├── core/
│       │   ├── downloader.py   # Orchestration
│       │   ├── crawler.py      # Data access
│       │   └── storage/        # Backends
│       ├── exchange/           # Adapters
│       └── services/           # Logic
│           ├── aggregator.py   # Aggregation
│           ├── synthetic.py    # Synthetic generation
│           └── ...
└── ...
```
