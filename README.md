# Neutron: High-Performance Crypto Data Ingestion Engine

Neutron is a production-grade, modular, and high-performance data ingestion system designed for quantitative finance and cryptocurrency market analysis. It provides a unified interface to fetch, normalize, aggregate, and store historical market data from multiple cryptocurrency exchanges.

Built with reliability and scalability as primary design tenets, Neutron handles the complexities of rate limits, data gaps, listing date detection, and parallel processing. It is engineered to allow quantitative researchers to focus on analysis rather than the intricacies of data engineering.

---

## Key Features

### 1. Hybrid Storage Architecture
Neutron leverages a dual-storage strategy to optimize for both retrieval speed and data volume:
- **Parquet (OHLCV)**: Columnar file storage for candlestick data, optimized for fast analytical queries with Pandas or Polars.
- **QuestDB (Tick Data)**: High-performance time-series database for high-frequency Tick and Generic data (Trades, Order Books, Funding Rates), enabling SQL-based analytics on billions of rows.

### 2. Multi-Exchange Support
A unified API wrapper around `ccxt` supports major exchanges with consistent data normalization:
- **Binance** (Spot & Swap)
- **Bybit** (Spot & Swap)
- **Bitstamp** (Spot)
- **Bitfinex** (Spot)
- **Coinbase** (Spot)
- **Hyperliquid** (Swap)
- **BitMEX** (Swap)

### 3. Advanced Data Aggregation
Neutron includes sophisticated services for creating composite market views:
- **OHLCV Aggregation**: Aggregates 1-minute candles from multiple exchanges into a unified "aggregated" dataset. This provides a holistic view of the market price and volume for a given asset.
    - **Price**: Volume-Weighted Average Price (VWAP) of the constituent exchanges.
    - **Volume**: Sum of volumes across all exchanges.
- **Synthetic Data Generation**: Creates synthetic OHLCV datasets by combining aggregated Spot and Swap markets. This allows for the analysis of the "total market" for an asset, smoothing out exchange-specific or instrument-specific anomalies.
    - **Methodology**: Calculates the VWAP of the aggregated Spot and Swap prices, weighted by their respective volumes.

### 4. Comprehensive Data Types
Supports ingestion of various market data primitives:
- **OHLCV**: Candlestick data (1m, 1h, 1d, etc.).
- **Trades**: Tick-level trade execution data.
- **AggTrades**: Aggregated trades (Binance style).
- **Funding Rates**: Historical funding rate history for perpetual swaps.
- **Liquidation Snapshots**: Rekt / liquidation events.
- **Order Book Tickers**: Best bid/ask snapshots.
- **Metrics**: Open Interest, Long/Short Ratios.
- **Book Depth**: Order book depth snapshots.

### 5. Smart Backfill Engine
- **Parallel Execution**: Utilizes `ThreadPoolExecutor` to download data from multiple exchanges simultaneously.
- **Gap Filling**: Automatically detects missing data ranges in the state registry and downloads only the required segments.
- **Listing Date Detection**: Intelligently detects when a symbol was listed to prevent unnecessary API calls for pre-listing dates.
- **State Persistence**: Maintains granular state of downloaded data to allow resumable backfills.

### 6. Data Quality Assurance
- **Rich Analytics**: Generates comprehensive reports including Volume, Price Range, Trade Counts, and Buy/Sell Ratios.
- **Granular Inspection**: Provides schema information and daily row count breakdowns to identify data density issues.
- **Gap Detection**: Automatically scans for and reports continuity gaps in Parquet, QuestDB, and Aggregated data.
- **Validation**: Includes scripts to validate synthetic data against raw trade data (True VWAP) to ensure aggregation accuracy.

---

## Installation

Neutron is built with modern Python tooling. We recommend using `uv` for dependency management.

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
   # Using uv (recommended)
   uv sync
   ```

3. **Start QuestDB (Docker):**
   ```bash
   docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
   ```

---

## Configuration

Neutron is driven by a JSON configuration file (e.g., `configs/config.json`). This file defines storage settings and a list of tasks to execute.

### Example Configuration
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

### Running the Downloader
To start the ingestion and aggregation process, run the `downloader` module with your configuration file:

```bash
uv run python -m neutron.core.downloader configs/config.json
```

### Data Quality Check
To generate a comprehensive data quality report, including rich analytics and gap detection for both raw and aggregated data:

```bash
uv run python scripts/data_quality_check.py
```

### Accessing Data (DataCrawler)
Neutron provides a unified `DataCrawler` to access data from Parquet (Raw & Aggregated) and QuestDB seamlessly.

```python
from neutron.core.crawler import DataCrawler

# Initialize Crawler
crawler = DataCrawler.from_config('configs/config.json')

# Get Raw OHLCV (from Parquet)
df_ohlcv = crawler.get_ohlcv(
    exchange='binance',
    symbol='BTC/USDT',
    timeframe='1m',
    start_date='2024-01-01'
)

# Get Aggregated Synthetic Data (from Parquet)
df_synthetic = crawler.aggregated_storage.load_ohlcv(
    exchange='aggregated',
    symbol='BTC',
    timeframe='1m',
    start_date='2024-01-01',
    end_date='2024-01-02',
    instrument_type='synthetic'
)

# Get Tick Data (from QuestDB)
df_trades = crawler.get_tick_data(
    exchange='binance',
    symbol='BTC/USDT',
    start_date='2024-01-01'
)
```

---

## Code Structure

```
neutron/
├── configs/                # Configuration files
├── data/                   # Local data storage (Parquet)
├── scripts/                # Utility and validation scripts
├── src/
│   └── neutron/
│       ├── core/
│       │   ├── downloader.py   # Main orchestration
│       │   ├── crawler.py      # Unified data access
│       │   └── storage/        # Storage backends (Parquet, QuestDB)
│       ├── exchange/           # Exchange adapters (CCXT wrappers)
│       └── services/           # Business logic
│           ├── aggregator.py   # OHLCV Aggregation service
│           ├── synthetic.py    # Synthetic data generation service
│           └── ...
└── ...
```
