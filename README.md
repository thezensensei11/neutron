# Neutron: High-Performance Crypto Data Ingestion Engine

Neutron is a production-grade, modular, and high-performance data ingestion system designed for quantitative finance and crypto market analysis. It provides a unified interface to fetch, normalize, and store historical market data from multiple cryptocurrency exchanges.

Built with reliability and scalability in mind, Neutron handles the complexities of rate limits, data gaps, listing date detection, and parallel processing, allowing you to focus on analysis rather than data engineering.

---

## 🌟 Key Features

### 1. Multi-Exchange Support
Unified API wrapper around `ccxt` to support major exchanges with consistent data normalization:
- **Binance** (Spot & Swap)
- **Bybit** (Spot & Swap)
- **Bitstamp** (Spot)
- **Bitfinex** (Spot)
- **Coinbase** (Spot)
- **Hyperliquid** (Swap)
- **BitMEX** (Swap)

### 2. Comprehensive Data Types
Supports ingestion of various market data primitives:
- **OHLCV**: Candlestick data (1m, 1h, 1d, etc.).
- **Trades**: Tick-level trade execution data.
- **AggTrades**: Aggregated trades (Binance style).
- **Funding Rates**: Historical funding rate history for perpetual swaps.
- **Liquidation Snapshots**: Rekt / liquidation events.
- **Order Book Tickers**: Best bid/ask snapshots.
- **Metrics**: Open Interest, Long/Short Ratios.
- **Advanced Klines**: Mark Price, Index Price, Premium Index Klines.
- **Book Depth**: Order book depth snapshots.

### 3. Dual Storage Architecture
Optimized storage strategy for different data types:
- **Parquet**: Columnar file storage for OHLCV data, optimized for fast analytical queries (Pandas/Polars).
- **QuestDB**: High-performance time-series database for high-frequency Tick and Generic data (Trades, Order Books, etc.).

### 4. Smart Backfill Engine
- **Parallel Execution**: Uses `ThreadPoolExecutor` to download data from multiple exchanges simultaneously.
- **Gap Filling**: Automatically detects missing data ranges in `data_state.json` and downloads only what's needed.
- **Listing Date Detection**: Intelligently detects when a symbol was listed to avoid useless API calls for pre-listing dates.
- **State Persistence**: Maintains granular state of downloaded data (start/end timestamps) to allow resumable backfills.

### 5. Robustness
- **Rate Limit Handling**: Built-in exponential backoff and retry logic using `tenacity`.
- **Data Integrity Checks**: Validates price (>0) and volume (>=0) during ingestion.
- **Exchange Metadata Sync**: Automatically syncs market metadata (symbols, precision, limits) to your database.

### 6. Data Quality Assurance
- **Gap Detection**: `InfoService` scans your data to identify missing ranges.
- **Smart Repair**: `GapFillService` attempts to re-download missing data or synthesize zero-volume candles ("zero-fill") for maintenance periods.
- **Interpolation Tracking**: Tracks synthesized data with an `is_interpolated` flag for transparency.

---

## 📂 Code Structure

The project follows a modular architecture separating core logic, exchange adapters, services, and storage.

```
neutron/
├── config.json             # Main configuration file
├── data_state.json         # Tracks downloaded OHLCV data ranges (auto-generated)
├── tick_data_state.json    # Tracks downloaded Tick/Generic data ranges (auto-generated)
├── exchange_state.json     # Caches exchange metadata & listing dates
├── scripts/                # Utility scripts (e.g., DB reset)
└── src/
    └── neutron/
        ├── core/
        │   ├── downloader.py   # Main entry point & orchestration
        │   ├── crawler.py      # Unified data retrieval interface
        │   ├── config.py       # Configuration loading & validation
        │   ├── storage.py      # Parquet storage implementation
        │   ├── questdb_storage.py # QuestDB storage implementation
        │   ├── state.py        # Data state management (gaps, ranges)
        │   └── exchange_state.py # Exchange metadata caching
        ├── db/
        │   ├── models.py       # SQLAlchemy ORM models (Legacy/Metadata)
        │   └── session.py      # Database connection management
        ├── exchange/           # Exchange adapters
        │   ├── base.py         # Abstract base class
        │   ├── ccxt_base.py    # Generic CCXT wrapper
        │   ├── binance.py      # Binance specific implementation
        │   └── ...             # Other exchange implementations
        └── services/           # Business logic services
            ├── metadata_sync.py    # Syncs markets/symbols
            ├── ohlcv_backfill.py   # OHLCV download logic
            ├── gap_fill_service.py # Smart gap detection & repair
            ├── info_service.py     # Data quality reporting
            └── ...                 # Other data services
```

---

## 🚀 Installation

Neutron is built with modern Python tooling. We recommend using `uv` or `poetry` for dependency management.

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

   # Or using pip
   pip install -r requirements.txt
   ```

3. **Start QuestDB (Docker):**
   ```bash
   docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 questdb/questdb
   ```

---

## ⚙️ Configuration

Neutron is driven by a JSON configuration file (e.g., `config.json`). This file defines **storage settings** and a list of **tasks** to execute.

### Structure

```json
{
    "storage": {
        "ohlcv_path": "data/ohlcv",
        "questdb_host": "localhost",
        "questdb_ilp_port": 9009,
        "questdb_pg_port": 8812
    },
    "tasks": [
        {
            "type": "backfill_ohlcv",
            "params": {
                "timeframe": "1h",
                "start_date": "2020-01-01T00:00:00",
                "rewrite": false
            },
            "exchanges": {
                "binance": {
                    "spot": { "symbols": ["BTC/USDT", "ETH/USDT"] },
                    "swap": { "symbols": ["BTC/USDT:USDT"] }
                }
            }
        },
        {
            "type": "backfill_generic",
            "params": {
                "data_type": "aggTrades",
                "start_date": "2025-11-01",
                "end_date": "2025-11-05",
                "rewrite": false
            },
            "exchanges": {
                "binance": {
                    "spot": { "symbols": ["BTC/USDT"] }
                }
            }
        }
    ]
}
```

### Task Types

| Task Type | Description | Params |
|-----------|-------------|--------|
| `sync_metadata` | Fetches market info (limits, precision) for all exchanges. | None |
| `backfill_ohlcv` | Downloads historical candlestick data. | `timeframe`, `start_date`, `end_date`, `rewrite` |
| `backfill_generic` | Downloads generic data (Trades, BookDepth, etc.). | `data_type`, `start_date`, `end_date`, `rewrite` |

---

## 💻 Usage

### Running the Downloader

To start the ingestion process, run the `downloader` module with your configuration file:

```bash
# Using uv
uv run python -m neutron.core.downloader config.json

# Standard python
python -m neutron.core.downloader config.json
```

### Monitoring
- **Logs**: Check `ohlcvdata.log` and `tickdata.log` for detailed progress.
- **State**: Watch `data_state.json` (OHLCV) and `tick_data_state.json` (Tick) to see the downloaded ranges updating in real-time.

### Accessing Data (DataCrawler)

Neutron provides a unified `DataCrawler` to access data from both Parquet and QuestDB seamlessly.

```python
from neutron.core.crawler import DataCrawler

# Initialize Crawler (loads config from config.json by default)
crawler = DataCrawler.from_config('config.json')

# Get OHLCV (from Parquet)
df_ohlcv = crawler.get_ohlcv(
    exchange='binance',
    symbol='BTC/USDT',
    timeframe='1h',
    start_date='2024-01-01'
)

# Get Tick Data (from QuestDB)
df_trades = crawler.get_tick_data(
    exchange='binance',
    symbol='BTC/USDT',
    start_date='2024-01-01'
)

# Get Generic Data (e.g., BookDepth from QuestDB)
df_depth = crawler.get_book_depth(
    exchange='binance',
    symbol='BTC/USDT',
    start_date='2024-01-01'
)
```

### Inspecting Available Data
To generate a rich summary of all downloaded data (coverage, gaps, counts):

```python
from neutron.core.crawler import DataCrawler

crawler = DataCrawler.from_config('config.json')
info = crawler.get_info_service()

# Generate report
print(info.generate_summary(deep_scan=False))
```

---

## 🧠 Advanced Concepts

### Parallelism
Neutron automatically parallelizes tasks across exchanges. If you configure Binance, Bybit, and Bitstamp in the same task, they will download concurrently. Adjust `max_workers` in `src/neutron/core/downloader.py` to tune performance.

### State Management
Neutron maintains state files to track data availability:
1. **`data_state.json`**: Tracks OHLCV data ranges.
2. **`tick_data_state.json`**: Tracks Tick/Generic data ranges.
3. **`exchange_state.json`**: Caches static exchange data like listing dates.

### Listing Date Optimization
When you request data from `2010-01-01`, Neutron checks the `exchange_state.json` or probes the exchange. If the symbol was listed in `2019`, it automatically fast-forwards the start date to `2019`, saving hours of empty API calls.

---

*Built with ❤️ for the crypto quant community.*

Built with reliability and scalability in mind, Neutron handles the complexities of rate limits, data gaps, listing date detection, and parallel processing, allowing you to focus on analysis rather than data engineering.

---

## 🌟 Key Features

### 1. Multi-Exchange Support
Unified API wrapper around `ccxt` to support major exchanges with consistent data normalization:
- **Binance** (Spot & Swap)
- **Bybit** (Spot & Swap)
- **Bitstamp** (Spot)
- **Bitfinex** (Spot)
- **Coinbase** (Spot)
- **Hyperliquid** (Swap)
- **BitMEX** (Swap)

### 2. Comprehensive Data Types
Supports ingestion of various market data primitives:
- **OHLCV**: Candlestick data (1m, 1h, 1d, etc.)
- **Trades**: Tick-level trade execution data.
- **AggTrades**: Aggregated trades (Binance style).
- **Funding Rates**: Historical funding rate history for perpetual swaps.
- **Liquidation Snapshots**: Rekt / liquidation events.
- **Order Book Tickers**: Best bid/ask snapshots.
- **Metrics**: Open Interest, Long/Short Ratios.
- **Advanced Klines**: Mark Price, Index Price, Premium Index Klines.
- **Book Depth**: Order book depth snapshots.

### 3. Dual Storage Backends
Flexible storage options to suit your infrastructure:
- **PostgreSQL (SQLAlchemy)**: Relational storage for structured querying and transactional integrity.
- **Parquet**: Columnar file storage optimized for big data analytics (Pandas/Polars/PyArrow).

### 4. Smart Backfill Engine
- **Parallel Execution**: Uses `ThreadPoolExecutor` to download data from multiple exchanges simultaneously.
- **Gap Filling**: Automatically detects missing data ranges in `data_state.json` and downloads only what's needed.
- **Listing Date Detection**: Intelligently detects when a symbol was listed to avoid useless API calls for pre-listing dates.
- **State Persistence**: Maintains granular state of downloaded data (start/end timestamps) to allow resumable backfills.

### 5. Robustness
- **Rate Limit Handling**: Built-in exponential backoff and retry logic using `tenacity`.
- **Data Integrity Checks**: Validates price (>0) and volume (>=0) during ingestion.
- **Exchange Metadata Sync**: Automatically syncs market metadata (symbols, precision, limits) to your database.

### 6. Data Quality Assurance
- **Gap Detection**: `InfoService` scans your data to identify missing ranges.
- **Smart Repair**: `GapFillService` attempts to re-download missing data or synthesize zero-volume candles ("zero-fill") for maintenance periods.
- **Interpolation Tracking**: Tracks synthesized data with an `is_interpolated` flag for transparency.

---

## 📂 Code Structure

The project follows a modular architecture separating core logic, exchange adapters, services, and storage.

```
neutron/
├── config.json             # Main configuration file
├── data_state.json         # Tracks downloaded data ranges (auto-generated)
├── exchange_state.json     # Caches exchange metadata & listing dates
├── scripts/                # Utility scripts (e.g., DB reset)
└── src/
    └── neutron/
        ├── core/
        │   ├── downloader.py   # Main entry point & orchestration
        │   ├── config.py       # Configuration loading & validation
        │   ├── storage.py      # Database & Parquet storage implementations
        │   ├── state.py        # Data state management (gaps, ranges)
        │   └── exchange_state.py # Exchange metadata caching
        ├── db/
        │   ├── models.py       # SQLAlchemy ORM models
        │   └── session.py      # Database connection management
        ├── exchange/           # Exchange adapters
        │   ├── base.py         # Abstract base class
        │   ├── ccxt_base.py    # Generic CCXT wrapper
        │   ├── binance.py      # Binance specific implementation
        │   └── ...             # Other exchange implementations
        └── services/           # Business logic services
            ├── metadata_sync.py    # Syncs markets/symbols
            ├── ohlcv_backfill.py   # OHLCV download logic
            ├── gap_fill_service.py # Smart gap detection & repair
            ├── info_service.py     # Data quality reporting
            └── ...                 # Other data services
```

---

## 🚀 Installation

Neutron is built with modern Python tooling. We recommend using `uv` or `poetry` for dependency management.

### Prerequisites
- Python 3.10+
- PostgreSQL (if using database storage)

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

   # Or using pip
   pip install -r requirements.txt
   ```

3. **Environment Configuration:**
   Create a `.env` file or export environment variables for database connection:
   ```bash
   export DATABASE_URL="postgresql://user:password@localhost:5432/neutron_db"
   ```

---

## ⚙️ Configuration

Neutron is driven by a JSON configuration file (e.g., `config.json`). This file defines **storage settings** and a list of **tasks** to execute.

### Structure

```json
{
    "storage": {
        "type": "database",  // Options: "database" or "parquet"
        "path": "data/"      // Required only for "parquet"
    },
    "tasks": [
        {
            "type": "sync_metadata",
            "params": {}
        },
        {
            "type": "backfill_ohlcv",
            "params": {
                "timeframe": "1h",
                "start_date": "2020-01-01T00:00:00",
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
        }
    ]
}
```

### Task Types

| Task Type | Description | Params |
|-----------|-------------|--------|
| `sync_metadata` | Fetches market info (limits, precision) for all exchanges. | None |
| `backfill_ohlcv` | Downloads historical candlestick data. | `timeframe`, `start_date`, `end_date`, `rewrite` |
| `backfill_tick_data` | Downloads individual trade executions. | `start_date`, `end_date` |
| `backfill_funding` | Downloads funding rate history (swaps only). | `start_date`, `end_date` |
| `backfill_agg_trades` | Downloads aggregated trades. | `start_date`, `end_date` |
| `backfill_liquidation`| Downloads liquidation events. | `start_date`, `end_date` |
| `backfill_metrics` | Downloads market metrics (Open Interest, etc.). | `start_date`, `end_date` |
| `backfill_mark_price_klines` | Downloads Mark Price Klines. | `timeframe`, `start_date`, `end_date` |
| `backfill_index_price_klines` | Downloads Index Price Klines. | `timeframe`, `start_date`, `end_date` |
| `backfill_premium_index_klines` | Downloads Premium Index Klines. | `timeframe`, `start_date`, `end_date` |

---

## 💻 Usage

### Running the Downloader

To start the ingestion process, run the `downloader` module with your configuration file:

```bash
# Using uv
uv run python -m neutron.core.downloader config.json

# Standard python
python -m neutron.core.downloader config.json
```

### Monitoring
- **Logs**: Check `data.log` for detailed progress, speed (candles/sec), and quality checks.
- **State**: Watch `data_state.json` to see the downloaded ranges updating in real-time.

### Resetting Data
To wipe the database tables (OHLCV, Trades, etc.) but keep metadata:

```bash
uv run python scripts/reset_db.py
```

### Inspecting Available Data
To generate a rich summary of all downloaded data (coverage, gaps, counts):

```python
from neutron.services.info_service import InfoService
from neutron.core.downloader import Downloader

# Initialize with your config
downloader = Downloader(config=config)
info = InfoService(downloader.storage)

# Generate report
info.generate_summary(deep_scan=True)
```

### Repairing Gaps
To automatically find and fix missing data:

```bash
# Smart Mode: Try to download, fallback to zero-fill if needed
python scripts/fill_gaps.py config.json --mode smart

# Zero-Fill Mode: Force zero-fill for all gaps (useful for known maintenance)
python scripts/fill_gaps.py config.json --mode zero_fill
```

---

## 🧠 Advanced Concepts

### Parallelism
Neutron automatically parallelizes tasks across exchanges. If you configure Binance, Bybit, and Bitstamp in the same task, they will download concurrently. Adjust `max_workers` in `src/neutron/core/downloader.py` to tune performance.

### State Management
Neutron maintains two state files:
1. **`data_state.json`**: Records the exact time ranges available locally. Used to calculate gaps.
   - *Format*: `{"exchange": {"type": {"symbol": {"timeframe": [[start, end], [start, end]]}}}}`
2. **`exchange_state.json`**: Caches static exchange data like listing dates.
   - *Format*: `{"exchange": {"listing_dates": {"symbol": "2020-01-01..."}}}`

### Listing Date Optimization
When you request data from `2010-01-01`, Neutron checks the `exchange_state.json` or probes the exchange. If the symbol was listed in `2019`, it automatically fast-forwards the start date to `2019`, saving hours of empty API calls.

---

*Built with ❤️ for the crypto quant community.*
