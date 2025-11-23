import json
import os

def create_notebook():
    nb = {
        "cells": [],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3"
            },
            "language_info": {
                "codemirror_mode": {
                    "name": "ipython",
                    "version": 3
                },
                "file_extension": ".py",
                "mimetype": "text/x-python",
                "name": "python",
                "nbconvert_exporter": "python",
                "pygments_lexer": "ipython3",
                "version": "3.8.5"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 4
    }
    
    def add_markdown(source):
        nb["cells"].append({
            "cell_type": "markdown",
            "metadata": {},
            "source": [line + "\n" for line in source.split("\n")]
        })
        
    def add_code(source):
        nb["cells"].append({
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [line + "\n" for line in source.split("\n")]
        })

    # Title
    add_markdown("""
# Neutron: Professional Market Data Pipeline

Welcome to the **Neutron** comprehensive tutorial. This notebook demonstrates how to build a production-grade market data pipeline capable of downloading, repairing, storing, and analyzing massive datasets from crypto exchanges.

## Key Capabilities
1.  **Multi-Exchange Support**: Architecture designed for Binance, Bitstamp, Bybit, and more.
2.  **Diverse Data Types**: 
    *   **OHLCV** (Candlesticks)
    *   **Trades** (Tick-level data)
    *   **Aggregated Trades** (Compressed ticks)
    *   **Metrics** (Open Interest, Long/Short Ratios)
    *   **Advanced Klines**: Mark Price, Index Price, Premium Index
3.  **Smart Gap Repair**: Automatically identify and fill missing data points using smart fallback strategies.
4.  **Flexible Storage**: Supports both **PostgreSQL** (for structured querying) and **Parquet** (for high-performance file storage).

---
""")

    # 1. Setup
    add_markdown("## 1. Setup & Initialization\nFirst, we set up the environment and initialize the core components.")
    add_code("""
import sys
import os
import logging
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta, timezone

# Add src to path
sys.path.append('../src')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
sns.set_theme(style="darkgrid")

from neutron.core.downloader import Downloader
from neutron.core.crawler import DataCrawler
from neutron.core.config import NeutronConfig, StorageConfig, TaskConfig
from neutron.services.info_service import InfoService
from neutron.services.gap_fill_service import GapFillService

print("Neutron libraries loaded successfully.")
""")

    # 2. Configuration
    add_markdown("""
## 2. Configuration: The Blueprint

Neutron uses a declarative configuration approach. You define *what* you want, and the system handles *how* to get it.

Below is a comprehensive configuration that fetches:
*   **Spot Data**: OHLCV and Aggregated Trades for BTC/USDT.
*   **Futures Data**: Mark Price, Index Price, Premium Index, and Open Interest Metrics.
""")
    add_code("""
# Define Configuration
config = NeutronConfig(
    storage=StorageConfig(type='database'), 
    data_state_path='data_state.json',
    exchange_state_path='exchange_state.json',
    max_workers=8, # High parallelism for multiple data types
    
    tasks=[
        # --- SPOT DATA ---
        # OHLCV (1 Day - Foundation)
        TaskConfig(
            type='backfill_ohlcv',
            params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
            exchanges={'binance': {'spot': {'symbols': ['BTC/USDT']}}}
        ),
        # Aggregated Trades (2 Minutes Sample)
        TaskConfig(
            type='backfill_agg_trades',
            params={'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-15T00:02:00', 'rewrite': True},
            exchanges={'binance': {'spot': {'symbols': ['BTC/USDT']}}}
        ),
        
        # --- FUTURES DATA (UM) ---
        # Mark Price Klines (1 Day)
        TaskConfig(
            type='backfill_mark_price_klines',
            params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
            exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
        ),
        # Index Price Klines (1 Day)
        TaskConfig(
            type='backfill_index_price_klines',
            params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
            exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
        ),
        # Premium Index Klines (1 Day)
        TaskConfig(
            type='backfill_premium_index_klines',
            params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
            exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
        ),
        # Metrics (Open Interest) (1 Day)
        TaskConfig(
            type='backfill_metrics',
            params={'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
            exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
        )
    ]
)

# Initialize Downloader with this config
downloader = Downloader(config=config)
print("Downloader initialized with task configuration.")
""")

    # 3. Execution
    add_markdown("""
## 3. Execution
Run the downloader. The system intelligently manages rate limits and parallelizes non-OHLCV tasks for maximum throughput.
""")
    add_code("""
print("🚀 Starting Comprehensive Download...")
# In a real run, you would uncomment the line below:
# downloader.run()
print("✅ Download Complete (Simulated for demo speed).")
""")

    # 4. Quality Assurance
    add_markdown("""
## 4. Data Quality & Gap Detection
Data integrity is critical for quantitative analysis. Neutron's `InfoService` provides deep introspection into your datasets.
""")
    add_code("""
info_service = InfoService(downloader.storage)

# Generate a summary report
# deep_scan=True scans every row to find gaps (slower but accurate)
# show_gaps=True lists specific gap ranges
print("Generating Data Quality Report...")
info_service.generate_summary(deep_scan=True, show_gaps=False)
""")

    # 5. Gap Repair
    add_markdown("""
## 5. Smart Gap Repair
Missing data can crash backtests. The `GapFillService` offers two modes to fix this:

*   **`smart` (Default)**: Attempts to download the missing data from the exchange. If the exchange returns no data (and the check is enabled), it can fallback.
*   **`zero_fill`**: Forces synthesis of zero-volume candles for the missing range. This is useful for "maintenance gaps" where the exchange truly has no data. Synthesized candles are marked with `is_interpolated=True`.
""")
    add_code("""
# 1. Get the gap report
gaps = info_service.get_gap_report()
print(f"Found {len(gaps)} gaps.")

# 2. Initialize Gap Filler
gap_filler = GapFillService(downloader)

# 3. Fill Gaps (Demonstration)
if gaps:
    print(f"Attempting to fill {min(5, len(gaps))} gaps using Zero-Fill mode...")
    subset_gaps = gaps[:5] 
    stats = gap_filler.fill_gaps(subset_gaps, mode='zero_fill')
    print("Gap Fill Stats:", stats)
else:
    print("No gaps to fill - Data is continuous!")
""")

    # 6. Data Retrieval & Analysis
    add_markdown("""
## 6. Data Retrieval & Analysis
Neutron provides a unified `DataCrawler` interface to access all stored data types as Pandas DataFrames.
""")
    add_code("""
crawler = DataCrawler(storage_type='database')
""")

    # Analysis A: Price Comparison
    add_markdown("""
### A. Price Analysis (Spot vs Mark vs Index)
Compare the Spot price against Futures Mark and Index prices to identify divergence or arbitrage opportunities.
""")
    add_code("""
# Fetch Data
df_spot = crawler.get_ohlcv('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='spot')
df_mark = crawler.get_mark_price_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')
df_index = crawler.get_index_price_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')

# Plot
if not df_spot.empty and not df_mark.empty:
    plt.figure(figsize=(12, 6))
    plt.plot(df_spot['time'], df_spot['close'], label='Spot Close', alpha=0.7)
    plt.plot(df_mark['time'], df_mark['close'], label='Mark Price', linestyle='--')
    if not df_index.empty:
        plt.plot(df_index['time'], df_index['close'], label='Index Price', linestyle=':')
        
    plt.title('BTC/USDT: Spot vs Mark vs Index Price')
    plt.xlabel('Time')
    plt.ylabel('Price (USDT)')
    plt.legend()
    plt.show()
else:
    print("Data not available for plotting.")
""")

    # Analysis B: Premium Index
    add_markdown("""
### B. Premium Index Analysis
The Premium Index indicates the funding rate direction. Positive values imply Longs pay Shorts.
""")
    add_code("""
df_premium = crawler.get_premium_index_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')

if not df_premium.empty:
    plt.figure(figsize=(12, 4))
    plt.plot(df_premium['time'], df_premium['close'], color='purple', label='Premium Index')
    plt.axhline(0, color='black', linewidth=0.8)
    plt.title('BTC/USDT Premium Index')
    plt.legend()
    plt.show()
""")

    # Analysis C: Metrics
    add_markdown("""
### C. Market Metrics (Open Interest)
Analyze Open Interest trends to gauge market sentiment and liquidity.
""")
    add_code("""
df_metrics = crawler.get_metrics('binance', 'BTC/USDT', '2024-11-15', '2024-11-16', instrument_type='swap')

if not df_metrics.empty:
    # Metrics data often comes in 5m intervals, let's resample to 1h for clarity
    df_metrics.set_index('create_time', inplace=True)
    df_resampled = df_metrics.resample('1h').last()
    
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=df_resampled, x=df_resampled.index, y='sum_open_interest', label='Open Interest (Contracts)')
    plt.title('BTC/USDT Open Interest')
    plt.ylabel('Open Interest')
    plt.show()
""")
    
    # Save
    out_path = 'examples/neutron_demo.ipynb'
    with open(out_path, 'w') as f:
        json.dump(nb, f, indent=2)
    
    print(f"Created professional notebook at {out_path}")

if __name__ == "__main__":
    create_notebook()
