import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import logging

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from neutron.core.crawler import DataCrawler

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_vwap():
    # Initialize crawler with both storages
    crawler = DataCrawler(
        ohlcv_path="data/ohlcv",
        aggregated_path="data/aggregated",
        questdb_config={
            'host': 'localhost',
            'ilp_port': 9009,
            'pg_port': 8812
        }
    )
    
    symbol = 'BTC/USDT'
    # Use asset name for aggregated data (BTC)
    asset = symbol.split('/')[0]
    
    start_date = '2025-11-01T00:00:00'
    end_date = '2025-11-10T00:00:00'
    
    logger.info(f"Analyzing VWAP for {symbol} from {start_date} to {end_date}")
    
    # 1. Get QuestDB VWAP (True VWAP from trades)
    logger.info("Fetching True VWAP from QuestDB (aggTrades)...")
    
    # We combine spot and swap trades for a "Global VWAP"
    # VWAP = sum(price * qty) / sum(qty) across ALL instruments
    query = f"""
        SELECT 
            time, 
            sum(price * qty) / sum(qty) as true_vwap
        FROM aggTrades
        WHERE symbol = :symbol 
        AND time >= :start_date 
        AND time < :end_date
        SAMPLE BY 1m ALIGN TO CALENDAR
    """
    
    params = {
        'symbol': symbol,
        'start_date': start_date,
        'end_date': end_date
    }
    
    # Access internal storage for raw query (helper method)
    if crawler.tick_storage:
        true_vwap_df = crawler.tick_storage._query_df(query, params)
    else:
        logger.error("Tick storage not available")
        return

    if true_vwap_df.empty:
        logger.warning("No QuestDB data found.")
        return

    true_vwap_df['time'] = pd.to_datetime(true_vwap_df['time'])
    if true_vwap_df['time'].dt.tz is None:
        true_vwap_df['time'] = true_vwap_df['time'].dt.tz_localize('UTC')
    true_vwap_df.set_index('time', inplace=True)
    logger.info(f"Fetched {len(true_vwap_df)} True VWAP bars.")

    # 2. Get Aggregated Synthetic Data
    logger.info("Fetching Aggregated Synthetic Data...")
    # Use ParquetStorage directly or via crawler helper if available
    # Crawler doesn't have a direct method for "aggregated" exchange yet, but we can use load_ohlcv
    # with exchange='aggregated' if we point to the right storage.
    # However, DataCrawler.get_ohlcv uses self.ohlcv_storage.
    
    # We'll access aggregated_storage directly
    if not crawler.aggregated_storage:
        logger.error("Aggregated storage not available")
        return
        
    # Load Synthetic (instrument_type='synthetic', symbol=asset, timeframe='1m')
    # Note: ParquetStorage.load_ohlcv signature: exchange, symbol, timeframe, start, end, instrument_type
    # For aggregated: exchange='aggregated' (ignored/implicit), symbol=asset, instrument_type='synthetic'
    
    synthetic_df = crawler.aggregated_storage.load_ohlcv(
        exchange='aggregated', # Placeholder
        symbol=asset,
        timeframe='1m',
        start_date=datetime.fromisoformat(start_date),
        end_date=datetime.fromisoformat(end_date),
        instrument_type='synthetic'
    )
    
    if synthetic_df.empty:
        logger.warning("No Synthetic data found.")
        return
        
    synthetic_df.set_index('time', inplace=True)
    logger.info(f"Fetched {len(synthetic_df)} Synthetic bars.")
    
    # 3. Compare
    # Align
    combined = pd.merge(true_vwap_df, synthetic_df, left_index=True, right_index=True, how='inner')
    
    # Compare True VWAP vs Synthetic Close (which is VWAP of closes)
    combined['diff'] = combined['true_vwap'] - combined['close']
    combined['diff_pct'] = (combined['diff'] / combined['true_vwap']) * 100
    
    logger.info(f"Aligned data has {len(combined)} rows")
    logger.info(f"Mean Difference: {combined['diff'].mean():.4f} USDT")
    logger.info(f"Mean Abs Difference %: {combined['diff_pct'].abs().mean():.4f}%")
    
    # Plotting
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
    
    # Plot VWAPs
    ax1.plot(combined.index, combined['true_vwap'], label='True VWAP (Trades)', color='blue', alpha=0.7)
    ax1.plot(combined.index, combined['close'], label='Synthetic Close (Aggregated)', color='orange', alpha=0.7, linestyle='--')
    ax1.set_title(f'{symbol} VWAP Comparison: True (Trades) vs Synthetic (Aggregated)')
    ax1.set_ylabel('Price (USDT)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot Difference
    ax2.plot(combined.index, combined['diff'], label='Difference (True - Synthetic)', color='red', alpha=0.8)
    ax2.set_title('Price Difference')
    ax2.set_ylabel('Diff (USDT)')
    ax2.set_xlabel('Time')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    
    plt.tight_layout()
    
    output_path = Path("data/vwap_comparison.png")
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")
    
    crawler.close()

if __name__ == "__main__":
    analyze_vwap()
