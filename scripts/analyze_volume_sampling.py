import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import logging
import numpy as np

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from neutron.core.storage.questdb import QuestDBStorage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_volume_candles(df, volume_threshold):
    """
    Generate volume-based candles from trade data.
    """
    candles = []
    current_candle = {
        'open': df.iloc[0]['price'],
        'high': df.iloc[0]['price'],
        'low': df.iloc[0]['price'],
        'close': df.iloc[0]['price'],
        'volume': 0,
        'start_time': df.iloc[0]['time'],
        'end_time': df.iloc[0]['time']
    }
    
    current_volume = 0
    
    # Iterate using numpy for speed
    times = df['time'].values
    prices = df['price'].values
    qtys = df['qty'].values
    
    for i in range(len(df)):
        price = prices[i]
        qty = qtys[i]
        time = times[i]
        
        current_volume += qty
        current_candle['high'] = max(current_candle['high'], price)
        current_candle['low'] = min(current_candle['low'], price)
        current_candle['close'] = price
        current_candle['end_time'] = time
        
        if current_volume >= volume_threshold:
            current_candle['volume'] = current_volume
            candles.append(current_candle.copy())
            
            # Reset candle
            if i + 1 < len(df):
                next_price = prices[i+1]
                next_time = times[i+1]
                current_candle = {
                    'open': next_price,
                    'high': next_price,
                    'low': next_price,
                    'close': next_price,
                    'volume': 0,
                    'start_time': next_time,
                    'end_time': next_time
                }
                current_volume = 0
            else:
                break
                
    return pd.DataFrame(candles)

def analyze_volume_sampling():
    storage = QuestDBStorage()
    
    symbol = 'BTC/USDT'
    start_date = '2025-11-01T00:00:00'
    end_date = '2025-11-11T00:00:00'
    
    # 1. Calculate Average Minutely Volume (First Hour)
    first_hour_end = '2025-11-01T01:00:00'
    logger.info(f"Calculating average minutely volume for first hour ({start_date} - {first_hour_end})...")
    
    thresholds = {}
    
    for instrument in ['spot', 'swap']:
        query = f"""
            SELECT 
                time, 
                sum(qty) as volume
            FROM aggTrades
            WHERE symbol = :symbol 
            AND instrument_type = :instrument_type
            AND time >= :start_date 
            AND time < :end_date
            SAMPLE BY 1m ALIGN TO CALENDAR
        """
        params = {
            'symbol': symbol,
            'instrument_type': instrument,
            'start_date': start_date,
            'end_date': first_hour_end
        }
        
        df = storage._query_df(query, params)
        if not df.empty:
            avg_vol = df['volume'].mean()
            thresholds[instrument] = avg_vol
            logger.info(f"[{instrument}] Average Minutely Volume: {avg_vol:.4f}")
        else:
            logger.warning(f"[{instrument}] No data for first hour")
            
    # 2. Perform Volume Sampling (Full Range)
    logger.info(f"Performing volume sampling for full range ({start_date} - {end_date})...")
    
    candle_counts = {}
    
    plt.figure(figsize=(12, 8))
    
    for instrument, threshold in thresholds.items():
        logger.info(f"Fetching raw trades for {instrument}...")
        
        # Fetch raw trades (time, price, qty)
        # Using a limit or chunking might be needed for huge datasets, 
        # but 17M rows total (split between spot/swap) should fit in memory (~1GB)
        query = f"""
            SELECT time, price, qty 
            FROM aggTrades
            WHERE symbol = :symbol 
            AND instrument_type = :instrument_type
            AND time >= :start_date 
            AND time < :end_date
            ORDER BY time
        """
        params = {
            'symbol': symbol,
            'instrument_type': instrument,
            'start_date': start_date,
            'end_date': end_date
        }
        
        trades_df = storage._query_df(query, params)
        
        if not trades_df.empty:
            logger.info(f"[{instrument}] Fetched {len(trades_df)} trades. Generating candles with threshold {threshold:.4f}...")
            
            candles_df = calculate_volume_candles(trades_df, threshold)
            logger.info(f"[{instrument}] Generated {len(candles_df)} volume candles")
            
            # Prepare data for plotting
            candles_df['end_time'] = pd.to_datetime(candles_df['end_time'])
            candles_df = candles_df.sort_values('end_time')
            
            # Calculate cumulative count over time
            candles_df['cumulative_count'] = range(1, len(candles_df) + 1)
            
            plt.plot(candles_df['end_time'], candles_df['cumulative_count'], label=f'{instrument.capitalize()} (Thresh: {threshold:.2f})')
            
            candle_counts[instrument] = len(candles_df)
            
        else:
            logger.warning(f"[{instrument}] No trades found for full range")
            
    plt.title(f'Cumulative Volume Candles Over Time ({symbol})')
    plt.xlabel('Time')
    plt.ylabel('Cumulative Candle Count')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    output_path = Path("data/volume_sampling_analysis.png")
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")
    
    # Print summary
    logger.info("Summary:")
    for inst, count in candle_counts.items():
        logger.info(f"- {inst}: {count} candles")

if __name__ == "__main__":
    analyze_volume_sampling()
