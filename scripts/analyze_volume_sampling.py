import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import FuncFormatter
from datetime import datetime, timedelta
import logging
import numpy as np

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from neutron.core.storage.questdb import QuestDBStorage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def calculate_dynamic_volume_candles(df, daily_thresholds):
    """
    Generate volume-based candles using daily dynamic thresholds.
    daily_thresholds: Dict[str (YYYY-MM-DD), float (threshold)]
    """
    candles = []
    
    if df.empty:
        return pd.DataFrame()

    # Initialize first candle
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
    
    # Pre-fetch thresholds for faster lookup
    # Convert numpy datetime64 to string YYYY-MM-DD for lookup
    # This might be slow inside loop, let's optimize
    # We can iterate and check if day changed
    
    current_day_str = str(times[0])[:10]
    current_threshold = daily_thresholds.get(current_day_str, 10.0) # Default fallback
    
    for i in range(len(df)):
        price = prices[i]
        qty = qtys[i]
        time = times[i]
        
        # Check for day change to update threshold
        day_str = str(time)[:10]
        if day_str != current_day_str:
            current_day_str = day_str
            current_threshold = daily_thresholds.get(current_day_str, current_threshold)
        
        current_volume += qty
        current_candle['high'] = max(current_candle['high'], price)
        current_candle['low'] = min(current_candle['low'], price)
        current_candle['close'] = price
        current_candle['end_time'] = time
        
        if current_volume >= current_threshold:
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

def plot_candlestick(ax, df, label):
    """Draw candlestick chart on the given axes."""
    # Ensure data is sorted
    df = df.sort_values('end_time').reset_index(drop=True)
    
    # Create index for x-axis (equi-distant bars)
    x = np.arange(len(df))
    
    # Define colors
    up_color = '#2ebd85'
    down_color = '#f6465d'
    
    # Separate up and down candles
    up = df[df['close'] >= df['open']]
    down = df[df['close'] < df['open']]
    
    # Plot Up Candles
    if not up.empty:
        up_indices = up.index
        # Wicks
        ax.vlines(up_indices, up['low'], up['high'], color=up_color, linewidth=1)
        # Bodies
        ax.bar(up_indices, up['close'] - up['open'], bottom=up['open'], color=up_color, width=0.8)
        
    # Plot Down Candles
    if not down.empty:
        down_indices = down.index
        # Wicks
        ax.vlines(down_indices, down['low'], down['high'], color=down_color, linewidth=1)
        # Bodies
        ax.bar(down_indices, down['open'] - down['close'], bottom=down['close'], color=down_color, width=0.8)
        
    ax.set_title(label)
    ax.grid(True, alpha=0.2)
    
    # Format X-axis to show dates periodically
    # Show ~10 labels
    step = max(1, len(df) // 10)
    ax.set_xticks(x[::step])
    
    # Convert numpy datetime64 to python datetime for strftime
    def format_date(x_val, pos):
        idx = int(x_val)
        if 0 <= idx < len(df):
            ts = pd.to_datetime(df.iloc[idx]['end_time'])
            return ts.strftime('%m-%d %H:%M')
        return ''
        
    ax.xaxis.set_major_formatter(FuncFormatter(format_date))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

def analyze_volume_sampling():
    storage = QuestDBStorage()
    
    symbol = 'BTC/USDT'
    start_date = '2025-11-01T00:00:00'
    end_date = '2025-11-11T00:00:00' # Extended range
    
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(16, 12), sharex=False)
    
    for i, instrument in enumerate(['spot', 'swap']):
        ax = ax1 if i == 0 else ax2
        
        logger.info(f"[{instrument}] Calculating daily dynamic thresholds...")
        
        # 1. Calculate Daily Average Minutely Volume
        # We can do this with a single aggregation query
        query_thresholds = f"""
            SELECT 
                time,
                sum(qty) as volume
            FROM aggTrades
            WHERE symbol = :symbol 
            AND instrument_type = :instrument_type
            AND time >= :start_date 
            AND time < :end_date
            SAMPLE BY 1d ALIGN TO CALENDAR
        """
        params = {
            'symbol': symbol,
            'instrument_type': instrument,
            'start_date': start_date,
            'end_date': end_date
        }
        
        daily_vols = storage._query_df(query_thresholds, params)
        
        if daily_vols.empty:
            logger.warning(f"[{instrument}] No data found.")
            continue
            
        # Calculate average minutely volume for each day
        # Total Volume / 1440 minutes
        daily_vols['avg_min_vol'] = daily_vols['volume'] / 1440.0
        
        # Create threshold map: 'YYYY-MM-DD' -> threshold
        threshold_map = {}
        for _, row in daily_vols.iterrows():
            day_str = str(row['time'])[:10]
            threshold_map[day_str] = row['avg_min_vol']
            logger.info(f"[{instrument}] {day_str} Threshold: {row['avg_min_vol']:.2f}")

        # 2. Fetch Raw Trades
        logger.info(f"[{instrument}] Fetching raw trades...")
        query_trades = f"""
            SELECT time, price, qty 
            FROM aggTrades
            WHERE symbol = :symbol 
            AND instrument_type = :instrument_type
            AND time >= :start_date 
            AND time < :end_date
            ORDER BY time
        """
        
        trades_df = storage._query_df(query_trades, params)
        
        if not trades_df.empty:
            logger.info(f"[{instrument}] Generating dynamic volume candles ({len(trades_df)} trades)...")
            candles_df = calculate_dynamic_volume_candles(trades_df, threshold_map)
            logger.info(f"[{instrument}] Generated {len(candles_df)} candles.")
            
            plot_candlestick(ax, candles_df, f"{symbol} {instrument.upper()} - Volume Klines (Dynamic Threshold)")
        else:
            logger.warning(f"[{instrument}] No trades found.")

    plt.tight_layout()
    output_path = Path("data/volume_sampling_analysis.png")
    plt.savefig(output_path)
    logger.info(f"Plot saved to {output_path}")

if __name__ == "__main__":
    analyze_volume_sampling()
