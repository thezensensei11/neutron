import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import logging

# Add src to path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from neutron.core.storage.questdb import QuestDBStorage

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def analyze_vwap():
    # Initialize storage
    storage = QuestDBStorage()
    
    symbol = 'BTC/USDT'
    start_date = '2025-11-01T00:00:00'
    end_date = '2025-11-02T00:00:00'
    
    logger.info(f"Analyzing VWAP for {symbol} from {start_date} to {end_date}")
    
    dfs = {}
    
    for instrument in ['spot', 'swap']:
        logger.info(f"Fetching data for {instrument}...")
        
        # Calculate VWAP using QuestDB's SAMPLE BY
        # VWAP = sum(price * amount) / sum(amount)
        query = f"""
            SELECT 
                time, 
                sum(price * qty) / sum(qty) as vwap
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
            'end_date': end_date
        }
        
        df = storage._query_df(query, params)
        if not df.empty:
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
            dfs[instrument] = df
            logger.info(f"Fetched {len(df)} 1-minute bars for {instrument}")
        else:
            logger.warning(f"No data found for {instrument}")
            
    if 'spot' in dfs and 'swap' in dfs:
        spot_df = dfs['spot']
        swap_df = dfs['swap']
        
        # Align dataframes
        combined = pd.merge(spot_df, swap_df, left_index=True, right_index=True, suffixes=('_spot', '_swap'), how='inner')
        
        # Calculate spread
        combined['spread'] = combined['vwap_spot'] - combined['vwap_swap']
        
        logger.info(f"Aligned data has {len(combined)} rows")
        
        # Plotting
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        
        # Plot VWAPs
        ax1.plot(combined.index, combined['vwap_spot'], label='Spot VWAP', color='blue', alpha=0.7)
        ax1.plot(combined.index, combined['vwap_swap'], label='Swap VWAP', color='orange', alpha=0.7)
        ax1.set_title(f'{symbol} 1-Minute VWAP (Spot vs Swap)')
        ax1.set_ylabel('Price (USDT)')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot Spread
        ax2.plot(combined.index, combined['spread'], label='Spread (Spot - Swap)', color='green', alpha=0.8)
        ax2.set_title('VWAP Spread (Spot - Swap)')
        ax2.set_ylabel('Spread (USDT)')
        ax2.set_xlabel('Time')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Add zero line to spread
        ax2.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        
        plt.tight_layout()
        
        output_path = Path("data/vwap_analysis.png")
        output_path.parent.mkdir(exist_ok=True)
        plt.savefig(output_path)
        logger.info(f"Plot saved to {output_path}")
        
    else:
        logger.error("Insufficient data for comparison")

if __name__ == "__main__":
    analyze_vwap()
