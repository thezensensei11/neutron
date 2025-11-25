import pandas as pd
import numpy as np
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_resampled_data(asset: str, timeframes: list, base_path: str = "data/aggregated/synthetic"):
    base_dir = Path(base_path) / asset
    
    for tf in timeframes:
        tf_dir = base_dir / tf
        if not tf_dir.exists():
            logger.warning(f"No data found for {asset} {tf}")
            continue
            
        logger.info(f"Verifying {asset} {tf}...")
        
        # Load all files
        files = sorted(list(tf_dir.glob("*.parquet")))
        if not files:
            logger.warning(f"No parquet files for {asset} {tf}")
            continue
            
        dfs = []
        for f in files:
            dfs.append(pd.read_parquet(f))
        
        df = pd.concat(dfs)
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
        df.sort_index(inplace=True)
        
        # 1. Check Time Continuity
        # Infer expected frequency
        inferred_freq = pd.infer_freq(df.index)
        logger.info(f"  Inferred Frequency: {inferred_freq}")
        
        # Check for gaps
        # Convert timeframe to pandas offset
        tf_map = {'m': 'min', 'h': 'h', 'd': 'D'}
        unit = tf[-1]
        val = tf[:-1]
        freq = f"{val}{tf_map.get(unit, '')}"
        
        expected_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq=freq)
        missing = expected_range.difference(df.index)
        
        if len(missing) > 0:
            logger.warning(f"  Found {len(missing)} missing timestamps!")
            logger.warning(f"  First 5 missing: {missing[:5]}")
        else:
            logger.info("  Time continuity: OK")
            
        # 2. Check Boundary Alignment
        # e.g. 5m candles should have minute % 5 == 0
        if unit == 'm':
            minutes = df.index.minute
            remainder = minutes % int(val)
            misaligned = remainder != 0
            if misaligned.any():
                logger.error(f"  Found {misaligned.sum()} misaligned timestamps!")
                logger.error(f"  Samples: {df.index[misaligned][:5]}")
            else:
                logger.info("  Boundary alignment: OK")
                
        # 3. Check Price Continuity (Close vs Next Open)
        # Calculate diff
        next_open = df['open'].shift(-1)
        close = df['close']
        diff = (next_open - close).abs()
        pct_diff = diff / close
        
        # Threshold: 0.5% gap is suspicious for 5m candles in liquid market?
        # Actually in crypto, gaps happen. But let's report stats.
        mean_diff = pct_diff.mean()
        max_diff = pct_diff.max()
        
        logger.info(f"  Close-Open Gap: Mean={mean_diff:.4%}, Max={max_diff:.4%}")
        
        large_gaps = pct_diff > 0.01 # 1% gap
        if large_gaps.any():
            logger.warning(f"  Found {large_gaps.sum()} gaps > 1%")
            logger.warning(f"  Samples:\n{df[large_gaps][['close', 'open']].head()}")
            
        # 4. Check Abnormal Data
        if df.isnull().any().any():
            logger.error("  Found NaNs in data!")
            logger.error(df.isnull().sum())
            
        zeros = (df[['open', 'high', 'low', 'close', 'volume']] == 0).sum()
        if zeros.any():
            logger.warning("  Found zeros in data:")
            logger.warning(zeros[zeros > 0])
            
        # 5. Check High/Low Logic
        invalid_hl = (df['high'] < df['low']) | (df['high'] < df['open']) | (df['high'] < df['close']) | (df['low'] > df['open']) | (df['low'] > df['close'])
        if invalid_hl.any():
            logger.error(f"  Found {invalid_hl.sum()} candles with invalid High/Low logic!")
            logger.error(f"  Samples:\n{df[invalid_hl][['open', 'high', 'low', 'close']].head()}")
            
        logger.info("-" * 30)

if __name__ == "__main__":
    verify_resampled_data("BTC", ["5m", "15m", "30m", "1h", "2h", "4h"])
