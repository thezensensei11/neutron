import logging
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class ResamplerService:
    """
    Service to resample 1-minute synthetic OHLCV data into higher timeframes.
    
    Logic:
        - Open: First
        - High: Max
        - Low: Min
        - Close: Last
        - Volume: Sum
        - TWAP: Mean of 1m TWAPs
        - VWAP: Volume-Weighted Mean of 1m VWAPs
        
    Boundaries:
        - Left-labeled, left-closed (standard crypto convention).
        - e.g., 10:00 5m candle covers [10:00, 10:05)
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Input is synthetic data path
        self.aggregated_path = Path(config['storage'].get('aggregated_path', 'data/aggregated'))
        self.synthetic_path = self.aggregated_path / "synthetic"
        self.max_workers = config.get('max_workers', 4)

    def run(self, params: Dict[str, Any]) -> Dict[str, Dict[str, pd.DataFrame]]:
        """
        Entry point for the resampling task.
        Returns a dictionary: {asset: {timeframe: DataFrame}}
        """
        assets = params.get('assets', [])
        timeframes = params.get('timeframes', [])
        rewrite = params.get('rewrite', False)
        
        results = {}
        
        if not assets:
            logger.warning("No assets specified for resampling.")
            return results
            
        if not timeframes:
            logger.warning("No timeframes specified for resampling.")
            return results
            
        logger.info(f"Starting resampling for {len(assets)} assets into {timeframes}")
        
        for asset in assets:
            asset_results = self.resample_asset(asset, timeframes, rewrite)
            results[asset] = asset_results
            
        return results

    def resample_asset(self, asset: str, timeframes: List[str], rewrite: bool) -> Dict[str, pd.DataFrame]:
        """
        Resamples a single asset into multiple timeframes using chunked processing.
        Returns a dictionary of {timeframe: DataFrame} (last chunk or empty if too large).
        """
        results = {}
        source_dir = self.synthetic_path / asset / "1m"
        
        if not source_dir.exists():
            logger.warning(f"No synthetic 1m data found for {asset}. Skipping.")
            return results
            
        # Group files by year to process in chunks
        files = sorted(list(source_dir.glob("*.parquet")))
        if not files:
            logger.warning(f"No parquet files found for {asset}.")
            return results
            
        files_by_year = {}
        for f in files:
            # Filename format: YYYY-MM-DD.parquet
            year = f.name.split('-')[0]
            if year not in files_by_year:
                files_by_year[year] = []
            files_by_year[year].append(f)
            
        logger.info(f"Processing {asset} in {len(files_by_year)} yearly chunks...")
        
        for year, year_files in sorted(files_by_year.items()):
            logger.info(f"  Processing year {year} ({len(year_files)} files)...")
            try:
                dfs = []
                for f in year_files:
                    try:
                        df = pd.read_parquet(f)
                        dfs.append(df)
                    except Exception as e:
                        logger.error(f"Error reading {f}: {e}")
                
                if not dfs: continue
                
                full_df = pd.concat(dfs)
                full_df['time'] = pd.to_datetime(full_df['time'])
                full_df.set_index('time', inplace=True)
                full_df.sort_index(inplace=True)
                full_df = full_df[~full_df.index.duplicated(keep='last')]
                
                for tf in timeframes:
                    self._process_timeframe_chunk(asset, full_df, tf, rewrite)
                    
            except Exception as e:
                logger.error(f"Error processing chunk {year} for {asset}: {e}")
                
        return results # Returning empty results as we save to disk incrementally

    def _process_timeframe_chunk(self, asset: str, df: pd.DataFrame, timeframe: str, rewrite: bool):
        """
        Resamples dataframe to a specific timeframe and saves it.
        Returns the resampled dataframe.
        """
        target_dir = self.synthetic_path / asset / timeframe
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Resample
        resampled_df = self._resample_dataframe(df, timeframe)
        
        if resampled_df.empty:
            logger.warning(f"Resampling to {timeframe} resulted in empty dataframe for {asset}.")
            return pd.DataFrame()
            
        # Save partitioned by day (or month/year for larger TFs?)
        # To maintain consistency with other data, let's save by day for < 1d, and maybe by month for >= 1d?
        # Actually, standardizing on daily files is safest for the existing storage/crawler logic which expects YYYY-MM-DD.parquet.
        # Even if a file has only 1 row (daily candle), it fits the pattern.
        
        # Add metadata columns
        resampled_df['symbol'] = asset
        resampled_df['instrument_type'] = 'synthetic'
        resampled_df['timeframe'] = timeframe
        resampled_df['exchange'] = 'aggregated'
        
        # Rounding
        numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'twap', 'vwap']
        # Ensure cols exist
        for col in numeric_cols:
            if col in resampled_df.columns:
                resampled_df[col] = resampled_df[col].round(2)

        # Enforce High/Low consistency after rounding
        if {'open', 'high', 'low', 'close'}.issubset(resampled_df.columns):
            resampled_df['high'] = resampled_df[['high', 'open', 'close']].max(axis=1)
            resampled_df['low'] = resampled_df[['low', 'open', 'close']].min(axis=1)
        
        # Save logic
        resampled_df['date'] = resampled_df.index.date
        
        saved_count = 0
        for date, group in resampled_df.groupby('date'):
            filename = f"{date}.parquet"
            output_path = target_dir / filename
            
            if output_path.exists() and not rewrite:
                continue
                
            save_df = group.drop(columns=['date']).reset_index() # Move time back to column
            save_df.rename(columns={'index': 'time'}, inplace=True) # Just in case
            
            # Ensure time is column 'time'
            if 'time' not in save_df.columns and save_df.index.name == 'time':
                 save_df.reset_index(inplace=True)
            
            save_df.to_parquet(output_path)
            saved_count += 1
            
        logger.info(f"  [{timeframe}] Saved {saved_count} files for {asset}.")

    def _resample_dataframe(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """
        Core resampling logic.
        """
        # Convert timeframe to pandas rule
        rule = self._convert_timeframe(timeframe)
        if not rule:
            logger.error(f"Invalid timeframe format: {timeframe}")
            return pd.DataFrame()
            
        # Custom Aggregation Logic
        # We need to handle VWAP carefully: Sum(VWAP * Vol) / Sum(Vol)
        # Pandas resample().agg() doesn't support weighted avg easily across multiple columns.
        # So we pre-calculate Price * Volume
        
        working_df = df.copy()
        working_df['pv_vwap'] = working_df['vwap'] * working_df['volume']
        
        # Define aggregation dictionary
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'quote_volume': 'sum', # Added quote_volume aggregation
            'twap': 'mean',     # Mean of TWAPs
            'pv_vwap': 'sum'    # Sum of (Price * Volume)
        }
        
        # Resample
        # label='left', closed='left' is default for most offsets but let's be explicit
        resampled = working_df.resample(rule, label='left', closed='left').agg(agg_dict)
        
        # Calculate final VWAP
        # VWAP = Sum(PV) / Sum(Volume)
        resampled['vwap'] = resampled['pv_vwap'] / resampled['volume']
        
        # Handle 0 volume cases (fill VWAP with Close or keep NaN?)
        # If volume is 0, VWAP is undefined.
        # Forward fill or use Close is common. Let's use Close (which is 'last' price).
        mask_zero_vol = resampled['volume'] == 0
        if mask_zero_vol.any():
            resampled.loc[mask_zero_vol, 'vwap'] = resampled.loc[mask_zero_vol, 'close']
            
        # Drop helper column
        resampled.drop(columns=['pv_vwap'], inplace=True)
        
        # Drop rows with no data (if any, though resample might create gaps)
        # If the source had gaps, resample creates empty rows (NaNs).
        # We should probably drop them if the source didn't have data there?
        # Or keep them to show gaps?
        # Standard behavior: dropna() if we only want candles where data existed.
        resampled.dropna(subset=['open'], inplace=True)
        
        return resampled

    def _convert_timeframe(self, tf: str) -> Optional[str]:
        """
        Converts standard timeframe strings (1m, 1h, 1d) to Pandas offset aliases.
        """
        if not tf: return None
        
        unit = tf[-1].lower()
        value = tf[:-1]
        
        if not value.isdigit():
            return None
            
        if unit == 'm':
            return f"{value}min" # Pandas uses 'min' or 'T'
        elif unit == 'h':
            return f"{value}h"
        elif unit == 'd':
            return f"{value}D"
        elif unit == 'w':
            return f"{value}W"
            
        return None
