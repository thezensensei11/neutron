import logging
import pandas as pd
import numpy as np
import gzip
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class SliderService:
    """
    Service to generate multi-timeframe sliding window datasets for ML.
    
    Logic:
        - Loads 1m synthetic data.
        - Performs hierarchical aggregation on-the-fly (1m -> 5m -> 30m...).
        - Aligns data to 1m base frequency.
        - Implements gap filling (Open = Prev Close).
        - Saves as compressed .npz files.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.aggregated_path = Path(config['storage'].get('aggregated_path', 'data/aggregated'))
        self.synthetic_path = self.aggregated_path / "synthetic"
        self.output_base_path = Path("data/sliding")
        self.max_workers = config.get('max_workers', 4)
        
        # Setup dedicated logging
        self._setup_file_logging()

    def _setup_file_logging(self):
        """Sets up a dedicated file handler for slider logs."""
        log_dir = Path("logs")
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "slider_generation.log"
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add to the module logger
        logger.addHandler(file_handler)
        logger.info(f"SliderService logging to {log_file}")

    def run(self, params: Dict[str, Any]):
        """
        Entry point for the slider task.
        Params:
            - assets: List[str]
            - config_name: str (e.g., "config1")
            - intervals: List[str] (e.g., ["1m", "5m", "1h"])
            - features: List[str] (e.g., ["open", "high", ...])
            - rewrite: bool
        """
        assets = params.get('assets', [])
        config_name = params.get('config_name', 'default')
        intervals = params.get('intervals', ['1m', '5m', '1h'])
        features = params.get('features', ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'twap', 'vwap'])
        rewrite = params.get('rewrite', False)
        
        if not assets:
            logger.warning("No assets specified for SliderService.")
            return

        logger.info(f"Starting SliderService for {len(assets)} assets. Config: {config_name}")
        
        for asset in assets:
            source_dir = self.synthetic_path / asset / "1m"
            if not source_dir.exists():
                logger.error(f"CONFIG MISMATCH: Asset '{asset}' requested but not found in synthetic data at {source_dir}")
                continue
                
            self.process_asset(asset, config_name, intervals, features, rewrite)

    def process_asset(self, asset: str, config_name: str, intervals: List[str], features: List[str], rewrite: bool):
        """
        Process a single asset.
        """
        # Source path: data/aggregated/synthetic/{asset}/1m
        source_dir = self.synthetic_path / asset / "1m"
        output_dir = self.output_base_path / asset / config_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if not source_dir.exists():
            logger.warning(f"No synthetic 1m data found for {asset}. Skipping.")
            return

        # Parse intervals to minutes
        interval_mins = [self._parse_interval(i) for i in intervals]
        # Ensure sorted
        sorted_indices = np.argsort(interval_mins)
        intervals = [intervals[i] for i in sorted_indices]
        interval_mins = [interval_mins[i] for i in sorted_indices]
        
        # Determine max lookback needed (largest interval)
        max_lookback_mins = interval_mins[-1]
        
        # Get all available dates
        files = sorted(list(source_dir.glob("*.parquet")))
        
        # Validate Features against first file
        if files:
            try:
                sample_df = pd.read_parquet(files[0])
                available_cols = set(sample_df.columns)
                # Map features to expected columns
                feature_map = {
                    'volume_in_usd': 'quote_volume',
                    'lifted_volume_in_usd': 'taker_buy_quote_volume'
                }
                
                missing_features = []
                for f in features:
                    col = feature_map.get(f, f)
                    if col not in available_cols:
                        missing_features.append(f)
                
                if missing_features:
                    logger.error(f"CONFIG MISMATCH: Features {missing_features} requested but not found in data columns: {sorted(list(available_cols))}")
                    # We continue, but these will likely result in zeros or errors
            except Exception as e:
                logger.warning(f"Could not validate features on sample file: {e}")

        # Check for gaps in daily files
        # We expect continuous days.
        file_dates = [datetime.strptime(f.stem, "%Y-%m-%d").date() for f in files]
        if len(file_dates) > 1:
            expected_range = (file_dates[-1] - file_dates[0]).days + 1
            if len(file_dates) != expected_range:
                # Find missing dates
                existing_set = set(file_dates)
                all_dates = {file_dates[0] + timedelta(days=i) for i in range(expected_range)}
                missing_dates = sorted(list(all_dates - existing_set))
                
                if missing_dates:
                    logger.warning(f"DATA GAP: Found {len(missing_dates)} missing daily files for {asset} between {file_dates[0]} and {file_dates[-1]}. Missing: {[d.strftime('%Y-%m-%d') for d in missing_dates[:5]]}...")

        # We process day by day
        for current_file in files:
            date_str = current_file.stem # YYYY-MM-DD
            output_file = output_dir / f"{date_str}.npz"
            
            if output_file.exists() and not rewrite:
                continue
                
            logger.info(f"Processing {asset} {date_str}...")
            
            try:
                # Load window of data needed for this day
                # We need [Current Day - Max Lookback, Current Day]
                # To be safe, we load enough previous days to cover max_lookback_mins
                # 1 day = 1440 mins. 
                days_needed = (max_lookback_mins // 1440) + 2 # +2 for buffer and current day
                
                current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
                window_files = []
                
                # Find relevant files
                for i in range(days_needed - 1, -1, -1):
                    d = current_date - timedelta(days=i)
                    f_name = f"{d}.parquet"
                    f_path = source_dir / f_name
                    if f_path.exists():
                        window_files.append(f_path)
                
                if not window_files:
                    continue
                    
                # Load and concat
                dfs = []
                for f in window_files:
                    df = pd.read_parquet(f)
                    dfs.append(df)
                    
                full_df = pd.concat(dfs)
                full_df['time'] = pd.to_datetime(full_df['time'])
                full_df.set_index('time', inplace=True)
                full_df.sort_index(inplace=True)
                full_df = full_df[~full_df.index.duplicated(keep='last')]
                
                # Generate tensor
                self._generate_tensor(full_df, current_date, intervals, interval_mins, features, output_file)
                
            except Exception as e:
                logger.error(f"Error processing {asset} {date_str}: {e}")
                import traceback
                traceback.print_exc()

    def _generate_tensor(self, df: pd.DataFrame, target_date: datetime.date, intervals: List[str], interval_mins: List[int], features: List[str], output_file: Path):
        """
        Generates the 3D tensor for the target date.
        """
        # Target timestamps: Every minute of the target date
        start_ts = datetime.combine(target_date, datetime.min.time()).replace(tzinfo=timezone.utc)
        end_ts = start_ts + timedelta(days=1) - timedelta(minutes=1)
        
        # Create target index (1440 steps)
        target_index = pd.date_range(start=start_ts, end=end_ts, freq='1min')
        
        # Filter DF to relevant range (up to end_ts)
        # We need data up to end_ts. Start can be earlier.
        df_subset = df[df.index <= end_ts].copy()
        
        if df_subset.empty:
            return

        # Gap Filling: Open = Prev Close
        # We do this on the 1m data BEFORE aggregation
        # Shift close by 1 to get prev close
        prev_close = df_subset['close'].shift(1)
        # Fill NaN open with prev close (except first row)
        # Note: This modifies the data. Reference code does this.
        # "candles[1:, 0, 0] = candles[:-1, 0, 3]"
        # We apply this to the base 1m data
        df_subset['open'] = df_subset['open'].fillna(prev_close)
        # Also fill other columns if needed, but Open is the critical one for continuity
        
        # Initialize Tensor: (TimeSteps, Num_Intervals, Num_Features)
        num_steps = len(target_index) # 1440
        num_intervals = len(intervals)
        num_features = len(features)
        
        tensor = np.zeros((num_steps, num_intervals, num_features))
        
        # Map features to columns
        feature_map = {
            'volume_in_usd': 'quote_volume',
            'lifted_volume_in_usd': 'taker_buy_quote_volume'
        }
        
        # We need to compute features on the FULL df_subset (which has lookback data)
        # and THEN slice to the target_index range.
        
        # Ensure df_subset covers the target range + lookback
        # We already loaded enough data into df_subset.
        # But we need to make sure it's continuous and aligned to 1m freq
        # Reindex df_subset to a continuous 1m index covering its entire range
        full_range = pd.date_range(start=df_subset.index.min(), end=df_subset.index.max(), freq='1min')
        df_full = df_subset.reindex(full_range, method='ffill')
        
        logger.info(f"df_full range: {df_full.index.min()} to {df_full.index.max()}")
        logger.info(f"target range: {target_index.min()} to {target_index.max()}")
        
        # Check if df_full covers enough history
        required_start = target_index.min() - timedelta(minutes=interval_mins[-1])
        if df_full.index.min() > required_start:
            logger.info(f"Insufficient history (expected at start of dataset). Have {df_full.index.min()}, need {required_start}. Handling with edge case logic (bfill/min_periods).")
        
        # Now compute features for each interval on df_full
        for i in range(num_intervals):
            window = interval_mins[i] # in minutes
            
            # Temporary storage for this interval's features
            # We will slice this later
            interval_data = np.zeros((len(df_full), num_features))
            
            # 1. Open
            if 'open' in features:
                f_idx = features.index('open')
                # Open at T for window W is Open at (T - W + 1)
                # Shift by (window - 1)
                # EDGE CASE HANDLING:
                # Reference code uses `max(0, index)` to clamp lookback to the start of data.
                # Here, `shift` introduces NaNs for indices < 0.
                # `bfill` fills these NaNs with the first valid value (index 0), which is equivalent to clamping to index 0.
                interval_data[:, f_idx] = df_full['open'].shift(window - 1).ffill().bfill().values
                
            # 2. Close
            if 'close' in features:
                f_idx = features.index('close')
                interval_data[:, f_idx] = df_full['close'].values
                
            # 3. High
            if 'high' in features:
                f_idx = features.index('high')
                # EDGE CASE HANDLING:
                # Reference code slices `[max(0, start):end]`. If start < 0, it slices `[0:end]`.
                # `min_periods=1` in rolling() achieves the same: it computes the max/min/sum 
                # on whatever data is available in the window, effectively handling the truncated start.
                interval_data[:, f_idx] = df_full['high'].rolling(window=window, min_periods=1).max().values
                
            # 4. Low
            if 'low' in features:
                f_idx = features.index('low')
                interval_data[:, f_idx] = df_full['low'].rolling(window=window, min_periods=1).min().values
                
            # 5. Volume & Quote Volume
            for feat in ['volume', 'quote_volume', 'volume_in_usd']:
                if feat in features:
                    f_idx = features.index(feat)
                    col = feature_map.get(feat, feat)
                    if col in df_full.columns:
                        interval_data[:, f_idx] = df_full[col].rolling(window=window, min_periods=1).sum().values
                        
            # 6. TWAP
            if 'twap' in features:
                f_idx = features.index('twap')
                interval_data[:, f_idx] = df_full['twap'].rolling(window=window, min_periods=1).mean().values
                
            # 7. VWAP
            if 'vwap' in features:
                f_idx = features.index('vwap')
                pv = df_full['vwap'] * df_full['volume']
                roll_pv = pv.rolling(window=window, min_periods=1).sum()
                roll_v = df_full['volume'].rolling(window=window, min_periods=1).sum()
                vwap = roll_pv / roll_v
                vwap = vwap.fillna(df_full['vwap'])
                # Also bfill vwap for start
                vwap = vwap.bfill()
                interval_data[:, f_idx] = vwap.values
            
            # Now slice to target_index
            # We need to find the indices in df_full that correspond to target_index
            # Since both are 1m freq, we can use searchsorted or just boolean indexing
            # But boolean indexing is safer
            mask = df_full.index.isin(target_index)
            
            # Check if we have enough data
            # If mask sum < 1440, we might be missing data at the end?
            # But target_index is fixed.
            
            # We need to align exactly.
            # Let's create a Series with the data and reindex to target_index
            # This handles alignment perfectly
            
            # Construct a DataFrame for this interval to easily reindex
            interval_df = pd.DataFrame(interval_data, index=df_full.index)
            aligned_data = interval_df.reindex(target_index).values
            
            # Assign to tensor
            tensor[:, i, :] = aligned_data

        # Data Quality Checks
        nan_count = np.isnan(tensor).sum()
        if nan_count > 0:
            logger.warning(f"DQ WARNING: Found {nan_count} NaNs in {output_file.name}")
        else:
            logger.info(f"DQ PASS: No NaNs in {output_file.name}")
            
        # Sanity Check: Open Price > 0
        if 'open' in features:
            open_idx = features.index('open')
            min_open = np.nanmin(tensor[:, :, open_idx])
            if min_open <= 0:
                logger.warning(f"DQ WARNING: Min Open Price <= 0 ({min_open}) in {output_file.name}")
            else:
                logger.info(f"DQ PASS: Prices valid (min open: {min_open:.2f})")

        # Save
        with gzip.GzipFile(output_file, "w") as f:
            np.save(file=f, arr=tensor)
            
        logger.info(f"Generated {output_file.name} | Shape: {tensor.shape} | Size: {output_file.stat().st_size if output_file.exists() else 'New'} bytes")

    def _parse_interval(self, interval: str) -> int:
        """Parses interval string to minutes."""
        unit = interval[-1]
        value = int(interval[:-1])
        if unit == 'm': return value
        if unit == 'h': return value * 60
        if unit == 'd': return value * 1440
        if unit == 'w': return value * 10080
        return value
