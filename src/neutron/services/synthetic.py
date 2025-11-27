import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

logger = logging.getLogger(__name__)

class SyntheticOHLCVService:
    """
    Service to create a synthetic OHLCV dataset by merging aggregated Spot and Swap data.
    
    Logic:
        - Price (O/H/L/C): VWAP of Spot and Swap
          Formula: (P_spot * V_spot + P_swap * V_swap) / (V_spot + V_swap)
        - Volume: Sum of Spot and Swap
        - Features:
            - twap: (open + high + low + close) / 4
            - vwap: (open + high + low + close) / 4 (per user definition)
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        # Input path is the aggregated data path
        self.aggregated_path = Path(config['storage'].get('aggregated_path', 'data/aggregated'))
        self.output_path = self.aggregated_path / "synthetic"
        self.max_workers = config.get('max_workers', 4)

    def run(self, params: Dict[str, Any]):
        """
        Entry point for the synthetic creation task.
        """
        rewrite = params.get('rewrite', False)
        
        # Discovery: Find assets that have BOTH spot and swap aggregated data (or at least one)
        # We look into data/aggregated/spot and data/aggregated/swap
        
        spot_path = self.aggregated_path / "spot"
        swap_path = self.aggregated_path / "swap"
        
        if not spot_path.exists() and not swap_path.exists():
            logger.warning("No aggregated data found to create synthetic dataset.")
            return

        # Find all assets
        assets = set()
        if spot_path.exists():
            for p in spot_path.iterdir():
                if p.is_dir(): assets.add(p.name)
        
        if swap_path.exists():
            for p in swap_path.iterdir():
                if p.is_dir(): assets.add(p.name)
        
        logger.info(f"Discovered {len(assets)} assets for synthetic creation: {assets}")
        
        for asset in assets:
            self.create_synthetic_asset(asset, rewrite)

    def create_synthetic_asset(self, asset: str, rewrite: bool):
        """
        Creates synthetic data for a specific asset.
        """
        # logger.info(f"Creating synthetic data for {asset}...")
        
        # Paths
        spot_dir = self.aggregated_path / "spot" / asset / "1m"
        swap_dir = self.aggregated_path / "swap" / asset / "1m"
        output_dir = self.output_path / asset / "1m"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Gather all dates
        all_dates = set()
        if spot_dir.exists():
            for f in spot_dir.glob("*.parquet"): all_dates.add(f.name)
        if swap_dir.exists():
            for f in swap_dir.glob("*.parquet"): all_dates.add(f.name)
            
        sorted_dates = sorted(list(all_dates))
        if not sorted_dates:
            logger.warning(f"  No aggregated data found for {asset}.")
            return

        # Filter dates to process
        dates_to_process = []
        for date_file in sorted_dates:
            output_file = output_dir / date_file
            if not output_file.exists() or rewrite:
                dates_to_process.append(date_file)
        
        if not dates_to_process:
            logger.info(f"  Skipping Synthetic {asset} - All data up to date.")
            return
            
        logger.info(f"Creating Synthetic {asset} - {len(dates_to_process)} days...")

        for date_file in dates_to_process:
            self._process_day(date_file, asset, spot_dir, swap_dir, output_dir)

    def _process_day(self, date_file: str, asset: str, spot_dir: Path, swap_dir: Path, output_dir: Path):
        try:
            spot_df = pd.DataFrame()
            swap_df = pd.DataFrame()
            
            spot_file = spot_dir / date_file
            swap_file = swap_dir / date_file
            
            if spot_file.exists():
                spot_df = pd.read_parquet(spot_file)
                if not spot_df.empty:
                    spot_df['time'] = pd.to_datetime(spot_df['time'])
                    spot_df.set_index('time', inplace=True)
            
            if swap_file.exists():
                swap_df = pd.read_parquet(swap_file)
                if not swap_df.empty:
                    swap_df['time'] = pd.to_datetime(swap_df['time'])
                    swap_df.set_index('time', inplace=True)
            
            if spot_df.empty and swap_df.empty:
                return

            # Align indices
            # If one is empty, we just use the other. If both exist, we merge.
            # We use an outer join on the index to get the union of timestamps
            
            # Create a combined index
            all_times = spot_df.index.union(swap_df.index).sort_values()
            
            # Reindex both to the combined index, filling missing with NaN
            spot_aligned = spot_df.reindex(all_times)
            swap_aligned = swap_df.reindex(all_times)
            
            # Prepare result DataFrame
            result = pd.DataFrame(index=all_times)
            
            # --- Volume Aggregation (Sum) ---
            # Handle missing volume column gracefully
            if 'volume' in spot_aligned.columns:
                v_spot = spot_aligned['volume'].fillna(0)
            else:
                v_spot = pd.Series(0, index=all_times)
                
            if 'volume' in swap_aligned.columns:
                v_swap = swap_aligned['volume'].fillna(0)
            else:
                v_swap = pd.Series(0, index=all_times)

            total_volume = v_spot + v_swap
            result['volume'] = total_volume
            
            # --- Price Aggregation (VWAP) ---
            # Formula: (P_spot * V_spot + P_swap * V_swap) / (V_spot + V_swap)
            # If volume is 0 for a source, it contributes 0 to the weighted sum.
            # If total volume is 0, we fallback to average of prices.
            
            for col in ['open', 'high', 'low', 'close']:
                p_spot = spot_aligned[col] if col in spot_aligned.columns else pd.Series(np.nan, index=all_times)
                p_swap = swap_aligned[col] if col in swap_aligned.columns else pd.Series(np.nan, index=all_times)
                
                # Weighted Sum Numerator
                # Handle NaNs in Price: if Price is NaN, treat as if Volume was 0 for that component (effectively ignoring it)
                # But we need to be careful. If P is NaN, P*V should be 0? No, if P is NaN, we can't use it.
                
                # Let's clean up inputs for calculation
                # We only use a source if both Price and Volume are valid (not NaN, Vol >= 0)
                
                # Mask for valid spot
                valid_spot = (~p_spot.isna()) & (~v_spot.isna())
                # Mask for valid swap
                valid_swap = (~p_swap.isna()) & (~v_swap.isna())
                
                # Initialize weighted sum and weight sum
                w_sum = pd.Series(0.0, index=all_times)
                vol_sum = pd.Series(0.0, index=all_times)
                
                # Add Spot contribution
                w_sum[valid_spot] += p_spot[valid_spot] * v_spot[valid_spot]
                vol_sum[valid_spot] += v_spot[valid_spot]
                
                # Add Swap contribution
                w_sum[valid_swap] += p_swap[valid_swap] * v_swap[valid_swap]
                vol_sum[valid_swap] += v_swap[valid_swap]
                
                # Calculate VWAP
                # Where vol_sum > 0
                vwap_price = pd.Series(np.nan, index=all_times)
                mask_vol = vol_sum > 0
                vwap_price[mask_vol] = w_sum[mask_vol] / vol_sum[mask_vol]
                
                # Fallback for 0 volume rows (or where both vols are 0)
                # If vol_sum == 0, we take simple average of available prices
                mask_no_vol = ~mask_vol
                if mask_no_vol.any():
                    # Get available prices for these rows
                    prices_to_avg = []
                    if valid_spot.any(): # Optimization check
                        prices_to_avg.append(p_spot)
                    else:
                        prices_to_avg.append(p_spot) # Just append, will be NaN/ignored by mean
                        
                    # Actually we need row-wise mean of (p_spot, p_swap) ignoring NaNs
                    # Construct a temp DF
                    temp_prices = pd.DataFrame({'spot': p_spot, 'swap': p_swap})
                    # Row-wise mean
                    avg_price = temp_prices.mean(axis=1)
                    
                    vwap_price[mask_no_vol] = avg_price[mask_no_vol]
                
                result[col] = vwap_price
                
            # Fill remaining NaNs
            # For Volume: Fill with 0 (no volume during gap)
            result['volume'] = result['volume'].fillna(0)
            
            # For Prices: Forward Fill (propagate last known price)
            # If gap is at start, backfill (rare but possible)
            for col in ['open', 'high', 'low', 'close']:
                result[col] = result[col].ffill()
                result[col] = result[col].bfill() # Fallback for start
            
            # Recalculate TWAP/VWAP for filled rows if they are NaN
            # If we ffilled prices, TWAP/VWAP should also be consistent with those prices
            # Simplest is to recalculate them based on the filled OHLC
            # But wait, we calculate them below in lines 217-219.
            # So we just need to ensure OHLC are filled first.
            
            # Note: If we ffill prices, it means "no trade happened, price stays same".
            # This is better than 0.
            
            # --- Feature Engineering ---
            # twap = (o+h+l+c)/4
            # vwap = (o+h+l+c)/4 (User definition)
            
            ohlc_avg = (result['open'] + result['high'] + result['low'] + result['close']) / 4
            result['twap'] = ohlc_avg
            result['vwap'] = ohlc_avg # Explicit user request
            
            # Quote Volume = mean(ohlc) * volume (User Definition)
            result['quote_volume'] = ohlc_avg * result['volume']
            
            # Metadata
            result.reset_index(inplace=True)
            result.rename(columns={'index': 'time'}, inplace=True)
            result['symbol'] = asset
            result['instrument_type'] = 'synthetic'
            result['timeframe'] = '1m'
            result['exchange'] = 'aggregated'
            
            # Rounding to 2 decimal places
            numeric_cols = ['open', 'high', 'low', 'close', 'volume', 'quote_volume', 'twap', 'vwap']
            result[numeric_cols] = result[numeric_cols].round(2)
            
            # Enforce High/Low consistency after rounding
            result['high'] = result[['high', 'open', 'close']].max(axis=1)
            result['low'] = result[['low', 'open', 'close']].min(axis=1)
            
            # Save
            output_file = output_dir / date_file
            result.to_parquet(output_file)
            
        except Exception as e:
            logger.error(f"Error creating synthetic data for {asset} on {date_file}: {e}")
            import traceback
            traceback.print_exc()
