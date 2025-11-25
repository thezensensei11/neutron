import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from neutron.core.state.ohlcv import OHLCVStateManager

logger = logging.getLogger(__name__)

class OHLCVAggregatorService:
    """
    Service to aggregate 1-minute OHLCV data across multiple exchanges.
    Logic:
        - Price (O/H/L/C): Volume-Weighted Average Price (VWAP)
        - Volume: Sum of volumes
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage_path = Path(config['storage']['ohlcv_path'])
        self.aggregated_path = Path(config['storage'].get('aggregated_path', 'data/aggregated'))
        self.state_manager = OHLCVStateManager()
        self.max_workers = config.get('max_workers', 4)

    def run(self, params: Dict[str, Any]):
        """
        Entry point for the aggregation task.
        """
        rewrite = params.get('rewrite', False)
        
        # If no specific assets provided, run "Aggregate All" mode
        if not params.get('assets') and not params.get('start_date'):
            logger.info("No specific parameters provided. Running 'Aggregate All' mode.")
            self.aggregate_all(rewrite=rewrite)
        else:
            # TODO: Implement specific range/asset aggregation if needed
            # For now, we focus on the "Aggregate All" or "Aggregate Specific Assets" use case
            logger.info("Running aggregation with specific parameters.")
            # This part can be expanded to filter the discovery based on params
            self.aggregate_all(rewrite=rewrite)

    def aggregate_all(self, rewrite: bool = False):
        """
        Scans the storage directory and aggregates all found assets.
        """
        # Discovery: Find all Instrument Types and Assets
        # Structure: data/ohlcv/{exchange}/{instrument_type}/{symbol}/1m
        
        tasks = []
        
        # We need to invert the structure: Find unique (Instrument Type, Symbol) pairs
        # and then find which exchanges have them.
        
        discovered_assets = set() # Set of (instrument_type, symbol)
        
        if not self.storage_path.exists():
            logger.error(f"Storage path {self.storage_path} does not exist.")
            return

        for exchange_dir in self.storage_path.iterdir():
            if not exchange_dir.is_dir(): continue
            
            for type_dir in exchange_dir.iterdir():
                if not type_dir.is_dir(): continue
                inst_type = type_dir.name
                
                for symbol_dir in type_dir.iterdir():
                    if not symbol_dir.is_dir(): continue
                    symbol = symbol_dir.name
                    
                    # Check if 1m data exists
                    if (symbol_dir / "1m").exists():
                        discovered_assets.add((inst_type, symbol))

        logger.info(f"Discovered {len(discovered_assets)} assets to aggregate.")
        
        # Process each asset
        for inst_type, symbol in discovered_assets:
            self.aggregate_symbol(inst_type, symbol, rewrite)

    def aggregate_symbol(self, instrument_type: str, symbol: str, rewrite: bool):
        """
        Aggregates data for a specific symbol across all available exchanges.
        """
        # logger.info(f"Aggregating {symbol} ({instrument_type})...")
        
        # 1. Identify available exchanges for this symbol
        exchanges = self._get_available_exchanges(instrument_type, symbol)
        if not exchanges:
            logger.warning(f"No exchanges found for {symbol} ({instrument_type}). Skipping.")
            return

        logger.info(f"  Found data in exchanges: {exchanges}")
        
        # 2. Determine date range (Union of all dates across exchanges)
        all_dates = set()
        for exc in exchanges:
            path = self.storage_path / exc / instrument_type / symbol / "1m"
            for f in path.glob("*.parquet"):
                # Filename format: YYYY-MM-DD.parquet
                all_dates.add(f.name)
        
        sorted_dates = sorted(list(all_dates))
        if not sorted_dates:
            logger.warning(f"  No data files found for {symbol} ({instrument_type}).")
            return

        # 3. Process each day
        output_dir = self.aggregated_path / instrument_type / symbol / "1m"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load state for validation
        state_data = self.state_manager.load()
        
        # Add dates from state to ensure we check for missing files
        for exc in exchanges:
            ranges = state_data.get(exc, {}).get(instrument_type, {}).get(symbol, {}).get("1m", [])
            for start_iso, end_iso in ranges:
                # Convert range to list of daily dates
                s = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
                e = datetime.fromisoformat(end_iso).replace(tzinfo=timezone.utc)
                
                curr = s
                while curr < e:
                    all_dates.add(curr.strftime("%Y-%m-%d"))
                    curr += timedelta(days=1)

        # Filter dates that need processing
        dates_to_process = []
        for date_file in sorted_dates:
            date_str = date_file.replace(".parquet", "")
            output_file = output_dir / date_file
            
            if not output_file.exists() or rewrite:
                dates_to_process.append(date_str)
        
        if not dates_to_process:
            logger.info(f"  Skipping {symbol} ({instrument_type}) - All data up to date.")
            return

        logger.info(f"Aggregating {symbol} ({instrument_type}) - {len(dates_to_process)} days...")

        for date_str in dates_to_process:
            output_file = output_dir / f"{date_str}.parquet"
            self._aggregate_day(date_str, instrument_type, symbol, exchanges, output_file, state_data)

    def _aggregate_day(self, date_str: str, instrument_type: str, symbol: str, exchanges: List[str], output_file: Path, state_data: Dict):
        """
        Aggregates a single day of data.
        """
        dfs = []
        
        for exc in exchanges:
            file_path = self.storage_path / exc / instrument_type / symbol / "1m" / f"{date_str}.parquet"
            
            # Integrity Check
            expected_in_state = self._check_state_coverage(state_data, exc, instrument_type, symbol, "1m", date_str)
            
            if file_path.exists():
                try:
                    df = pd.read_parquet(file_path)
                    if not df.empty:
                        # Normalize Volume for BitMEX (USD Contracts -> BTC)
                        # Check symbol before filtering columns
                        if exc == 'bitmex' and 'symbol' in df.columns:
                            first_sym = df['symbol'].iloc[0]
                            # BitMEX XBTUSD (BTC/USD) is inverse, Volume is in USD.
                            # Linear contracts usually have USDT.
                            if 'USD' in first_sym and 'USDT' not in first_sym:
                                # Quote Volume (BTC) = Volume_USD / Avg_Price
                                avg_price = (df['open'] + df['high'] + df['low'] + df['close']) / 4
                                df['volume'] = df['volume'] / avg_price

                        # Standardize columns just in case
                        # Required: time, open, high, low, close, volume
                        df = df[['time', 'open', 'high', 'low', 'close', 'volume']].copy()
                        
                        # Set index to time for alignment
                        if 'time' in df.columns:
                            df['time'] = pd.to_datetime(df['time'])
                            df.set_index('time', inplace=True)
                        
                        # Rename columns to include exchange suffix for clarity during debug/merge
                        # But for vectorized ops, it's easier to keep list of DFs
                        dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {e}")
            else:
                if expected_in_state:
                    logger.warning(f"Integrity Warning: State claims data exists for {exc} on {date_str}, but file is missing.")

        if not dfs:
            return

        # Align timestamps using Outer Join
        # We concat all DFs and group by index (time)
        try:
            # Concatenate all dataframes
            # We need to preserve which values belong to which exchange for VWAP
            # Strategy:
            # 1. Create a master index (union of all indices)
            # 2. Reindex all DFs to master index (filling missing with NaN)
            
            # Efficient way:
            # pd.concat with keys?
            
            combined = pd.concat(dfs, axis=1, keys=range(len(dfs)))
            
            # combined columns will be MultiIndex: (0, 'open'), (0, 'volume'), (1, 'open')...
            
            # Calculate Total Volume
            # Sum of volumes across all exchanges (level 1 column == 'volume')
            vol_cols = combined.xs('volume', axis=1, level=1)
            total_volume = vol_cols.sum(axis=1) # NaNs treated as 0 by default
            
            # Calculate Weighted Sums for Prices
            # P_agg = Sum(P_i * V_i) / Sum(V_i)
            
            weighted_sums = {}
            for col in ['open', 'high', 'low', 'close']:
                # Get all price columns for this type
                price_cols = combined.xs(col, axis=1, level=1)
                
                # Multiply Price * Volume (element-wise)
                # We need to match columns. 
                # price_cols and vol_cols have same columns (0, 1, 2...)
                
                pv = price_cols * vol_cols
                weighted_sum = pv.sum(axis=1)
                weighted_sums[col] = weighted_sum
            
            # Construct Result DataFrame
            result = pd.DataFrame(index=combined.index)
            result['volume'] = total_volume
            
            # Avoid division by zero
            # If total_volume is 0, we can't do VWAP.
            # Fallback: Simple average of available prices
            
            mask_valid_vol = total_volume > 0
            
            for col in ['open', 'high', 'low', 'close']:
                # Default to NaN
                result[col] = np.nan
                
                # VWAP where volume > 0
                result.loc[mask_valid_vol, col] = weighted_sums[col][mask_valid_vol] / total_volume[mask_valid_vol]
                
                # Fallback where volume == 0
                if (~mask_valid_vol).any():
                    # Calculate simple mean of prices for these rows
                    price_cols = combined.xs(col, axis=1, level=1)
                    result.loc[~mask_valid_vol, col] = price_cols.loc[~mask_valid_vol].mean(axis=1)

            # Final cleanup
            result.reset_index(inplace=True)
            result.rename(columns={'index': 'time'}, inplace=True)
            result['time'] = result['time'].dt.tz_convert(timezone.utc) if result['time'].dt.tz is None else result['time']
            
            # Add metadata columns
            result['symbol'] = symbol
            result['instrument_type'] = instrument_type
            result['timeframe'] = '1m'
            result['exchange'] = 'aggregated'
            
            # Rounding to 2 decimal places as requested
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            result[numeric_cols] = result[numeric_cols].round(2)
            
            # Save
            result.to_parquet(output_file)
            # logger.info(f"  Saved aggregated data to {output_file.name}")

        except Exception as e:
            logger.error(f"Error aggregating day {date_str} for {symbol}: {e}")
            import traceback
            traceback.print_exc()

    def _get_available_exchanges(self, instrument_type: str, symbol: str) -> List[str]:
        """
        Finds which exchanges have data for the given symbol.
        """
        exchanges = []
        for exchange_dir in self.storage_path.iterdir():
            if not exchange_dir.is_dir(): continue
            
            target_path = exchange_dir / instrument_type / symbol / "1m"
            if target_path.exists() and any(target_path.iterdir()):
                exchanges.append(exchange_dir.name)
        return exchanges

    def _check_state_coverage(self, state: Dict, exchange: str, inst_type: str, symbol: str, timeframe: str, date_str: str) -> bool:
        """
        Checks if the state file indicates coverage for the given date.
        """
        try:
            ranges = state.get(exchange, {}).get(inst_type, {}).get(symbol, {}).get(timeframe, [])
            target_date = datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
            # Check if target_date falls within any range
            # Note: ranges are [start, end). A file for 2024-01-01 covers 2024-01-01T00:00 to 2024-01-02T00:00
            target_end = target_date + timedelta(days=1)
            
            for start_iso, end_iso in ranges:
                s = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
                e = datetime.fromisoformat(end_iso).replace(tzinfo=timezone.utc)
                
                # Simple overlap check: (StartA <= EndB) and (EndA >= StartB)
                if s < target_end and e > target_date:
                    return True
            return False
        except Exception:
            return False
