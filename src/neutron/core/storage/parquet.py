import logging
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any
from .base import StorageBackend, DataQualityReport

logger = logging.getLogger(__name__)

class ParquetStorage(StorageBackend):
    def __init__(self, base_dir: str, is_aggregated: bool = False):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.is_aggregated = is_aggregated

    def _get_path(self, exchange: str, instrument_type: str, symbol: str, timeframe: str, date: datetime) -> Path:
        # Parse base asset from symbol (e.g., BTC/USDT -> BTC)
        base_asset = symbol.split('/')[0]
        
        if self.is_aggregated:
            # Structure: Data/instrument_type/base_asset/timeframe/date.parquet
            path = self.base_dir / instrument_type / base_asset / timeframe
        else:
            # Structure: Data/exchange/instrument/base_asset/frequency/date.parquet
            path = self.base_dir / exchange / instrument_type / base_asset / timeframe
            
        path.mkdir(parents=True, exist_ok=True)
        
        filename = f"{date.strftime('%Y-%m-%d')}.parquet"
        return path / filename

    def save_ohlcv(self, data: List[Dict[str, Any]]):
        if not data:
            return
        
        df = pd.DataFrame(data)
        
        # Group by day to save into separate files
        df['date'] = df['time'].dt.date
        
        for date, group in df.groupby('date'):
            first_row = group.iloc[0]
            exchange = first_row['exchange']
            instrument_type = group.get('instrument_type', pd.Series(['spot'] * len(group))).iloc[0]
            symbol = first_row['symbol']
            timeframe = first_row['timeframe']
            
            date_obj = datetime.combine(date, datetime.min.time())
            
            path = self._get_path(exchange, instrument_type, symbol, timeframe, date_obj)
            
            save_df = group.drop(columns=['date'])
            
            if path.exists():
                existing_df = pd.read_parquet(path)
                combined_df = pd.concat([existing_df, save_df])
                combined_df = combined_df.drop_duplicates(subset=['time'])
                combined_df.to_parquet(path)
            else:
                save_df.to_parquet(path)
                
        logger.debug(f"Saved {len(data)} OHLCV records to parquet.")

    def save_tick_data(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        df = pd.DataFrame(data)
        df['date'] = df['time'].dt.date
        
        for date, group in df.groupby('date'):
            first_row = group.iloc[0]
            exchange = first_row['exchange']
            instrument_type = group.get('instrument_type', pd.Series(['spot'] * len(group))).iloc[0]
            symbol = first_row['symbol']
            
            date_obj = datetime.combine(date, datetime.min.time())
            path = self._get_path(exchange, instrument_type, symbol, "tick", date_obj)
            
            save_df = group.drop(columns=['date'])
            
            if path.exists():
                existing_df = pd.read_parquet(path)
                combined_df = pd.concat([existing_df, save_df])
                combined_df = combined_df.drop_duplicates(subset=['time', 'trade_id'])
                combined_df.to_parquet(path)
            else:
                save_df.to_parquet(path)
                
        logger.debug(f"Saved {len(data)} tick records to parquet.")

    def save_funding_rates(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        df = pd.DataFrame(data)
        df['date'] = df['time'].dt.date
        
        for date, group in df.groupby('date'):
            first_row = group.iloc[0]
            exchange = first_row['exchange']
            instrument_type = group.get('instrument_type', pd.Series(['swap'] * len(group))).iloc[0]
            symbol = first_row['symbol']
            
            date_obj = datetime.combine(date, datetime.min.time())
            path = self._get_path(exchange, instrument_type, symbol, "funding", date_obj)
            
            save_df = group.drop(columns=['date'])
            
            if path.exists():
                existing_df = pd.read_parquet(path)
                combined_df = pd.concat([existing_df, save_df])
                combined_df = combined_df.drop_duplicates(subset=['time'])
                combined_df.to_parquet(path)
            else:
                save_df.to_parquet(path)
        
    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        if not data:
            return
            
        df = pd.DataFrame(data)
        
        # Ensure time column exists and is datetime
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            if df['time'].dt.tz is None:
                df['time'] = df['time'].dt.tz_localize('UTC')
            df['date'] = df['time'].dt.date
        else:
            # Fallback for data without explicit time (should be rare for time-series)
            logger.warning(f"No time column in {data_type} data, using current date for partitioning")
            df['date'] = datetime.now(timezone.utc).date()
        
        # Group by date to partition files
        for date, group in df.groupby('date'):
            if group.empty: continue
            
            first_row = group.iloc[0]
            exchange = first_row.get('exchange', 'unknown')
            instrument_type = first_row.get('instrument_type', 'spot')
            symbol = first_row.get('symbol', 'unknown')
            
            # Create path: data/parquet/exchange/instrument/base_asset/data_type/date.parquet
            date_obj = datetime.combine(date, datetime.min.time())
            path = self._get_path(exchange, instrument_type, symbol, data_type, date_obj)
            
            # Drop partition column before saving
            save_df = group.drop(columns=['date'])
            
            # Ensure consistent schema by converting object columns to appropriate types if possible
            # This helps with PyArrow compatibility
            for col in save_df.columns:
                if save_df[col].dtype == 'object':
                    try:
                        save_df[col] = save_df[col].astype('string')
                    except:
                        pass

            if path.exists():
                try:
                    existing_df = pd.read_parquet(path)
                    # Align columns
                    combined_df = pd.concat([existing_df, save_df])
                    # Deduplicate based on available unique keys
                    dedup_subset = ['time']
                    if 'trade_id' in combined_df.columns:
                        dedup_subset.append('trade_id')
                    elif 'agg_trade_id' in combined_df.columns:
                        dedup_subset.append('agg_trade_id')
                    elif 'update_id' in combined_df.columns:
                        dedup_subset.append('update_id')
                        
                    combined_df = combined_df.drop_duplicates(subset=dedup_subset, keep='last')
                    combined_df.to_parquet(path, index=False)
                except Exception as e:
                    logger.error(f"Error appending to parquet {path}: {e}")
                    # Fallback: write to new file with timestamp to avoid data loss
                    timestamp = int(datetime.now().timestamp())
                    backup_path = path.with_name(f"{path.stem}_{timestamp}.parquet")
                    save_df.to_parquet(backup_path, index=False)
            else:
                save_df.to_parquet(path, index=False)
        
        logger.debug(f"Saved {len(data)} {data_type} records to parquet.")

    def _load_parquet_range(self, exchange: str, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, instrument_type: str) -> pd.DataFrame:
        dfs = []
        current_date = start_date.date()
        end_date_date = end_date.date()
        
        while current_date <= end_date_date:
            date_obj = datetime.combine(current_date, datetime.min.time())
            path = self._get_path(exchange, instrument_type, symbol, timeframe, date_obj)
            
            if path.exists():
                try:
                    df = pd.read_parquet(path)
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading parquet file {path}: {e}")
            
            current_date += timedelta(days=1)
            
        if not dfs:
            return pd.DataFrame()
            
        combined_df = pd.concat(dfs)
        
        if 'time' in combined_df.columns:
            if combined_df['time'].dt.tz is None:
                combined_df['time'] = combined_df['time'].dt.tz_localize('UTC')
            
            if start_date.tzinfo is None:
                start_date = start_date.replace(tzinfo=timezone.utc)
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)
                
            combined_df = combined_df[
                (combined_df['time'] >= start_date) & 
                (combined_df['time'] < end_date)
            ]
            
        return combined_df.sort_values('time')

    def load_ohlcv(self, exchange: str, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        return self._load_parquet_range(exchange, symbol, timeframe, start_date, end_date, instrument_type)

    def load_tick_data(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        return self._load_parquet_range(exchange, symbol, "tick", start_date, end_date, instrument_type)

    def load_funding_rates(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'swap') -> pd.DataFrame:
        return self._load_parquet_range(exchange, symbol, "funding", start_date, end_date, instrument_type)

    def load_generic_data(self, data_type: str, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        return self._load_parquet_range(exchange, symbol, data_type, start_date, end_date, instrument_type)

    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        results = []
        if not self.base_dir.exists():
            return results
            
        if self.is_aggregated:
            # 3-Level Structure: InstrumentType / Symbol / Timeframe
            for instrument_path in self.base_dir.iterdir():
                if not instrument_path.is_dir(): continue
                instrument = instrument_path.name
                
                for asset_path in instrument_path.iterdir():
                    if not asset_path.is_dir(): continue
                    symbol = asset_path.name 
                    
                    for category_path in asset_path.iterdir():
                        if not category_path.is_dir(): continue
                        category = category_path.name 
                        
                        # In aggregated, category is usually timeframe (1m)
                        is_timeframe = category[0].isdigit()
                        data_type = 'ohlcv' if is_timeframe else category
                        timeframe = category if is_timeframe else None
                        
                        files = list(category_path.glob('*.parquet'))
                        if not files: continue
                        
                        self._process_files_for_list(files, "aggregated", symbol, instrument, data_type, timeframe, deep_scan, results)
        else:
            # 4-Level Structure: Exchange / InstrumentType / Symbol / Timeframe
            for exchange_path in self.base_dir.iterdir():
                if not exchange_path.is_dir(): continue
                exchange = exchange_path.name
                
                for instrument_path in exchange_path.iterdir():
                    if not instrument_path.is_dir(): continue
                    instrument = instrument_path.name
                    
                    for asset_path in instrument_path.iterdir():
                        if not asset_path.is_dir(): continue
                        symbol = asset_path.name 
                        
                        for category_path in asset_path.iterdir():
                            if not category_path.is_dir(): continue
                            category = category_path.name 
                            
                            is_timeframe = category[0].isdigit()
                            data_type = 'ohlcv' if is_timeframe else category
                            timeframe = category if is_timeframe else None
                            
                            files = list(category_path.glob('*.parquet'))
                            if not files: continue
                            
                            self._process_files_for_list(files, exchange, symbol, instrument, data_type, timeframe, deep_scan, results)
                        
        return results

    def _process_files_for_list(self, files, exchange, symbol, instrument, data_type, timeframe, deep_scan, results):
        dates = []
        total_rows = 0
        
        for f in files:
            try:
                date_str = f.stem
                dates.append(datetime.strptime(date_str, "%Y-%m-%d").date())
                
                if deep_scan:
                    try:
                        meta = pd.read_parquet(f, columns=[])
                        total_rows += len(meta)
                    except:
                        pass
            except:
                pass
        
        if not dates: return
        
        min_date = min(dates)
        max_date = max(dates)
        
        results.append({
            'exchange': exchange,
            'symbol': symbol,
            'instrument_type': instrument,
            'data_type': data_type,
            'timeframe': timeframe,
            'start_date': min_date,
            'end_date': max_date,
            'count': total_rows if deep_scan else len(files),
            'count_unit': 'rows' if deep_scan else 'days',
            'gaps': []
        })

    def analyze_ohlcv_quality(self, exchange: str, symbol: str, timeframe: str, instrument_type: str = 'spot') -> DataQualityReport:
        report = DataQualityReport(
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            data_type='ohlcv',
            timeframe=timeframe
        )
        
        if self.is_aggregated:
             base_path = self.base_dir / instrument_type / symbol.split('/')[0] / timeframe
        else:
             base_path = self.base_dir / exchange / instrument_type / symbol.split('/')[0] / timeframe
             
        if not base_path.exists():
            return report
            
        files = sorted(list(base_path.glob('*.parquet')))
        if not files:
            return report
            
        try:
            dfs = []
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    dfs.append(df)
                except Exception as e:
                    logger.error(f"Error reading {f}: {e}")
            
            if not dfs:
                return report
                
            full_df = pd.concat(dfs)
            
            if 'time' not in full_df.columns:
                return report
                
            full_df['time'] = pd.to_datetime(full_df['time'])
            if full_df['time'].dt.tz is None:
                full_df['time'] = full_df['time'].dt.tz_localize('UTC')
            
            full_df = full_df.sort_values('time').drop_duplicates(subset=['time'])
            
            report.start_date = full_df['time'].min()
            report.end_date = full_df['time'].max()
            report.total_rows = len(full_df)
            
            tf_map = {'1m': 60, '5m': 300, '15m': 900, '1h': 3600, '4h': 14400, '1d': 86400}
            seconds_per_candle = tf_map.get(timeframe)
            
            if seconds_per_candle:
                expected_range = pd.date_range(
                    start=report.start_date, 
                    end=report.end_date, 
                    freq=f"{seconds_per_candle}s",
                    tz='UTC'
                )
                report.expected_rows = len(expected_range)
                report.missing_rows = report.expected_rows - report.total_rows
                
                time_diffs = full_df['time'].diff()
                gap_mask = time_diffs > pd.Timedelta(seconds=seconds_per_candle * 1.1)
                
                gap_starts = full_df.loc[gap_mask, 'time'] - time_diffs.loc[gap_mask]
                gap_ends = full_df.loc[gap_mask, 'time']
                
                for start, end in zip(gap_starts, gap_ends):
                    duration = end - start
                    if duration.total_seconds() > seconds_per_candle * 1.5:
                            report.gaps.append((start, end, duration))

            prev_close = full_df['close'].shift(1)
            current_open = full_df['open']
            
            time_diffs = full_df['time'].diff()
            consecutive_mask = (time_diffs <= pd.Timedelta(seconds=seconds_per_candle * 1.1)) & (time_diffs >= pd.Timedelta(seconds=seconds_per_candle * 0.9))
            
            aligned_mask = (abs(current_open - prev_close) < (current_open * 0.001))
            
            valid_comparisons = consecutive_mask.sum()
            aligned_count = (aligned_mask & consecutive_mask).sum()
            
            if valid_comparisons > 0:
                report.alignment_score = (aligned_count / valid_comparisons) * 100.0
            else:
                report.alignment_score = 100.0
                
            anomalies = (full_df['close'] <= 0) | (full_df['high'] <= 0) | (full_df['low'] <= 0) | (full_df['open'] <= 0)
            anomalies |= (full_df['high'] < full_df['low'])
            anomalies |= (full_df['volume'] < 0)
            
            report.anomalies = anomalies.sum()
            
            completeness_score = 100.0
            if report.expected_rows > 0:
                completeness_score = (report.total_rows / report.expected_rows) * 100.0
                
            report.quality_score = (
                (completeness_score * 0.5) + 
                (report.alignment_score * 0.3) + 
                (max(0, 100 - report.anomalies) * 0.2)
            )
            
            # Populate Statistics
            report.price_stats = {
                'min': float(full_df['low'].min()),
                'max': float(full_df['high'].max()),
                'avg': float(full_df['close'].mean())
            }
            
            report.volume_stats = {
                'total': float(full_df['volume'].sum()),
                'avg': float(full_df['volume'].mean()),
                'min': float(full_df['volume'].min()),
                'max': float(full_df['volume'].max())
            }
            
        except Exception as e:
            logger.error(f"Error analyzing OHLCV quality for {symbol}: {e}")
            
        return report

    def analyze_tick_quality(self, exchange: str, symbol: str, instrument_type: str = 'spot', data_type: str = 'tick') -> DataQualityReport:
        report = DataQualityReport(
            exchange=exchange,
            symbol=symbol,
            instrument_type=instrument_type,
            data_type=data_type
        )
        
        # Determine path based on data type
        # tick -> trades, funding -> funding
        path_type = "tick" if data_type in ['tick', 'aggTrades', 'trades'] else "funding"
        base_path = self.base_dir / exchange / instrument_type / symbol.split('/')[0] / path_type
        
        if not base_path.exists():
            return report
            
        files = sorted(list(base_path.glob('*.parquet')))
        if not files:
            return report
            
        try:
            dates = []
            total_size_bytes = 0
            
            for f in files:
                try:
                    date_str = f.stem.split('_')[0] # Handle potential timestamps in filenames
                    dates.append(datetime.strptime(date_str, "%Y-%m-%d"))
                    total_size_bytes += f.stat().st_size
                except:
                    pass
            
            if dates:
                report.start_date = min(dates)
                report.end_date = max(dates)
                
                # Check for missing days
                day_count = (report.end_date - report.start_date).days + 1
                report.expected_rows = day_count # Using expected rows as expected days for tick data summary
                report.total_rows = len(dates) # Using total rows as actual days found
                
                missing_days = day_count - len(dates)
                report.missing_rows = missing_days
                
                # Rough quality score based on day continuity
                report.quality_score = (len(dates) / day_count) * 100.0 if day_count > 0 else 0
                
                report.details = {
                    'file_count': len(files),
                    'total_size_mb': total_size_bytes / (1024 * 1024),
                    'avg_daily_size_mb': (total_size_bytes / len(files)) / (1024 * 1024) if files else 0
                }
                
                # --- Granular Metrics ---
                # 1. Schema Info (from first file)
                if files:
                    try:
                        # Read only schema/metadata from first file
                        first_df = pd.read_parquet(files[0])
                        for col, dtype in first_df.dtypes.items():
                            report.schema_info[str(col)] = str(dtype)
                    except Exception as e:
                        logger.warning(f"Error reading schema from {files[0]}: {e}")
                        
                # 2. Daily Stats
                for f in files:
                    try:
                        date_str = f.stem.split('_')[0]
                        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
                        
                        # Get row count from file metadata if possible, else read file
                        # For accuracy we read the file, but for speed on large datasets we might want metadata
                        # Given this is a "check" script, accuracy is preferred.
                        df = pd.read_parquet(f, columns=[]) # Read only metadata/index
                        count = len(df)
                        
                        report.daily_stats.append({
                            'date': date_obj,
                            'count': count,
                            'min_time': None, # Not easily available without reading full file
                            'max_time': None
                        })
                    except Exception as e:
                        logger.warning(f"Error getting daily stats for {f}: {e}")
                
        except Exception as e:
            logger.error(f"Error analyzing tick quality for {symbol}: {e}")
            
        return report
