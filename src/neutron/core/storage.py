from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
import logging
from ..db.session import ScopedSession
# Models might still be used for type hints or other logic if needed, but DatabaseStorage used them heavily.
# We can keep them if they are used elsewhere or remove if strictly coupled to DatabaseStorage.
# For now, keeping them as they might be useful for data structure reference.
from ..db.models import OHLCV, Trade, FundingRate, AggTrade, BookTicker, LiquidationSnapshot, OpenInterest, BookDepth, IndexPriceKline, MarkPriceKline, PremiumIndexKline

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
# ... (interface remains same)
    @abstractmethod
    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        pass

    @abstractmethod
    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        """
        List all available data in storage with summary statistics.
        
        Returns:
            List of dicts with keys:
            - exchange
            - symbol
            - instrument_type (if available)
            - data_type
            - timeframe (optional)
            - start_date
            - end_date
            - count
            - gaps (list of tuples (start, end)) if deep_scan is True
        """
        pass
# ...

# DatabaseStorage removed as part of cleanup
# Use QuestDBStorage for database operations

class ParquetStorage(StorageBackend):
    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _get_path(self, exchange: str, instrument_type: str, symbol: str, timeframe: str, date: datetime) -> Path:
        # Parse base asset from symbol (e.g., BTC/USDT -> BTC)
        base_asset = symbol.split('/')[0]
        # Sanitize symbol for filename if needed, but directory structure handles most
        
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
        # Assuming data might span multiple days, though usually backfill chunks are small
        # But safer to group
        df['date'] = df['time'].dt.date
        
        for date, group in df.groupby('date'):
            # Extract metadata from first row
            first_row = group.iloc[0]
            exchange = first_row['exchange']
            # Instrument type is not in OHLCV model directly, usually passed in context
            # But here we only have the dict. 
            # We might need to inject instrument_type into the data dicts in the service
            # OR we can infer it or pass it to save method.
            # For now, let's assume it's passed in the data dict or we default to 'spot'
            # Wait, OHLCV model doesn't have instrument_type. 
            # The user wants: Data -> exchange -> instrument -> ...
            # I need to ensure instrument_type is available.
            # I will update the services to include 'instrument_type' in the data dicts passed to storage.
            
            instrument_type = group.get('instrument_type', pd.Series(['spot'] * len(group))).iloc[0]
            symbol = first_row['symbol']
            timeframe = first_row['timeframe']
            
            # Convert date back to datetime for _get_path (it expects datetime object or we adjust)
            # _get_path uses strftime, so date object is fine if we cast or adjust
            date_obj = datetime.combine(date, datetime.min.time())
            
            path = self._get_path(exchange, instrument_type, symbol, timeframe, date_obj)
            
            # Drop helper column
            save_df = group.drop(columns=['date'])
            
            # Write to parquet
            # If file exists, we might want to append or overwrite. 
            # For simplicity and idempotency, we overwrite for that day.
            # Or better: read existing, combine, drop duplicates, write back.
            
            if path.exists():
                existing_df = pd.read_parquet(path)
                combined_df = pd.concat([existing_df, save_df])
                combined_df = combined_df.drop_duplicates(subset=['time'])
                combined_df.to_parquet(path)
            else:
                save_df.to_parquet(path)
                
        logger.info(f"Saved {len(data)} OHLCV records to parquet.")

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
                
        logger.info(f"Saved {len(data)} tick records to parquet.")

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
        if 'time' in df.columns:
            df['date'] = df['time'].dt.date
        else:
            # Fallback if no time column, use current date or raise
            logger.warning(f"No time column in {data_type} data, using current date for partitioning")
            df['date'] = datetime.now().date()
        
        for date, group in df.groupby('date'):
            first_row = group.iloc[0]
            exchange = first_row.get('exchange', 'unknown')
            instrument_type = first_row.get('instrument_type', 'spot')
            symbol = first_row.get('symbol', 'unknown')
            
            date_obj = datetime.combine(date, datetime.min.time())
            path = self._get_path(exchange, instrument_type, symbol, data_type, date_obj)
            
            save_df = group.drop(columns=['date'])
            
            if path.exists():
                existing_df = pd.read_parquet(path)
                combined_df = pd.concat([existing_df, save_df])
                # Deduplication strategy depends on data type
                # For now, just append or maybe drop duplicates if all cols match
                combined_df = combined_df.drop_duplicates()
                combined_df.to_parquet(path)
            else:
                save_df.to_parquet(path)
        
        logger.info(f"Saved {len(data)} {data_type} records to parquet.")

    def _load_parquet_range(self, exchange: str, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, instrument_type: str) -> pd.DataFrame:
        """Helper to load parquet files for a date range."""
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
        
        # Filter exact time range
        # Ensure 'time' column is datetime
        if 'time' in combined_df.columns:
            if combined_df['time'].dt.tz is None:
                # Assuming UTC if no timezone
                combined_df['time'] = combined_df['time'].dt.tz_localize('UTC')
            
            # Ensure start/end are tz-aware
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
            
        # Structure: base_dir / exchange / instrument / base_asset / category / date.parquet
        for exchange_path in self.base_dir.iterdir():
            if not exchange_path.is_dir(): continue
            exchange = exchange_path.name
            
            for instrument_path in exchange_path.iterdir():
                if not instrument_path.is_dir(): continue
                instrument = instrument_path.name
                
                for asset_path in instrument_path.iterdir():
                    if not asset_path.is_dir(): continue
                    symbol = asset_path.name # This is base asset, effectively
                    
                    for category_path in asset_path.iterdir():
                        if not category_path.is_dir(): continue
                        category = category_path.name # timeframe or data_type
                        
                        # Determine if category is a timeframe or data type
                        # Heuristic: if it starts with digit, it's timeframe (e.g. 1h, 1m)
                        # Else it's data type (e.g. trades, aggTrades)
                        is_timeframe = category[0].isdigit()
                        data_type = 'ohlcv' if is_timeframe else category
                        timeframe = category if is_timeframe else None
                        
                        files = list(category_path.glob('*.parquet'))
                        if not files: continue
                        
                        dates = []
                        total_rows = 0
                        
                        for f in files:
                            try:
                                date_str = f.stem
                                dates.append(datetime.strptime(date_str, "%Y-%m-%d").date())
                                
                                if deep_scan:
                                    # Try to get row count from metadata
                                    try:
                                        # Read only index to be faster
                                        meta = pd.read_parquet(f, columns=[])
                                        total_rows += len(meta)
                                    except:
                                        pass
                            except:
                                pass
                        
                        if not dates: continue
                        
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
                        
        return results
