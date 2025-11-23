from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime
import pandas as pd
from pathlib import Path
import logging
from sqlalchemy.dialects.postgresql import insert
from ..db.session import ScopedSession
from ..db.models import OHLCV, Trade, FundingRate

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
    @abstractmethod
    def save_ohlcv(self, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_tick_data(self, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_funding_rates(self, data: List[Dict[str, Any]]):
        pass

class DatabaseStorage(StorageBackend):
    def _filter_data(self, data: List[Dict[str, Any]], model_class) -> List[Dict[str, Any]]:
        """Filter data dictionaries to only include keys present in the model columns."""
        valid_keys = {c.name for c in model_class.__table__.columns}
        return [{k: v for k, v in d.items() if k in valid_keys} for d in data]

    def save_ohlcv(self, data: List[Dict[str, Any]]):
        if not data:
            return
        
        clean_data = self._filter_data(data, OHLCV)
        
        with ScopedSession() as db:
            stmt = insert(OHLCV).values(clean_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange', 'timeframe'])
            db.execute(stmt)
            db.commit()
        logger.info(f"Saved {len(data)} OHLCV records to database.")

    def save_tick_data(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        clean_data = self._filter_data(data, Trade)
        
        with ScopedSession() as db:
            # Chunk inserts for large tick data
            chunk_size = 5000
            for i in range(0, len(clean_data), chunk_size):
                chunk = clean_data[i:i + chunk_size]
                stmt = insert(Trade).values(chunk)
                # Trade ID might not be unique across exchanges, relying on composite PK
                stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange', 'trade_id'])
                db.execute(stmt)
            db.commit()
        logger.info(f"Saved {len(data)} tick records to database.")

    def save_funding_rates(self, data: List[Dict[str, Any]]):
        if not data:
            return
            
        clean_data = self._filter_data(data, FundingRate)
        
        with ScopedSession() as db:
            stmt = insert(FundingRate).values(clean_data)
            stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange'])
            db.execute(stmt)
            db.commit()
        logger.info(f"Saved {len(data)} funding rates to database.")

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
