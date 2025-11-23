from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select
from ..db.session import ScopedSession
from ..db.models import OHLCV, Trade, FundingRate, AggTrade, BookTicker, LiquidationSnapshot
from ..db.models import OHLCV, Trade, FundingRate, AggTrade, BookTicker, LiquidationSnapshot, OpenInterest, BookDepth, IndexPriceKline, MarkPriceKline, PremiumIndexKline

logger = logging.getLogger(__name__)

class StorageBackend(ABC):
# ... (interface remains same)
    @abstractmethod
    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        pass
# ...

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

    def load_ohlcv(self, exchange: str, symbol: str, timeframe: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        with ScopedSession() as db:
            stmt = select(OHLCV).where(
                OHLCV.exchange == exchange,
                OHLCV.symbol == symbol,
                OHLCV.timeframe == timeframe,
                OHLCV.time >= start_date,
                OHLCV.time < end_date
            ).order_by(OHLCV.time)
            
            result = db.execute(stmt)
            # Convert to list of dicts then DataFrame
            # This is more efficient than pd.read_sql for large datasets with ORM
            data = [row[0].__dict__ for row in result]
            
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        # Remove SQLAlchemy internal state
        if '_sa_instance_state' in df.columns:
            df = df.drop(columns=['_sa_instance_state'])
            
        return df

    def load_tick_data(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        with ScopedSession() as db:
            stmt = select(Trade).where(
                Trade.exchange == exchange,
                Trade.symbol == symbol,
                Trade.time >= start_date,
                Trade.time < end_date
            ).order_by(Trade.time)
            
            result = db.execute(stmt)
            data = [row[0].__dict__ for row in result]
            
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        if '_sa_instance_state' in df.columns:
            df = df.drop(columns=['_sa_instance_state'])
        return df

    def load_funding_rates(self, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'swap') -> pd.DataFrame:
        with ScopedSession() as db:
            stmt = select(FundingRate).where(
                FundingRate.exchange == exchange,
                FundingRate.symbol == symbol,
                FundingRate.time >= start_date,
                FundingRate.time < end_date
            ).order_by(FundingRate.time)
            
            result = db.execute(stmt)
            data = [row[0].__dict__ for row in result]
            
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        if '_sa_instance_state' in df.columns:
            df = df.drop(columns=['_sa_instance_state'])
        return df

    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        if not data:
            return

        # Data mapping for specific types
        if data_type == 'trades':
            for item in data:
                # Map qty -> amount
                if 'qty' in item and 'amount' not in item:
                    item['amount'] = item['qty']
                
                # Map is_buyer_maker -> side
                if 'is_buyer_maker' in item and 'side' not in item:
                    # is_buyer_maker=True -> Sell (Maker was Buyer)
                    # is_buyer_maker=False -> Buy (Maker was Seller)
                    item['side'] = 'sell' if item['is_buyer_maker'] else 'buy'

        model_map = {
            'aggTrades': AggTrade,
            'bookTicker': BookTicker,
            'liquidationSnapshot': LiquidationSnapshot,
            'trades': Trade,
            'funding': FundingRate,
            'metrics': OpenInterest, 
            'openInterest': OpenInterest,
            'bookDepth': BookDepth,
            'indexPriceKlines': IndexPriceKline,
            'markPriceKlines': MarkPriceKline,
            'premiumIndexKlines': PremiumIndexKline
        }
        
        model_class = model_map.get(data_type)
        if not model_class:
            logger.warning(f"Unknown data type {data_type} for DatabaseStorage")
            return

        # Special handling for metrics -> OpenInterest mapping
        if data_type == 'metrics':
            transformed_data = []
            for d in data:
                new_d = d.copy()
                # Map create_time to time if not already done
                if 'create_time' in new_d and 'time' not in new_d:
                    new_d['time'] = new_d['create_time']
                
                # Map sum_open_interest to value
                if 'sum_open_interest' in new_d:
                    new_d['value'] = new_d['sum_open_interest']
                
                transformed_data.append(new_d)
            data = transformed_data
        
        # Map open_time to time for Klines
        # This is necessary because Binance Vision CSVs use 'open_time' but our DB models use 'time' as the primary key
        if data_type in ['markPriceKlines', 'indexPriceKlines', 'premiumIndexKlines']:
            transformed_data = []
            for d in data:
                new_d = d.copy()
                if 'open_time' in new_d and 'time' not in new_d:
                    new_d['time'] = new_d['open_time']
                transformed_data.append(new_d)
            data = transformed_data

        clean_data = self._filter_data(data, model_class)
        
        with ScopedSession() as db:
            # Chunk inserts
            chunk_size = 5000
            for i in range(0, len(clean_data), chunk_size):
                chunk = clean_data[i:i + chunk_size]
                stmt = insert(model_class).values(chunk)
                
                # Determine conflict columns based on PK
                pk_cols = [c.name for c in model_class.__table__.primary_key.columns]
                stmt = stmt.on_conflict_do_nothing(index_elements=pk_cols)
                
                db.execute(stmt)
            db.commit()
        logger.info(f"Saved {len(data)} {data_type} records to database.")

    def load_generic_data(self, data_type: str, exchange: str, symbol: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot') -> pd.DataFrame:
        """
        Load generic data from database.
        """
        model_map = {
            'aggTrades': AggTrade,
            'bookTicker': BookTicker,
            'liquidationSnapshot': LiquidationSnapshot,
            'trades': Trade,
            'funding': FundingRate,
            'metrics': OpenInterest,
            'openInterest': OpenInterest,
            'bookDepth': BookDepth,
            'indexPriceKlines': IndexPriceKline,
            'markPriceKlines': MarkPriceKline,
            'premiumIndexKlines': PremiumIndexKline
        }
        
        model_class = model_map.get(data_type)
        if not model_class:
            logger.warning(f"Unknown data type {data_type} for DatabaseStorage")
            return pd.DataFrame()

        with ScopedSession() as db:
            stmt = select(model_class).where(
                model_class.exchange == exchange,
                model_class.symbol == symbol,
                model_class.time >= start_date,
                model_class.time < end_date
            ).order_by(model_class.time)
            
            result = db.execute(stmt)
            data = [row[0].__dict__ for row in result]
            
        if not data:
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        if '_sa_instance_state' in df.columns:
            df = df.drop(columns=['_sa_instance_state'])
        return df

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
