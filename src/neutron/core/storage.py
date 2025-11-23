from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime, timedelta, timezone
import pandas as pd
from pathlib import Path
import logging
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import select, func, text, case
from ..db.session import ScopedSession
from ..db.models import OHLCV, Trade, FundingRate, AggTrade, BookTicker, LiquidationSnapshot
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
        df = pd.DataFrame(data)
        if '_sa_instance_state' in df.columns:
            df = df.drop(columns=['_sa_instance_state'])
        return df

    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        results = []
        
        # Map data types to models
        # Note: Some models might be used for multiple data types (e.g. OpenInterest)
        model_map = {
            'ohlcv': OHLCV,
            'trades': Trade,
            'funding': FundingRate,
            'aggTrades': AggTrade,
            'bookTicker': BookTicker,
            'liquidationSnapshot': LiquidationSnapshot,
            'metrics': OpenInterest,
            'bookDepth': BookDepth,
            'indexPriceKlines': IndexPriceKline,
            'markPriceKlines': MarkPriceKline,
            'premiumIndexKlines': PremiumIndexKline
        }
        
        with ScopedSession() as db:
            for data_type, model in model_map.items():
                try:
                    # Inspect columns to see what we can group by
                    columns = {c.name for c in model.__table__.columns}
                    
                    group_cols = []
                    if 'exchange' in columns: group_cols.append(model.exchange)
                    if 'symbol' in columns: group_cols.append(model.symbol)
                    if 'instrument_type' in columns: group_cols.append(model.instrument_type)
                    if 'timeframe' in columns: group_cols.append(model.timeframe)
                    
                    # Basic stats query
                    stmt = select(
                        *group_cols,
                        func.min(model.time).label('start_date'),
                        func.max(model.time).label('end_date'),
                        func.count().label('count'),
                        func.sum(case((model.is_interpolated == True, 1), else_=0)).label('interpolated_count') if 'is_interpolated' in columns else text("0").label('interpolated_count')
                    ).group_by(*group_cols)
                    
                    query_results = db.execute(stmt).all()
                    
                    for row in query_results:
                        # Convert row to dict
                        # row is a Row object, keys correspond to select expressions
                        # We need to map back to our expected output format
                        
                        entry = {
                            'data_type': data_type,
                            'exchange': getattr(row, 'exchange', None),
                            'symbol': getattr(row, 'symbol', None),
                            'instrument_type': getattr(row, 'instrument_type', 'spot'), # Default if missing
                            'timeframe': getattr(row, 'timeframe', None),
                            'start_date': row.start_date,
                            'end_date': row.end_date,
                            'count': row.count,
                            'interpolated_count': getattr(row, 'interpolated_count', 0),
                            'gaps': []
                        }
                        
                        # Deep scan for gaps
                        if deep_scan and entry['count'] > 0:
                            # Gap detection using window functions
                            # We look for rows where time - prev_time > threshold
                            # Threshold depends on data type/timeframe
                            
                            threshold = None
                            if data_type == 'ohlcv' or entry['timeframe']:
                                # Parse timeframe
                                tf = entry['timeframe']
                                if tf == '1h': threshold = timedelta(hours=1, minutes=5) # 5 min tolerance
                                elif tf == '1m': threshold = timedelta(minutes=1, seconds=30)
                                elif tf == '1d': threshold = timedelta(days=1, hours=1)
                                else: threshold = timedelta(hours=1) # Default
                            elif data_type == 'metrics':
                                # Metrics usually every 5 mins
                                threshold = timedelta(minutes=10)
                            else:
                                # For tick data/aggTrades, gaps are harder to define without expected frequency
                                # Maybe skip or use a large threshold like 1 hour
                                threshold = timedelta(hours=1)

                            if threshold:
                                # Build filter conditions dynamically based on available columns
                                filters = []
                                if 'exchange' in columns and entry['exchange']:
                                    filters.append(model.exchange == entry['exchange'])
                                if 'symbol' in columns and entry['symbol']:
                                    filters.append(model.symbol == entry['symbol'])
                                if 'instrument_type' in columns and entry['instrument_type']:
                                    filters.append(model.instrument_type == entry['instrument_type'])
                                if 'timeframe' in columns and entry['timeframe']:
                                    filters.append(model.timeframe == entry['timeframe'])

                                # Subquery to calculate gaps
                                subq = select(
                                    model.time,
                                    func.lag(model.time).over(order_by=model.time).label('prev_time')
                                ).where(*filters).subquery()

                                gap_stmt = select(
                                    subq.c.prev_time,
                                    subq.c.time
                                ).where(
                                    (subq.c.time - subq.c.prev_time) > threshold
                                )
                                
                                gaps = db.execute(gap_stmt).all()
                                for g in gaps:
                                    entry['gaps'].append((g.prev_time, g.time))
                            
                        results.append(entry)
                        
                except Exception as e:
                    logger.error(f"Error listing data for {data_type}: {e}")
                    
        return results

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
