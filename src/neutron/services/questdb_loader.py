import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, List
from pathlib import Path

from ..core.storage.questdb import QuestDBStorage
from ..core.storage.parquet import ParquetStorage

logger = logging.getLogger(__name__)

class QuestDBLoader:
    def __init__(self, parquet_storage: ParquetStorage, questdb_storage: QuestDBStorage):
        self.parquet_storage = parquet_storage
        self.questdb_storage = questdb_storage

    def load_range(self, exchange: str, symbol: str, data_type: str, start_date: datetime, end_date: datetime, instrument_type: str = 'spot'):
        """
        Load data from Parquet files into QuestDB for a specific range.
        """
        logger.info(f"Loading {data_type} for {symbol} from Parquet to QuestDB ({start_date} - {end_date})")
        
        # Load data from Parquet (Source of Truth)
        try:
            if data_type == 'ohlcv':
                # timeframe is required for OHLCV, assuming '1m' or passed in params if we change signature
                # For now, let's assume this loader is mostly for tick/generic data as requested
                logger.warning("OHLCV loading not fully implemented in this generic loader yet.")
                return
            
            # Check if data already exists in QuestDB to avoid redundant work
            if self.questdb_storage.has_data(data_type, exchange, symbol, start_date, end_date, instrument_type):
                logger.info(f"Data for {symbol} {data_type} ({start_date} - {end_date}) already exists in QuestDB. Skipping.")
                return

            # Use the existing load methods in ParquetStorage which handle date ranges
            if data_type == 'aggTrades' or data_type == 'trades':
                df = self.parquet_storage.load_generic_data(data_type, exchange, symbol, start_date, end_date, instrument_type)
            elif data_type == 'bookDepth':
                df = self.parquet_storage.load_generic_data(data_type, exchange, symbol, start_date, end_date, instrument_type)
            else:
                df = self.parquet_storage.load_generic_data(data_type, exchange, symbol, start_date, end_date, instrument_type)
                
            if df.empty:
                logger.info(f"No data found in Parquet for {symbol} {data_type} in range.")
                return

            logger.info(f"Read {len(df)} records from Parquet. Ingesting to QuestDB...")
            
            # Convert DataFrame to list of dicts for QuestDBStorage
            # TODO: Optimize this to use a bulk DataFrame writer in QuestDBStorage if possible, 
            # but for now reuse the existing ILP sender which is robust.
            
            # We need to handle timestamps correctly
            if 'time' in df.columns:
                if df['time'].dt.tz is None:
                    df['time'] = df['time'].dt.tz_localize('UTC')
            
            # Handle boolean columns that might be NaN (float) or string "nan"
            bool_cols = ['is_buyer_maker', 'is_best_match', 'is_interpolated']
            for col in bool_cols:
                if col in df.columns:
                    # Convert to string first to handle mixed types
                    # Then map known string representations
                    # Then fillna and convert
                    if df[col].dtype == 'object' or df[col].dtype == 'string':
                         df[col] = df[col].astype(str).str.lower().replace({
                             'nan': False, 'none': False, 'null': False, 'true': True, 't': True, 'false': False, 'f': False
                         })
                    
                    # Final cleanup for actual NaNs and conversion
                    # Use infer_objects to avoid FutureWarning about downcasting
                    df[col] = df[col].fillna(False).infer_objects(copy=False).astype(bool)
            
            # Handle integer IDs that might be float due to NaN (though shouldn't be for IDs)
            id_cols = ['agg_trade_id', 'trade_id', 'first_trade_id', 'last_trade_id', 'update_id']
            for col in id_cols:
                if col in df.columns:
                    # Fill NaN with -1 (or 0) and convert to int, or keep as is if no NaNs
                    if df[col].isnull().any():
                         df[col] = df[col].fillna(-1).astype(int)
                    else:
                         df[col] = df[col].astype(int)
            
            # Handle generic NaN in object columns (replace with empty string or similar if needed)
            # QuestDB handles null strings okay usually, but let's be safe
            # df = df.fillna('') # Be careful with this, might affect numeric cols if not careful
            
            records = df.to_dict('records')
            
            # Chunking is handled inside QuestDBStorage._send_ilp (batch_size=100)
            # But we might want to chunk here to avoid massive memory usage if df is huge
            chunk_size = 100000
            total_chunks = (len(records) + chunk_size - 1) // chunk_size
            
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                current_chunk = i // chunk_size + 1
                
                # Log only every 5 chunks (500k records) or for the last chunk to reduce spam
                if current_chunk % 5 == 0 or current_chunk == total_chunks:
                    logger.info(f"Ingesting chunk {current_chunk}/{total_chunks} ({len(chunk)} records)")
                
                self.questdb_storage.save_generic_data(chunk, data_type)
                
            logger.info(f"Successfully loaded {len(records)} records into QuestDB.")
            
        except Exception as e:
            logger.error(f"Failed to load data into QuestDB: {e}")
            raise
