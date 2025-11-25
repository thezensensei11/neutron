import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Union, List, Dict
from pathlib import Path

from .storage import ParquetStorage, QuestDBStorage, StorageBackend
from .config import ConfigLoader

logger = logging.getLogger(__name__)

class DataCrawler:
    """
    Generalized service for retrieving market data from storage.
    Acts as a facade over the storage backend.
    """

    def __init__(self, ohlcv_path: Optional[str] = None, aggregated_path: Optional[str] = None, questdb_config: Optional[Dict] = None):
        """
        Initialize the DataCrawler with dual storage support.
        
        Args:
            ohlcv_path: Path to parquet data directory for OHLCV
            aggregated_path: Path to parquet data directory for Aggregated/Synthetic data
            questdb_config: Dict with host, ilp_port, pg_port for QuestDB (Tick/Generic)
        """
        self.ohlcv_storage: Optional[ParquetStorage] = None
        self.aggregated_storage: Optional[ParquetStorage] = None
        self.tick_storage: Optional[QuestDBStorage] = None
        
        # Initialize OHLCV Storage (Parquet)
        if ohlcv_path:
            self.ohlcv_storage = ParquetStorage(ohlcv_path)
            logger.info(f"DataCrawler initialized with Parquet storage at {ohlcv_path}")
        else:
            logger.warning("No OHLCV path provided. OHLCV data retrieval will fail.")

        # Initialize Aggregated Storage (Parquet)
        if aggregated_path:
            self.aggregated_storage = ParquetStorage(aggregated_path, is_aggregated=True)
            logger.info(f"DataCrawler initialized with Aggregated storage at {aggregated_path}")

        # Initialize Tick Storage (QuestDB)
        if questdb_config:
            self.tick_storage = QuestDBStorage(
                host=questdb_config.get('host', 'localhost'),
                ilp_port=questdb_config.get('ilp_port', 9009),
                pg_port=questdb_config.get('pg_port', 8812)
            )
            logger.info("DataCrawler initialized with QuestDB storage")
        else:
            logger.warning("No QuestDB config provided. Tick/Generic data retrieval will fail.")

    @classmethod
    def from_config(cls, config_path: str = 'config.json'):
        """
        Initialize DataCrawler from a configuration file.
        """
        config = ConfigLoader.load(config_path)
        
        # Aggregated path might not be in config object if it's new, check dict or attribute
        agg_path = getattr(config.storage, 'aggregated_path', 'data/aggregated')
        
        return cls(
            ohlcv_path=config.storage.ohlcv_path,
            aggregated_path=agg_path,
            questdb_config={
                'host': config.storage.questdb_host,
                'ilp_port': config.storage.questdb_ilp_port,
                'pg_port': config.storage.questdb_pg_port
            }
        )

    def get_ohlcv(
        self, 
        exchange: str, 
        symbol: str, 
        timeframe: str, 
        start_date: Union[str, datetime], 
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve OHLCV data from Parquet storage."""
        if not self.ohlcv_storage:
            logger.error("OHLCV storage not initialized.")
            return pd.DataFrame()

        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching OHLCV for {exchange} {symbol} {timeframe} from {start_dt} to {end_dt}")
        return self.ohlcv_storage.load_ohlcv(exchange, symbol, timeframe, start_dt, end_dt, instrument_type)

    def get_tick_data(
        self, 
        exchange: str, 
        symbol: str, 
        start_date: Union[str, datetime], 
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve tick/trade data from QuestDB."""
        if not self.tick_storage:
            logger.error("Tick storage (QuestDB) not initialized.")
            return pd.DataFrame()

        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching Tick Data for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.tick_storage.load_tick_data(exchange, symbol, start_dt, end_dt, instrument_type)

    def get_funding_rates(
        self, 
        exchange: str, 
        symbol: str, 
        start_date: Union[str, datetime], 
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'swap'
    ) -> pd.DataFrame:
        """Retrieve funding rate history from QuestDB."""
        if not self.tick_storage:
            logger.error("Tick storage (QuestDB) not initialized.")
            return pd.DataFrame()

        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching Funding Rates for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.tick_storage.load_funding_rates(exchange, symbol, start_dt, end_dt, instrument_type)

    def get_data(
        self,
        data_type: str,
        exchange: str,
        symbol: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve generic data from QuestDB."""
        if not self.tick_storage:
            logger.error("Tick storage (QuestDB) not initialized.")
            return pd.DataFrame()

        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching {data_type} for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.tick_storage.load_generic_data(data_type, exchange, symbol, start_dt, end_dt, instrument_type)

    def get_book_depth(
        self,
        exchange: str,
        symbol: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve Order Book Depth snapshots."""
        return self.get_data('bookDepth', exchange, symbol, start_date, end_date, instrument_type)

    def get_index_price_klines(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve Index Price Klines."""
        df = self.get_data('indexPriceKlines', exchange, symbol, start_date, end_date, instrument_type)
        if not df.empty and 'timeframe' in df.columns:
            return df[df['timeframe'] == timeframe]
        return df

    def get_mark_price_klines(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve Mark Price Klines."""
        df = self.get_data('markPriceKlines', exchange, symbol, start_date, end_date, instrument_type)
        if not df.empty and 'timeframe' in df.columns:
            return df[df['timeframe'] == timeframe]
        return df

    def get_premium_index_klines(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve Premium Index Klines."""
        df = self.get_data('premiumIndexKlines', exchange, symbol, start_date, end_date, instrument_type)
        if not df.empty and 'timeframe' in df.columns:
            return df[df['timeframe'] == timeframe]
        return df

    def _parse_date(self, date_input: Union[str, datetime]) -> datetime:
        """Helper to parse date strings to timezone-aware datetime."""
        if isinstance(date_input, datetime):
            if date_input.tzinfo is None:
                return date_input.replace(tzinfo=timezone.utc)
            return date_input
        
        try:
            dt = datetime.fromisoformat(date_input.replace('Z', '+00:00'))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            try:
                dt = datetime.strptime(date_input, "%Y-%m-%d")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"Could not parse date: {date_input}")

    def get_info_service(self):
        """Get the InfoService for summarizing storage."""
        from ..services.info_service import InfoService
        storages = []
        if self.ohlcv_storage: storages.append(self.ohlcv_storage)
        if self.aggregated_storage: storages.append(self.aggregated_storage)
        if self.tick_storage: storages.append(self.tick_storage)
        return InfoService(storages)

    def close(self):
        """Close connections."""
        if self.tick_storage:
            self.tick_storage.close()
