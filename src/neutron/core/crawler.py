import logging
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Union, List
from pathlib import Path

from .storage import DatabaseStorage, ParquetStorage, StorageBackend
from .config import ConfigLoader

logger = logging.getLogger(__name__)

class DataCrawler:
    """
    Generalized service for retrieving market data from storage.
    Acts as a facade over the storage backend.
    """

    def __init__(self, storage_type: str = 'database', storage_path: Optional[str] = None):
        """
        Initialize the DataCrawler.
        
        Args:
            storage_type: 'database' or 'parquet'
            storage_path: Path to parquet data directory (required if storage_type is 'parquet')
        """
        self.storage: StorageBackend
        
        if storage_type == 'database':
            self.storage = DatabaseStorage()
            logger.info("DataCrawler initialized with Database storage")
        elif storage_type == 'parquet':
            if not storage_path:
                raise ValueError("storage_path is required for parquet storage")
            self.storage = ParquetStorage(storage_path)
            logger.info(f"DataCrawler initialized with Parquet storage at {storage_path}")
        else:
            raise ValueError(f"Unknown storage type: {storage_type}")

    @classmethod
    def from_config(cls, config_path: str = 'config.json'):
        """Initialize DataCrawler from a configuration file."""
        config = ConfigLoader.load(config_path)
        return cls(
            storage_type=config.storage.type,
            storage_path=config.storage.path
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
        """
        Retrieve OHLCV data.
        
        Args:
            exchange: Exchange name (e.g., 'binance')
            symbol: Symbol pair (e.g., 'BTC/USDT')
            timeframe: Candle timeframe (e.g., '1h')
            start_date: Start datetime or ISO string
            end_date: End datetime or ISO string (defaults to now)
            instrument_type: 'spot' or 'swap'
            
        Returns:
            pd.DataFrame with OHLCV data
        """
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching OHLCV for {exchange} {symbol} {timeframe} from {start_dt} to {end_dt}")
        return self.storage.load_ohlcv(exchange, symbol, timeframe, start_dt, end_dt, instrument_type)

    def get_tick_data(
        self, 
        exchange: str, 
        symbol: str, 
        start_date: Union[str, datetime], 
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve tick/trade data."""
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching Tick Data for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.storage.load_tick_data(exchange, symbol, start_dt, end_dt, instrument_type)

    def get_funding_rates(
        self, 
        exchange: str, 
        symbol: str, 
        start_date: Union[str, datetime], 
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'swap'
    ) -> pd.DataFrame:
        """Retrieve funding rate history."""
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching Funding Rates for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.storage.load_funding_rates(exchange, symbol, start_dt, end_dt, instrument_type)

    def get_data(
        self,
        data_type: str,
        exchange: str,
        symbol: str,
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """
        Retrieve generic data (e.g., aggTrades, bookTicker, liquidationSnapshot).
        
        Args:
            data_type: Type of data to retrieve (e.g., 'aggTrades', 'bookTicker')
            exchange: Exchange name
            symbol: Symbol pair
            start_date: Start datetime
            end_date: End datetime
            instrument_type: 'spot', 'swap', etc.
        """
        start_dt = self._parse_date(start_date)
        end_dt = self._parse_date(end_date) if end_date else datetime.now(timezone.utc)
        
        logger.info(f"Fetching {data_type} for {exchange} {symbol} from {start_dt} to {end_dt}")
        return self.storage.load_generic_data(data_type, exchange, symbol, start_dt, end_dt, instrument_type)

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
        timeframe: str, # Note: Generic load might need to filter by timeframe if model has it. 
                        # The generic loader in storage.py currently doesn't filter by timeframe for generic types!
                        # I need to check storage.py load_generic_data.
        start_date: Union[str, datetime],
        end_date: Optional[Union[str, datetime]] = None,
        instrument_type: str = 'spot'
    ) -> pd.DataFrame:
        """Retrieve Index Price Klines."""
        # TODO: The generic loader needs to support timeframe filtering for these kline types.
        # For now, we'll fetch and filter in memory or update storage.
        # Let's update storage.py first to handle timeframe for generic klines?
        # Or just rely on get_data and assume the user knows.
        # Actually, the models IndexPriceKline etc HAVE a timeframe column.
        # But load_generic_data doesn't accept a timeframe argument.
        # It selects * from model where time range...
        # So it will return ALL timeframes mixed. That's bad.
        # I should update storage.py to handle timeframe for these types.
        
        # For now, let's just return what get_data returns, but I should fix storage.
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
            # Try simple date format YYYY-MM-DD
            try:
                dt = datetime.strptime(date_input, "%Y-%m-%d")
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                raise ValueError(f"Could not parse date: {date_input}")
