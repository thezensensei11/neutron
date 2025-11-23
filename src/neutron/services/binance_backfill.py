import logging
from datetime import datetime, timedelta
from typing import Optional

from ..data_source.binance_vision import BinanceVisionDownloader

logger = logging.getLogger(__name__)

class BinanceBackfillService:
    def __init__(self, download_dir: str = "data/downloads", state_manager=None, exchange_name="binance", instrument_type="spot", storage=None):
        self.downloader = BinanceVisionDownloader(download_dir=download_dir)
        self.state_manager = state_manager
        self.exchange_name = exchange_name
        self.instrument_type = instrument_type
        self.storage = storage

    def backfill_range(self, symbol: str, start_date: datetime, end_date: datetime, data_type: str, timeframe: str = None):
        """
        Backfill generic data for a symbol over a range of dates.
        
        This method downloads daily data files, filters them to the specific time range requested
        (down to the millisecond), and saves the result to the configured storage.
        
        Args:
            symbol: The trading symbol (e.g., 'BTC/USDT')
            start_date: Start datetime (inclusive)
            end_date: End datetime (exclusive)
            data_type: Type of data to backfill (e.g., 'trades', 'aggTrades', 'metrics')
            timeframe: Optional timeframe for kline data (e.g., '1h')
        """
        logger.info(f"Starting {data_type} backfill for {symbol} from {start_date.date()} to {end_date.date()}")
        
        current_date = start_date
        while current_date <= end_date:
            try:
                data = self.downloader.download_daily_data(
                    symbol=symbol, 
                    date=current_date, 
                    data_type=data_type, 
                    instrument_type=self.instrument_type,
                    timeframe=timeframe
                )
                
                if data:
                    # Filter data to exact time range if provided
                    # Most generic data has 'time' or 'transactTime' or 'T' (aggTrades) or 't' (klines)
                    # We need to normalize or check multiple fields
                    filtered_data = []
                    
                    # Determine time field
                    time_field = None
                    if data and isinstance(data[0], dict):
                        if 'time' in data[0]: time_field = 'time'
                        elif 'transactTime' in data[0]: time_field = 'transactTime'
                        elif 'T' in data[0]: time_field = 'T' # AggTrades
                        elif 't' in data[0]: time_field = 't' # Klines/MarkPrice
                    
                    if time_field:
                        start_ts = start_date.timestamp() * 1000
                        end_ts = end_date.timestamp() * 1000
                        
                        for item in data:
                            item_time = item.get(time_field)
                            
                            # Normalize item_time to float timestamp (ms)
                            try:
                                if isinstance(item_time, (int, float)):
                                    pass # Already numeric
                                elif isinstance(item_time, str):
                                    # Try parsing ISO string or numeric string
                                    try:
                                        item_time = float(item_time)
                                    except ValueError:
                                        # Assume ISO format
                                        dt = datetime.fromisoformat(item_time.replace('Z', '+00:00'))
                                        item_time = dt.timestamp() * 1000
                                elif hasattr(item_time, 'timestamp'): # datetime or pd.Timestamp
                                    item_time = item_time.timestamp() * 1000
                                else:
                                    continue # Unknown format, skip
                                    
                                if start_ts <= item_time < end_ts:
                                    filtered_data.append(item)
                            except Exception as e:
                                logger.warning(f"Error parsing time for item: {e}")
                                continue
                        
                        if not filtered_data:
                            logger.info(f"No data found in range {start_date} - {end_date} for {symbol} (Day file contained {len(data)} records)")
                            # We still might want to update state? No, if no data in range, maybe not.
                            # But if the day file was downloaded and empty for our range, we technically "checked" it.
                            # However, for 2 min range, we don't want to mark the whole day as done?
                            # The state manager marks the *day*. 
                            # If we are doing sub-daily backfills, our state management (daily resolution) is imperfect.
                            # But for this specific user request (manageable data), filtering is key.
                            pass
                        else:
                            data = filtered_data
                            logger.info(f"Filtered {len(data)} records for {symbol} in range {start_date} - {end_date}")
                    
                    if data and self.storage:
                        # Storage expects generic data save
                        if hasattr(self.storage, 'save_generic_data'):
                            self.storage.save_generic_data(data, data_type)
                        else:
                            logger.warning("Storage backend does not support generic data saving.")
                    
                    # Update state only if data was found
                    if self.state_manager:
                        day_end = current_date + timedelta(days=1)
                        self.state_manager.update_state(
                            exchange=self.exchange_name,
                            instrument_type=self.instrument_type,
                            symbol=symbol,
                            timeframe=data_type, # Use data_type as timeframe identifier
                            start_date=current_date,
                            end_date=day_end
                        )
                    
            except Exception as e:
                logger.error(f"Failed to backfill {data_type} for {symbol} on {current_date.date()}: {e}")
            
            current_date += timedelta(days=1)
            
        logger.info(f"{data_type} backfill complete for {symbol}")
