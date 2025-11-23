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

    def backfill_range(self, symbol: str, start_date: datetime, end_date: datetime, data_type: str):
        """
        Backfill generic data for a symbol over a range of dates.
        """
        logger.info(f"Starting {data_type} backfill for {symbol} from {start_date.date()} to {end_date.date()}")
        
        current_date = start_date
        while current_date <= end_date:
            try:
                data = self.downloader.download_daily_data(
                    symbol=symbol, 
                    date=current_date, 
                    data_type=data_type, 
                    instrument_type=self.instrument_type
                )
                
                if data and self.storage:
                    # Storage expects generic data save
                    if hasattr(self.storage, 'save_generic_data'):
                        self.storage.save_generic_data(data, data_type)
                    else:
                        logger.warning("Storage backend does not support generic data saving.")
                
                # Update state
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
