import logging
from datetime import datetime, timedelta
from typing import Optional
import pandas as pd
import math

from ..data_source.binance_vision import BinanceVisionDownloader

logger = logging.getLogger("tick_monitor")

class BinanceBackfillService:
    def __init__(self, download_dir: str = "data/downloads", state_manager=None, exchange_name="binance", instrument_type="spot", storage=None, progress_manager=None):
        self.downloader = BinanceVisionDownloader(download_dir=download_dir)
        self.state_manager = state_manager
        self.exchange_name = exchange_name
        self.instrument_type = instrument_type
        self.storage = storage # Expecting ParquetStorage instance here
        self.progress_manager = progress_manager
        self.progress_manager = progress_manager

    def backfill_range(self, symbol: str, start_date: datetime, end_date: datetime, data_type: str, timeframe: str = None):
        """
        Backfill generic data for a symbol over a range of dates.
        """
        msg = f"Starting {data_type} backfill for {symbol} from {start_date.date()} to {end_date.date()}"
        logger.info(msg)
        
        # Create Progress Bar
        bar = None
        if self.progress_manager:
            total_days = (end_date - start_date).days
            bar = self.progress_manager.create_bar(
                task_id=f"{self.exchange_name}_{symbol}_{data_type}",
                desc=f"[{self.exchange_name}] {symbol} {data_type}",
                total=total_days,
                unit="d"
            )

        current_date = start_date
        while current_date < end_date:
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
                                    # Try to convert other fields to numeric
                                    for k, v in item.items():
                                        if k == time_field: continue
                                        if isinstance(v, str):
                                            # Check if it looks like a number
                                            if v.replace('.', '', 1).replace('-', '', 1).isdigit():
                                                try:
                                                    if '.' in v:
                                                        item[k] = float(v)
                                                    else:
                                                        item[k] = int(v)
                                                except ValueError:
                                                    pass # Keep as string
                                            
                                            # Explicit boolean conversion
                                            if k in ['is_buyer_maker', 'is_best_match', 'is_interpolated']:
                                                if isinstance(v, str):
                                                    if v.lower() == 'true': item[k] = True
                                                    elif v.lower() == 'false': item[k] = False
                                                elif isinstance(v, float):
                                                    if pd.isna(v) or math.isnan(v):
                                                        item[k] = False # Default to False for NaN
                                                    else:
                                                        item[k] = bool(v)
                                                elif v is None:
                                                    item[k] = False
                                            
                                            # Explicit integer conversion for IDs
                                            if k in ['agg_trade_id', 'trade_id', 'first_trade_id', 'last_trade_id', 'update_id']:
                                                try:
                                                    item[k] = int(v)
                                                except (ValueError, TypeError):
                                                    pass

                                    filtered_data.append(item)
                            except Exception as e:
                                logger.warning(f"Error parsing item: {e}")
                                continue
                        
                        if filtered_data:
                            # Save to Parquet (Source of Truth)
                            if self.storage:
                                self.storage.save_generic_data(filtered_data, data_type)
                            
                            # total_records += len(filtered_data) # This variable is not defined in the original code.
                            logger.info(f"Filtered {len(filtered_data)} records for {symbol} in range {start_date} - {end_date}")
                        else:
                            logger.debug(f"No relevant data found in daily file for {symbol} on {current_date.date()}")
                            logger.info(f"No data found in range {start_date} - {end_date} for {symbol} (Day file contained {len(data)} records)")
                        # Storage expects generic data save
                        if hasattr(self.storage, 'save_generic_data'):
                            self.storage.save_generic_data(data, data_type)
                        else:
                            logger.warning("Storage backend does not support generic data saving.")
                    
                    # Update state only if data was found
                    if self.state_manager:
                        day_end = current_date + timedelta(days=1)
                        # Use provided timeframe or default to 'raw' if None
                        tf = timeframe if timeframe else 'raw'
                        
                        self.state_manager.update_state(
                            exchange=self.exchange_name,
                            instrument_type=self.instrument_type,
                            symbol=symbol,
                            data_type=data_type,
                            timeframe=tf,
                            start_date=current_date,
                            end_date=day_end
                        )
                    
            except Exception as e:
                logger.error(f"Failed to backfill {data_type} for {symbol} on {current_date.date()}: {e}")
            
            current_date += timedelta(days=1)
            
            if bar:
                self.progress_manager.update_bar(
                    task_id=f"{self.exchange_name}_{symbol}_{data_type}",
                    advance=1
                )
            
        if bar:
            self.progress_manager.close_bar(f"{self.exchange_name}_{symbol}_{data_type}")
            
        logger.info(f"{data_type} backfill complete for {symbol}")
