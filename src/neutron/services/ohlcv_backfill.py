import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from ..exchange import BinanceExchange
from ..db.models import OHLCV
from ..db.session import ScopedSession


logger = logging.getLogger(__name__)

class OHLCVBackfillService:
    def __init__(self, db_url: str = None, exchange=None, state_manager=None):
        self.db = ScopedSession()
        self.exchange = exchange or BinanceExchange()
        self.state_manager = state_manager

    def backfill_symbol(self, symbol: str, timeframe: str, start_date: datetime, end_date: Optional[datetime] = None):
        """Backfill OHLCV data for a single symbol."""
        logger.info(f"Starting backfill for {symbol} {timeframe} from {start_date} to {end_date}")
        
        current_since = start_date
        total_candles_session = 0
        last_log_time = time.time()
        
        # Track effective start for state update
        effective_start = start_date

        while True:
            try:
                # Stop if we've reached the end_date
                if end_date and current_since >= end_date:
                    break

                candles = self.exchange.fetch_ohlcv(
                    symbol=symbol, 
                    timeframe=timeframe, 
                    since=current_since, 
                    limit=1000
                )
                
                if not candles:
                    logger.info(f"No more data found for {symbol} after {current_since}")
                    break
                
                # Bulk Insert / Upsert
                stmt = insert(OHLCV).values(candles)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['time', 'symbol', 'exchange', 'timeframe'],
                    set_={
                        'open': stmt.excluded.open,
                        'high': stmt.excluded.high,
                        'low': stmt.excluded.low,
                        'close': stmt.excluded.close,
                        'volume': stmt.excluded.volume
                    }
                )
                self.db.execute(stmt)
                self.db.commit()
                
                count = len(candles)
                total_candles_session += count
                
                # Update current_since to the timestamp of the last candle + 1ms
                last_candle_time = candles[-1]['time']
                # Ensure last_candle_time is aware
                if last_candle_time.tzinfo is None:
                    last_candle_time = last_candle_time.replace(tzinfo=timezone.utc)
                    
                current_since = last_candle_time + timedelta(milliseconds=1)

                # Log progress every 30 seconds
                if time.time() - last_log_time > 30:
                    logger.info(f"[{symbol}] Progress: {last_candle_time} | Synced {total_candles_session} candles so far...")
                    last_log_time = time.time()
                
            except Exception as e:
                logger.error(f"Error during backfill: {e}")
                # In a real robust system, we might want to retry or save state
                raise

        logger.info(f"Backfill complete for {symbol}. Total candles: {total_candles_session}")
        
        # Update state if successful
        if self.state_manager and total_candles_session > 0:
            # Use effective end date (last candle time or requested end date)
            # If we hit end_date, use that. If we ran out of data, use the last candle time.
            state_end = end_date if end_date and current_since >= end_date else current_since
            
            self.state_manager.update_state(
                exchange=self.exchange.exchange_id,
                instrument_type=getattr(self.exchange, 'default_type', 'spot'), # Assuming exchange has this or we pass it
                symbol=symbol,
                timeframe=timeframe,
                start_date=effective_start,
                end_date=state_end
            )
            logger.info(f"Updated data state for {symbol} [{effective_start} - {state_end}]")
