import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

from ..exchange import BinanceExchange
from ..db.models import OHLCV
from ..db.session import ScopedSession
from ..core.state import ExchangeStateManager


logger = logging.getLogger(__name__)

class OHLCVBackfillService:
    def __init__(self, exchange, state_manager=None, storage=None, progress_manager=None):
        self.exchange = exchange
        self.state_manager = state_manager
        self.storage = storage
        self.progress_manager = progress_manager
        self.data_logger = logging.getLogger("data_monitor")

    def backfill_symbol(self, symbol: str, timeframe: str, start_date: datetime, end_date: Optional[datetime] = None, instrument_type: str = 'spot'):
        """
        Backfill OHLCV data for a symbol.
        """
        if end_date is None:
            end_date = datetime.now(timezone.utc)

        # Exchange context for logging
        exch_id = self.exchange.exchange_id

        # Check for listing date to optimize start_date
        exchange_state = ExchangeStateManager()
        listing_date = exchange_state.get_listing_date(self.exchange.exchange_id, instrument_type, symbol)
        
        if listing_date:
            # Ensure listing_date is UTC
            if listing_date.tzinfo is None:
                listing_date = listing_date.replace(tzinfo=timezone.utc)
                
            if listing_date > start_date:
                msg = f"[{exch_id}] Adjusting start date for {symbol} from {start_date} to {listing_date} (Listing Date)"
                self.data_logger.info(msg)
                start_date = listing_date

        msg = f"[{exch_id}] Starting backfill for {symbol} {timeframe} from {start_date} to {end_date}"
        self.data_logger.info(msg)

        # Determine gaps to fill
        gaps_to_fill = [(start_date, end_date)]
        if self.state_manager:
            gaps_to_fill = self.state_manager.get_gaps(
                exchange=exch_id,
                instrument_type=instrument_type,
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date
            )
            if not gaps_to_fill:
                msg = f"[{exch_id}] No gaps found for {symbol} {timeframe}. Data already exists."
                self.data_logger.info(msg)
                return

        # Create Progress Bar
        bar = None
        if self.progress_manager and gaps_to_fill:
            # Calculate total duration from gaps
            total_duration = sum((end - start).total_seconds() for start, end in gaps_to_fill)
            
            # We can track progress by time covered
            bar = self.progress_manager.create_bar(
                task_id=f"{exch_id}_{symbol}_{timeframe}",
                desc=f"[{exch_id}] {symbol} {timeframe}",
                total=int(total_duration),
                unit="s" # Seconds of data
            )

        total_candles = 0
        start_time = time.time()
        
        for gap_start, gap_end in gaps_to_fill:
            self.data_logger.info(f"[{exch_id}] Filling gap for {symbol}: {gap_start} -> {gap_end}")
            self._backfill_range(symbol, timeframe, gap_start, gap_end, instrument_type, exch_id, bar)
            
        duration = time.time() - start_time
        
        if bar:
            self.progress_manager.close_bar(f"{exch_id}_{symbol}_{timeframe}")

        summary = f"[{exch_id}] Backfill complete for {symbol}."
        self.data_logger.info(summary)

    def _backfill_range(self, symbol, timeframe, start_date, end_date, instrument_type, exch_id, bar=None):
        current_since = start_date
        total_candles = 0
        start_time = time.time()
        
        # Track batches for periodic state update
        batch_count = 0
        
        while current_since < end_date:
            try:
                batch_start_time = time.time()
                candles = self.exchange.fetch_ohlcv(symbol, timeframe, since=current_since, limit=1000)
                
                if not candles:
                    self.data_logger.info(f"[{exch_id}] No more data for {symbol} after {current_since}")
                    break
                
                # Filter candles beyond end_date
                if end_date:
                    candles = [c for c in candles if c['time'] < end_date]
                    if not candles:
                        self.data_logger.info(f"[{exch_id}] Reached end date {end_date} for {symbol}")
                        break
                
                # Adjust start_date based on actual data received (if this is the first batch)
                if total_candles == 0:
                    first_candle_time = candles[0]['time']
                    # Ensure timezone awareness for comparison
                    if first_candle_time.tzinfo is None:
                        first_candle_time = first_candle_time.replace(tzinfo=timezone.utc)
                    
                    if first_candle_time > start_date:
                        msg = f"[{exch_id}] Adjusting start date for {symbol} from {start_date} to {first_candle_time} based on actual data."
                        self.data_logger.info(msg)
                        start_date = first_candle_time

                # Quality Checks
                for c in candles:
                    if c['close'] <= 0:
                        self.data_logger.warning(f"[{exch_id}] Invalid price detected for {symbol} at {c['time']}: {c['close']}")
                    if c['volume'] < 0:
                        self.data_logger.warning(f"[{exch_id}] Negative volume detected for {symbol} at {c['time']}: {c['volume']}")

                # Inject instrument_type for storage
                for c in candles:
                    c['instrument_type'] = instrument_type
                
                # Save using storage backend
                if self.storage:
                    self.storage.save_ohlcv(candles)
                else:
                    # Fallback to DB if no storage provided (legacy support or default)
                    from sqlalchemy.dialects.postgresql import insert
                    
                    # Filter out extra keys like instrument_type
                    valid_keys = {c.name for c in OHLCV.__table__.columns}
                    clean_candles = [{k: v for k, v in c.items() if k in valid_keys} for c in candles]
                    
                    with ScopedSession() as db:
                        stmt = insert(OHLCV).values(clean_candles)
                        stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange', 'timeframe'])
                        db.execute(stmt)
                        db.commit()

                count = len(candles)
                total_candles += count
                batch_count += 1
                
                # Stats
                batch_duration = time.time() - batch_start_time
                speed = count / batch_duration if batch_duration > 0 else 0
                last_candle_time = candles[-1]['time']
                
                # Log to file only
                self.data_logger.info(
                    f"[{exch_id}] [{symbol}] Downloaded {count} candles. "
                    f"Range: {candles[0]['time']} -> {last_candle_time}. "
                    f"Speed: {speed:.1f} candles/s."
                )
                
                # Update Progress Bar
                if bar:
                    # Calculate time covered in this batch
                    time_covered = (last_candle_time - current_since).total_seconds()
                    # Ensure positive
                    time_covered = max(0, time_covered)
                    
                    self.progress_manager.update_bar(
                        task_id=f"{exch_id}_{symbol}_{timeframe}",
                        advance=int(time_covered),
                        desc=f"[{exch_id}] {symbol} {timeframe} ({speed:.0f} c/s)"
                    )
                
                # Update current_since to the timestamp of the last candle + 1ms
                current_since = last_candle_time + timedelta(milliseconds=1)
                
                # Update state after every batch for robustness
                if self.state_manager:
                    self.state_manager.update_state(
                        exchange=exch_id,
                        instrument_type=instrument_type,
                        symbol=symbol,
                        timeframe=timeframe,
                        start_date=start_date,
                        end_date=min(current_since, end_date) # Update up to where we are now, clamped
                    )
                
                if current_since >= end_date:
                    break
                    
            except Exception as e:
                err_msg = f"[{exch_id}] Error backfilling {symbol}: {e}"
                logger.error(err_msg)
                self.data_logger.error(err_msg)
                time.sleep(5)
                continue

        duration = time.time() - start_time
        avg_speed = total_candles / duration if duration > 0 else 0
        summary = f"[{exch_id}] Range backfill complete for {symbol}. Total: {total_candles}. Avg Speed: {avg_speed:.1f} candles/s"
        self.data_logger.info(summary)
        
        if self.state_manager:
            self.state_manager.update_state(
                exchange=self.exchange.exchange_id,
                instrument_type=instrument_type,
                symbol=symbol,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date
            )
