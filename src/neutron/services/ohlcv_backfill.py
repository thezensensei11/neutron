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
        # Check for listing date to optimize start_date
        # Use exchange instance to get listing date (handles caching and probing)
        listing_date = self.exchange.get_listing_date(symbol)
        self.data_logger.info(f"[{exch_id}] Listing date for {symbol}: {listing_date}")
        
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
            self.data_logger.info(f"[{exch_id}] Gaps to fill for {symbol}: {len(gaps_to_fill)}")
            for g_start, g_end in gaps_to_fill:
                self.data_logger.info(f"  Gap: {g_start} -> {g_end}")

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
                unit="s", # Seconds of data
                leave=False # Auto-close bar when done to prevent flooding
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
                    # Smart Gap Handling: If we are not near the end_date, assume it's a gap and skip forward
                    time_to_end = (end_date - current_since).total_seconds()
                    
                    # If less than 1 day remaining, assume it's truly the end of data
                    if time_to_end < 86400:
                        self.data_logger.info(f"[{exch_id}] No more data for {symbol} after {current_since}")
                        break
                    
                    # Otherwise, skip forward by one batch size (limit * timeframe)
                    duration_seconds = self.exchange.client.parse_timeframe(timeframe)
                    skip_seconds = duration_seconds * 1000
                    next_since = current_since + timedelta(seconds=skip_seconds)
                    
                    self.data_logger.warning(f"[{exch_id}] Gap detected for {symbol} at {current_since}. Skipping to {next_since}...")
                    
                    # Mark gap as processed in state to prevent infinite retries
                    if self.state_manager:
                        self.state_manager.update_state(
                            exchange=exch_id,
                            instrument_type=instrument_type,
                            symbol=symbol,
                            timeframe=timeframe,
                            start_date=start_date,
                            end_date=min(next_since, end_date)
                        )
                        
                    # Update progress bar
                    if bar:
                        self.progress_manager.update_bar(
                            task_id=f"{exch_id}_{symbol}_{timeframe}",
                            advance=int(skip_seconds),
                            desc=f"[{exch_id}] {symbol} {timeframe} (Gap)"
                        )
                        
                    current_since = next_since
                    continue
                
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
                
                # Update current_since to the timestamp of the last candle + 1ms (to fetch next candle)
                current_since = last_candle_time + timedelta(milliseconds=1)
                
                # Calculate candle duration for accurate state end date
                # parse_timeframe returns seconds
                duration_seconds = self.exchange.client.parse_timeframe(timeframe)
                last_candle_end = last_candle_time + timedelta(seconds=duration_seconds)

                # Update state after every batch for robustness
                if self.state_manager:
                    self.state_manager.update_state(
                        exchange=exch_id,
                        instrument_type=instrument_type,
                        symbol=symbol,
                        timeframe=timeframe,
                        start_date=start_date,
                        end_date=min(last_candle_end, end_date) # Update up to the end of the last candle
                    )
                
                if current_since >= end_date:
                    break
                    
            except Exception as e:
                err_msg = str(e)
                is_rate_limit = "Too many visits" in err_msg or "10006" in err_msg or "429" in err_msg
                
                if is_rate_limit:
                    backoff_delay = min(300, 5 * (2 ** batch_count)) # Cap at 5 mins
                    # Reset batch count on success? No, this is retrying the SAME batch? 
                    # Actually the loop continues, so it retries the same `current_since`.
                    # But `batch_count` increments on success. We need a separate retry counter.
                    # For now, let's just use a simple increasing backoff based on consecutive failures?
                    # Since we don't have a consecutive failure counter here easily without restructuring,
                    # let's just use a larger fixed sleep for rate limits or a randomized one.
                    # Better: sleep 60s for rate limits.
                    logger.warning(f"[{exch_id}] Rate limit hit for {symbol}. Sleeping 60s...")
                    time.sleep(60)
                else:
                    logger.error(f"[{exch_id}] Error backfilling {symbol}: {e}")
                    self.data_logger.error(f"[{exch_id}] Error backfilling {symbol}: {e}")
                    time.sleep(5)
                continue

        duration = time.time() - start_time
        avg_speed = total_candles / duration if duration > 0 else 0
        summary = f"[{exch_id}] Range backfill complete for {symbol}. Total: {total_candles}. Avg Speed: {avg_speed:.1f} candles/s"
        self.data_logger.info(summary)
        
        # REMOVED: Final blind state update. 
        # State is now only updated incrementally based on actual data fetched.
        # This prevents marking empty ranges (e.g. pre-listing) as "downloaded".
