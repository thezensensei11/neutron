import json
import os
import threading
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

class TickDataStateManager:
    """
    Manages the state of downloaded tick/generic data.
    Hierarchy: exchange -> instrument -> symbol -> data_type -> timeframe -> ranges
    Persists state to a JSON file.
    Thread-safe.
    """

    def __init__(self, state_file: str = "tick_data_state.json"):
        self.state_file = state_file
        self._lock = threading.Lock()
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        with self._lock:
            if os.path.exists(self.state_file):
                try:
                    with open(self.state_file, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    logger.error(f"Failed to load state file {self.state_file}: {e}")
                    return {}
            return {}

    def _save_state_internal(self):
        try:
            # Create a temporary file first to avoid corruption
            temp_file = f"{self.state_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            os.replace(temp_file, self.state_file)
            logger.info(f"Saved tick state to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state file {self.state_file}: {e}")

    def get_gaps(
        self, 
        exchange: str, 
        instrument_type: str, 
        symbol: str, 
        data_type: str,
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Calculate the gaps (missing data ranges) for the requested period.
        """
        # Ensure dates are UTC aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        with self._lock:
            # Get existing ranges
            # exchange -> instrument -> symbol -> data_type -> timeframe
            ranges = self.state.get(exchange, {}).get(instrument_type, {}).get(symbol, {}).get(data_type, {}).get(timeframe, [])
            
            existing_intervals = []
            for r in ranges:
                s = datetime.fromisoformat(r[0])
                e = datetime.fromisoformat(r[1])
                if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
                if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
                existing_intervals.append((s, e))

        existing_intervals.sort(key=lambda x: x[0])

        gaps = []
        current_start = start_date

        for exist_start, exist_end in existing_intervals:
            if current_start >= end_date:
                break
            if exist_end <= current_start:
                continue
            if exist_start > current_start:
                gap_end = min(exist_start, end_date)
                gaps.append((current_start, gap_end))
            current_start = max(current_start, exist_end)

        if current_start < end_date:
            gaps.append((current_start, end_date))

        return gaps

    def update_state(
        self, 
        exchange: str, 
        instrument_type: str, 
        symbol: str, 
        data_type: str,
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ):
        """
        Update the state with a newly downloaded range.
        """
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        logger.info(f"Updating tick state for {exchange} {instrument_type} {symbol} {data_type} {timeframe}: {start_date} - {end_date}")

        with self._lock:
            # Reload state from disk
            try:
                if os.path.exists(self.state_file):
                    with open(self.state_file, 'r') as f:
                        self.state = json.load(f)
            except Exception as e:
                logger.error(f"Failed to reload tick state: {e}")

            # Initialize structure
            if exchange not in self.state: self.state[exchange] = {}
            if instrument_type not in self.state[exchange]: self.state[exchange][instrument_type] = {}
            if symbol not in self.state[exchange][instrument_type]: self.state[exchange][instrument_type][symbol] = {}
            if data_type not in self.state[exchange][instrument_type][symbol]: self.state[exchange][instrument_type][symbol][data_type] = {}
            if timeframe not in self.state[exchange][instrument_type][symbol][data_type]: self.state[exchange][instrument_type][symbol][data_type][timeframe] = []

            ranges = self.state[exchange][instrument_type][symbol][data_type][timeframe]
            intervals = []
            for r in ranges:
                s = datetime.fromisoformat(r[0])
                e = datetime.fromisoformat(r[1])
                if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
                if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
                intervals.append((s, e))
            
            intervals.append((start_date, end_date))
            intervals.sort(key=lambda x: x[0])
            
            merged = []
            if intervals:
                curr_start, curr_end = intervals[0]
                for next_start, next_end in intervals[1:]:
                    if next_start <= curr_end:
                        curr_end = max(curr_end, next_end)
                    else:
                        merged.append((curr_start, curr_end))
                        curr_start, curr_end = next_start, next_end
                merged.append((curr_start, curr_end))

            new_ranges = [[s.isoformat(), e.isoformat()] for s, e in merged]
            self.state[exchange][instrument_type][symbol][data_type][timeframe] = new_ranges
            
            self._save_state_internal()
