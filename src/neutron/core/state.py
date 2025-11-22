import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any
import logging

logger = logging.getLogger(__name__)

class DataStateManager:
    """
    Manages the state of downloaded data to avoid redundant downloads.
    Persists state to a JSON file.
    """

    def __init__(self, state_file: str = "data_state.json"):
        self.state_file = state_file
        self.state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Failed to load state file {self.state_file}: {e}")
                return {}
        return {}

    def _save_state(self):
        try:
            # Create a temporary file first to avoid corruption
            temp_file = f"{self.state_file}.tmp"
            with open(temp_file, 'w') as f:
                json.dump(self.state, f, indent=2)
            os.replace(temp_file, self.state_file)
        except Exception as e:
            logger.error(f"Failed to save state file {self.state_file}: {e}")

    def get_gaps(
        self, 
        exchange: str, 
        instrument_type: str, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ) -> List[Tuple[datetime, datetime]]:
        """
        Calculate the gaps (missing data ranges) for the requested period.
        Returns a list of (start, end) tuples that need to be downloaded.
        """
        # Ensure dates are UTC aware
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        # Get existing ranges
        ranges = self.state.get(exchange, {}).get(instrument_type, {}).get(symbol, {}).get(timeframe, [])
        
        # Convert stored strings back to datetime objects
        existing_intervals = []
        for r in ranges:
            s = datetime.fromisoformat(r[0])
            e = datetime.fromisoformat(r[1])
            if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
            existing_intervals.append((s, e))

        # Sort intervals by start time
        existing_intervals.sort(key=lambda x: x[0])

        gaps = []
        current_start = start_date

        for exist_start, exist_end in existing_intervals:
            # If current_start is beyond the requested end_date, we are done
            if current_start >= end_date:
                break

            # If existing interval ends before current_start, it's irrelevant
            if exist_end <= current_start:
                continue

            # If existing interval starts after current_start, we have a gap
            if exist_start > current_start:
                # Gap from current_start to exist_start (clamped by end_date)
                gap_end = min(exist_start, end_date)
                gaps.append((current_start, gap_end))
            
            # Move current_start to the end of this existing interval
            current_start = max(current_start, exist_end)

        # If there is still time left after the last interval
        if current_start < end_date:
            gaps.append((current_start, end_date))

        return gaps

    def update_state(
        self, 
        exchange: str, 
        instrument_type: str, 
        symbol: str, 
        timeframe: str, 
        start_date: datetime, 
        end_date: datetime
    ):
        """
        Update the state with a newly downloaded range.
        Automatically merges overlapping or adjacent ranges.
        """
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        # Initialize structure if missing
        if exchange not in self.state: self.state[exchange] = {}
        if instrument_type not in self.state[exchange]: self.state[exchange][instrument_type] = {}
        if symbol not in self.state[exchange][instrument_type]: self.state[exchange][instrument_type][symbol] = {}
        if timeframe not in self.state[exchange][instrument_type][symbol]: self.state[exchange][instrument_type][symbol][timeframe] = []

        # Get existing ranges
        ranges = self.state[exchange][instrument_type][symbol][timeframe]
        intervals = []
        for r in ranges:
            s = datetime.fromisoformat(r[0])
            e = datetime.fromisoformat(r[1])
            if s.tzinfo is None: s = s.replace(tzinfo=timezone.utc)
            if e.tzinfo is None: e = e.replace(tzinfo=timezone.utc)
            intervals.append((s, e))
        
        # Add new range
        intervals.append((start_date, end_date))
        
        # Sort and merge
        intervals.sort(key=lambda x: x[0])
        
        merged = []
        if intervals:
            curr_start, curr_end = intervals[0]
            for next_start, next_end in intervals[1:]:
                if next_start <= curr_end:
                    # Overlap or adjacent, merge
                    curr_end = max(curr_end, next_end)
                else:
                    # No overlap, push current and start new
                    merged.append((curr_start, curr_end))
                    curr_start, curr_end = next_start, next_end
            merged.append((curr_start, curr_end))

        # Serialize back to strings
        new_ranges = [[s.isoformat(), e.isoformat()] for s, e in merged]
        self.state[exchange][instrument_type][symbol][timeframe] = new_ranges
        
        self._save_state()
