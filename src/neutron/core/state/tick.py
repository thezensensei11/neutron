import logging
from datetime import datetime, timezone
from typing import List, Tuple
from .base import StateManager

logger = logging.getLogger(__name__)

class TickDataStateManager(StateManager):
    """
    Manages the state of downloaded tick/generic data.
    """
    def __init__(self, state_file: str = "tick_data_state.json"):
        super().__init__(state_file)

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
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        state = self.load()
        ranges = state.get(exchange, {}).get(instrument_type, {}).get(symbol, {}).get(data_type, {}).get(timeframe, [])
        
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
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None:
            end_date = end_date.replace(tzinfo=timezone.utc)

        logger.info(f"Updating tick state for {exchange} {instrument_type} {symbol} {data_type} {timeframe}: {start_date} - {end_date}")

        def updater(state):
            if exchange not in state: state[exchange] = {}
            if instrument_type not in state[exchange]: state[exchange][instrument_type] = {}
            if symbol not in state[exchange][instrument_type]: state[exchange][instrument_type][symbol] = {}
            if data_type not in state[exchange][instrument_type][symbol]: state[exchange][instrument_type][symbol][data_type] = {}
            if timeframe not in state[exchange][instrument_type][symbol][data_type]: state[exchange][instrument_type][symbol][data_type][timeframe] = []

            ranges = state[exchange][instrument_type][symbol][data_type][timeframe]
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
            state[exchange][instrument_type][symbol][data_type][timeframe] = new_ranges

        self.atomic_update(updater)
