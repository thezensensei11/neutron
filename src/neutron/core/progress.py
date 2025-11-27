import threading
from typing import Dict, Optional
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

class ProgressManager:
    """
    Manages progress bars for concurrent tasks using tqdm.
    """
    def __init__(self):
        self._bars: Dict[str, tqdm] = {}
        self._lock = threading.RLock()
        self._position_counter = 0

    def create_bar(self, task_id: str, desc: str, total: Optional[int] = None, unit: str = "it", leave: bool = True) -> tqdm:
        """
        Create a new progress bar.
        """
        with self._lock:
            if task_id in self._bars:
                return self._bars[task_id]
            
            # Create bar at the next available position
            bar = tqdm(
                total=total,
                desc=desc,
                unit=unit,
                position=self._position_counter,
                leave=leave, # Configurable leave behavior
                dynamic_ncols=True
            )
            self._bars[task_id] = bar
            self._position_counter += 1
            return bar

    def update_bar(self, task_id: str, advance: int = 1, desc: Optional[str] = None):
        """
        Update an existing progress bar.
        """
        with self._lock:
            if task_id in self._bars:
                bar = self._bars[task_id]
                bar.update(advance)
                if desc:
                    bar.set_description(desc)

    def close_bar(self, task_id: str):
        """
        Close and remove a progress bar.
        """
        with self._lock:
            if task_id in self._bars:
                bar = self._bars[task_id]
                bar.close()
                del self._bars[task_id]
                # Note: We don't decrement position_counter to avoid overlapping with completed bars
                # In a long running process, this might drift down, but for backfill batches it's fine.

    def log(self, message: str):
        """
        Log a message to the console without breaking progress bars.
        """
        tqdm.write(message)
