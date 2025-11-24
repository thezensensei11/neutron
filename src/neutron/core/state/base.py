import json
import os
import threading
import logging
import fcntl
import uuid
import time
from typing import Dict, Any, Callable
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class StateManager:
    """
    Base class for state management with robust JSON I/O and cross-process locking.
    """
    def __init__(self, state_file: str):
        self.state_file = state_file
        
        # Ensure state directory exists
        state_dir = os.path.dirname(state_file)
        if state_dir:
            os.makedirs(state_dir, exist_ok=True)
        
        # Store lock files in a hidden directory to avoid clutter
        lock_dir = ".neutron/locks"
        os.makedirs(lock_dir, exist_ok=True)
        self.lock_file = os.path.join(lock_dir, f"{os.path.basename(state_file)}.lock")
        
        self._thread_lock = threading.RLock()
        self.state: Dict[str, Any] = {}
        # Ensure lock file exists
        if not os.path.exists(self.lock_file):
            try:
                with open(self.lock_file, 'w') as f:
                    pass
            except Exception:
                pass # Might be created by another process

    @contextmanager
    def _file_lock(self, exclusive: bool = True):
        """Context manager for cross-process file locking."""
        lock_mode = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
        with open(self.lock_file, 'r+') as f:
            try:
                fcntl.flock(f, lock_mode)
                yield
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def _read_from_disk(self) -> Dict[str, Any]:
        """Internal method to read from disk without locking (caller must lock)."""
        if not os.path.exists(self.state_file):
            return {}
            
        for attempt in range(5):
            try:
                with open(self.state_file, 'r') as f:
                    content = f.read()
                    if not content.strip():
                        logger.warning(f"State file {self.state_file} is empty (attempt {attempt+1}). Retrying...")
                        time.sleep(0.2)
                        continue
                    return json.loads(content)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error in {self.state_file} (attempt {attempt+1}): {e}")
                if attempt == 4:
                    backup_name = f"{self.state_file}.corrupt.{int(time.time())}"
                    try:
                        os.replace(self.state_file, backup_name)
                        logger.error(f"Moved corrupted state file to {backup_name}")
                    except Exception:
                        pass
            except Exception as e:
                logger.error(f"Failed to load state file {self.state_file}: {e}")
            
            time.sleep(0.2)
        return {}

    def _write_to_disk(self, state: Dict[str, Any]):
        """Internal method to write to disk without locking (caller must lock)."""
        # Use unique temp file to avoid collisions between processes
        temp_file = f"{self.state_file}.{uuid.uuid4()}.tmp"
        try:
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_file, self.state_file)
        except Exception as e:
            logger.error(f"Failed to save state file {self.state_file}: {e}")
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except:
                    pass

    def load(self) -> Dict[str, Any]:
        """Public load method with shared lock."""
        with self._thread_lock:
            with self._file_lock(exclusive=False):
                self.state = self._read_from_disk()
                return self.state

    def save(self):
        """Public save method with exclusive lock."""
        with self._thread_lock:
            with self._file_lock(exclusive=True):
                self._write_to_disk(self.state)

    def atomic_update(self, update_fn: Callable[[Dict[str, Any]], None]):
        """
        Perform an atomic read-modify-write operation.
        update_fn: A function that takes the state dict and modifies it in place.
        """
        with self._thread_lock:
            with self._file_lock(exclusive=True):
                # Reload latest state
                self.state = self._read_from_disk()
                # Apply update
                update_fn(self.state)
                # Save
                self._write_to_disk(self.state)

