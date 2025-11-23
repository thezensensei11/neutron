import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class ExchangeStateManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ExchangeStateManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if getattr(self, '_initialized', False):
            return
            
        # Use absolute path to ensure file is created in project root
        self.state_file = Path("/Users/arch/Desktop/neutron/exchange_state.json")
        self.state: Dict[str, Any] = {}
        self.file_lock = threading.RLock()
        self._load_state()
        self._initialized = True

    def _load_state(self):
        """Load state from JSON file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    self.state = json.load(f)
            except json.JSONDecodeError:
                logger.warning(f"Corrupted exchange state file {self.state_file}. Starting fresh.")
                self.state = {}
            except Exception as e:
                logger.error(f"Error loading exchange state: {e}")
                self.state = {}
        else:
            self.state = {}

    def _save_state(self):
        """Save state to JSON file."""
        try:
            with self.file_lock:
                with open(self.state_file, 'w') as f:
                    json.dump(self.state, f, indent=2, sort_keys=True)
        except Exception as e:
            logger.error(f"Error saving exchange state: {e}")

    def get_listing_date(self, exchange_id: str, instrument_type: str, symbol: str) -> Optional[datetime]:
        """Retrieve cached listing date."""
        try:
            date_str = self.state.get(exchange_id, {}).get(instrument_type, {}).get(symbol, {}).get("listing_date")
            if date_str:
                return datetime.fromisoformat(date_str)
        except Exception as e:
            logger.warning(f"Error retrieving listing date from cache: {e}")
        return None

    def set_listing_date(self, exchange_id: str, instrument_type: str, symbol: str, date: datetime):
        """Cache listing date."""
        try:
            with self.file_lock:
                if exchange_id not in self.state:
                    self.state[exchange_id] = {}
                if instrument_type not in self.state[exchange_id]:
                    self.state[exchange_id][instrument_type] = {}
                if symbol not in self.state[exchange_id][instrument_type]:
                    self.state[exchange_id][instrument_type][symbol] = {}
                
                self.state[exchange_id][instrument_type][symbol]["listing_date"] = date.isoformat()
                self._save_state()
        except Exception as e:
            logger.error(f"Error caching listing date: {e}")
    def update_market_metadata(self, exchange_id: str, markets: Dict[str, Any]):
        """Update exchange metadata with full market info."""
        try:
            with self.file_lock:
                if exchange_id not in self.state:
                    self.state[exchange_id] = {}
                
                # Store under 'metadata' key
                if 'metadata' not in self.state[exchange_id]:
                    self.state[exchange_id]['metadata'] = {}
                
                # We might want to store a timestamp of when this was updated
                self.state[exchange_id]['metadata']['last_updated'] = datetime.now().isoformat()
                self.state[exchange_id]['metadata']['markets'] = markets
                
                self._save_state()
        except Exception as e:
            logger.error(f"Error updating market metadata: {e}")
