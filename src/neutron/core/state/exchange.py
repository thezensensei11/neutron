import logging
from datetime import datetime
from typing import Dict, Optional, Any
from .base import StateManager

logger = logging.getLogger(__name__)

class ExchangeStateManager(StateManager):
    """
    Manages exchange metadata state (listing dates, etc).
    Singleton pattern.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            # We need a lock for singleton creation, but StateManager has _lock instance attribute
            # So we use a class level lock here or just rely on module import being thread safe in Python
            # But for safety:
            import threading
            with threading.Lock():
                 if cls._instance is None:
                    cls._instance = super(ExchangeStateManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self, state_file: str = "states/exchange_state.json"):
        if getattr(self, '_initialized', False):
            return
        super().__init__(state_file)
        self._initialized = True

    def get_listing_date(self, exchange_id: str, instrument_type: str, symbol: str) -> Optional[datetime]:
        try:
            state = self.load()
            date_str = state.get(exchange_id, {}).get(instrument_type, {}).get(symbol, {}).get("listing_date")
            if date_str:
                return datetime.fromisoformat(date_str)
        except Exception as e:
            logger.warning(f"Error retrieving listing date from cache: {e}")
        return None

    def set_listing_date(self, exchange_id: str, instrument_type: str, symbol: str, date: datetime):
        def updater(state):
            if exchange_id not in state:
                state[exchange_id] = {}
            if instrument_type not in state[exchange_id]:
                state[exchange_id][instrument_type] = {}
            if symbol not in state[exchange_id][instrument_type]:
                state[exchange_id][instrument_type][symbol] = {}
            
            state[exchange_id][instrument_type][symbol]["listing_date"] = date.isoformat()

        try:
            self.atomic_update(updater)
        except Exception as e:
            logger.error(f"Error caching listing date: {e}")

    def update_market_metadata(self, exchange_id: str, markets: Dict[str, Any]):
        def updater(state):
            if exchange_id not in state:
                state[exchange_id] = {}
            
            if 'metadata' not in state[exchange_id]:
                state[exchange_id]['metadata'] = {}
            
            state[exchange_id]['metadata']['last_updated'] = datetime.now().isoformat()
            state[exchange_id]['metadata']['markets'] = markets

        try:
            self.atomic_update(updater)
        except Exception as e:
            logger.error(f"Error updating market metadata: {e}")
