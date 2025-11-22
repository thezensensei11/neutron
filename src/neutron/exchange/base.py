from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from datetime import datetime
import pandas as pd

class Exchange(ABC):
    """Abstract base class for all exchanges."""

    def __init__(self, exchange_id: str):
        self.exchange_id = exchange_id

    @abstractmethod
    def load_markets(self) -> Dict[str, Any]:
        """Load and return market metadata."""
        pass

    @abstractmethod
    def fetch_ohlcv(
        self, 
        symbol: str, 
        timeframe: str = '1m', 
        since: Optional[datetime] = None, 
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch OHLCV data."""
        pass

    @abstractmethod
    def get_rate_limit(self) -> int:
        """Return rate limit in milliseconds."""
        pass
