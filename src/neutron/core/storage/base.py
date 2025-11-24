from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field

@dataclass
class DataQualityReport:
    exchange: str
    symbol: str
    instrument_type: str
    data_type: str
    timeframe: str = None
    start_date: datetime = None
    end_date: datetime = None
    total_rows: int = 0
    expected_rows: int = 0
    missing_rows: int = 0
    quality_score: float = 0.0
    alignment_score: float = 0.0 # % of candles where Open[t] == Close[t-1]
    anomalies: int = 0 # Count of suspicious data points (e.g. price < 0)
    gaps: List[Tuple[datetime, datetime, timedelta]] = field(default_factory=list) # (start, end, duration)
    
    # Rich Analytics
    volume_stats: Dict[str, float] = field(default_factory=dict) # total, avg, min, max
    price_stats: Dict[str, float] = field(default_factory=dict) # min, max, avg
    trade_stats: Dict[str, float] = field(default_factory=dict) # count, avg_size, buy_sell_ratio
    details: Dict[str, Any] = field(default_factory=dict) # Flexible storage for extra analytics
    
    # Granular Metrics
    schema_info: Dict[str, str] = field(default_factory=dict) # column_name -> data_type
    daily_stats: List[Dict[str, Any]] = field(default_factory=list) # [{date, count, min_time, max_time}, ...]

class StorageBackend(ABC):
    @abstractmethod
    def save_ohlcv(self, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_tick_data(self, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_funding_rates(self, data: List[Dict[str, Any]]):
        pass

    @abstractmethod
    def save_generic_data(self, data: List[Dict[str, Any]], data_type: str):
        pass

    @abstractmethod
    def list_available_data(self, deep_scan: bool = False) -> List[Dict[str, Any]]:
        pass

    @abstractmethod
    def analyze_ohlcv_quality(self, exchange: str, symbol: str, timeframe: str, instrument_type: str = 'spot') -> DataQualityReport:
        """
        Perform a deep quality analysis of OHLCV data.
        Checks for gaps, alignment (Open[t] == Close[t-1]), and anomalies.
        """
        pass

    @abstractmethod
    def analyze_tick_quality(self, exchange: str, symbol: str, instrument_type: str = 'spot', data_type: str = 'tick') -> DataQualityReport:
        """
        Perform a deep quality analysis of Tick data.
        Checks for large time gaps.
        """
        pass

    def close(self):
        """Optional close method for cleanup."""
        pass
