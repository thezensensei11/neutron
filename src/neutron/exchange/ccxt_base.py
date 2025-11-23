import ccxt
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from .base import Exchange

logger = logging.getLogger(__name__)

class CCXTExchange(Exchange):
    """
    Base class for all CCXT-based exchanges.
    Implements common functionality for fetching data, handling rate limits, and error handling.
    """

    def __init__(self, exchange_id: str, default_type: str = 'spot', config: Dict[str, Any] = None, supports_fetch_since_0: bool = True):
        super().__init__(exchange_id)
        self.default_type = default_type
        self.config = config or {}
        self.supports_fetch_since_0 = supports_fetch_since_0
        
        # Initialize CCXT client
        exchange_class = getattr(ccxt, exchange_id)
        self.client = exchange_class({
            'enableRateLimit': True,
            'options': {'defaultType': default_type},
            **self.config
        })

    def get_rate_limit(self) -> int:
        return self.client.rateLimit

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((ccxt.NetworkError, ccxt.RateLimitExceeded))
    )
    def load_markets(self) -> Dict[str, Any]:
        """Load markets with retry logic."""
        try:
            return self.client.load_markets()
        except Exception as e:
            logger.error(f"[{self.exchange_id}] Error loading markets: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((ccxt.NetworkError, ccxt.RateLimitExceeded))
    )
    def fetch_ohlcv(
        self, 
        symbol: str, 
        timeframe: str = '1m', 
        since: Optional[datetime] = None, 
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """
        Fetch OHLCV data with robust error handling and retries.
        Returns a list of dictionaries with keys: time, open, high, low, close, volume.
        """
        since_ts = int(since.timestamp() * 1000) if since else None
        
        try:
            ohlcv = self.client.fetch_ohlcv(symbol, timeframe, since=since_ts, limit=limit)
            
            # Parse into list of dicts
            data = []
            for candle in ohlcv:
                # CCXT returns [timestamp, open, high, low, close, volume]
                data.append({
                    'time': datetime.fromtimestamp(candle[0] / 1000, tz=timezone.utc),
                    'symbol': symbol,
                    'exchange': self.exchange_id,
                    'timeframe': timeframe,
                    'open': float(candle[1]),
                    'high': float(candle[2]),
                    'low': float(candle[3]),
                    'close': float(candle[4]),
                    'volume': float(candle[5])
                })
            return data
            
        except Exception as e:
            logger.error(f"[{self.exchange_id}] Error fetching OHLCV for {symbol}: {e}")
            raise

    def get_listing_date(self, symbol: str) -> Optional[datetime]:
        """
        Determine the listing date of a symbol.
        Strategies:
        1. Check cache (ExchangeStateManager).
        2. Probe fetch_ohlcv with since=0 (if supported).
        3. Probe yearly from 2011 to present.
        4. Metadata (if available).
        """
        from ..core.exchange_state import ExchangeStateManager
        state_manager = ExchangeStateManager()
        
        # Strategy 1: Check cache
        cached_date = state_manager.get_listing_date(self.exchange_id, self.default_type, symbol)
        if cached_date:
            logger.info(f"[{self.exchange_id}] Found listing date for {symbol} in cache: {cached_date}")
            return cached_date

        # Strategy 2: Probe from timestamp 0
        if self.supports_fetch_since_0:
            try:
                ohlcv = self.client.fetch_ohlcv(symbol, '1d', since=0, limit=1)
                if ohlcv:
                    found_date = datetime.fromtimestamp(ohlcv[0][0] / 1000, tz=timezone.utc)
                    logger.info(f"[{self.exchange_id}] Found listing date for {symbol} at {found_date} (probed 0)")
                    state_manager.set_listing_date(self.exchange_id, self.default_type, symbol, found_date)
                    return found_date
            except Exception as e:
                pass # Expected for some exchanges

        # Strategy 3: Probe yearly from 2011 to present
        current_year = datetime.now().year
        for year in range(2011, current_year + 1):
            try:
                logger.info(f"[{self.exchange_id}] Probing listing date for {symbol} at year {year}...")
                since_ts = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
                ohlcv = self.client.fetch_ohlcv(symbol, '1d', since=since_ts, limit=1)
                if ohlcv:
                    found_date = datetime.fromtimestamp(ohlcv[0][0] / 1000, tz=timezone.utc)
                    
                    # Validate returned date is close to requested year
                    # Some exchanges return the LATEST candle if 'since' is too old
                    if found_date.year - year > 1:
                        # logger.debug(f"[{self.exchange_id}] Ignoring candle at {found_date} for probe year {year} (too recent)")
                        continue

                    logger.info(f"[{self.exchange_id}] Found listing date for {symbol} at {found_date} (probed {year})")
                    state_manager.set_listing_date(self.exchange_id, self.default_type, symbol, found_date)
                    return found_date
            except Exception as e:
                # logger.debug(f"[{self.exchange_id}] Probe failed for {symbol} at year {year}: {e}")
                continue
        
        logger.warning(f"[{self.exchange_id}] Could not determine listing date for {symbol}")
        return None
