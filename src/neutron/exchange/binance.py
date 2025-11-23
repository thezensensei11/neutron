import ccxt
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging

from .base import Exchange
from .ccxt_base import CCXTExchange

logger = logging.getLogger(__name__)

class BinanceExchange(CCXTExchange):
    """Binance exchange implementation."""

    def __init__(self, default_type: str = 'spot'):
        super().__init__("binance", default_type=default_type)

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((ccxt.NetworkError, ccxt.RateLimitExceeded))
    )
    def fetch_open_interest_history(
        self, 
        symbol: str, 
        timeframe: str = '1h',
        since: Optional[datetime] = None, 
        limit: Optional[int] = 500
    ) -> List[Dict[str, Any]]:
        """Fetch open interest history."""
        since_ts = int(since.timestamp() * 1000) if since else None
        try:
            # CCXT method for open interest history
            # Note: Binance supports this, but not all exchanges do in the same way
            oi_history = self.client.fetch_open_interest_history(symbol, timeframe, since=since_ts, limit=limit)
            
            data = []
            for entry in oi_history:
                data.append({
                    'time': datetime.fromtimestamp(entry['timestamp'] / 1000, tz=timezone.utc),
                    'symbol': symbol,
                    'exchange': self.exchange_id,
                    'value': float(entry['openInterestAmount']) # or openInterestValue depending on preference
                })
            return data
        except Exception as e:
            logger.error(f"Error fetching open interest for {symbol}: {e}")
            raise

    def get_listing_date(self, symbol: str) -> Optional[datetime]:
        """
        Get the effective listing date (start of data) for a symbol.
        Prioritizes probing the API for the first actual candle.
        Falls back to metadata 'onboardDate' if probing fails.
        """
        try:
            # Strategy 1: Probe first candle (Gold Standard for Data Availability)
            # Fetch 1 candle starting from timestamp 0
            try:
                ohlcv = self.client.fetch_ohlcv(symbol, '1d', since=0, limit=1)
                if ohlcv:
                    return datetime.fromtimestamp(ohlcv[0][0] / 1000, tz=timezone.utc)
            except Exception as e:
                logger.warning(f"Probe failed for {symbol}: {e}")

            # Strategy 2: Check metadata (Fallback)
            # Ensure markets are loaded
            if not self.client.markets:
                self.load_markets()
            
            market = self.client.markets.get(symbol)
            if market:
                info = market.get('info', {})
                onboard_date_ms = info.get('onboardDate')
                if onboard_date_ms:
                    return datetime.fromtimestamp(int(onboard_date_ms) / 1000, tz=timezone.utc)
            
            return None

        except Exception as e:
            logger.error(f"Error determining listing date for {symbol}: {e}")
            return None

    def close(self):
        """Close the client connection if needed (async mostly, but good practice)."""
        # CCXT sync client doesn't strictly need close, but keeping for API consistency
        pass
