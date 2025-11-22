import ccxt
import pandas as pd
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import logging

from .base import Exchange

logger = logging.getLogger(__name__)

class BinanceExchange(Exchange):
    """Binance exchange implementation using CCXT."""

    def __init__(self, default_type: str = 'spot'):
        super().__init__("binance")
        self.default_type = default_type
        self.client = ccxt.binance({
            'enableRateLimit': True,
            'options': {
                'defaultType': default_type,  # 'spot', 'future', 'swap' (perp)
            }
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
            logger.error(f"Error loading markets: {e}")
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
            logger.error(f"Error fetching OHLCV for {symbol}: {e}")
            raise

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=retry_if_exception_type((ccxt.NetworkError, ccxt.RateLimitExceeded))
    )
    def fetch_funding_rate_history(
        self, 
        symbol: str, 
        since: Optional[datetime] = None, 
        limit: Optional[int] = 1000
    ) -> List[Dict[str, Any]]:
        """Fetch funding rate history."""
        since_ts = int(since.timestamp() * 1000) if since else None
        try:
            # CCXT method for funding rate history
            funding_rates = self.client.fetch_funding_rate_history(symbol, since=since_ts, limit=limit)
            
            data = []
            for rate in funding_rates:
                data.append({
                    'time': datetime.fromtimestamp(rate['timestamp'] / 1000, tz=timezone.utc),
                    'symbol': symbol,
                    'exchange': self.exchange_id,
                    'rate': float(rate['fundingRate']),
                    'mark_price': float(rate['markPrice']) if rate.get('markPrice') is not None else None
                })
            return data
        except Exception as e:
            logger.error(f"Error fetching funding rates for {symbol}: {e}")
            raise

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
