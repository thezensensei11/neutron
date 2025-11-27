import logging
import ccxt
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_gap():
    exchange = ccxt.coinbase({
        'enableRateLimit': True,
        'options': {'defaultType': 'spot'}
    })
    
    # The time where it stops
    stop_time = datetime.fromisoformat("2017-02-28T19:42:00+00:00")
    
    assets = ["BTC/USD", "ETH/USD"]
    
    for symbol in assets:
        logger.info(f"\n--- Checking {symbol} ---")
        
        # Check day by day from Feb 28
        check_times = []
        curr = stop_time
        for _ in range(7):
            check_times.append(curr)
            curr += timedelta(days=1)
        
        for t in check_times:
            ts = int(t.timestamp() * 1000)
            try:
                candles = exchange.fetch_ohlcv(symbol, '1m', since=ts, limit=5)
                if candles:
                    first_candle_time = datetime.fromtimestamp(candles[0][0]/1000, tz=timezone.utc)
                    logger.info(f"Time {t}: FOUND data. First candle: {first_candle_time}")
                else:
                    logger.info(f"Time {t}: NO data returned.")
            except Exception as e:
                logger.error(f"Time {t}: Error - {e}")

if __name__ == "__main__":
    check_gap()
