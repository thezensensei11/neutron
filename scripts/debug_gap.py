import ccxt
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_gap(exchange_id, symbol, start_ts_ms, end_ts_ms, timeframe='1m', exchange_type='spot'):
    logger.info(f"--- Checking {exchange_id} ({exchange_type}) {symbol} Gap ---")
    logger.info(f"Range: {datetime.fromtimestamp(start_ts_ms/1000, tz=timezone.utc)} -> {datetime.fromtimestamp(end_ts_ms/1000, tz=timezone.utc)}")
    
    try:
        exchange_class = getattr(ccxt, exchange_id)
        options = {'defaultType': exchange_type}
        if exchange_type == 'swap':
            options['adjustForTimeDifference'] = True
            
        exchange = exchange_class({
            'enableRateLimit': True,
            'options': options
        })
        
        # Calculate expected number of candles
        duration_ms = end_ts_ms - start_ts_ms
        expected_candles = int(duration_ms / 60000)
        
        logger.info(f"Expected duration: {duration_ms/1000}s (~{expected_candles} candles)")
        
        # Fetch with a buffer
        fetch_since = start_ts_ms - 60000 # Start 1 min early
        limit = min(expected_candles + 5, 1000) # Fetch a few more, cap at 1000
        
        candles = exchange.fetch_ohlcv(symbol, timeframe, since=fetch_since, limit=limit)
        
        logger.info(f"Fetched {len(candles)} candles:")
        found_in_gap = 0
        
        for c in candles:
            ts = c[0]
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc)
            
            in_gap = start_ts_ms <= ts < end_ts_ms
            marker = "[IN GAP]" if in_gap else "        "
            if in_gap: found_in_gap += 1
            
            # Print only first/last few if too many
            if len(candles) > 20 and 5 < found_in_gap < len(candles) - 5:
                continue
                
            print(f"{marker} {dt} | Open: {c[1]} | Vol: {c[5]}")
            
        if found_in_gap == 0:
            logger.info("RESULT: CONFIRMED MISSING. No candles found within the gap range.")
        else:
            logger.info(f"RESULT: DATA EXISTS (PARTIAL/FULL). Found {found_in_gap} candles inside the gap.")
            
    except Exception as e:
        logger.error(f"Error: {e}")
    print("\n")

def main():
    # 1. Binance Spot Gap (Long): 2018-02-08 00:28 -> 2018-02-09 10:00 (Maintenance?)
    start = int(datetime(2018, 2, 8, 0, 28, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2018, 2, 9, 10, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    check_gap('binance', 'BTC/USDT', start, end, exchange_type='spot')

    # 2. Binance Swap Gap: 2019-09-08 18:59 -> 19:01 (2 mins)
    start = int(datetime(2019, 9, 8, 18, 59, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2019, 9, 8, 19, 1, 0, tzinfo=timezone.utc).timestamp() * 1000)
    check_gap('binance', 'BTC/USDT:USDT', start, end, exchange_type='swap')

    # 3. Bitfinex Spot Gap: 2013-04-01 00:13 -> 00:26
    start = int(datetime(2013, 4, 1, 0, 13, 0, tzinfo=timezone.utc).timestamp() * 1000)
    end = int(datetime(2013, 4, 1, 0, 26, 0, tzinfo=timezone.utc).timestamp() * 1000)
    check_gap('bitfinex', 'BTC/USD', start, end, exchange_type='spot')

if __name__ == "__main__":
    main()
