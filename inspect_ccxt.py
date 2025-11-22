import ccxt
import json
from datetime import datetime

def check_market_info():
    # Check Swap
    print("\n--- Checking Swap (Futures) ---")
    exchange_swap = ccxt.binance({'options': {'defaultType': 'swap'}})
    markets_swap = exchange_swap.load_markets()
    symbol_swap = "BTC/USDT:USDT"
    
    if symbol_swap in markets_swap:
        market = markets_swap[symbol_swap]
        info = market.get('info', {})
        print(f"Symbol: {symbol_swap}")
        print(f"Onboard Date (info): {info.get('onboardDate')}")
    
    # Check Probe Approach (Spot)
    print("\n--- Checking Probe Approach (Spot) ---")
    exchange_spot = ccxt.binance()
    # Fetch first candle ever
    ohlcv = exchange_spot.fetch_ohlcv("BTC/USDT", '1d', since=0, limit=1)
    if ohlcv:
        first_candle_ts = ohlcv[0][0]
        first_date = datetime.fromtimestamp(first_candle_ts / 1000)
        print(f"First Candle (BTC/USDT): {first_date}")
    else:
        print("No candles found.")

if __name__ == "__main__":
    check_market_info()
