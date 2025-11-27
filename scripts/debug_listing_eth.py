
import logging
import sys
import os
from datetime import datetime, timezone

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.exchange.coinbase import CoinbaseExchange

# Configure logging to stdout
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def test_listing_date():
    exchange = CoinbaseExchange()
    symbol = "ETH/USD"
    
    print(f"Testing listing date for {symbol}...")
    
    # Force bypass cache if possible, or just see what it returns
    # The get_listing_date method uses the state manager, which loads from file.
    # We want to test the *probing* logic, which is inside `_find_first_trade_timestamp` or similar if not cached.
    # But `get_listing_date` wraps it.
    
    # Let's call the internal probing method directly if we can, or just clear the cache first.
    # Actually, let's just call get_listing_date and see logs.
    
    # Clear cache for ETH/USD to force re-probing
    from neutron.core.state import ExchangeStateManager
    state_manager = ExchangeStateManager()
    # Access internal state directly to delete (hacky but effective for debug)
    if "coinbase" in state_manager.state and "spot" in state_manager.state["coinbase"]:
        if symbol in state_manager.state["coinbase"]["spot"]:
            del state_manager.state["coinbase"]["spot"][symbol]
            state_manager.save_state()
            print("Cleared cache for ETH/USD")
    
    listing_date = exchange.get_listing_date(symbol)
    print(f"Result: {listing_date}")

if __name__ == "__main__":
    test_listing_date()
