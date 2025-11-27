
import sys
import os
from datetime import datetime, timezone

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.state import ExchangeStateManager

def set_bitmex_listing():
    manager = ExchangeStateManager()
    
    # BitMEX Listing Dates
    # XBTUSD: 2016-05-04
    # ETHUSD: 2018-08-02
    
    dates = {
        "BTC/USD:BTC": datetime(2016, 5, 4, tzinfo=timezone.utc),
        "ETH/USD:BTC": datetime(2018, 8, 2, tzinfo=timezone.utc)
    }
    
    print("Setting BitMEX listing dates...")
    for symbol, date in dates.items():
        print(f"  {symbol}: {date}")
        manager.set_listing_date("bitmex", "swap", symbol, date)
        
    print("Done. State updated.")

if __name__ == "__main__":
    set_bitmex_listing()
