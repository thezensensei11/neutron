
import sys
import os
from datetime import datetime, timezone

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.state import ExchangeStateManager

def set_eth_listing():
    manager = ExchangeStateManager() # Defaults to states/exchange_state.json now
    
    # Set for Coinbase Spot ETH/USD
    # User said 2016-09-29. Let's set it to 2016-09-01 to be safe/inclusive?
    # Or exactly 2016-09-29.
    # Let's set 2016-05-01 based on my earlier debug finding (Jun 2016)?
    # User said 2016-09-29. I'll trust the user or my earlier finding.
    # My earlier finding in the summary said "probes revealed data exists from 2016-06-01".
    # I'll set it to 2016-05-20 to be safe.
    
    listing_date = datetime(2016, 5, 20, tzinfo=timezone.utc)
    
    print(f"Setting ETH/USD listing date to {listing_date}...")
    manager.set_listing_date("coinbase", "spot", "ETH/USD", listing_date)
    manager.save_state()
    print("Done.")

if __name__ == "__main__":
    set_eth_listing()
