import logging
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy.dialects.postgresql import insert

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from neutron.exchange.binance import BinanceExchange
from neutron.db.session import ScopedSession
from neutron.db.models import FundingRate, OpenInterest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_perps():
    logger.info("Starting Perpetual Data Verification...")
    
    # Initialize Exchange for Futures (Swap)
    exchange = BinanceExchange(default_type='swap')
    db = ScopedSession()
    
    # For Binance Swaps (Linear Perps), CCXT uses 'BTC/USDT:USDT'
    symbol = "BTC/USDT:USDT"
    start_date = datetime.now() - timedelta(days=2)
    
    # 1. Test Funding Rates
    logger.info(f"\n--- Fetching Funding Rates for {symbol} ---")
    try:
        rates = exchange.fetch_funding_rate_history(symbol, since=start_date)
        logger.info(f"Fetched {len(rates)} funding rate records.")
        if rates:
            logger.info(f"Sample: {rates[0]}")
            
            # Insert into DB
            stmt = insert(FundingRate).values(rates)
            stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange'])
            db.execute(stmt)
            db.commit()
            logger.info("✅ Funding Rates inserted successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch/insert funding rates: {e}")

    # 2. Test Open Interest
    logger.info(f"\n--- Fetching Open Interest for {symbol} ---")
    try:
        oi = exchange.fetch_open_interest_history(symbol, timeframe='1h', since=start_date)
        logger.info(f"Fetched {len(oi)} open interest records.")
        if oi:
            logger.info(f"Sample: {oi[0]}")
            
            # Insert into DB
            stmt = insert(OpenInterest).values(oi)
            stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange'])
            db.execute(stmt)
            db.commit()
            logger.info("✅ Open Interest inserted successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to fetch/insert open interest: {e}")

    db.close()
    logger.info("\nPerpetual Verification Complete.")

if __name__ == "__main__":
    verify_perps()
