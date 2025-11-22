import logging
import sys
import os
from datetime import datetime, timedelta
from sqlalchemy import text

# Add project root to path
# sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from neutron.db.session import ScopedSession, engine
from neutron.services.metadata_sync import MetadataService
from neutron.services.ohlcv_backfill import OHLCVBackfillService
from neutron.data_source.binance_vision import BinanceVisionDownloader
from neutron.db.models import Symbol, OHLCV, Trade

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def count_rows(table_model):
    with ScopedSession() as db:
        return db.query(table_model).count()

def main():
    logger.info("Starting Integration Test...")

    # 1. Check Database Connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful.")
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("Please ensure Docker is running: docker-compose up -d")
        return

    # 2. Test Metadata Sync
    logger.info("\n--- Testing Metadata Sync ---")
    try:
        service = MetadataService()
        service.sync_metadata()
        symbol_count = count_rows(Symbol)
        logger.info(f"✅ Metadata Sync successful. Total Symbols: {symbol_count}")
    except Exception as e:
        logger.error(f"❌ Metadata Sync failed: {e}")

    # 3. Test OHLCV Backfill
    logger.info("\n--- Testing OHLCV Backfill (BTC/USDT, 1h, 2 days) ---")
    try:
        backfill_service = OHLCVBackfillService()
        start_date = datetime.now() - timedelta(days=2)
        backfill_service.backfill_symbol("BTC/USDT", "1h", start_date)
        
        ohlcv_count = count_rows(OHLCV)
        logger.info(f"✅ OHLCV Backfill successful. Total OHLCV records: {ohlcv_count}")
    except Exception as e:
        logger.error(f"❌ OHLCV Backfill failed: {e}")

    # 4. Test Tick Data Download (Binance Vision)
    logger.info("\n--- Testing Tick Data Download (BTC/USDT, Yesterday) ---")
    try:
        downloader = BinanceVisionDownloader()
        # Use a date that definitely has data (e.g., 2 days ago to be safe from timezone issues)
        target_date = datetime.now() - timedelta(days=2)
        downloader.download_and_process_day("BTC/USDT", target_date)
        
        trade_count = count_rows(Trade)
        logger.info(f"✅ Tick Data Download successful. Total Trades: {trade_count}")
    except Exception as e:
        logger.error(f"❌ Tick Data Download failed: {e}")

    logger.info("\nIntegration Test Complete.")

if __name__ == "__main__":
    main()
