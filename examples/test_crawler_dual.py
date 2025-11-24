
import logging
import pandas as pd
from neutron.core.crawler import DataCrawler
from neutron.core.config import NeutronConfig, StorageConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_dual_crawler():
    logger.info("Verifying Dual Storage DataCrawler...")

    # Initialize Crawler with both paths
    # We assume standard paths used in backfill
    crawler = DataCrawler(
        ohlcv_path="data/ohlcv",
        questdb_config={
            'host': 'localhost',
            'ilp_port': 9009,
            'pg_port': 8812
        }
    )

    # 1. Verify OHLCV (Parquet)
    logger.info("--- Testing OHLCV Retrieval (Parquet) ---")
    try:
        df_ohlcv = crawler.get_ohlcv(
            exchange='binance',
            symbol='BTC/USDT',
            timeframe='1m',
            start_date='2025-11-01',
            end_date='2025-11-02',
            instrument_type='spot'
        )
        if not df_ohlcv.empty:
            logger.info(f"SUCCESS: Retrieved {len(df_ohlcv)} OHLCV rows.")
            logger.info(f"Sample: \n{df_ohlcv.head(2)}")
        else:
            logger.warning("FAILURE: OHLCV DataFrame is empty.")
    except Exception as e:
        logger.error(f"FAILURE: Error fetching OHLCV: {e}")

    # 2. Verify Tick Data (QuestDB)
    logger.info("--- Testing Tick Data Retrieval (QuestDB) ---")
    try:
        df_tick = crawler.get_data(
            data_type='aggTrades',
            exchange='binance',
            symbol='BTC/USDT',
            start_date='2025-11-01',
            end_date='2025-11-02',
            instrument_type='spot'
        )
        if not df_tick.empty:
            logger.info(f"SUCCESS: Retrieved {len(df_tick)} Tick rows.")
            logger.info(f"Sample: \n{df_tick.head(2)}")
        else:
            logger.warning("FAILURE: Tick DataFrame is empty.")
    except Exception as e:
        logger.error(f"FAILURE: Error fetching Tick Data: {e}")

    # 3. Verify Info Service (Aggregation)
    logger.info("--- Testing Info Service (Aggregation) ---")
    try:
        info_service = crawler.get_info_service()
        summary = info_service.generate_summary(deep_scan=False)
        logger.info(f"Summary Report:\n{summary}")
        
        if "ohlcv" in summary.lower() and "aggtrades" in summary.lower():
             logger.info("SUCCESS: InfoService reports both OHLCV and Tick data.")
        else:
             logger.warning("FAILURE: InfoService summary missing expected data types.")
             
    except Exception as e:
        logger.error(f"FAILURE: Error generating summary: {e}")

    crawler.close()

if __name__ == "__main__":
    verify_dual_crawler()
