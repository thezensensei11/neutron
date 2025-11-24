
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

    # 1. Generate Comprehensive Report
    logger.info("--- Generating Data Quality Report ---")
    try:
        info_service = crawler.get_info_service()
        summary = info_service.generate_summary(deep_scan=True)
        print("\n" + "="*50)
        print(summary)
        print("="*50 + "\n")
        
    except Exception as e:
        logger.error(f"FAILURE: Error generating summary: {e}")

    crawler.close()

if __name__ == "__main__":
    verify_dual_crawler()
