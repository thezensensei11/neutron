import logging
import sys
import os
import argparse

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.crawler import DataCrawler
from neutron.core.downloader import Downloader
from neutron.services.gap_fill_service import GapFillService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="Neutron Gap Filler")
    parser.add_argument("config_path", help="Path to the downloader config file")
    parser.add_argument("--mode", choices=['smart', 'zero_fill'], default='smart', help="Gap fill mode: 'smart' (try download first) or 'zero_fill' (force zero-fill)")
    args = parser.parse_args()

    logger.info(f"Initializing Gap Filler (Mode: {args.mode})...")
    
    # 1. Initialize Downloader (for config, storage, state manager)
    downloader = Downloader(args.config_path)
    
    # 2. Initialize Crawler (for InfoService)
    # We reuse the storage from downloader to ensure consistency
    crawler = DataCrawler(storage_type='database') 
    # Hack: Inject the configured storage from downloader into crawler if needed
    # But DataCrawler initializes its own storage. 
    # Ideally we should pass the storage instance.
    # Let's just use the downloader's storage directly for InfoService
    
    from neutron.services.info_service import InfoService
    info_service = InfoService(downloader.storage)
    
    # 3. Get Gap Report
    logger.info("Scanning for gaps (Deep Scan)...")
    gaps = info_service.get_gap_report(deep_scan=True)
    
    if not gaps:
        logger.info("No gaps found! Data is complete.")
        return
        
    logger.info(f"Found {len(gaps)} gaps.")
    
    # 4. Initialize Gap Fill Service
    gap_filler = GapFillService(downloader)
    
    # 5. Execute Repair
    stats = gap_filler.fill_gaps(gaps, mode=args.mode)
    
    logger.info("==========================================")
    logger.info("Gap Fill Summary")
    logger.info(f"Total Gaps: {stats['total']}")
    logger.info(f"Filled: {stats['filled']}")
    logger.info(f"Failed: {stats['failed']}")
    logger.info("==========================================")

if __name__ == "__main__":
    main()
