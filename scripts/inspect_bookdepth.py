import sys
import os
import logging
from datetime import datetime
import pandas as pd

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.data_source.binance_vision import BinanceVisionDownloader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inspect_bookdepth():
    downloader = BinanceVisionDownloader()
    symbol = "BTC/USDT"
    date = datetime(2025, 11, 1) # Use a date we know exists from previous logs
    
    logger.info(f"Downloading bookDepth for {symbol} on {date.date()}...")
    data = downloader.download_daily_data(symbol, date, "bookDepth", "swap")
    
    if not data:
        logger.error("No data found.")
        return

    logger.info(f"Downloaded {len(data)} records.")
    if len(data) > 0:
        sample = data[0]
        logger.info(f"Sample keys: {sample.keys()}")
        for k, v in sample.items():
            val_str = str(v)
            logger.info(f"Key: {k}, Type: {type(v)}, Length: {len(val_str)}")
            if len(val_str) > 100:
                logger.info(f"Value (truncated): {val_str[:100]}...")
            else:
                logger.info(f"Value: {val_str}")
                
if __name__ == "__main__":
    inspect_bookdepth()
