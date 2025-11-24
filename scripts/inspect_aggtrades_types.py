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

def inspect_aggtrades():
    downloader = BinanceVisionDownloader()
    symbol = "BTC/USDT"
    date = datetime(2025, 11, 1)
    
    logger.info(f"Downloading aggTrades for {symbol} on {date.date()}...")
    data = downloader.download_daily_data(symbol, date, "aggTrades", "swap")
    
    if not data:
        logger.error("No data found.")
        return

    logger.info(f"Downloaded {len(data)} records.")
    if len(data) > 0:
        sample = data[0]
        logger.info(f"Sample keys: {sample.keys()}")
        for k, v in sample.items():
            logger.info(f"Key: {k}, Type: {type(v)}, Value: {v}")
            
        # Check specifically for boolean-like fields
        bool_fields = ['is_buyer_maker', 'is_best_match']
        for field in bool_fields:
            if field in sample:
                logger.info(f"Field {field} type: {type(sample[field])}")

if __name__ == "__main__":
    inspect_aggtrades()
