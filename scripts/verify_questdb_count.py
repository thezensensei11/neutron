import sys
import os
import logging
import pandas as pd

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.storage.questdb import QuestDBStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_counts():
    storage = QuestDBStorage()
    try:
        tables = ['aggTrades', 'bookDepth', 'trades', 'fundingRate']
        for table in tables:
            try:
                df = storage._query_df(f'SELECT count() FROM "{table}"')
                if not df.empty:
                    count = df.iloc[0]['count']
                    logger.info(f"Table {table}: {count} rows")
                else:
                    logger.info(f"Table {table}: 0 rows (or table not found)")
            except Exception as e:
                logger.warning(f"Could not query {table}: {e}")
    finally:
        storage.close()

if __name__ == "__main__":
    verify_counts()
