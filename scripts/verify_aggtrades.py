import sys
import os
import logging
import pandas as pd

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.storage.questdb import QuestDBStorage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_aggtrades():
    storage = QuestDBStorage()
    try:
        logger.info("Listing tables...")
        tables_df = storage._query_df("SHOW TABLES")
        if not tables_df.empty:
            tables = tables_df['table_name'].tolist()
            logger.info(f"Tables found: {tables}")
            
            for table in tables:
                if table in ['telemetry', 'telemetry_config']: continue
                try:
                    df = storage._query_df(f'SELECT count() FROM "{table}"')
                    if not df.empty:
                        count_col = 'count' if 'count' in df.columns else 'count()'
                        count = df.iloc[0][count_col]
                        logger.info(f"Table {table}: {count} rows")
                    else:
                        logger.info(f"Table {table}: 0 rows")
                except Exception as e:
                    logger.warning(f"Could not query {table}: {e}")
        else:
            logger.info("No tables found.")
    finally:
        storage.close()

if __name__ == "__main__":
    verify_aggtrades()
