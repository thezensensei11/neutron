
import logging
import pandas as pd
from neutron.core.questdb_storage import QuestDBStorage
from neutron.core.config import StorageConfig

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_data():
    logger.info("Verifying data in QuestDB...")

    # Initialize Storage
    storage = QuestDBStorage(
        host='localhost',
        ilp_port=9009,
        pg_port=8812
    )

    # Define checks
    checks = [
        {'table': 'aggTrades', 'symbol': 'BTC/USDT', 'start': '2025-11-01', 'end': '2025-11-05'},
        {'table': 'bookDepth', 'symbol': 'BTC/USDT', 'start': '2025-11-01', 'end': '2025-11-05'}
    ]

    for check in checks:
        table = check['table']
        symbol = check['symbol']
        
        try:
            # Check total count
            query_count = f"SELECT count() FROM {table}"
            df_count = storage._query_df(query_count)
            
            if not df_count.empty:
                count = df_count.iloc[0, 0]
                logger.info(f"Table '{table}' total rows: {count}")
            else:
                logger.warning(f"Table '{table}' does not exist or is empty.")
                continue

            # Check specific date range and symbol
            # Note: Symbol format in DB might differ (e.g. BTC/USDT vs BTCUSDT)
            # Let's check what symbols are there
            query_symbols = f"SELECT distinct symbol FROM {table}"
            df_symbols = storage._query_df(query_symbols)
            logger.info(f"Symbols in '{table}': {df_symbols['symbol'].tolist() if not df_symbols.empty else 'None'}")

            # Check count for specific range
            # QuestDB uses 'timestamp' column usually
            query_range = f"""
                SELECT count() FROM {table} 
                WHERE timestamp >= '{check['start']}' AND timestamp < '{check['end']}'
            """
            df_range = storage._query_df(query_range)
            if not df_range.empty:
                range_count = df_range.iloc[0, 0]
                logger.info(f"Rows for {check['start']} to {check['end']} in '{table}': {range_count}")
            else:
                logger.info(f"No rows found for {check['start']} to {check['end']} in '{table}'")

        except Exception as e:
            logger.error(f"Error querying table '{table}': {e}")

    storage.close()

if __name__ == "__main__":
    verify_data()
