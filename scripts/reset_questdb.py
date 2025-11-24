import sys
import os
import logging

import requests

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.core.storage.questdb import QuestDBStorage
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

QUESTDB_URL = "http://localhost:9000"

def reset_questdb():
    storage = QuestDBStorage()
    
    logger.info("Connecting to QuestDB...")
    try:
        # Get list of tables
        tables_df = storage._query_df("SHOW TABLES")
        
        if not tables_df.empty:
            tables = tables_df['table_name'].tolist()
            logger.info(f"Found {len(tables)} tables: {tables}")
            
            # Drop existing tables using HTTP API
            for table in tables:
                # Skip system tables or tables we don't want to drop
                if table in ['telemetry', 'telemetry_config']:
                    continue
                logger.info(f"Dropping table {table}...")
                try:
                    # Using requests.get for dropping tables as per instruction
                    response = requests.get(f"{QUESTDB_URL}/exec", params={'query': f"DROP TABLE '{table}'"})
                    response.raise_for_status() # Raise an exception for HTTP errors
                    logger.info(f"Dropped {table}")
                except requests.exceptions.RequestException as e:
                    logger.error(f"Failed to drop {table} via HTTP: {e}")
                except Exception as e:
                    logger.error(f"An unexpected error occurred while dropping {table}: {e}")
        else:
            logger.info("No tables found in QuestDB.")
        
        # Re-create aggTrades with DEDUP enabled
        # Keys: symbol, time, agg_trade_id (if available) or just time/symbol/price/qty?
        # Best practice for aggTrades: symbol, time, agg_trade_id
        logger.info("Creating aggTrades table with DEDUP enabled...")
        create_query = """
        CREATE TABLE aggTrades (
            symbol SYMBOL,
            price DOUBLE,
            qty DOUBLE,
            first_trade_id LONG,
            last_trade_id LONG,
            time TIMESTAMP,
            is_buyer_maker BOOLEAN,
            is_best_match BOOLEAN,
            agg_trade_id LONG,
            exchange SYMBOL,
            instrument_type SYMBOL
        ) TIMESTAMP(time) PARTITION BY DAY WAL
        DEDUP UPSERT KEYS(time, symbol, agg_trade_id);
        """
        try:
            response = requests.get(f"{QUESTDB_URL}/exec", params={'query': create_query})
            response.raise_for_status() # Raise an exception for HTTP errors
            logger.info("Created aggTrades table with DEDUP")
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create aggTrades table via HTTP: {e}")
            # Don't raise here, we might want to continue or just log it if it already exists (though we dropped it)
            
        logger.info("QuestDB reset complete.")
        
    except Exception as e:
        logger.error(f"Error resetting QuestDB: {e}")
    finally:
        storage.close()

if __name__ == "__main__":
    reset_questdb()
