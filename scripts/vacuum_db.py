import logging
import sys
import os
from sqlalchemy import text

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.db.session import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def vacuum_db():
    logger.info("Starting VACUUM FULL to reclaim disk space...")
    
    # VACUUM cannot run inside a transaction block
    # We need to get a raw connection and set autocommit
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        try:
            connection.execute(text("VACUUM FULL"))
            logger.info("VACUUM FULL complete. Disk space should be reclaimed.")
        except Exception as e:
            logger.error(f"Failed to vacuum database: {e}")

if __name__ == "__main__":
    vacuum_db()
