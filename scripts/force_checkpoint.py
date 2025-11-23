import logging
import sys
import os
from sqlalchemy import text

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.db.session import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def force_checkpoint():
    logger.info("Forcing PostgreSQL CHECKPOINT...")
    
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        try:
            connection.execute(text("CHECKPOINT"))
            logger.info("CHECKPOINT complete. WAL logs should be recycled.")
        except Exception as e:
            logger.error(f"Failed to checkpoint: {e}")

if __name__ == "__main__":
    force_checkpoint()
