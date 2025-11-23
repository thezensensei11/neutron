import logging
import sys
import os
from sqlalchemy import text

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.db.session import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def add_is_interpolated_column():
    logger.info("Adding 'is_interpolated' column to OHLCV table...")
    
    with engine.connect() as connection:
        connection = connection.execution_options(isolation_level="AUTOCOMMIT")
        try:
            # Check if column exists
            result = connection.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_name='ohlcv' AND column_name='is_interpolated'"
            ))
            if result.fetchone():
                logger.info("Column 'is_interpolated' already exists.")
                return

            # Add column
            connection.execute(text("ALTER TABLE ohlcv ADD COLUMN is_interpolated BOOLEAN DEFAULT FALSE"))
            logger.info("Successfully added 'is_interpolated' column.")
            
        except Exception as e:
            logger.error(f"Failed to add column: {e}")

if __name__ == "__main__":
    add_is_interpolated_column()
