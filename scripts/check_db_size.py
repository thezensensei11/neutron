import logging
import sys
import os
from sqlalchemy import text

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../src'))

from neutron.db.session import engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_size():
    logger.info("Checking database size...")
    
    with engine.connect() as conn:
        # Check total DB size
        result = conn.execute(text("SELECT pg_size_pretty(pg_database_size(current_database()));")).scalar()
        logger.info(f"Total Database Size: {result}")
        
        # Check top 10 largest relations (tables/indexes)
        logger.info("Top 10 Largest Tables/Indexes:")
        query = text("""
            SELECT
                relname AS "relation",
                pg_size_pretty(pg_total_relation_size(C.oid)) AS "total_size"
            FROM pg_class C
            LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
            WHERE nspname NOT IN ('pg_catalog', 'information_schema')
            AND C.relkind <> 'i'
            AND nspname !~ '^pg_toast'
            ORDER BY pg_total_relation_size(C.oid) DESC
            LIMIT 10;
        """)
        
        rows = conn.execute(query).fetchall()
        for row in rows:
            logger.info(f"{row[0]}: {row[1]}")

if __name__ == "__main__":
    check_size()
