import logging
from neutron.db.session import ScopedSession
from neutron.db.models import (
    OHLCV, Trade, FundingRate, OpenInterest, AggTrade, BookTicker, 
    LiquidationSnapshot, BookDepth, IndexPriceKline, MarkPriceKline, 
    PremiumIndexKline
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def reset_db():
    logger.info("Resetting database tables (OHLCV, Trades, FundingRate, OpenInterest)...")
    with ScopedSession() as db:
        try:
            # Using TRUNCATE for speed on Postgres, or delete for generic
            # SQLAlchemy doesn't have a generic truncate, so we use delete or raw sql
            # For safety and compatibility, we'll use delete()
            
            db.query(OHLCV).delete()
            db.query(Trade).delete()
            db.query(FundingRate).delete()
            db.query(OpenInterest).delete()
            db.query(AggTrade).delete()
            db.query(BookTicker).delete()
            db.query(LiquidationSnapshot).delete()
            db.query(BookDepth).delete()
            db.query(IndexPriceKline).delete()
            db.query(MarkPriceKline).delete()
            db.query(PremiumIndexKline).delete()
            
            db.commit()
            logger.info("Database reset complete.")
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to reset database: {e}")

if __name__ == "__main__":
    reset_db()
