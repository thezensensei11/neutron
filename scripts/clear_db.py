from neutron.db.session import ScopedSession
from sqlalchemy import text

def clear_db():
    with ScopedSession() as db:
        print("Clearing tables...")
        db.execute(text("TRUNCATE TABLE ohlcv, funding_rates, open_interest, trades CASCADE"))
        db.commit()
        print("Tables cleared.")

if __name__ == "__main__":
    clear_db()
