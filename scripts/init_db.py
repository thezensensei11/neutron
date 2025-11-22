import sys
import os
from sqlalchemy import text

# Add project root to path to allow imports - not strictly needed with uv run but good for standalone
# sys.path.append(os.path.join(os.path.dirname(__file__), "../../.."))

from neutron.db.session import init_db, engine

def create_hypertables():
    """Convert standard tables to TimescaleDB hypertables."""
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        
        # Convert OHLCV to hypertable
        try:
            print("Converting 'ohlcv' to hypertable...")
            conn.execute(text("SELECT create_hypertable('ohlcv', 'time', if_not_exists => TRUE);"))
            print("Success.")
        except Exception as e:
            print(f"Error converting ohlcv: {e}")

        # Convert Trades to hypertable
        try:
            print("Converting 'trades' to hypertable...")
            conn.execute(text("SELECT create_hypertable('trades', 'time', if_not_exists => TRUE);"))
            print("Success.")
        except Exception as e:
            print(f"Error converting trades: {e}")

        # Convert Funding Rates to hypertable
        try:
            print("Converting 'funding_rates' to hypertable...")
            conn.execute(text("SELECT create_hypertable('funding_rates', 'time', if_not_exists => TRUE);"))
            print("Success.")
        except Exception as e:
            print(f"Error converting funding_rates: {e}")

        # Convert Open Interest to hypertable
        try:
            print("Converting 'open_interest' to hypertable...")
            conn.execute(text("SELECT create_hypertable('open_interest', 'time', if_not_exists => TRUE);"))
            print("Success.")
        except Exception as e:
            print(f"Error converting open_interest: {e}")

if __name__ == "__main__":
    print("Initializing database tables...")
    init_db()
    print("Tables created.")
    
    print("Setting up TimescaleDB hypertables...")
    create_hypertables()
    print("Database setup complete.")
