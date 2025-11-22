import os
import requests
import zipfile
import io
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from ..db.models import Trade
from ..db.session import ScopedSession

logger = logging.getLogger(__name__)

class BinanceVisionDownloader:
    BASE_URL = "https://data.binance.vision/data/spot/daily/trades"

    def __init__(self, download_dir: str = "data/downloads", db: Session = None):
        self.download_dir = download_dir
        self.db = db or ScopedSession()
        os.makedirs(self.download_dir, exist_ok=True)

    def _generate_url(self, symbol: str, date: datetime) -> str:
        """Generate URL for daily trades ZIP."""
        # Symbol format for URL is usually uppercase without slash, e.g., BTCUSDT
        symbol_formatted = symbol.replace("/", "").upper()
        date_str = date.strftime("%Y-%m-%d")
        filename = f"{symbol_formatted}-trades-{date_str}.zip"
        return f"{self.BASE_URL}/{symbol_formatted}/{filename}"

    def download_and_process_day(self, symbol: str, date: datetime):
        """Download, extract, parse, and insert tick data for a single day."""
        url = self._generate_url(symbol, date)
        logger.info(f"Processing {symbol} for {date.date()} from {url}")

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 404:
                logger.warning(f"Data not found for {symbol} on {date.date()}")
                return
            response.raise_for_status()

            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Usually contains one CSV file
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    # Binance Vision Trades CSV columns:
                    # id, price, qty, quote_qty, time, is_buyer_maker, is_best_match
                    df = pd.read_csv(
                        f, 
                        header=None, 
                        names=["trade_id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match"]
                    )
            
            # Transform for DB
            # Detect timestamp unit based on magnitude
            # Current ms timestamp is ~1.7e12
            # If value > 1e14, it's likely microseconds
            sample_ts = df['time'].iloc[0]
            if sample_ts > 1e14:
                logger.info("Detected microsecond timestamps.")
                df['time'] = pd.to_datetime(df['time'], unit='us')
            else:
                df['time'] = pd.to_datetime(df['time'], unit='ms')
            df['symbol'] = symbol
            df['exchange'] = 'binance'
            df['side'] = df['is_buyer_maker'].apply(lambda x: 'sell' if x else 'buy')
            
            # Select and rename columns to match model
            # Model: time, symbol, exchange, trade_id, price, amount, side
            df_db = df[['time', 'symbol', 'exchange', 'trade_id', 'price', 'qty', 'side']].copy()
            df_db.rename(columns={'qty': 'amount'}, inplace=True)
            
            # Convert to list of dicts for bulk insert
            records = df_db.to_dict('records')
            
            if not records:
                logger.info("No records found in CSV.")
                return

            # Bulk Insert
            # Using chunking if data is huge, but daily trades for one symbol might fit in memory
            # For high frequency pairs, chunking is safer
            chunk_size = 10000
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                stmt = insert(Trade).values(chunk)
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=['time', 'symbol', 'exchange', 'trade_id']
                )
                self.db.execute(stmt)
                self.db.commit()
            
            logger.info(f"Inserted {len(records)} trades for {symbol} on {date.date()}")

        except Exception as e:
            logger.error(f"Failed to process {symbol} on {date.date()}: {e}")
            self.db.rollback()
            raise

    def backfill_range(self, symbol: str, start_date: datetime, end_date: datetime):
        """Backfill a range of dates."""
        current_date = start_date
        while current_date <= end_date:
            self.download_and_process_day(symbol, current_date)
            current_date += timedelta(days=1)


