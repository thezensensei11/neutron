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
import numpy as np

logger = logging.getLogger(__name__)

class BinanceVisionDownloader:
    BASE_URL_SPOT = "https://data.binance.vision/data/spot/daily"
    BASE_URL_FUTURES_UM = "https://data.binance.vision/data/futures/um/daily"
    BASE_URL_FUTURES_CM = "https://data.binance.vision/data/futures/cm/daily"

    # Map data types to URL path segments and CSV headers
    DATA_TYPES = {
        "trades": {
            "path": "trades",
            "headers": ["trade_id", "price", "qty", "quote_qty", "time", "is_buyer_maker", "is_best_match"],
            "time_col": "time"
        },
        "aggTrades": {
            "path": "aggTrades",
            "headers": ["agg_trade_id", "price", "qty", "first_trade_id", "last_trade_id", "time", "is_buyer_maker", "is_best_match"],
            "time_col": "time"
        },
        "bookTicker": {
            "path": "bookTicker",
            "headers": ["update_id", "best_bid_price", "best_bid_qty", "best_ask_price", "best_ask_qty", "time", "symbol"], # Note: symbol might not be in CSV depending on file
            # Actually bookTicker CSV usually: updateId, bestBidPrice, bestBidQty, bestAskPrice, bestAskQty, transactionTime, symbol
            # Let's verify standard headers. 
            # For now using standard assumption.
            "time_col": "time"
        },
        "liquidationSnapshot": {
            "path": "liquidationSnapshot",
            "headers": ["time", "side", "order_type", "time_in_force", "original_quantity", "price", "average_price", "order_status", "last_fill_quantity", "accumulated_fill_quantity"],
            "time_col": "time"
        },
        "metrics": {
            "path": "metrics",
            "headers": ["create_time", "symbol", "sum_open_interest", "sum_open_interest_value", "count_top_trader_long_short_ratio", "sum_top_trader_long_short_ratio", "count_long_short_ratio", "sum_long_short_ratio"],
            "time_col": "create_time"
        },
        "fundingRate": {
            "path": "fundingRate",
            "headers": ["calc_time", "symbol", "last_funding_rate"], # Standard headers
            "time_col": "calc_time"
        },
        "markPriceKlines": {
            "path": "markPriceKlines",
            "headers": ["open_time", "open", "high", "low", "close", "ignore_1", "ignore_2", "ignore_3", "count", "ignore_4", "ignore_5", "ignore_6"],
            "time_col": "open_time"
        },
        "indexPriceKlines": {
            "path": "indexPriceKlines",
            "headers": ["open_time", "open", "high", "low", "close", "ignore_1", "ignore_2", "ignore_3", "count", "ignore_4", "ignore_5", "ignore_6"],
            "time_col": "open_time"
        },
        "premiumIndexKlines": {
            "path": "premiumIndexKlines",
            # Note: 'ignore' columns are renamed to ensure uniqueness for pandas DataFrame creation
            "headers": ["open_time", "open", "high", "low", "close", "ignore_1", "ignore_2", "ignore_3", "count", "ignore_4", "ignore_5", "ignore_6"],
            "time_col": "open_time"
        }
    }

    # Define availability based on instrument type and frequency (daily/monthly)
    # Currently we only support daily downloads in this class
    AVAILABILITY_MAP = {
        "spot": {
            "daily": {"aggTrades", "klines", "trades"},
            "monthly": {"aggTrades", "klines", "trades"}
        },
        "swap": { # USDT-M Futures (UM)
            "daily": {"aggTrades", "bookDepth", "bookTicker", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades"},
            "monthly": {"aggTrades", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "premiumIndexKlines", "trades"}
        },
        "future": { # COIN-M Futures (CM)
            # Assuming similar to UM for now, but can be refined
            "daily": {"aggTrades", "bookDepth", "bookTicker", "indexPriceKlines", "klines", "markPriceKlines", "metrics", "premiumIndexKlines", "trades", "liquidationSnapshot"},
            "monthly": {"aggTrades", "bookTicker", "fundingRate", "indexPriceKlines", "klines", "markPriceKlines", "premiumIndexKlines", "trades"}
        }
    }

    def __init__(self, download_dir: str = "data/downloads"):
        self.download_dir = download_dir
        os.makedirs(self.download_dir, exist_ok=True)

    def _get_base_url(self, instrument_type: str) -> str:
        if instrument_type == "spot":
            return self.BASE_URL_SPOT
        elif instrument_type == "swap": # USDT-M Futures
            return self.BASE_URL_FUTURES_UM
        elif instrument_type == "future": # COIN-M Futures
            return self.BASE_URL_FUTURES_CM
        else:
            return self.BASE_URL_SPOT

    def _generate_url(self, symbol: str, date: datetime, data_type: str, instrument_type: str, timeframe: str = None) -> str:
        """Generate URL for daily data ZIP."""
        symbol_formatted = symbol.replace("/", "").upper()
        if instrument_type == "swap" and symbol_formatted.endswith("USDT"):
             # For swap, symbol usually doesn't have slash in URL, e.g. BTCUSDT
             pass
        
        base_url = self._get_base_url(instrument_type)
        path_segment = self.DATA_TYPES.get(data_type, {}).get("path", data_type)
        
        date_str = date.strftime("%Y-%m-%d")
        
        # Handle Klines which have timeframe in path
        kline_types = {"klines", "markPriceKlines", "indexPriceKlines", "premiumIndexKlines"}
        if data_type in kline_types and timeframe:
            # e.g. klines/BTCUSDT/1m/BTCUSDT-1m-2023-01-01.zip
            filename = f"{symbol_formatted}-{timeframe}-{date_str}.zip"
            return f"{base_url}/{path_segment}/{symbol_formatted}/{timeframe}/{filename}"
        else:
            filename = f"{symbol_formatted}-{path_segment}-{date_str}.zip"
            return f"{base_url}/{path_segment}/{symbol_formatted}/{filename}"

    def download_daily_data(self, symbol: str, date: datetime, data_type: str = "trades", instrument_type: str = "spot", timeframe: str = None) -> Optional[List[dict]]:
        """Download and parse daily data file."""
        # Validate availability
        available_types = self.AVAILABILITY_MAP.get(instrument_type, {}).get("daily", set())
        if data_type not in available_types:
            logger.warning(f"Data type '{data_type}' is not available for {instrument_type} (daily). Skipping download.")
            return None

        url = self._generate_url(symbol, date, data_type, instrument_type, timeframe)
        logger.info(f"Processing {symbol} {data_type} for {date.date()} from {url}")

        try:
            response = requests.get(url, stream=True)
            if response.status_code == 404:
                logger.warning(f"Data not found for {symbol} {data_type} on {date.date()}")
                return None
            response.raise_for_status()

            config = self.DATA_TYPES.get(data_type)
            if not config:
                logger.warning(f"Unknown data type: {data_type}, trying default parsing")
                # Fallback or error?
                
            headers = config.get("headers") if config else None
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                csv_filename = z.namelist()[0]
                with z.open(csv_filename) as f:
                    df = pd.read_csv(f, header=None, names=headers)
            
            # Check if the first row is actually the header
            if not df.empty and config and "time_col" in config:
                time_col = config["time_col"]
                first_val = df.iloc[0][time_col]
                
                is_header = False
                # Check if it matches known header names
                if str(first_val) in [time_col, "time", "transactTime", "openTime", "t", "T"]:
                    is_header = True
                # Check if it's a string that is NOT numeric
                # This handles cases where the header row is present but doesn't match the expected column name exactly
                elif isinstance(first_val, str) and not first_val.replace('.','',1).isdigit():
                    is_header = True
                    
                if is_header:
                     logger.info(f"Detected header row in {data_type} for {symbol}, dropping it.")
                     df = df.iloc[1:].reset_index(drop=True)
                     # Convert time column to numeric since it was likely inferred as object due to the header string
                     try:
                         df[time_col] = pd.to_numeric(df[time_col])
                     except Exception as e:
                         logger.warning(f"Failed to convert {time_col} to numeric: {e}")

            # Post-processing
            if config and "time_col" in config:
                time_col = config["time_col"]
                if time_col in df.columns and not df.empty:
                    # Heuristic for timestamp unit detection
                    # We check the magnitude of the timestamp to determine if it's in seconds, milliseconds, or microseconds
                    sample_ts = df[time_col].iloc[0]
                    
                    # Check for numeric types, including numpy types (e.g. np.int64) which are common in pandas
                    if isinstance(sample_ts, (int, float, np.number)):
                        if sample_ts > 1e14: # Microseconds
                            df[time_col] = pd.to_datetime(df[time_col], unit='us')
                        elif sample_ts > 1e11: # Milliseconds
                            df[time_col] = pd.to_datetime(df[time_col], unit='ms')
                        else: # Seconds
                            df[time_col] = pd.to_datetime(df[time_col], unit='s')
                    else:
                        # Check if string is numeric (e.g. "1731628800000")
                        if isinstance(sample_ts, str) and sample_ts.replace('.','',1).isdigit():
                            try:
                                df[time_col] = pd.to_numeric(df[time_col])
                                # Re-evaluate sample_ts
                                sample_ts = df[time_col].iloc[0]
                                if sample_ts > 1e14: # Microseconds
                                    df[time_col] = pd.to_datetime(df[time_col], unit='us')
                                elif sample_ts > 1e11: # Milliseconds
                                    df[time_col] = pd.to_datetime(df[time_col], unit='ms')
                                else: # Seconds
                                    df[time_col] = pd.to_datetime(df[time_col], unit='s')
                            except:
                                # Fallback
                                df[time_col] = pd.to_datetime(df[time_col])
                        else:
                            # Assume string date or already datetime
                            df[time_col] = pd.to_datetime(df[time_col])
            
            df['symbol'] = symbol
            df['exchange'] = 'binance'
            df['instrument_type'] = instrument_type
            if timeframe:
                df['timeframe'] = timeframe
            
            return df.to_dict('records')

        except Exception as e:
            logger.error(f"Failed to process {symbol} {data_type} on {date.date()}: {e}")
            return None


