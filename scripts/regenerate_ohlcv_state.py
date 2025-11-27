import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/ohlcv")
STATE_FILE = Path("states/ohlcv_data_state.json")

def parse_timeframe(tf_str):
    """Parses a timeframe string (e.g., '1m', '1h', '1d') into a timedelta."""
    amount = int(tf_str[:-1])
    unit = tf_str[-1]
    if unit == 'm':
        return timedelta(minutes=amount)
    elif unit == 'h':
        return timedelta(hours=amount)
    elif unit == 'd':
        return timedelta(days=amount)
    elif unit == 'w':
        return timedelta(weeks=amount)
    else:
        raise ValueError(f"Unknown timeframe unit: {unit}")

def get_contiguous_ranges(files):
    """
    Groups files into contiguous date ranges.
    files: List of Path objects (e.g., 2017-08-17.parquet)
    Returns: List of lists of files [[f1, f2, ...], [f10, f11, ...]]
    """
    if not files:
        return []

    # Sort files by date
    sorted_files = sorted(files, key=lambda f: f.name)
    
    ranges = []
    current_range = [sorted_files[0]]
    
    for i in range(1, len(sorted_files)):
        prev_file = sorted_files[i-1]
        curr_file = sorted_files[i]
        
        prev_date = datetime.strptime(prev_file.stem, "%Y-%m-%d").date()
        curr_date = datetime.strptime(curr_file.stem, "%Y-%m-%d").date()
        
        if (curr_date - prev_date).days == 1:
            current_range.append(curr_file)
        else:
            ranges.append(current_range)
            current_range = [curr_file]
    
    ranges.append(current_range)
    return ranges

def process_range(file_range, timeframe_delta):
    """
    Reads the first and last file of a range to determine the exact start and end time.
    """
    first_file = file_range[0]
    last_file = file_range[-1]
    
    try:
        # Read first file for start time and symbol info
        df_start = pd.read_parquet(first_file)
        if df_start.empty:
            logger.warning(f"Empty file found: {first_file}")
            return None
        
        start_time = df_start['time'].min()
        symbol = df_start['symbol'].iloc[0]
        # Ensure start_time is datetime
        if not isinstance(start_time, datetime):
             start_time = pd.to_datetime(start_time).to_pydatetime()

        # Read last file for end time
        if first_file == last_file:
            df_end = df_start
        else:
            df_end = pd.read_parquet(last_file)
        
        if df_end.empty:
            logger.warning(f"Empty file found: {last_file}")
            return None

        last_candle_time = df_end['time'].max()
        if not isinstance(last_candle_time, datetime):
            last_candle_time = pd.to_datetime(last_candle_time).to_pydatetime()
            
        # Calculate exclusive end time
        end_time = last_candle_time + timeframe_delta
        
        return {
            "symbol": symbol,
            "start_date": start_time.isoformat(),
            "end_date": end_time.isoformat()
        }

    except Exception as e:
        logger.error(f"Error processing range {first_file.name} to {last_file.name}: {e}")
        return None

def main():
    if not DATA_DIR.exists():
        logger.error(f"Data directory {DATA_DIR} does not exist.")
        return

    state_data = {}

    # Walk through the directory structure
    # Expected: data/ohlcv/{exchange}/{type}/{base_currency}/{timeframe}
    for exchange_dir in DATA_DIR.iterdir():
        if not exchange_dir.is_dir() or exchange_dir.name.startswith('.'): continue
        exchange = exchange_dir.name
        
        for type_dir in exchange_dir.iterdir():
            if not type_dir.is_dir() or type_dir.name.startswith('.'): continue
            inst_type = type_dir.name
            
            for base_dir in type_dir.iterdir():
                if not base_dir.is_dir() or base_dir.name.startswith('.'): continue
                # base_currency = base_dir.name # Not strictly needed if we get symbol from parquet
                
                for tf_dir in base_dir.iterdir():
                    if not tf_dir.is_dir() or tf_dir.name.startswith('.'): continue
                    timeframe = tf_dir.name
                    
                    logger.info(f"Processing {exchange} {inst_type} {base_dir.name} {timeframe}...")
                    
                    files = list(tf_dir.glob("*.parquet"))
                    if not files:
                        continue
                        
                    timeframe_delta = parse_timeframe(timeframe)
                    file_ranges = get_contiguous_ranges(files)
                    
                    ranges_info = []
                    symbol = None
                    
                    for fr in file_ranges:
                        result = process_range(fr, timeframe_delta)
                        if result:
                            ranges_info.append({
                                "start_date": result["start_date"],
                                "end_date": result["end_date"]
                            })
                            # Assume symbol is consistent across files in the same folder
                            if symbol is None:
                                symbol = result["symbol"]
                            elif symbol != result["symbol"]:
                                logger.warning(f"Symbol mismatch in {tf_dir}: {symbol} vs {result['symbol']}")
                    
                    if symbol and ranges_info:
                        # Ensure nested structure exists
                        if exchange not in state_data:
                            state_data[exchange] = {}
                        if inst_type not in state_data[exchange]:
                            state_data[exchange][inst_type] = {}
                        if symbol not in state_data[exchange][inst_type]:
                            state_data[exchange][inst_type][symbol] = {}
                        
                        # Sort ranges by start date
                        ranges_info.sort(key=lambda x: x["start_date"])
                        
                        # Convert to list of lists [start, end] as expected by OHLCVStateManager
                        formatted_ranges = [[r["start_date"], r["end_date"]] for r in ranges_info]
                        
                        state_data[exchange][inst_type][symbol][timeframe] = formatted_ranges
                        logger.info(f"Added {len(formatted_ranges)} ranges for {exchange} {inst_type} {symbol} {timeframe}")

    # Write state file
    logger.info(f"Writing state to {STATE_FILE}...")
    with open(STATE_FILE, 'w') as f:
        json.dump(state_data, f, indent=4)
    logger.info("Done.")

if __name__ == "__main__":
    main()
