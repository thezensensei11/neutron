from pathlib import Path
import pandas as pd
import logging
import sys
import json
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
            return str(obj)
        return super(NpEncoder, self).default(obj)

def inspect_schemas():
    base_dir = Path("data/ohlcv")
    output_file = Path("data/ohlcv_schemas.json")
    
    if not base_dir.exists():
        logger.error(f"Directory {base_dir} does not exist.")
        return

    logger.info(f"Scanning {base_dir} for 1m OHLCV data...")
    
    schemas = {}
    
    # Scan standard OHLCV data
    _scan_directory(base_dir, schemas)
    
    # Scan Aggregated data
    agg_dir = Path("data/aggregated")
    if agg_dir.exists():
        logger.info(f"Scanning {agg_dir} for Aggregated data...")
        _scan_directory(agg_dir, schemas, is_aggregated=True)

    # Write to JSON
    with open(output_file, 'w') as f:
        json.dump(schemas, f, indent=4, cls=NpEncoder)
        
    logger.info(f"\nSchema info saved to {output_file}")
    # Print summary to console
    print(json.dumps(schemas, indent=4, cls=NpEncoder))

def _scan_directory(base_dir: Path, schemas: dict, is_aggregated: bool = False):
    # Sort to ensure deterministic output order
    for exchange_dir in sorted(base_dir.iterdir()):
        if not exchange_dir.is_dir(): continue
        
        exchange = exchange_dir.name
        # For aggregated, the top level might be instrument type (spot/swap) if structure is data/aggregated/spot/BTC
        # BUT user said "aggregated exchange". 
        # Let's check structure.
        # Standard: data/ohlcv/{exchange}/{type}/{symbol}/1m
        # Aggregated: data/aggregated/{type}/{symbol}/1m  (Wait, where is exchange?)
        # Ah, for aggregated, the "exchange" is effectively "aggregated".
        # But the folder structure is data/aggregated/spot/BTC...
        # So "spot" is a directory under "aggregated".
        
        if is_aggregated:
            # Structure: data/aggregated/{instrument_type}/{symbol}/1m
            # We want to capture schema per instrument type under "aggregated" exchange key?
            # Or "aggregated" as the exchange key, and then schemas for each type?
            # The current JSON structure is schemas[exchange] = {columns, example}.
            # It assumes one schema per exchange.
            # If aggregated has different schemas for spot/swap/synthetic (unlikely, but possible),
            # we might need to adjust the structure or just take the first one found.
            # User asked for "aggregated exchange and all spot, swap and synthetic instrument types".
            # This implies schemas[aggregated][spot], schemas[aggregated][swap], etc.
            # The current structure is flat: schemas[binance] -> schema.
            # If we want to support multiple types per exchange, we should probably change the structure 
            # OR just add keys like "aggregated_spot", "aggregated_swap", "aggregated_synthetic".
            # Let's use "aggregated_{type}" for clarity if they differ, or just "aggregated" if they are the same.
            # But user explicitly asked for "all spot, swap and synthetic instrument types".
            
            instrument_type = exchange_dir.name # spot, swap, synthetic
            exchange_name = "aggregated"
            
            # We want to capture this specific type.
            # Let's use a key like "aggregated" but maybe we need to upgrade the JSON structure?
            # Existing JSON: "binance": { ... }
            # If we change it, we might break consumers.
            # But if we just add "aggregated_spot": { ... }, "aggregated_swap": { ... } it might be safer.
            # Let's try to fit it into the existing logic first.
            
            # Actually, let's look at how _scan_directory works.
            # It iterates exchange_dir.
            # For aggregated, base_dir is data/aggregated.
            # exchange_dir is "spot", "swap", "synthetic".
            # So "exchange" variable becomes "spot", "swap"...
            # This is confusing.
            
            # Let's handle aggregated separately or adjust the loop.
            
            # Let's treat "aggregated" as the exchange name, and we want to find schemas for each type.
            # So we might want keys: "aggregated_spot", "aggregated_swap", "aggregated_synthetic".
            
            schema_key = f"aggregated_{instrument_type}"
            logger.info(f"Checking aggregated type: {instrument_type}")
            
            found_for_type = False
            for symbol_dir in sorted(exchange_dir.iterdir()):
                if not symbol_dir.is_dir(): continue
                if found_for_type: break
                
                timeframe_dir = symbol_dir / "1m"
                if not timeframe_dir.exists(): continue
                
                parquet_files = sorted(list(timeframe_dir.glob("*.parquet")))
                if parquet_files:
                    _extract_schema(parquet_files[0], schemas, schema_key)
                    found_for_type = True
                    
        else:
            # Standard Structure: data/ohlcv/{exchange}/{type}/{symbol}/1m
            logger.info(f"Checking exchange: {exchange}")
            found_for_exchange = False
            
            for type_dir in sorted(exchange_dir.iterdir()):
                if not type_dir.is_dir(): continue
                # We scan all types but currently only store one schema per exchange.
                # Maybe we should store per exchange_type?
                # "binance_spot", "binance_swap"?
                # The current script just does `schemas[exchange] = ...` and breaks after first find.
                # This implies it assumes uniform schema per exchange.
                # Let's keep that behavior for standard exchanges to avoid breaking changes,
                # unless we want to be more granular.
                # But for aggregated, we MUST be granular as requested.
                
                if found_for_exchange: break
                
                for symbol_dir in sorted(type_dir.iterdir()):
                    if not symbol_dir.is_dir(): continue
                    if found_for_exchange: break
                    
                    timeframe_dir = symbol_dir / "1m"
                    if not timeframe_dir.exists(): continue
                    
                    parquet_files = sorted(list(timeframe_dir.glob("*.parquet")))
                    if parquet_files:
                        _extract_schema(parquet_files[0], schemas, exchange)
                        found_for_exchange = True

def _extract_schema(file_path: Path, schemas: dict, key: str):
    try:
        df = pd.read_parquet(file_path)
        if not df.empty:
            columns = list(df.columns)
            first_row = df.iloc[0].to_dict()
            for k, v in first_row.items():
                if isinstance(v, (pd.Timestamp, pd.DatetimeIndex)):
                    first_row[k] = str(v)
            
            schemas[key] = {
                "columns": columns,
                "example_data": first_row
            }
            logger.info(f"  -> Found schema for {key} from {file_path.name}")
    except Exception as e:
        logger.error(f"  -> Error reading {file_path}: {e}")
        
    except Exception as e:
        logger.error(f"  -> Error reading {file_path}: {e}")

if __name__ == "__main__":
    inspect_schemas()
