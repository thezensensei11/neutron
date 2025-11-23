import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta

# Add src to path so we can import neutron
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from neutron.core.downloader import Downloader
from neutron.core.crawler import DataCrawler
from neutron.core.config import NeutronConfig, StorageConfig, TaskConfig
from neutron.db.session import engine, Base
from neutron.db.models import * # Import all models to register them

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_reproduction():
    # Initialize DB tables
    Base.metadata.create_all(bind=engine)
    
    # Define Configuration
    config = NeutronConfig(
        storage=StorageConfig(type='database'), 
        data_state_path='examples/data_state.json', # Use local path for examples
        exchange_state_path='examples/exchange_state.json', # Use local path for examples
        max_workers=8,
        
        tasks=[
            # --- SPOT DATA ---
            # OHLCV (1 Day - Foundation)
            TaskConfig(
                type='backfill_ohlcv',
                params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
                exchanges={'binance': {'spot': {'symbols': ['BTC/USDT']}}}
            ),
            # Aggregated Trades (2 Minutes)
            TaskConfig(
                type='backfill_agg_trades',
                params={'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-15T00:02:00', 'rewrite': True},
                exchanges={'binance': {'spot': {'symbols': ['BTC/USDT']}}}
            ),
            
            # --- FUTURES DATA (UM) ---
            # Mark Price Klines (1 Day)
            TaskConfig(
                type='backfill_mark_price_klines',
                params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
                exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
            ),
            # Index Price Klines (1 Day)
            TaskConfig(
                type='backfill_index_price_klines',
                params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
                exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
            ),
            # Premium Index Klines (1 Day)
            TaskConfig(
                type='backfill_premium_index_klines',
                params={'timeframe': '1h', 'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-16T00:00:00', 'rewrite': True},
                exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
            ),
            # Metrics (Open Interest) (2 Minutes)
            TaskConfig(
                type='backfill_metrics',
                params={'start_date': '2024-11-15T00:00:00', 'end_date': '2024-11-15T00:02:00', 'rewrite': True},
                exchanges={'binance': {'swap': {'symbols': ['BTC/USDT']}}}
            )
        ]
    )

    downloader = Downloader(config=config, log_file='examples/neutron_demo.log')
    print("🚀 Starting Comprehensive Download...")
    downloader.run()
    print("✅ Download Complete!")

    # Analysis
    crawler = DataCrawler(storage_type='database')
    
    print("\n--- Analysis Results ---")
    
    # Spot vs Mark vs Index
    df_spot = crawler.get_ohlcv('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='spot')
    df_mark = crawler.get_mark_price_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')
    df_index = crawler.get_index_price_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')
    
    print(f"Spot Data Rows: {len(df_spot)}")
    print(f"Mark Price Data Rows: {len(df_mark)}")
    print(f"Index Price Data Rows: {len(df_index)}")

    # Premium Index
    df_premium = crawler.get_premium_index_klines('binance', 'BTC/USDT', '1h', '2024-11-15', '2024-11-16', instrument_type='swap')
    print(f"Premium Index Data Rows: {len(df_premium)}")

    # Open Interest
    df_oi = crawler.get_data('metrics', 'binance', 'BTC/USDT', '2024-11-15', '2024-11-16', instrument_type='swap')
    print(f"Open Interest Data Rows: {len(df_oi)}")

    # Aggregated Trades
    df_agg = crawler.get_data(
        data_type='aggTrades', exchange='binance', symbol='BTC/USDT',
        start_date='2024-11-15T00:00:00', end_date='2024-11-15T00:02:00'
    )
    print(f"Aggregated Trades Rows: {len(df_agg)}")
    if not df_agg.empty:
        df_agg['volume_usd'] = df_agg['price'] * df_agg['qty']
        buy_vol = df_agg[df_agg['is_buyer_maker'] == False]['volume_usd'].sum()
        sell_vol = df_agg[df_agg['is_buyer_maker'] == True]['volume_usd'].sum()
        print(f"Buy Volume: ${buy_vol:,.2f}")
        print(f"Sell Volume: ${sell_vol:,.2f}")

if __name__ == "__main__":
    run_reproduction()
