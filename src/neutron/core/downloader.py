import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import NeutronConfig, ConfigLoader
from .state import DataStateManager
from .storage import StorageBackend, DatabaseStorage, ParquetStorage
from ..db.session import engine
from ..services.metadata_sync import MetadataService
from ..services.ohlcv_backfill import OHLCVBackfillService
from ..services.binance_backfill import BinanceBackfillService
from ..exchange.binance import BinanceExchange
from ..exchange.bitstamp import BitstampExchange

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader()
        self.state_manager = DataStateManager()
        self.config = self.config_loader.load(config_path)
        self.exchange_cache = {}
        
        # Initialize Storage Backend
        if self.config.storage.type == "parquet":
            if not self.config.storage.path:
                raise ValueError("Storage path is required for parquet storage")
            self.storage = ParquetStorage(self.config.storage.path)
            logger.info(f"Using Parquet storage at {self.config.storage.path}")
        else:
            self.storage = DatabaseStorage()
            logger.info("Using Database storage")

    def _run_backfill_generic(self, params: Dict[str, Any], data_type: str):
        symbol = params.get('symbol')
        start_date = datetime.fromisoformat(params.get('start_date'))
        end_date = datetime.fromisoformat(params.get('end_date'))
        rewrite = params.get('rewrite', False)
        
        exchange_name = params.get('exchange', 'binance')
        instrument_type = params.get('instrument_type', 'spot')
        
        if start_date.tzinfo is None: start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None: end_date = end_date.replace(tzinfo=timezone.utc)

        service = BinanceBackfillService(
            state_manager=self.state_manager,
            exchange_name=exchange_name,
            instrument_type=instrument_type,
            storage=self.storage
        )

        if not rewrite:
            check_end_date = end_date + timedelta(days=1)
            gaps = self.state_manager.get_gaps(
                exchange_name, 
                instrument_type, 
                symbol, 
                data_type, 
                start_date, 
                check_end_date
            )
            
            if not gaps:
                logger.info(f"{data_type} already exists for {symbol}. Skipping.")
                return

            for gap_start, gap_end in gaps:
                current_gap_end = gap_end - timedelta(days=1)
                if current_gap_end < gap_start: current_gap_end = gap_start
                service.backfill_range(symbol, gap_start, current_gap_end, data_type)
        else:
            service.backfill_range(symbol, start_date, end_date, data_type)

    def _execute_single_task(self, task_type: str, task_params: Dict[str, Any]):
        """Execute a single task with given params (helper for parallelism)."""
        try:
            if task_type == 'sync_metadata':
                self._run_sync_metadata(task_params)
            elif task_type == 'backfill_ohlcv':
                self._run_backfill_ohlcv(task_params)
            elif task_type == 'backfill_tick_data':
                self._run_backfill_tick_data(task_params)
            elif task_type == 'backfill_funding':
                self._run_backfill_funding(task_params)
            elif task_type in ['backfill_agg_trades', 'backfill_book_ticker', 'backfill_liquidation']:
                data_type_map = {
                    'backfill_agg_trades': 'aggTrades',
                    'backfill_book_ticker': 'bookTicker',
                    'backfill_liquidation': 'liquidationSnapshot'
                }
                self._run_backfill_generic(task_params, data_type_map[task_type])
            else:
                logger.warning(f"Unknown task type: {task_type}")
        except Exception as e:
            logger.error(f"Task {task_type} failed for params {task_params}: {e}")
            raise

    def run(self):
        logger.info(f"Starting Neutron Downloader with {len(self.config.tasks)} tasks.")
        
        for task in self.config.tasks:
            logger.info(f"Executing task: {task.type}")
            
            # Flatten params for each exchange/symbol if provided
            # Flatten params for each exchange/symbol if provided
            if task.exchanges:
                # Use ThreadPoolExecutor for parallel exchange processing
                max_workers = 5 # Adjust based on system capacity
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for exchange_name, types in task.exchanges.items():
                        for instrument_type, params in types.items():
                            symbols = params.get('symbols', [])
                            for symbol in symbols:
                                task_params = task.params.copy()
                                task_params.update({
                                    'exchange': exchange_name,
                                    'instrument_type': instrument_type,
                                    'symbol': symbol
                                })
                                
                                # Submit task to executor
                                futures.append(executor.submit(self._execute_single_task, task.type, task_params))
                    
                    # Wait for all tasks to complete
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Parallel task execution failed: {e}")
            else:
                # Single task execution (legacy or simple)
                try:
                    if task.type == 'sync_metadata':
                        self._run_sync_metadata(task.params)
                    elif task.type == 'backfill_ohlcv':
                        self._run_backfill_ohlcv(task.params)
                    elif task.type == 'backfill_tick_data':
                        self._run_backfill_tick_data(task.params)
                    elif task.type == 'backfill_funding':
                        self._run_backfill_funding(task.params)
                    elif task.type in ['backfill_agg_trades', 'backfill_book_ticker', 'backfill_liquidation']:
                        data_type_map = {
                            'backfill_agg_trades': 'aggTrades',
                            'backfill_book_ticker': 'bookTicker',
                            'backfill_liquidation': 'liquidationSnapshot'
                        }
                        self._run_backfill_generic(task.params, data_type_map[task.type])
                    else:
                        logger.warning(f"Unknown task type: {task.type}")
                except Exception as e:
                    logger.error(f"Task {task.type} failed: {e}")

    def _process_exchange_task(self, exchange_name: str, instruments: Dict[str, Any], task_type: str, global_params: Dict[str, Any]):
        """Process a single exchange's part of a task."""
        try:
            # Iterate through instrument types sequentially to respect exchange rate limits
            for instrument_type, instrument_data in instruments.items():
                symbols = instrument_data.get('symbols', [])
                if not symbols:
                    continue
                    
                logger.info(f"Processing {exchange_name} ({instrument_type}) - {len(symbols)} symbols")
                
                # Merge global params with specific context
                context_params = global_params.copy()
                context_params['exchange'] = exchange_name
                context_params['instrument_type'] = instrument_type
                
                for symbol in symbols:
                    # Smart Start Date Validation
                    if 'start_date' in global_params:
                        exchange = self._get_exchange(exchange_name, instrument_type)
                        listing_date = exchange.get_listing_date(symbol)
                        
                        if listing_date:
                            config_start = datetime.fromisoformat(global_params['start_date'])
                            if config_start.tzinfo is None:
                                config_start = config_start.replace(tzinfo=timezone.utc)
                                
                            if config_start < listing_date:
                                logger.warning(f"Config start date {config_start} is before listing date {listing_date} for {symbol}. Adjusting to listing date.")
                                # Update context params with adjusted start date
                                context_params = global_params.copy()
                                context_params['exchange'] = exchange_name
                                context_params['instrument_type'] = instrument_type
                                context_params['start_date'] = listing_date.isoformat()
                                context_params['symbol'] = symbol
                                self._dispatch_task(task_type, context_params)
                                continue
                        elif listing_date is None:
                            logger.error(f"Could not determine listing date for {symbol}. Skipping validation but proceeding with risk.")

                    context_params['symbol'] = symbol
                    self._dispatch_task(task_type, context_params)
        except Exception as e:
            logger.error(f"Error processing exchange {exchange_name}: {e}")
            raise

    def _dispatch_task(self, task_type: str, params: Dict[str, Any]):
        try:
            exchange_name = params.get('exchange', 'binance')
            instrument_type = params.get('instrument_type', 'spot')
            rewrite = params.get('rewrite', False)

            if task_type == "sync_metadata":
                self._run_sync_metadata(params)
            elif task_type == "backfill_ohlcv":
                symbol = params.get('symbol')
                timeframe = params.get('timeframe', '1h')
                start_date_str = params.get('start_date')
                end_date_str = params.get('end_date')

                start_date = None
                if start_date_str:
                    start_date = datetime.fromisoformat(start_date_str)
                    if start_date.tzinfo is None:
                        start_date = start_date.replace(tzinfo=timezone.utc)
                
                end_date = None
                if end_date_str:
                    end_date = datetime.fromisoformat(end_date_str)
                    if end_date.tzinfo is None:
                        end_date = end_date.replace(tzinfo=timezone.utc)

                # Smart Data State Check
                if not rewrite and start_date and end_date:
                    gaps = self.state_manager.get_gaps(
                        exchange_name, 
                        instrument_type, 
                        symbol, 
                        timeframe, 
                        start_date, 
                        end_date
                    )
                    
                    if not gaps:
                        logger.info(f"Data already exists for {symbol} {timeframe} [{start_date} - {end_date}]. Skipping.")
                        return

                    logger.info(f"Found {len(gaps)} gaps for {symbol} {timeframe}. Processing...")
                    for gap_start, gap_end in gaps:
                        # Create a sub-task for each gap
                        sub_params = params.copy()
                        sub_params['start_date'] = gap_start.isoformat()
                        sub_params['end_date'] = gap_end.isoformat()
                        self._run_backfill_ohlcv(sub_params)
                else:
                    # Rewrite=True or no date range, run normally
                    self._run_backfill_ohlcv(params)
                    
            elif task_type == "backfill_tick_data":
                self._run_backfill_tick_data(params)
            elif task_type == "backfill_funding":
                self._run_backfill_funding(params)
            else:
                logger.warning(f"Unknown task type: {task_type}")
        except Exception as e:
            logger.error(f"Task {task_type} failed for params {params}: {e}")

    def _get_exchange(self, exchange_name: str, instrument_type: str) -> Any:
        """Get or create an exchange instance, caching it for reuse."""
        key = f"{exchange_name}_{instrument_type}"
        if key not in self.exchange_cache:
            logger.info(f"Initializing new exchange instance for {key}")
            
            if exchange_name == "binance":
                from ..exchange.binance import BinanceExchange
                self.exchange_cache[key] = BinanceExchange(default_type=instrument_type)
            elif exchange_name == "bitstamp":
                from ..exchange.bitstamp import BitstampExchange
                self.exchange_cache[key] = BitstampExchange(default_type=instrument_type)
            elif exchange_name == "bitmex":
                from ..exchange.bitmex import BitmexExchange
                self.exchange_cache[key] = BitmexExchange(default_type=instrument_type)
            elif exchange_name == "bitfinex":
                from ..exchange.bitfinex import BitfinexExchange
                self.exchange_cache[key] = BitfinexExchange(default_type=instrument_type)
            elif exchange_name == "bybit":
                from ..exchange.bybit import BybitExchange
                self.exchange_cache[key] = BybitExchange(default_type=instrument_type)
            elif exchange_name == "coinbase":
                from ..exchange.coinbase import CoinbaseExchange
                self.exchange_cache[key] = CoinbaseExchange(default_type=instrument_type)
            elif exchange_name == "hyperliquid":
                from ..exchange.hyperliquid import HyperliquidExchange
                self.exchange_cache[key] = HyperliquidExchange(default_type=instrument_type)
            else:
                raise ValueError(f"Unsupported exchange: {exchange_name}")
        return self.exchange_cache[key]

    def _run_sync_metadata(self, params: Dict[str, Any]):
        # If specific exchanges are requested in params, use them.
        # Otherwise, try to sync all exchanges defined in any task in the config?
        # Or just a hardcoded list of supported exchanges for now?
        # Better: Look at the 'exchanges' block of the task if it exists (passed in params usually flattened).
        # But sync_metadata task in config usually has empty params.
        
        # Let's iterate over a set of known exchanges or those found in other tasks.
        # For simplicity, let's sync all exchanges that we have classes for or are in the config.
        # Let's look at the config to find unique exchange names.
        
        exchanges_to_sync = set()
        if 'exchange' in params:
            exchanges_to_sync.add(params['exchange'])
        else:
            # Scan config for exchanges
            for task in self.config.tasks:
                if task.exchanges:
                    exchanges_to_sync.update(task.exchanges.keys())
        
        if not exchanges_to_sync:
            # Default fallback
            exchanges_to_sync = {'binance', 'bitstamp'} 
            
        logger.info(f"Syncing metadata for exchanges: {exchanges_to_sync}")
        
        for exchange_name in exchanges_to_sync:
            try:
                # We need to instantiate with a default type, usually 'spot' is safe for metadata
                exchange = self._get_exchange(exchange_name, 'spot')
                service = MetadataService(exchange)
                service.sync_metadata()
            except Exception as e:
                logger.error(f"Failed to sync metadata for {exchange_name}: {e}")

    def _run_backfill_ohlcv(self, params: Dict[str, Any]):
        symbol = params.get('symbol')
        timeframe = params.get('timeframe', '1h')
        start_date = datetime.fromisoformat(params.get('start_date'))
        if start_date.tzinfo is None:
            start_date = start_date.replace(tzinfo=timezone.utc)
            
        end_date = None
        if params.get('end_date'):
            end_date = datetime.fromisoformat(params.get('end_date'))
            if end_date.tzinfo is None:
                end_date = end_date.replace(tzinfo=timezone.utc)
        
        exchange_name = params.get('exchange', 'binance')
        instrument_type = params.get('instrument_type', 'spot')
        
        exchange = self._get_exchange(exchange_name, instrument_type)
        service = OHLCVBackfillService(
            exchange=exchange, 
            state_manager=self.state_manager,
            storage=self.storage
        )
        
        service.backfill_symbol(symbol, timeframe, start_date, end_date, instrument_type=instrument_type)

    def _run_backfill_tick_data(self, params: Dict[str, Any]):
        # Reuse generic backfill with data_type='trades'
        self._run_backfill_generic(params, 'trades')

    def _run_backfill_funding(self, params: Dict[str, Any]):
        symbol = params.get('symbol')
        start_date = datetime.fromisoformat(params.get('start_date'))
        exchange_name = params.get('exchange', 'binance')
        instrument_type = params.get('instrument_type', 'swap') # Default to swap if not specified
        
        exchange = self._get_exchange(exchange_name, instrument_type)
        rates = exchange.fetch_funding_rate_history(symbol, since=start_date)
        
        if rates:
            # Inject instrument_type for Parquet storage
            for r in rates:
                r['instrument_type'] = instrument_type
                
            self.storage.save_funding_rates(rates)
            logger.info(f"Backfilled {len(rates)} funding rates for {symbol}")

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 2:
        print("Usage: python -m neutron.core.downloader <config_path>")
        sys.exit(1)
        
    downloader = Downloader(sys.argv[1])
    downloader.run()
