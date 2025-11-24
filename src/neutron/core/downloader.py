import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import NeutronConfig, ConfigLoader
from .state import OHLCVStateManager, TickDataStateManager, ExchangeStateManager
from .storage import StorageBackend
from ..core.storage.questdb import QuestDBStorage
from ..core.storage.parquet import ParquetStorage
from ..services.questdb_loader import QuestDBLoader
from .progress import ProgressManager
from .config import NeutronConfig
from ..db.session import engine
from ..services.metadata_sync import MetadataService
from ..services.ohlcv_backfill import OHLCVBackfillService
from ..services.binance_backfill import BinanceBackfillService
from ..exchange.binance import BinanceExchange
from ..exchange.bitstamp import BitstampExchange

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self, config_path: str = None, config: NeutronConfig = None, log_file: str = "logs/ohlcvdata.log"):
        self.config_loader = ConfigLoader()
        
        if config:
            self.config = config
        elif config_path:
            self.config = self.config_loader.load(config_path)
        else:
            raise ValueError("Either config_path or config object must be provided")

        self.log_file = log_file
        self.progress_manager = ProgressManager() # Initialize Progress Manager
        
        # Initialize State Managers with configured paths
        self.ohlcv_state_manager = OHLCVStateManager(state_file=self.config.data_state_path)
        self.tick_state_manager = TickDataStateManager(state_file=self.config.tick_data_state_path)
        
        # ExchangeStateManager is a singleton, so we initialize it here to ensure path is set if first time
        self.exchange_state_manager = ExchangeStateManager(state_file=self.config.exchange_state_path)
            
        self.exchange_cache = {}
        
        # Initialize Storage Backends (Dual Storage Strategy)
        
        # 1. OHLCV Storage -> Parquet
        if not self.config.storage.ohlcv_path:
             # Fallback or error? Config default is "data/ohlcv"
             self.config.storage.ohlcv_path = "data/ohlcv"
             
        self.ohlcv_storage = ParquetStorage(self.config.storage.ohlcv_path)
        logger.info(f"Using Parquet storage for OHLCV at {self.config.storage.ohlcv_path}")
        
        # 2. Tick/Generic Storage -> QuestDB
        self.tick_storage = QuestDBStorage(
            host=self.config.storage.questdb_host,
            ilp_port=self.config.storage.questdb_ilp_port,
            pg_port=self.config.storage.questdb_pg_port,
            username=self.config.storage.questdb_username,
            password=self.config.storage.questdb_password,
            database=self.config.storage.questdb_database
        )
        logger.info(f"Using QuestDB storage for Tick/Generic data at {self.config.storage.questdb_host}")

        # Parquet storage for tick/generic data (Source of Truth)
        # Assuming a default path if not specified in config, or add to config
        parquet_tick_path = getattr(self.config.storage, 'parquet_tick_path', "data/parquet_tick")
        self.parquet_storage = ParquetStorage(base_dir=parquet_tick_path)
        logger.info(f"Using Parquet storage for Tick/Generic data at {parquet_tick_path}")

        # Initialize QuestDBLoader
        self.questdb_loader = QuestDBLoader(self.parquet_storage, self.tick_storage)
        logging.getLogger("neutron.services.questdb_loader").setLevel(logging.INFO)
        
        self._setup_ohlcv_logger()
        self._setup_tick_logger()
        self._setup_questdb_logger()
        
        self.ohlcv_logger = logging.getLogger("data_monitor")
        self.tick_logger = logging.getLogger("tick_monitor")

    def _setup_questdb_logger(self):
        """Setup the QuestDB logger."""
        qdb_logger = logging.getLogger("neutron.core.storage.questdb")
        qdb_logger.setLevel(logging.DEBUG)
        qdb_logger.propagate = False
        
        log_path = "logs/questdb.log"
        import os
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        
        fh = logging.FileHandler(log_path)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        qdb_logger.addHandler(fh)

    def _setup_ohlcv_logger(self):
        """Setup the OHLCV data monitor logger globally."""
        data_logger = logging.getLogger("data_monitor")
        data_logger.setLevel(logging.INFO)
        data_logger.propagate = False  # Prevent propagation to root logger
        
        # Check if we already have the correct file handler
        has_file_handler = any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith(self.log_file) for h in data_logger.handlers)
        
        if not has_file_handler:
            # Remove existing handlers to avoid duplicates if re-initializing with different file
            for h in data_logger.handlers:
                data_logger.removeHandler(h)
                
            import os
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            fh = logging.FileHandler(self.log_file)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            data_logger.addHandler(fh)

    def _setup_tick_logger(self):
        """Setup the tick data logger."""
        tick_logger = logging.getLogger("tick_monitor")
        tick_logger.setLevel(logging.INFO)
        tick_logger.propagate = False
        
        has_file_handler = any(isinstance(h, logging.FileHandler) and h.baseFilename.endswith("logs/tickdata.log") for h in tick_logger.handlers)
        
        if not has_file_handler:
            for h in tick_logger.handlers:
                tick_logger.removeHandler(h)
                
            import os
            os.makedirs("logs", exist_ok=True)
            fh = logging.FileHandler("logs/tickdata.log")
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            tick_logger.addHandler(fh)

    def _run_backfill_generic(self, params: Dict[str, Any], data_type: str):
        symbol = params.get('symbol')
        exchange_name = params.get('exchange')
        start_date = params.get('start_date')
        end_date = params.get('end_date')
        instrument_type = params.get('instrument_type', 'spot')
        rewrite = params.get('rewrite', False)

        if not (symbol and exchange_name and start_date and end_date):
            self.tick_logger.error(f"Missing required params for {data_type} backfill")
            return

        if isinstance(start_date, str): start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
        if isinstance(end_date, str): end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))

        # Ensure timezone awareness
        if start_date.tzinfo is None: start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None: end_date = end_date.replace(tzinfo=timezone.utc)

        service = BinanceBackfillService(
            state_manager=self.tick_state_manager,
            exchange_name=exchange_name,
            instrument_type=instrument_type,
            storage=self.parquet_storage, # Use ParquetStorage for download
            progress_manager=self.progress_manager
        )

        timeframe = params.get('timeframe', 'raw') # Default to 'raw' if no timeframe

        if not rewrite:
            # check_end_date should be end_date (exclusive)
            # get_gaps expects exclusive end_date
            gaps = self.tick_state_manager.get_gaps(
                exchange_name, 
                instrument_type, 
                symbol, 
                data_type,
                timeframe, 
                start_date, 
                end_date
            )
            
            if not gaps:
                self.tick_logger.info(f"{data_type} already exists for {symbol}. Skipping.")
                return

            for gap_start, gap_end in gaps:
                # backfill_range expects exclusive end_date
                service.backfill_range(symbol, gap_start, gap_end, data_type, timeframe=timeframe)
        else:
            service.backfill_range(symbol, start_date, end_date, data_type, timeframe=timeframe)

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
            elif task_type == 'load_questdb':
                self._run_load_questdb(task_params)
            elif task_type in [
                'backfill_agg_trades', 'backfill_book_ticker', 'backfill_liquidation', 'backfill_metrics',
                'backfill_book_depth', 'backfill_index_price_klines', 'backfill_mark_price_klines', 'backfill_premium_index_klines'
            ]:
                data_type_map = {
                    'backfill_agg_trades': 'aggTrades',
                    'backfill_book_ticker': 'bookTicker',
                    'backfill_liquidation': 'liquidationSnapshot',
                    'backfill_metrics': 'metrics',
                    'backfill_book_depth': 'bookDepth',
                    'backfill_index_price_klines': 'indexPriceKlines',
                    'backfill_mark_price_klines': 'markPriceKlines',
                    'backfill_premium_index_klines': 'premiumIndexKlines'
                }
                self._run_backfill_generic(task_params, data_type_map[task_type])
            else:
                logger.warning(f"Unknown task type: {task_type}")
        except Exception as e:
            logger.error(f"Task {task_type} failed for params {task_params}: {e}")
            raise

    def run(self):
        """
        Execute all tasks defined in the configuration.
        """
        self.progress_manager.log(f"Starting Neutron Downloader with {len(self.config.tasks)} tasks.")
        
        ohlcv_tasks = []
        generic_tasks = []
        
        # 1. Categorize Tasks
        for task in self.config.tasks:
            if task.type == 'backfill_ohlcv':
                ohlcv_tasks.append(task)
            else:
                generic_tasks.append(task)
                
        # ---------------------------------------------------------
        # Phase 1: OHLCV Tasks (Parallel Exchanges, Sequential Symbols)
        # ---------------------------------------------------------
        if ohlcv_tasks:
            self.progress_manager.log(f"Phase 1: Processing {len(ohlcv_tasks)} OHLCV tasks.")
            self.progress_manager.log("Strategy: Parallel Exchanges, Sequential Symbols within Exchange.")
            
            # We use a separate executor for exchanges to ensure we don't overload connection pools
            # Default to 5 concurrent exchanges if not specified, usually enough.
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = []
                for task in ohlcv_tasks:
                    if task.exchanges:
                        for exchange_name, types in task.exchanges.items():
                            # Submit the entire exchange workload as one job
                            futures.append(executor.submit(
                                self._process_exchange_sequentially, 
                                exchange_name, 
                                types, 
                                task
                            ))
                    else:
                        # Legacy single task
                        futures.append(executor.submit(self._execute_single_task, task.type, task.params))
                
                # Wait for all exchanges to finish
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"OHLCV Exchange Task failed: {e}")
                
        # ---------------------------------------------------------
        # Phase 2: Generic Tasks (Fully Parallel)
        # ---------------------------------------------------------
        if generic_tasks:
            max_workers = self.config.max_workers
            self.progress_manager.log(f"Phase 2: Processing {len(generic_tasks)} generic tasks.")
            self.progress_manager.log(f"Strategy: High-Throughput Parallelism (max_workers={max_workers}).")
            
            # Flatten generic tasks into individual units of work (per symbol)
            work_items = []
            for task in generic_tasks:
                if task.exchanges:
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
                                work_items.append((task.type, task_params))
                else:
                    # Single task without exchanges block
                    work_items.append((task.type, task.params))

            if work_items:
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = [
                        executor.submit(self._execute_single_task, t_type, t_params)
                        for t_type, t_params in work_items
                    ]
                    
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Generic Parallel Task failed: {e}")

    def _process_exchange_sequentially(self, exchange_name: str, types: Dict[str, Any], task: Any):
        """
        Process all symbols for a single exchange sequentially.
        Used in Phase 1 to maintain stability and respect exchange-level constraints.
        """
        logger.info(f"Starting sequential processing for exchange: {exchange_name}")
        for instrument_type, params in types.items():
            symbols = params.get('symbols', [])
            for symbol in symbols:
                task_params = task.params.copy()
                task_params.update({
                    'exchange': exchange_name,
                    'instrument_type': instrument_type,
                    'symbol': symbol
                })
                try:
                    self._execute_single_task(task.type, task_params)
                except Exception as e:
                    logger.error(f"Failed processing {symbol} on {exchange_name}: {e}")
        logger.info(f"Finished sequential processing for exchange: {exchange_name}")

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
            logger.info(f"Dispatching task: {task_type}")
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
                    gaps = self.ohlcv_state_manager.get_gaps(
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
            elif task_type == "load_questdb":
                self._run_load_questdb(params)
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
            
        self.progress_manager.log(f"Syncing metadata for exchanges: {exchanges_to_sync}")
        
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
            state_manager=self.ohlcv_state_manager,
            storage=self.ohlcv_storage,
            progress_manager=self.progress_manager # Pass progress manager
        )
        
        service.backfill_symbol(symbol, timeframe, start_date, end_date, instrument_type=instrument_type)

    def _run_backfill_tick_data(self, params: Dict[str, Any]):
        # Reuse generic backfill with data_type='trades'
        self._run_backfill_generic(params, 'trades')

    def _run_backfill_funding(self, params: Dict[str, Any]):
        # Use generic backfill with 'fundingRate' type
        # This leverages Binance Vision files instead of API
        self._run_backfill_generic(params, 'fundingRate')

    def _run_load_questdb(self, params: Dict[str, Any]):
        symbol = params.get('symbol')
        exchange_name = params.get('exchange', 'binance')
        start_date_str = params.get('start_date')
        end_date_str = params.get('end_date')
        instrument_type = params.get('instrument_type', 'spot')
        data_type = params.get('data_type', 'aggTrades')
        
        if not (symbol and start_date_str and end_date_str):
            logger.error("Missing required params for load_questdb")
            return

        start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
        end_date = datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
        
        if start_date.tzinfo is None: start_date = start_date.replace(tzinfo=timezone.utc)
        if end_date.tzinfo is None: end_date = end_date.replace(tzinfo=timezone.utc)
        
        try:
            self.questdb_loader.load_range(
                exchange=exchange_name,
                symbol=symbol,
                data_type=data_type,
                start_date=start_date,
                end_date=end_date,
                instrument_type=instrument_type
            )
        except Exception as e:
            logger.error(f"Failed to load QuestDB: {e}")

def main():
    import sys
    # Configure logging to file only or minimal console output
    # Since we use tqdm, we want to avoid logger printing to stderr/stdout directly if possible
    # But basicConfig sets up a StreamHandler by default if filename is not specified.
    # We should probably set up a file handler here for the root logger or just rely on module loggers.
    # For now, let's keep basicConfig but maybe raise level to WARNING for root to avoid spam
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    
    if len(sys.argv) < 2:
        print("Usage: neutron-backfill <config_path>")
        sys.exit(1)
        
    downloader = Downloader(sys.argv[1])
    downloader.run()

if __name__ == "__main__":
    main()
