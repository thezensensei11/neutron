import logging
from datetime import datetime, timezone
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from .config import NeutronConfig, ConfigLoader
from .state import DataStateManager
from ..db.session import engine
from ..services.metadata_sync import MetadataService
from ..services.ohlcv_backfill import OHLCVBackfillService
from ..data_source.binance_vision import BinanceVisionDownloader
from ..exchange.binance import BinanceExchange

logger = logging.getLogger(__name__)

class Downloader:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader()
        self.state_manager = DataStateManager()
        self.config = self.config_loader.load(config_path)
        self.exchange_cache = {}
        # Ideally, we would update the global DB engine here if database_url is provided
        # But for now, we rely on env vars or default, as changing engine at runtime requires care

    def run(self):
        logger.info(f"Starting Neutron Downloader with {len(self.config.tasks)} tasks.")
        
        for task in self.config.tasks:
            logger.info(f"Executing task: {task.type}")
            
            # If exchanges are defined, iterate through them
            # If exchanges are defined, iterate through them in parallel
            if task.exchanges:
                with ThreadPoolExecutor() as executor:
                    futures = []
                    for exchange_name, instruments in task.exchanges.items():
                        futures.append(
                            executor.submit(
                                self._process_exchange_task, 
                                exchange_name, 
                                instruments, 
                                task.type, 
                                task.params
                            )
                        )
                    
                    # Wait for all exchanges to complete for this task
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Exchange task failed: {e}")
            else:
                # Legacy/Simple mode (params directly in task)
                self._dispatch_task(task.type, task.params)

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
                    
            elif task_type == "download_tick_data":
                self._run_download_tick_data(params)
            elif task_type == "backfill_funding":
                self._run_backfill_funding(params)
            else:
                logger.warning(f"Unknown task type: {task_type}")
        except Exception as e:
            logger.error(f"Task {task_type} failed for params {params}: {e}")

    def _get_exchange(self, exchange_name: str, instrument_type: str) -> BinanceExchange:
        """Get or create an exchange instance, caching it for reuse."""
        key = f"{exchange_name}_{instrument_type}"
        if key not in self.exchange_cache:
            logger.info(f"Initializing new exchange instance for {key}")
            # Currently only supports Binance, but could be extended
            if exchange_name == "binance":
                self.exchange_cache[key] = BinanceExchange(default_type=instrument_type)
            else:
                raise ValueError(f"Unsupported exchange: {exchange_name}")
        return self.exchange_cache[key]

    def _run_sync_metadata(self, params: Dict[str, Any]):
        service = MetadataService()
        service.sync_metadata()

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
        service = OHLCVBackfillService(exchange=exchange, state_manager=self.state_manager)
        
        service.backfill_symbol(symbol, timeframe, start_date, end_date)

    def _run_download_tick_data(self, params: Dict[str, Any]):
        downloader = BinanceVisionDownloader()
        symbol = params.get('symbol')
        date_str = params.get('date')
        target_date = datetime.fromisoformat(date_str)
        
        downloader.download_and_process_day(symbol, target_date)

    def _run_backfill_funding(self, params: Dict[str, Any]):
        # Quick implementation of funding backfill downloader logic
        # We need to instantiate the exchange with 'swap' if not already
        from ..db.session import ScopedSession
        from ..db.models import FundingRate
        from sqlalchemy.dialects.postgresql import insert
        
        symbol = params.get('symbol')
        start_date = datetime.fromisoformat(params.get('start_date'))
        exchange_name = params.get('exchange', 'binance')
        instrument_type = params.get('instrument_type', 'swap') # Default to swap if not specified
        
        exchange = self._get_exchange(exchange_name, instrument_type)
        rates = exchange.fetch_funding_rate_history(symbol, since=start_date)
        
        if rates:
            with ScopedSession() as db:
                stmt = insert(FundingRate).values(rates)
                stmt = stmt.on_conflict_do_nothing(index_elements=['time', 'symbol', 'exchange'])
                db.execute(stmt)
                db.commit()
            logger.info(f"Backfilled {len(rates)} funding rates for {symbol}")

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    
    if len(sys.argv) < 2:
        print("Usage: python -m neutron.core.downloader <config_path>")
        sys.exit(1)
        
    downloader = Downloader(sys.argv[1])
    downloader.run()
