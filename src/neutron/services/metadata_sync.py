import logging
from sqlalchemy.orm import Session
from ..exchange import BinanceExchange
from ..db.models import ExchangeMetadata, Symbol
from ..db.session import ScopedSession
from ..core.exchange_state import ExchangeStateManager

logger = logging.getLogger(__name__)

class MetadataService:
    def __init__(self, exchange, db: Session = None):
        self.db = db or ScopedSession()
        self.exchange = exchange
        self.state_manager = ExchangeStateManager()

    def sync_metadata(self):
        """Sync exchange metadata and symbols to the database and state file."""
        exchange_id = self.exchange.exchange_id
        logger.info(f"Starting metadata sync for {exchange_id}...")
        
        try:
            markets = self.exchange.load_markets()
            
            # 0. Update Exchange State (JSON)
            self.state_manager.update_market_metadata(exchange_id, markets)
            logger.info(f"Updated exchange state for {exchange_id}")

            # 1. Update Exchange Metadata (DB)
            # Check if exchange exists
            exchange_meta = self.db.query(ExchangeMetadata).filter_by(id=exchange_id).first()
            if not exchange_meta:
                exchange_meta = ExchangeMetadata(id=exchange_id, name=exchange_id.capitalize())
                self.db.add(exchange_meta)
            
            exchange_meta.rate_limit = self.exchange.get_rate_limit()
            exchange_meta.raw_metadata = {"count": len(markets)} # Store summary or full if needed
            
            # 2. Update Symbols (DB)
            for symbol_str, market in markets.items():
                symbol = self.db.query(Symbol).filter_by(symbol=symbol_str, exchange_id=exchange_id).first()
                if not symbol:
                    symbol = Symbol(symbol=symbol_str, exchange_id=exchange_id)
                    self.db.add(symbol)
                
                symbol.base_asset = market.get('base', '')
                symbol.quote_asset = market.get('quote', '')
                symbol.active = market.get('active', True)
                symbol.price_precision = market.get('precision', {}).get('price')
                symbol.amount_precision = market.get('precision', {}).get('amount')
                
                limits = market.get('limits', {})
                symbol.min_amount = limits.get('amount', {}).get('min')
                symbol.min_cost = limits.get('cost', {}).get('min')
                symbol.raw_info = market

            self.db.commit()
            logger.info(f"Successfully synced {len(markets)} symbols for {exchange_id} to DB.")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to sync metadata for {exchange_id}: {e}")
            raise
        finally:
            self.db.close()


