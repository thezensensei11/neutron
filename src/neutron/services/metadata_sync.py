import logging
from sqlalchemy.orm import Session
from ..exchange import BinanceExchange
from ..db.models import ExchangeMetadata, Symbol
from ..db.session import ScopedSession

logger = logging.getLogger(__name__)

class MetadataService:
    def __init__(self, db: Session = None):
        self.db = db or ScopedSession()
        self.exchange = BinanceExchange()

    def sync_metadata(self):
        """Sync exchange metadata and symbols to the database."""
        logger.info("Starting metadata sync for Binance...")
        
        try:
            markets = self.exchange.load_markets()
            
            # 1. Update Exchange Metadata
            # Check if exchange exists
            exchange_meta = self.db.query(ExchangeMetadata).filter_by(id="binance").first()
            if not exchange_meta:
                exchange_meta = ExchangeMetadata(id="binance", name="Binance")
                self.db.add(exchange_meta)
            
            exchange_meta.rate_limit = self.exchange.get_rate_limit()
            exchange_meta.raw_metadata = {"count": len(markets)} # Store summary or full if needed
            
            # 2. Update Symbols
            for symbol_str, market in markets.items():
                symbol = self.db.query(Symbol).filter_by(symbol=symbol_str, exchange_id="binance").first()
                if not symbol:
                    symbol = Symbol(symbol=symbol_str, exchange_id="binance")
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
            logger.info(f"Successfully synced {len(markets)} symbols for Binance.")
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to sync metadata: {e}")
            raise
        finally:
            self.db.close()


