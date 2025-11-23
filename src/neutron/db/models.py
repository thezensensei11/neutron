from sqlalchemy import Column, String, Float, DateTime, Integer, BigInteger, Index, Boolean, JSON, PrimaryKeyConstraint
from sqlalchemy.sql import text
from .session import Base

class ExchangeMetadata(Base):
    __tablename__ = "exchange_metadata"

    id = Column(String, primary_key=True)  # e.g., "binance"
    name = Column(String, nullable=False)
    url = Column(String)
    version = Column(String)
    rate_limit = Column(Integer)
    has_public_api = Column(Boolean, default=True)
    has_private_api = Column(Boolean, default=False)
    # Store raw JSON metadata from CCXT for flexibility
    raw_metadata = Column(JSON)
    last_updated = Column(DateTime(timezone=True), server_default=text("now()"))

class Symbol(Base):
    __tablename__ = "symbols"

    symbol = Column(String, primary_key=True)  # e.g., "BTC/USDT"
    exchange_id = Column(String, nullable=False) # e.g., "binance"
    base_asset = Column(String, nullable=False)
    quote_asset = Column(String, nullable=False)
    active = Column(Boolean, default=True)
    # Precision and limits
    price_precision = Column(Integer)
    amount_precision = Column(Integer)
    min_amount = Column(Float)
    min_cost = Column(Float)
    raw_info = Column(JSON)
    
    __table_args__ = (
        Index("idx_symbols_exchange", "exchange_id"),
    )

class OHLCV(Base):
    __tablename__ = "ohlcv"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='spot')
    is_interpolated = Column(Boolean, default=False)
    timeframe = Column(String, nullable=False)  # e.g., "1m", "1h"
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    
    # Composite primary key for uniqueness
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "timeframe"),
    )

class Trade(Base):
    __tablename__ = "trades"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='spot')
    trade_id = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    amount = Column(Float, nullable=False)
    side = Column(String)  # "buy" or "sell"
    
    # Note: trade_id might not be unique across exchanges or even within one if reset
    # So we use time + symbol + exchange + trade_id as PK or just rely on time/symbol index
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "trade_id"),
    )

class FundingRate(Base):
    __tablename__ = "funding_rates"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    rate = Column(Float, nullable=False)
    mark_price = Column(Float)
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange"),
    )

class OpenInterest(Base):
    __tablename__ = "open_interest"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    value = Column(Float, nullable=False) # The open interest amount (in contracts or base asset depending on exchange)
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange"),
    )

class AggTrade(Base):
    __tablename__ = "agg_trades"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='spot')
    agg_trade_id = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    qty = Column(Float, nullable=False)
    first_trade_id = Column(String)
    last_trade_id = Column(String)
    is_buyer_maker = Column(Boolean)
    is_best_match = Column(Boolean)

    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "agg_trade_id"),
    )

class BookTicker(Base):
    __tablename__ = "book_tickers"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='spot')
    update_id = Column(String, nullable=False)
    best_bid_price = Column(Float, nullable=False)
    best_bid_qty = Column(Float, nullable=False)
    best_ask_price = Column(Float, nullable=False)
    best_ask_qty = Column(Float, nullable=False)

    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "update_id"),
    )

class LiquidationSnapshot(Base):
    __tablename__ = "liquidation_snapshots"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    side = Column(String)
    order_type = Column(String)
    time_in_force = Column(String)
    original_quantity = Column(Float)
    price = Column(Float)
    average_price = Column(Float)
    order_status = Column(String)
    last_fill_quantity = Column(Float)
    accumulated_fill_quantity = Column(Float)

    # No unique ID provided in standard snapshot, so PK is composite of time/symbol/exchange + maybe side/price?
    # Or just use time/symbol/exchange if snapshots are per-second/timestamp unique.
    # But multiple liquidations can happen at same time.
    # We might need a surrogate key or rely on all fields.
    # For now, let's assume time+symbol+exchange+price+side is unique enough or use a UUID if we had one.
    # Actually, let's just use time/symbol/exchange/price/side/original_quantity as PK to be safe?
    # Or better, just add an autoincrement ID if we were using standard SQL, but here we want to upsert.
    # Let's use a broad composite PK.
    # Let's use a broad composite PK.
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "side", "price", "original_quantity"),
    )

class BookDepth(Base):
    __tablename__ = "book_depths"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='spot')
    update_id = Column(String, nullable=False) # lastUpdateId
    bids = Column(JSON, nullable=False) # List of [price, qty]
    asks = Column(JSON, nullable=False) # List of [price, qty]

    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "update_id"),
    )

class IndexPriceKline(Base):
    __tablename__ = "index_price_klines"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    timeframe = Column(String, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    # Volume is usually 0 or not applicable for index price, but we'll keep it optional or omit
    # Binance index klines don't have volume in the same way, but let's check.
    # Usually it's just price.
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "timeframe"),
    )

class MarkPriceKline(Base):
    __tablename__ = "mark_price_klines"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    timeframe = Column(String, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "timeframe"),
    )

class PremiumIndexKline(Base):
    __tablename__ = "premium_index_klines"

    time = Column(DateTime(timezone=True), nullable=False)
    symbol = Column(String, nullable=False)
    exchange = Column(String, nullable=False)
    instrument_type = Column(String, default='swap')
    timeframe = Column(String, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange", "timeframe"),
    )
