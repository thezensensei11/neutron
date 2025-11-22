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
    value = Column(Float, nullable=False) # The open interest amount (in contracts or base asset depending on exchange)
    
    __table_args__ = (
        PrimaryKeyConstraint("time", "symbol", "exchange"),
    )
