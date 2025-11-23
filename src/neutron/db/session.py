import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

# Default to local docker instance if not specified
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://neutron:password@localhost:5433/neutron"
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
ScopedSession = scoped_session(SessionLocal)

Base = declarative_base()

def get_db():
    """Dependency for getting a DB session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db():
    """Initialize database tables."""
    # Import models here to ensure they are registered with Base
    from . import models
    Base.metadata.create_all(bind=engine)

def configure_db(url: str):
    """Reconfigure the database engine with a new URL."""
    global engine, SessionLocal, ScopedSession
    
    if engine:
        engine.dispose()
        
    engine = create_engine(url, pool_pre_ping=True)
    SessionLocal.configure(bind=engine)
    # ScopedSession automatically uses the new SessionLocal factory
    # But we should remove any existing sessions
    ScopedSession.remove()
