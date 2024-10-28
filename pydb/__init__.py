from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from typing import TypeVar, Generic, List, Optional
from pydb.config import DATABASE_URL
from pydb.logger import logger

ModelType = TypeVar('ModelType', bound='BaseModel')

engine = create_engine(DATABASE_URL)
Base = declarative_base()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db() -> Session:
    """Dependency that provides a database session.

    Yields:
        Session: A SQLAlchemy session for database operations.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def init_db(model=None) -> None:
    """Initialize the database by creating all tables."""
    if model:
        model.__table__.create(bind=engine)
    else:
        Base.metadata.create_all(bind=engine)
