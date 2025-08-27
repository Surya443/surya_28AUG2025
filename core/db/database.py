from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_tables():   
    
    try:
        from .models.store_status import StoreStatus
        from .models.store_business_hours import StoreBusinessHours
        from .models.store_timezone import StoreTimezone
        
        logger.info(f"tables: {list(Base.metadata.tables.keys())}")
        
        Base.metadata.create_all(bind=engine)
        logger.info("Tables created successfully!")
            
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise 