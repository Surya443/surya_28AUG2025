from sqlalchemy import Column, Integer, String
from ..database import Base

class StoreTimezone(Base):
    
    __tablename__ = "store_timezones"

    store_id = Column(Integer, primary_key=True, nullable=False)
    timezone_str = Column(String, nullable=False, default="America/Chicago")
