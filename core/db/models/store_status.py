from sqlalchemy import Column, Integer, String, TIMESTAMP, PrimaryKeyConstraint
from ..database import Base

class StoreStatus(Base):

    __tablename__ = "store_status"

    store_id = Column(Integer, nullable=False)
    timestamp_utc = Column(TIMESTAMP(timezone=True), nullable=False)  # always UTC
    status = Column(String(10), nullable=False)  
    #again no single column alone is unique, but their combination is, so.....
    __table_args__ = (PrimaryKeyConstraint("store_id", "timestamp_utc"),)
