from sqlalchemy import Column, Integer, String, Time, PrimaryKeyConstraint
from ..database import Base

class StoreBusinessHours(Base):

    __tablename__ = "store_business_hours"

    store_id = Column(String, nullable=False)
    day_of_week = Column(Integer, nullable=False) 
    start_time_local = Column(Time, nullable=False)
    end_time_local = Column(Time, nullable=False)
    #no single column alone is unique, but their combination is, so.....
    __table_args__ = (PrimaryKeyConstraint("store_id", "day_of_week", "start_time_local", "end_time_local"),)
