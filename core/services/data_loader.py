import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, time
import pytz
from typing import Optional
import logging

from ..db.models.store_status import StoreStatus
from ..db.models.store_business_hours import StoreBusinessHours
from ..db.models.store_timezone import StoreTimezone

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, db: Session, batch_size: int = 5000):
        self.db = db
        self.batch_size = batch_size  
        self.engine = db.bind 

    def _log_progress(self, processed: int, total: int, batch_num: int):

        percentage = (processed / total) * 100
        logger.info(f"processed batch  {percentage}%")
    
    # def store_status_data(self, csv_path):
        
    #     logger.info(f"upload from {csv_path}")        
        
    #     df = pd.read_csv(csv_path)
    #     total_records = len(df)
    #     logger.info(f"Found {total_records} records")
       
    #     df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
    #     df['store_id'] = df['store_id'].astype(str)
        
    #     with self.engine.connect() as conn:
    #         conn.execute(text("TRUNCATE TABLE store_status"))
    #         conn.commit()
        
    #     # pandas -> sql for fsaterrr batch insertion
    #     df.to_sql('store_status', self.engine, if_exists='append', index=False, method='multi')        
    #     logger.info(f"upload completed: {total_records} records")
    #     return total_records
    
    def load_store_status(self, csv_path):
    
        logger.info(f"Starting store status upload from {csv_path}")
        
        df = pd.read_csv(csv_path)
        total_records = len(df)
        logger.info(f"Found {total_records} records")
        
        self.db.query(StoreStatus).delete()
        self.db.commit()
 
        df['timestamp_utc'] = pd.to_datetime(df['timestamp_utc'])
        df['store_id'] = df['store_id'].astype(str)
        
        records_loaded = 0
        batch_num = 0
        
        for start_idx in range(0, total_records, self.batch_size):
            batch_num += 1
            end_idx = min(start_idx + self.batch_size, total_records)
            #end_idx = total_records if start_idx + self.batch_size > total_records else start_idx + self.batch_size
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_data = []
            for _, row in batch_df.iterrows():
                batch_data.append({
                    'store_id': str(row['store_id']),
                    'timestamp_utc': row['timestamp_utc'],
                    'status': row['status']
                })
            
            
            self.db.bulk_insert_mappings(StoreStatus, batch_data)
            self.db.commit()
            
            records_loaded += len(batch_data)
            self._log_progress(records_loaded, total_records, batch_num)
        
        logger.info(f"Store status upload completed: {records_loaded} records")
        return records_loaded
    
    def load_business_hours(self, csv_path):
        logger.info(f"Starting business hours upload from {csv_path}")
        
        df = pd.read_csv(csv_path)
        total_records = len(df)
        logger.info(f"Found {total_records} records")
        
        self.db.query(StoreBusinessHours).delete()
        self.db.commit()
        
        records_loaded = 0
        batch_num = 0
        
        for start_idx in range(0, total_records, self.batch_size):
            batch_num += 1
            end_idx = min(start_idx + self.batch_size, total_records)
            #end_idx = total_records if start_idx + self.batch_size > total_records else start_idx + self.batch_size
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_data = []
            for _, row in batch_df.iterrows():
                start_time = datetime.strptime(row['start_time_local'], '%H:%M:%S').time()
                end_time = datetime.strptime(row['end_time_local'], '%H:%M:%S').time()
                
                batch_data.append({
                    'store_id': str(row['store_id']),
                    'day_of_week': int(row['dayOfWeek']),
                    'start_time_local': start_time,
                    'end_time_local': end_time
                })
            
            self.db.bulk_insert_mappings(StoreBusinessHours, batch_data)
            self.db.commit()
            
            records_loaded += len(batch_data)
            self._log_progress(records_loaded, total_records, batch_num)
        
        logger.info(f"Business hours upload completed: {records_loaded} records")
        return records_loaded
    
    def load_timezones(self, csv_path):
        logger.info(f"Starting timezones upload from {csv_path}")
        
        df = pd.read_csv(csv_path)
        total_records = len(df)
        logger.info(f"Found {total_records} records")
        
        self.db.query(StoreTimezone).delete()
        self.db.commit()
        
        records_loaded = 0
        batch_num = 0
        
        for start_idx in range(0, total_records, self.batch_size):
            batch_num += 1
            end_idx = min(start_idx + self.batch_size, total_records)
            batch_df = df.iloc[start_idx:end_idx]
            
            batch_data = []
            for _, row in batch_df.iterrows():
                batch_data.append({
                    'store_id': str(row['store_id']),
                    'timezone_str': row['timezone_str']
                })
            
            self.db.bulk_insert_mappings(StoreTimezone, batch_data)
            self.db.commit()
            
            records_loaded += len(batch_data)
            self._log_progress(records_loaded, total_records, batch_num)
        
        logger.info(f"Timezones upload completed: {records_loaded} records")
        return records_loaded
    
     