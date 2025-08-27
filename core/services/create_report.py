from __future__ import annotations
import os
import csv
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, time, timezone
from typing import List, Optional, Tuple, Dict, Union
from functools import lru_cache


import pandas as pd
from dateutil import parser
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from ..db.models.store_timezone import StoreTimezone
from ..db.models.store_business_hours import StoreBusinessHours
from ..db.models.store_status import StoreStatus

import os       

DB_URL = os.getenv("DATABASE_URL")

engine = create_engine(
    DB_URL, 
    future=True,
    poolclass=QueuePool,
    pool_size=10,           # No of connections to maintain
    max_overflow=20,        # when pool is full
    pool_timeout=30,        
)

SessionLocal = sessionmaker(bind=engine, future=True)   
#connpoo

@dataclass
class StatusObservation:
    utc_time: datetime
    local_time: datetime
    status: str
    day: int

@dataclass
class BusinessHours:
    day: int
    start_time: time
    end_time: time
    crosses_midnight: bool = False

@dataclass
class UptimeStats:
    uptime_minutes: float
    downtime_minutes: float
    total_business_minutes: float


class TimeHandler:        
    def __init__(self, session):
        self.db = session
        # use redis for caching?
        #use LRU fro local
        # self.tz_cache = {}
        # self.hours_cache = {}
    
    @lru_cache(maxsize=1000)  
    def get_timezone(self, store_id):       
        tz_row = self.db.query(StoreTimezone).filter(StoreTimezone.store_id == store_id).first()            
        if tz_row:
            return tz_row.timezone_str
        else:
            return "America/Chicago"
    
    @lru_cache(maxsize=1000)
    def get_business_hours(self, store_id):
        hours = self.db.query(StoreBusinessHours).filter(StoreBusinessHours.store_id == store_id).all()
        
        if hours:
            business_hours = []
            for hour in hours:
                crosses_midnight = hour.end_time_local < hour.start_time_local
                business_hours.append(BusinessHours(
                    day=hour.day_of_week,
                    start_time=hour.start_time_local,
                    end_time=hour.end_time_local,
                    crosses_midnight=crosses_midnight
                ))

            # Fill missing days with 24/7 hours
            existing_days = {i.day for i in business_hours}
            for i in range(7):
                if i not in existing_days:
                    business_hours.append(BusinessHours(
                        day=i,
                        start_time=time(0, 0),
                        end_time=time(23, 59, 59),
                        crosses_midnight=False
                    ))
            return business_hours
        else:
            return [
                BusinessHours(
                    day=i,
                    start_time=time(0, 0),
                    end_time=time(23, 59, 59),
                    crosses_midnight=False
                ) for i in range(7)
            ]
    
    def utc_to_local(self, utc_timestamp, store_id):
        
        timezone_str = self.get_timezone(store_id)
        local_tz = ZoneInfo(timezone_str)
        
        if utc_timestamp.tzinfo is None:
            utc_timestamp = utc_timestamp.replace(tzinfo=timezone.utc)
        elif utc_timestamp.tzinfo != timezone.utc:
            utc_timestamp = utc_timestamp.astimezone(timezone.utc)        
        return utc_timestamp.astimezone(local_tz)
    
    def day_of_week(self, local_timestamp):
        return local_timestamp.weekday()
    
    def is_within_business_hours(self, local_timestamp, business_hours):
        #note:we are not calling this function once for a giveb storeId's  time
        #but rather each given time is compared with all the business hours for that day
        given_time = local_timestamp.time()
        
        if not business_hours.crosses_midnight:
            return business_hours.start_time <= given_time <= business_hours.end_time
        else:
            return given_time >= business_hours.start_time or given_time <= business_hours.end_time
    
    def get_hours_for_day(self, store_id, day_of_week):
        hours = self.get_business_hours(store_id)
        return [h for h in hours if h.day == day_of_week]
    
    def filter_by_business_hours(self, hours,store_id):
        filtered = []
        
        for obs in hours:
            day_hours = self.get_hours_for_day(store_id, obs.day)
            
            # check if this time falls within ANY business interval for the day
            found_match = False
            for interval in day_hours:
                if self.is_within_business_hours(obs.local_time, interval):
                    found_match = True
                    break  
            
            if found_match:
                filtered.append(obs)
        
        return filtered
    
    def minutes(self, store_id, start_local,end_local):
        
        total_minutes = 0.0
        current = start_local.replace(hour=0, minute=0, second=0, microsecond=0)
        
        while current.date() <= end_local.date():
            day_of_week = self.day_of_week(current)
            day_business_hours = self.get_hours_for_day(store_id, day_of_week)
            
            for bh in day_business_hours:
                day_start = current.replace(
                    hour=bh.start_time.hour,
                    minute=bh.start_time.minute,
                    second=bh.start_time.second
                )
                
                if bh.crosses_midnight:
                    # Handle midnight crossing
                    day_end = (current + timedelta(days=1)).replace(
                        hour=bh.end_time.hour,
                        minute=bh.end_time.minute,
                        second=bh.end_time.second
                    )
                else:
                    day_end = current.replace(
                        hour=bh.end_time.hour,
                        minute=bh.end_time.minute,
                        second=bh.end_time.second
                    )
                
                #period window
                period_start = max(day_start, start_local)
                period_end = min(day_end, end_local)
                
                if period_start < period_end:
                    minutes = (period_end - period_start).total_seconds() / 60
                    total_minutes += minutes
            
            current += timedelta(days=1)
        
        return total_minutes
    
    def calc_uptime_downtime(self, observations, store_id, start_local, end_local):
        
        total_biz_mins = self.minutes(store_id, start_local, end_local)
        
        if total_biz_mins == 0:
            return UptimeStats(
                uptime_minutes=0.0,
                downtime_minutes=0.0,
                total_business_minutes=0.0
            )
        
        if not observations:
            # no data = assume everything is down
            return UptimeStats(
                uptime_minutes=0.0,
                downtime_minutes=total_biz_mins,
                total_business_minutes=total_biz_mins
            )
        
        observations.sort(key=lambda x: x.local_time)
        
        # single observation case
        if len(observations) == 1:
            obs = observations[0]            
            
            prev_obs = self._get_previous_observation(store_id, obs.local_time)
            #note here prev observation need not be in business hours , we purely just consider the prev obs            

            if prev_obs:
                if prev_obs.status == obs.status:
                    if obs.status.lower() == 'active':
                        return UptimeStats(
                            uptime_minutes=total_biz_mins,
                            downtime_minutes=0.0,
                            total_business_minutes=total_biz_mins
                        )
                    else:
                        return UptimeStats(
                            uptime_minutes=0.0,
                            downtime_minutes=total_biz_mins,
                            total_business_minutes=total_biz_mins
                        )
                else:
                    mins_before = self.minutes(store_id, start_local, obs.local_time)
                    mins_after = total_biz_mins - mins_before
                    
                    if obs.status.lower() == 'active':
                        return UptimeStats(
                            uptime_minutes=mins_after,
                            downtime_minutes=mins_before,
                            total_business_minutes=total_biz_mins
                        )
                    else:
                        return UptimeStats(
                            uptime_minutes=mins_before,
                            downtime_minutes=mins_after,
                            total_business_minutes=total_biz_mins
                        )
            else:
                # no previous context - assume entire period has the observed status
                if obs.status.lower() == 'active':
                    return UptimeStats(
                        uptime_minutes=total_biz_mins,
                        downtime_minutes=0.0,
                        total_business_minutes=total_biz_mins
                    )
                else:
                    return UptimeStats(
                        uptime_minutes=0.0,
                        downtime_minutes=total_biz_mins,
                        total_business_minutes=total_biz_mins
                    )
        
        uptime_minutes = 0.0
        downtime_minutes = 0.0
        intervals = []
        
        # Before first observation: assume state of first observation
        first_obs = observations[0]
        if start_local < first_obs.local_time:
            intervals.append({
                'start': start_local,
                'end': first_obs.local_time,
                'status': first_obs.status
            })
        
        # Between observations - each observation defines status until next observation
        for i in range(len(observations)):
            current_obs = observations[i]
            
            # Determine when this observation's status starts
            obs_start = max(start_local, current_obs.local_time)
            
            # Determine when this observation's status ends
            if i < len(observations) - 1:
                # Status lasts until next observation
                obs_end = min(observations[i + 1].local_time, end_local)
            else:
                # Last observation: status lasts until end of period
                obs_end = end_local
            
            # Only add interval if it has positive duration
            if obs_start < obs_end:
                intervals.append({
                    'start': obs_start,
                    'end': obs_end,
                    'status': current_obs.status
                })
        
        # Calculate uptime/downtime interval-wise
        for interval in intervals:
            interval_minutes = self.minutes(
                store_id, interval['start'], interval['end']
            )
            
            if interval['status'].lower() == 'active':
                uptime_minutes += interval_minutes
            else:
                downtime_minutes += interval_minutes
        
        return UptimeStats(
            uptime_minutes=uptime_minutes,
            downtime_minutes=downtime_minutes,
            total_business_minutes=total_biz_mins
        )
    
    def _get_previous_observation(self, store_id, before_time):       
       
        previous_observation = self.db.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc < before_time.astimezone(timezone.utc)
        ).order_by(StoreStatus.timestamp_utc.desc()).first()
        
        if not previous_observation:
            return None
        
        local_time = self.utc_to_local(previous_observation.timestamp_utc, store_id)
        
        return StatusObservation(
            utc_time=previous_observation.timestamp_utc,
            local_time=local_time,
            status=previous_observation.status,
            day=local_time.weekday()
        )
    
    def process_store_observations(self, store_id, utc_timestamps, statuses):
        obs = []
        
        for utc_ts, status in zip(utc_timestamps, statuses):
            local_ts = self.utc_to_local(utc_ts, store_id)
            day_of_week = self.day_of_week(local_ts)
            
            obs.append(StatusObservation(
                utc_time=utc_ts,
                local_time=local_ts,
                status=status,
                day=day_of_week
            ))
        
        return obs
    
    def calculate_store_metrics(self, store_id, reference_time_utc):
        
        # Get store observations from database
        hour_ago = reference_time_utc - timedelta(hours=1)
        day_ago = reference_time_utc - timedelta(days=1)
        week_ago = reference_time_utc - timedelta(weeks=1)
        
        # Query all observations for the store in the last week
        week_observations = self.db.query(StoreStatus).filter(
            StoreStatus.store_id == store_id,
            StoreStatus.timestamp_utc >= week_ago,
            StoreStatus.timestamp_utc <= reference_time_utc
        ).order_by(StoreStatus.timestamp_utc).all()
        
        if not week_observations:
            return {
                'uptime_last_hour': 0.0,
                'uptime_last_day': 0.0,
                'uptime_last_week': 0.0,
                'downtime_last_hour': 0.0,
                'downtime_last_day': 0.0,
                'downtime_last_week': 0.0
            }
        
        
        observations = self.process_store_observations(
            store_id,
            [obs.timestamp_utc for obs in week_observations],
            [obs.status for obs in week_observations]
        )
        
        # Filter by business hours
        business_observations = self.filter_by_business_hours(observations, store_id)
        
        # Convert reference time to local
        reference_local = self.utc_to_local(reference_time_utc, store_id)
        hour_ago_local = self.utc_to_local(hour_ago, store_id)
        day_ago_local = self.utc_to_local(day_ago, store_id)
        week_ago_local = self.utc_to_local(week_ago, store_id)
        
        # Calculate metrics for each period
        hour_result = self.calc_uptime_downtime(
            [obs for obs in business_observations if obs.local_time >= hour_ago_local],
            store_id, hour_ago_local, reference_local
        )
        
        day_result = self.calc_uptime_downtime(
            [obs for obs in business_observations if obs.local_time >= day_ago_local],
            store_id, day_ago_local, reference_local
        )
        
        week_result = self.calc_uptime_downtime(
            business_observations,
            store_id, week_ago_local, reference_local
        )
        
        return {
            'uptime_last_hour': hour_result.uptime_minutes,
            'uptime_last_day': day_result.uptime_minutes / 60.0,  # Convert to hours
            'uptime_last_week': week_result.uptime_minutes / 60.0,  # Convert to hours
            'downtime_last_hour': hour_result.downtime_minutes,
            'downtime_last_day': day_result.downtime_minutes / 60.0,  # Convert to hours
            'downtime_last_week': week_result.downtime_minutes / 60.0  # Convert to hours
        }






