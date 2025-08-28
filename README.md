# **Setting up** 
1. Pull this Repo
2. Initialize UV
``` bash
init uv
 ```
sync uv packages
```bash
sync uv
```
4 . Crete/Activate  venv
5. Add packages
```bash
uv add -r requirements.txt
```
6. Configure .env file . It should have your postgresql URL
```bash
DATABASE_URL = ' '
```

5. Run main.py [have to add this as docler container]
```bash
python main.py
```
6. Docker compose up for prometheus and grafana services
```bash
docker compose up -d
```

# **API Structure:**
```
1. /trigger_report : triggers report generation
2. /get_report : makes generated report available as csv and also gives the status on report generation(completed/running)
3. /upload_store_status: endpoint for uploading 'store_status' csv
4. /upload_business_hours : endpoint for uploading 'menu_hours' csv
5. /upload_timezone : endpoint for uploading 'timezone' csv
6. /metrics : converts report csv file contents to prometheus query which in turn is connected to grafana dashboard
```



## Assumptions :
1. Took the most recent time stamp from 'store_status' csv file as present time.
2. 'store_status' data is not static ..so created an endpoint to upload recent store_status data (other file upload endpoints also in api for easy data changing).

## Edge Cases Handled:
(Did simple EDA to better understand data)
1. **Midnight crossing business hours**

    Say a store's business hour is from 11 pm to 3 am.
    it takes 11pm to 11:55:55:99 pm as that days business hour and 12:00:00 asm to 3 am as next day's business hr
2. Observations **outside business hours (ignored)** 
3. **Missing timezone data** (defaults to America/Chicago)
4. Missing business hours data (defaults to 24/7) **[missing business hours for a store on a particular day is empty ? Its 24/7 work hour for that company on that day alone.]**
5. **Multiple business hour periods** per day (create intervals)

# **Uptime/Dowtime Interpolation Logic:**

 - Lets imagine we have a store with working hours : **9am - 6pm**  on a particular day

 - And then we get two status observation/shearbeats at 
 ```
    9:45 am -> active
	   12:00 pm -> inactive
```
 - Now the way we approach to calculate uptime and downtime is simple:
   -  Time interval after a particular heartbeat/observation is going to have the status of that observation until we get a new observation inside business hours.


- which mean its safe to say 
```
9:45 a.m. - 12:00 p.m. -> active
12:00 p.m - 6:00 p.m   -> inactive
```

- but what about the interval **9:00 a.m to 9:45 a.m.** [time interval between start business hour to first status heartbeat/obs]?

To handle this we consider the **observation what we got before 9:45 a.m**.(lets say at 7:00 a.m. we  got inactive status)
```
7:00 a.m. -> inactive
```
and then assign that status to time interval uptil 9:45 a.m.

**[note that the previous observation that we are considering here doesn't have to be in business hour]**

 - So the final timeline graph would like this:
<pre> 

                 |--inactive--|        |------active--------|          |-----inactive-----|
|===============||============[9:45 a.m]====================[12:00 p.m.]==================||6 p.m.
7:00 am	      9:00 am
(inactive)          	       (active)                      (inactive)
</pre>

```
final uptime -> 2hrs 15 mins
final downtime -> 6hrs 45 mins
```
**Lets see how we do this**
#### **Interpolation `calc_uptime_downtime()` walkthrough:**



#####  **1: Get Business Hours**
```python
total_biz_mins = self.minutes(store_id, start_local, end_local)

if total_biz_mins == 0:
    return UptimeStats(uptime_minutes=0.0, downtime_minutes=0.0, total_business_minutes=0.0)
```


##### **2: If No Data found store down for entire business hours**
```python
if not observations:
    return UptimeStats(
        uptime_minutes=0.0,
        downtime_minutes=total_biz_mins,
        total_business_minutes=total_biz_mins
    )
```


##### **Single Observation**
```python
if len(observations) == 1:
    obs = observations[0]
    prev_obs = self._get_previous_observation(store_id, obs.local_time)
    
    if prev_obs:
        if prev_obs.status == obs.status:
            # Same status throughout the period
            if obs.status.lower() == 'active':
                return UptimeStats(uptime_minutes=total_biz_mins, ...)
        else:
            # Different status - split the period
            mins_before = self.minutes(store_id, start_local, obs.local_time)
            mins_after = total_biz_mins - mins_before
            
            if obs.status.lower() == 'active':
                return UptimeStats(
                    uptime_minutes=mins_after,      # After observation
                    downtime_minutes=mins_before,   # Before observation
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
```

**Logic:**
- Most of the stores given has only one observation per day
- We look up for previous observation (even outside business hours)
- If same status: entire period has that status
- If different status: splits period at observation time.Interval before present observation time will have previous observation status and interval after present observation will have will have present observation's status
- If we get no prev status, entire day takes that observation's status

##### **4: Multiple Observations**
```python
# Handle time before first observation
first_obs = observations[0]
if start_local < first_obs.local_time:
    prev_obs = self._get_previous_observation(store_id, first_obs.local_time)
    
    if prev_obs:
        if prev_obs.status == first_obs.status:
            before_status = first_obs.status
        else:
            before_status = prev_obs.status
    else:
        before_status = first_obs.status
```

**Creates time intervals:**
```python
for i in range(len(observations)):
    current_obs = observations[i]
    obs_start = max(start_local, current_obs.local_time)
    
    if i < len(observations) - 1:
        obs_end = min(observations[i + 1].local_time, end_local)
    else:
        obs_end = end_local
    
    intervals.append({
        'start': obs_start,
        'end': obs_end,
        'status': current_obs.status
    })
```

**Calculates totals:**
```python
for interval in intervals:
    interval_minutes = self.minutes(store_id, interval['start'], interval['end'])
    
    if interval['status'].lower() == 'active':
        uptime_minutes += interval_minutes
    else:
        downtime_minutes += interval_minutes
```


# **Performance Improvements Done:**
1. Batching database read that reduced >1 Million  table entries time from
30+ mins to ~ 2 mins
2. Implemented LRU caching for db reads that involve getting business hours and time zone for each store
3. Connection pool added in SQLalchemy engine intialization

# **Improvements that can be done**
 - Employ Machine Learning Algorithms to better predict downtime and uptime in a day's business hours(kind of overkill but does the job)
 - Create a MCP server that connects to these endpoints and use any LLM as client that would help us answer questions based on the report generated.
 - Use of DuckDB for faster analysis and read/write (compatible with pandas and SQL)
 - Use of Redis cache in create_report.py to make it more production standard