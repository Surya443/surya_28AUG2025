from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import StreamingResponse, PlainTextResponse  
from sqlalchemy.orm import Session
from typing import List, Optional, Dict
import io
import tempfile
import os
import uuid
import csv
from datetime import datetime, timezone
from enum import Enum
import logging
from ..db.database import get_db
from ..db.models.store_status import StoreStatus
from ..services.create_report import TimeHandler
from ..services.data_loader import DataLoader

router = APIRouter()

logger = logging.getLogger(__name__)

# can use redis here
class ReportStatus(Enum):
    RUNNING = "Running"
    COMPLETE = "Complete"
    ERROR = "Error"

# Global storage for reports
reports_storage: Dict[str, Dict] = {}

def current_time(db):    
    result = db.query(StoreStatus.timestamp_utc).order_by( StoreStatus.timestamp_utc.desc()).first()
    
    if result and result[0]:
        timestamp = result[0]
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp
    else:
        return datetime.now(timezone.utc)

def generate_csv(report_data):
    
    output = io.StringIO()
    
    fieldnames = ['store_id','uptime_last_hour','uptime_last_day','uptime_last_week',
    'downtime_last_hour','downtime_last_day','downtime_last_week']
    
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    
    for row in report_data:
        writer.writerow(row)    
    return output.getvalue()

@router.post("/trigger_report")
async def trigger_report(db: Session = Depends(get_db)):
    
    try:    
        report_id = str(uuid.uuid4())
        logger.info(f"Report ID: {report_id}")
        
        reports_storage[report_id] = {
            "status": ReportStatus.RUNNING,
            "created_at": datetime.utcnow(),
            "csv_data": None,
            "error": None
        }

        reference_time = current_time(db)
        store_ids = [row[0] for row in db.query(StoreStatus.store_id).distinct().all()]
        
        try:
            time_handler = TimeHandler(db)
            report_data = []            
            for store_id in store_ids:
                try:
                    metrics = time_handler.calculate_store_metrics(store_id, reference_time)

                    report_row = {
                        "store_id": store_id,
                        "uptime_last_hour": round(metrics.get('uptime_last_hour', 0.0), 2),
                        "uptime_last_day": round(metrics.get('uptime_last_day', 0.0), 2),
                        "uptime_last_week": round(metrics.get('uptime_last_week', 0.0), 2),
                        "downtime_last_hour": round(metrics.get('downtime_last_hour', 0.0), 2),
                        "downtime_last_day": round(metrics.get('downtime_last_day', 0.0), 2),
                        "downtime_last_week": round(metrics.get('downtime_last_week', 0.0), 2)
                    }
                    report_data.append(report_row)
                    
                except Exception as e:
                    report_row = {
                        "store_id": store_id,
                        "uptime_last_hour": 0.0,
                        "uptime_last_day": 0.0,
                        "uptime_last_week": 0.0,
                        "downtime_last_hour": 0.0,
                        "downtime_last_day": 0.0,
                        "downtime_last_week": 0.0
                    }
                    report_data.append(report_row)
                    print(f"Error calculating metrics for store {store_id}: {str(e)}")
            
            # Generate CSV
            csv_data = generate_csv(report_data)
            
            # Update report status to complete
            reports_storage[report_id]["status"] = ReportStatus.COMPLETE
            reports_storage[report_id]["csv_data"] = csv_data
            reports_storage[report_id]["completed_at"] = datetime.utcnow()
            reports_storage[report_id]["total_stores"] = len(report_data)
            
        except Exception as e:
            # Update report status to error
            reports_storage[report_id]["status"] = ReportStatus.ERROR
            reports_storage[report_id]["error"] = str(e)
            print(f"Error generating report {report_id}: {str(e)}")
        
        return {"report_id": report_id}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to trigger report: {str(e)}")

@router.get("/get_report")
async def get_report(report_id: str):
    try:
        if report_id not in reports_storage:
            raise HTTPException(status_code=404, detail="Report not found")
        
        report = reports_storage[report_id]
        
        print(f"Report {report_id} current status: {report['status'].value}")
        
        if report["status"] == ReportStatus.COMPLETE:
            csv_data = report["csv_data"]
            
            print(f"Report {report_id} response: CSV file download")
            return StreamingResponse(
                io.StringIO(csv_data),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename=store_report_{report_id}.csv"}
            )       
        else:            
            return {"status": "Running"}
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get report: {str(e)}")

@router.post("/upload_store_status")
async def upload_store_status(file: UploadFile = File(...), db: Session = Depends(get_db)):
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a valid CSV")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name

        loader = DataLoader(db)
        records_loaded = loader.load_store_status(temp_file_path)

        os.unlink(temp_file_path)        
        return {
            "message": "Store status data uploaded successfully",
            "filename": file.filename,
            "records_loaded": records_loaded
        }
        
    except Exception as e:        
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to upload store status: {str(e)}")

@router.post("/upload_business_hours")
async def upload_business_hours(file: UploadFile = File(...), db: Session = Depends(get_db)):
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        loader = DataLoader(db)
        records_loaded = loader.load_business_hours(temp_file_path)
        
        os.unlink(temp_file_path)        
        return {
            "message": "Business hours data uploaded successfully",
            "filename": file.filename,
            "records_loaded": records_loaded
        }
        
    except Exception as e:
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to upload business hours: {str(e)}")

@router.post("/upload_timezones")
async def upload_timezones(file: UploadFile = File(...), db: Session = Depends(get_db)):
    
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as temp_file:
            content = await file.read()
            temp_file.write(content)
            temp_file_path = temp_file.name
        
        loader = DataLoader(db)
        records_loaded = loader.load_timezones(temp_file_path)
        
        os.unlink(temp_file_path)        
        return {
            "message": "Timezones data uploaded successfully",
            "filename": file.filename,
            "records_loaded": records_loaded
        }
        
    except Exception as e:
        if 'temp_file_path' in locals():
            try:
                os.unlink(temp_file_path)
            except:
                pass
        raise HTTPException(status_code=500, detail=f"Failed to upload timezones: {str(e)}")

@router.get("/metrics", response_class=PlainTextResponse)
async def metrics_endpoint():
    lines = []

    for report_id, report in reports_storage.items():
        if report["status"] != ReportStatus.COMPLETE:
            continue

        import csv
        from io import StringIO
        csv_reader = csv.DictReader(StringIO(report["csv_data"]))
        for row in csv_reader:
            store_id = row["store_id"]
            
            lines.append(f'store_uptime_hours{{store_id="{store_id}",period="last_hour"}} {row["uptime_last_hour"]}')
            lines.append(f'store_uptime_hours{{store_id="{store_id}",period="last_day"}} {row["uptime_last_day"]}')
            lines.append(f'store_uptime_hours{{store_id="{store_id}",period="last_week"}} {row["uptime_last_week"]}' )
            
            lines.append(f'store_downtime_hours{{store_id="{store_id}",period="last_hour"}} {row["downtime_last_hour"]}')
            lines.append(f'store_downtime_hours{{store_id="{store_id}",period="last_day"}} {row["downtime_last_day"]}')
            lines.append(f'store_downtime_hours{{store_id="{store_id}",period="last_week"}} {row["downtime_last_week"]}')

    return "\n".join(lines) + "\n"