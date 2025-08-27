from fastapi import FastAPI
import uvicorn

from core.db.database import create_tables
from core.routes.endpoints import router

app = FastAPI(title="Store Monitoring API")

app.include_router(router, prefix="/api/v1")

@app.on_event("startup")
async def startup():
    create_tables()

@app.get("/")
async def root():
    return {
        "message": "Store Monitoring API",
        "endpoints": {
            "trigger_report": "/api/v1/trigger_report",
            "get_report": "/api/v1/get_report?report_id=<id>",
            "load_data": "/api/v1/load_data",
            "upload_store_status": "/api/v1/upload_store_status",
            "upload_business_hours": "/api/v1/upload_business_hours", 
            "upload_timezones": "/api/v1/upload_timezones",  
            "metrics": "/api/v1/metrics"          
        }
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
