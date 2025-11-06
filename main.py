from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from db import fetch_calls_ending_in_each_call_stage_stats, fetch_carrier_asked_transfer_over_total_transfer_attempts_stats, fetch_carrier_asked_transfer_over_total_call_attempts_stats,fetch_load_not_found_stats, fetch_successfully_transferred_for_booking_stats
from typing import Optional
import os
from pathlib import Path

# Load environment variables from .env file
# Get the directory where this file is located
env_path = Path(__file__).parent / '.env'
print(f"[DEBUG] Looking for .env file at: {env_path}")
print(f"[DEBUG] .env file exists: {env_path.exists()}")

# Load the .env file
result = load_dotenv(dotenv_path=env_path)
print(f"[DEBUG] load_dotenv() result: {result}")

# Verify some env vars after loading
if env_path.exists():
    print(f"[DEBUG] After load_dotenv - CLICKHOUSE_HOST: {os.getenv('CLICKHOUSE_HOST', 'NOT SET')}")
else:
    print(f"[WARNING] .env file not found at {env_path}")
    print(f"[WARNING] Current working directory: {os.getcwd()}")
    print("[WARNING] Trying to load from current directory...")
    load_dotenv()  # Fallback to default behavior

app = FastAPI(
    title="Pepsi Weekly Analytics API",
    description="API server for PepsiCo weekly analytics",
    version="1.0.0"
)

# Configure CORS
# Parse ALLOWED_EMBED_ORIGINS from environment variable (comma-separated list)
allowed_origins_str = os.getenv("ALLOWED_EMBED_ORIGINS", "*")
if allowed_origins_str == "*":
    allowed_origins = ["*"]
else:
    # Split comma-separated origins and strip whitespace
    allowed_origins = [origin.strip() for origin in allowed_origins_str.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Welcome to Pepsi Weekly Analytics API"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.get("/call-stage-stats")
async def get_call_stage_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get call stage stats"""
    try:
        results = fetch_calls_ending_in_each_call_stage_stats(start_date, end_date)
        # Convert dataclass objects to dictionaries for JSON serialization
        return [{"call_stage": r.call_stage, "count": r.count, "percentage": r.percentage} for r in results]
    except Exception as e:
        # Log the error and return a proper HTTP error response
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_call_stage_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching call stage stats: {str(e)}")

@app.get("/carrier-asked-transfer-over-total-transfer-attempts-stats")
async def get_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get carrier asked transfer over total transfer attempts stats"""
    try:
        result = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No carrier asked transfer over total transfer attempts stats found")
        # Convert dataclass object to dictionary for JSON serialization
        return {
            "carrier_asked_count": result.carrier_asked_count,
            "total_transfer_attempts": result.total_transfer_attempts,
            "carrier_asked_percentage": result.carrier_asked_percentage
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_carrier_asked_transfer_over_total_transfer_attempts_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching carrier asked transfer over total transfer attempts stats: {str(e)}")

@app.get("/carrier-asked-transfer-over-total-call-attempts-stats")
async def get_carrier_asked_transfer_over_total_call_attempts_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get carrier asked transfer over total call attempts stats"""
    try:
        result = fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No carrier asked transfer over total call attempts stats found")
        # Convert dataclass object to dictionary for JSON serialization
        return {
            "carrier_asked_count": result.carrier_asked_count,
            "total_call_attempts": result.total_call_attempts,
            "carrier_asked_percentage": result.carrier_asked_percentage
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_carrier_asked_transfer_over_total_call_attempts_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching carrier asked transfer over total call attempts stats: {str(e)}")

@app.get("/load-not-found-stats")
async def get_load_not_found_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get load not found stats"""
    try:
        result = fetch_load_not_found_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No load not found stats found")
        # Convert dataclass object to dictionary for JSON serialization
        return {
            "load_not_found_count": result.load_not_found_count,
            "total_calls": result.total_calls,
            "load_not_found_percentage": result.load_not_found_percentage
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_load_not_found_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching load not found stats: {str(e)}")

@app.get("/successfully-transferred-for-booking-stats")
async def get_successfully_transferred_for_booking_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get successfully transferred for booking stats"""
    try:
        result = fetch_successfully_transferred_for_booking_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No successfully transferred for booking stats found")
        # Convert dataclass object to dictionary for JSON serialization
        return {
            "successfully_transferred_for_booking_count": result.successfully_transferred_for_booking_count,
            "total_calls": result.total_calls,
            "successfully_transferred_for_booking_percentage": result.successfully_transferred_for_booking_percentage
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_successfully_transferred_for_booking_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching successfully transferred for booking stats: {str(e)}")