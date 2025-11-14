from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from db import fetch_calls_ending_in_each_call_stage_stats, fetch_carrier_asked_transfer_over_total_transfer_attempts_stats, fetch_carrier_asked_transfer_over_total_call_attempts_stats,fetch_load_not_found_stats, fetch_load_status_stats, fetch_successfully_transferred_for_booking_stats, fetch_call_classifcation_stats, fetch_carrier_qualification_stats, fetch_pricing_stats, fetch_carrier_end_state_stats, fetch_percent_non_convertible_calls, fetch_number_of_unique_loads
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

@app.get("/load-status-stats")
async def get_load_status_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get load status stats"""
    try:
        result = fetch_load_status_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No load status stats found")
        return [{"load_status": r.load_status, "count": r.count, "total_calls": r.total_calls, "load_status_percentage": r.load_status_percentage} for r in result]
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_load_status_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching load status stats: {str(e)}")

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

@app.get("/call-classification-stats")
async def get_call_classification_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get call classification stats"""
    try:
        results = fetch_call_classifcation_stats(start_date, end_date)
        return [{"call_classification": r.call_classification, "count": r.count, "percentage": r.percentage} for r in results]
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_call_classification_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching call classification stats: {str(e)}")

@app.get("/carrier-qualification-stats")
async def get_carrier_qualification_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get carrier qualification stats"""
    try:
        results = fetch_carrier_qualification_stats(start_date, end_date)
        return [{"carrier_qualification": r.carrier_qualification, "count": r.count, "percentage": r.percentage} for r in results]
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_carrier_qualification_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching carrier qualification stats: {str(e)}")


@app.get("/pricing-stats")
async def get_pricing_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get pricing stats"""
    try:
        results = fetch_pricing_stats(start_date, end_date)
        return [{"pricing_notes": r.pricing_notes, "count": r.count, "percentage": r.percentage} for r in results]
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_pricing_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching pricing stats: {str(e)}")

@app.get("/carrier-end-state-stats")
async def get_carrier_end_state_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get carrier end state stats"""
    try:
        results = fetch_carrier_end_state_stats(start_date, end_date)
        return [{"carrier_end_state": r.carrier_end_state, "count": r.count, "percentage": r.percentage} for r in results]
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_carrier_end_state_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching carrier end state stats: {str(e)}")

@app.get("/percent-non-convertible-calls-stats")
async def get_percent_non_convertible_calls_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get percent non convertible calls stats"""
    try:
        result = fetch_percent_non_convertible_calls(start_date, end_date)
        return {
            "non_convertible_calls_count": result.non_convertible_calls_count,
            "total_calls_count": result.total_calls_count,
            "non_convertible_calls_percentage": result.non_convertible_calls_percentage
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_percent_non_convertible_calls_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching percent non convertible calls stats: {str(e)}")

@app.get("/number-of-unique-loads-stats")
async def get_number_of_unique_loads_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get number of unique loads stats"""
    try:
        result = fetch_number_of_unique_loads(start_date, end_date)
        return {
            "number_of_unique_loads": result.number_of_unique_loads,
            "total_calls_count": result.total_calls_count,
            "number_of_unique_loads_percentage": result.number_of_unique_loads_percentage

        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_number_of_unique_loads_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching number of unique loads stats: {str(e)}")

@app.get("/all-stats")
async def get_all_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get all stats aggregated with labels"""
    import logging
    logger = logging.getLogger(__name__)
    
    stats = {}
    errors = {}
    
    # Call stage stats
    try:
        call_stage_results = fetch_calls_ending_in_each_call_stage_stats(start_date, end_date)
        stats["call_stage_stats"] = [{"call_stage": r.call_stage, "count": r.count, "percentage": r.percentage} for r in call_stage_results]
    except Exception as e:
        logger.exception("Error fetching call stage stats")
        errors["call_stage_stats"] = str(e)
        stats["call_stage_stats"] = None
    
    # Carrier asked transfer over total transfer attempts
    try:
        carrier_transfer_result = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
        if carrier_transfer_result:
            stats["carrier_asked_transfer_over_total_transfer_attempts"] = {
                "carrier_asked_count": carrier_transfer_result.carrier_asked_count,
                "total_transfer_attempts": carrier_transfer_result.total_transfer_attempts,
                "carrier_asked_percentage": carrier_transfer_result.carrier_asked_percentage
            }
        else:
            stats["carrier_asked_transfer_over_total_transfer_attempts"] = None
    except Exception as e:
        logger.exception("Error fetching carrier asked transfer over total transfer attempts stats")
        errors["carrier_asked_transfer_over_total_transfer_attempts"] = str(e)
        stats["carrier_asked_transfer_over_total_transfer_attempts"] = None
    
    # Carrier asked transfer over total call attempts
    try:
        carrier_call_result = fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date, end_date)
        if carrier_call_result:
            stats["carrier_asked_transfer_over_total_call_attempts"] = {
                "carrier_asked_count": carrier_call_result.carrier_asked_count,
                "total_call_attempts": carrier_call_result.total_call_attempts,
                "carrier_asked_percentage": carrier_call_result.carrier_asked_percentage
            }
        else:
            stats["carrier_asked_transfer_over_total_call_attempts"] = None
    except Exception as e:
        logger.exception("Error fetching carrier asked transfer over total call attempts stats")
        errors["carrier_asked_transfer_over_total_call_attempts"] = str(e)
        stats["carrier_asked_transfer_over_total_call_attempts"] = None
    
    # Load not found stats
    try:
        load_not_found_result = fetch_load_not_found_stats(start_date, end_date)
        if load_not_found_result:
            stats["load_not_found"] = {
                "load_not_found_count": load_not_found_result.load_not_found_count,
                "total_calls": load_not_found_result.total_calls,
                "load_not_found_percentage": load_not_found_result.load_not_found_percentage
            }
        else:
            stats["load_not_found"] = None
    except Exception as e:
        logger.exception("Error fetching load not found stats")
        errors["load_not_found"] = str(e)
        stats["load_not_found"] = None
    
    # Load status stats
    try:
        load_status_results = fetch_load_status_stats(start_date, end_date)
        if load_status_results:
            stats["load_status"] = [{"load_status": r.load_status, "count": r.count, "total_calls": r.total_calls, "load_status_percentage": r.load_status_percentage} for r in load_status_results]
        else:
            stats["load_status"] = None
    except Exception as e:
        logger.exception("Error fetching load status stats")
        errors["load_status"] = str(e)
        stats["load_status"] = None
    
    # Successfully transferred for booking stats
    try:
        transferred_result = fetch_successfully_transferred_for_booking_stats(start_date, end_date)
        if transferred_result:
            stats["successfully_transferred_for_booking"] = {
                "successfully_transferred_for_booking_count": transferred_result.successfully_transferred_for_booking_count,
                "total_calls": transferred_result.total_calls,
                "successfully_transferred_for_booking_percentage": transferred_result.successfully_transferred_for_booking_percentage
            }
        else:
            stats["successfully_transferred_for_booking"] = None
    except Exception as e:
        logger.exception("Error fetching successfully transferred for booking stats")
        errors["successfully_transferred_for_booking"] = str(e)
        stats["successfully_transferred_for_booking"] = None
    
    # Call classification stats
    try:
        call_classification_results = fetch_call_classifcation_stats(start_date, end_date)
        if call_classification_results:
            stats["call_classification"] = [{"call_classification": r.call_classification, "count": r.count, "percentage": r.percentage} for r in call_classification_results]
        else:
            stats["call_classification"] = None
    except Exception as e:
        logger.exception("Error fetching call classification stats")
        errors["call_classification"] = str(e)
        stats["call_classification"] = None
    
    # Carrier qualification stats
    try:
        carrier_qualification_results = fetch_carrier_qualification_stats(start_date, end_date)
        if carrier_qualification_results:
            stats["carrier_qualification"] = [{"carrier_qualification": r.carrier_qualification, "count": r.count, "percentage": r.percentage} for r in carrier_qualification_results]
        else:
            stats["carrier_qualification"] = None
    except Exception as e:
        logger.exception("Error fetching carrier qualification stats")
        errors["carrier_qualification"] = str(e)
        stats["carrier_qualification"] = None
    
    # Pricing stats
    try:
        pricing_results = fetch_pricing_stats(start_date, end_date)
        if pricing_results:
            stats["pricing"] = [{"pricing_notes": r.pricing_notes, "count": r.count, "percentage": r.percentage} for r in pricing_results]
        else:
            stats["pricing"] = None
    except Exception as e:
        logger.exception("Error fetching pricing stats")
        errors["pricing"] = str(e)
        stats["pricing"] = None

     # Carrier end state stats
    try:
        carrier_end_state_results = fetch_carrier_end_state_stats(start_date, end_date)
        if carrier_end_state_results:
            stats["carrier_end_state"] = [{"carrier_end_state": r.carrier_end_state, "count": r.count, "percentage": r.percentage} for r in carrier_end_state_results]
        else:
            stats["carrier_end_state"] = None
    except Exception as e:
        logger.exception("Error fetching carrier end state stats")
        errors["carrier_end_state"] = str(e)
        stats["carrier_end_state"] = None
    
    # Percent non convertible calls stats
    try:
        percent_non_convertible_calls_result = fetch_percent_non_convertible_calls(start_date, end_date)
        if percent_non_convertible_calls_result:
            stats["percent_non_convertible_calls"] = {
                "non_convertible_calls_count": percent_non_convertible_calls_result.non_convertible_calls_count,
                "total_calls_count": percent_non_convertible_calls_result.total_calls_count,
                "non_convertible_calls_percentage": percent_non_convertible_calls_result.non_convertible_calls_percentage
            }
        else:
            stats["percent_non_convertible_calls"] = None
    except Exception as e:
        logger.exception("Error fetching percent non convertible calls stats")
        errors["percent_non_convertible_calls"] = str(e)
        stats["percent_non_convertible_calls"] = None

    # Number of unique loads stats
    try:
        number_of_unique_loads_result = fetch_number_of_unique_loads(start_date, end_date)
        if number_of_unique_loads_result:
            stats["number_of_unique_loads"] = {
                "number_of_unique_loads": number_of_unique_loads_result.number_of_unique_loads,
                "total_calls": number_of_unique_loads_result.total_calls,
                "calls_per_unique_load": number_of_unique_loads_result.calls_per_unique_load
            }
        else:
            stats["number_of_unique_loads"] = None
    except Exception as e:
        logger.exception("Error fetching number of unique loads stats")
        errors["number_of_unique_loads"] = str(e)
        stats["number_of_unique_loads"] = None

    
    response = {
        "stats": stats,
        "date_range": {
            "start_date": start_date,
            "end_date": end_date
        }
    }
    
    if errors:
        response["errors"] = errors
    
    return response