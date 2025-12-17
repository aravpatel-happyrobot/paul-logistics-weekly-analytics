from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from db import fetch_calls_ending_in_each_call_stage_stats, fetch_carrier_asked_transfer_over_total_transfer_attempts_stats, fetch_carrier_asked_transfer_over_total_call_attempts_stats,fetch_load_not_found_stats, fetch_load_status_stats, fetch_successfully_transferred_for_booking_stats, fetch_call_classifcation_stats, fetch_carrier_qualification_stats, fetch_pricing_stats, fetch_carrier_end_state_stats, fetch_percent_non_convertible_calls, fetch_non_convertible_calls_with_carrier_not_qualified, fetch_non_convertible_calls_without_carrier_not_qualified, fetch_carrier_not_qualified_stats, fetch_number_of_unique_loads, fetch_list_of_unique_loads, fetch_calls_without_carrier_asked_for_transfer, fetch_total_calls_and_total_duration, fetch_duration_carrier_asked_for_transfer, fetch_daily_node_outputs, fetch_table_schema, fetch_node_output_counts, fetch_node_output_orgs
from typing import Optional, List
from pydantic import BaseModel
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
from contextlib import asynccontextmanager

# Storage and scheduler imports
from storage import (
    ensure_db_initialized,
    seed_default_organization,
    get_all_organizations,
    get_organization,
    create_organization,
    update_organization,
    upsert_organization,
    get_daily_report,
    get_latest_report,
    get_recent_reports,
    get_reports_in_range,
    get_all_report_dates,
    get_report_count,
    get_date_range,
    Organization,
    DailyReport,
)
from scheduler import (
    start_scheduler,
    stop_scheduler,
    trigger_daily_report_now,
    backfill_reports,
)

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# Load environment variables from `.env` if present (file is gitignored)
env_path = Path(__file__).parent / ".env"
load_dotenv(dotenv_path=env_path, override=False)

client_name = os.getenv("CLIENT_NAME", "Logistics")


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events."""
    # Startup
    logger.info("Starting up...")
    ensure_db_initialized()
    seed_default_organization()
    start_scheduler()
    logger.info("Startup complete")

    yield

    # Shutdown
    logger.info("Shutting down...")
    stop_scheduler()
    logger.info("Shutdown complete")


app = FastAPI(
    title=f"{client_name} Analytics API",
    description=f"API server for {client_name} analytics (ClickHouse-backed)",
    version="1.0.0",
    lifespan=lifespan,
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
    return {"message": f"Welcome to {client_name} Analytics API"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


@app.get("/debug-config")
async def debug_config():
    """
    Show the effective runtime configuration (no secrets).
    Useful when you're unsure what env vars are being picked up.
    """
    clickhouse_host = os.getenv("CLICKHOUSE_HOST")
    clickhouse_url = os.getenv("CLICKHOUSE_URL")
    clickhouse_user = os.getenv("CLICKHOUSE_USERNAME") or os.getenv("CLICKHOUSE_USER")
    clickhouse_db = os.getenv("CLICKHOUSE_DATABASE")
    clickhouse_secure = os.getenv("CLICKHOUSE_SECURE")

    return {
        "client_name": os.getenv("CLIENT_NAME"),
        "org_id_present": bool(os.getenv("ORG_ID")),
        "org_id_prefix": (os.getenv("ORG_ID") or "")[:8] if os.getenv("ORG_ID") else None,
        "broker_node_persistent_id": os.getenv("BROKER_NODE_PERSISTENT_ID"),
        "fbr_node_persistent_id": os.getenv("FBR_NODE_PERSISTENT_ID"),
        "excluded_user_numbers": [n.strip() for n in (os.getenv("EXCLUDED_USER_NUMBERS") or "").split(",") if n.strip()],
        "default_timezone": os.getenv("DEFAULT_TIMEZONE", "UTC"),
        "cors_allowed_embed_origins": os.getenv("ALLOWED_EMBED_ORIGINS", "*"),
        "clickhouse": {
            "url": clickhouse_url,
            "host": clickhouse_host,
            "database": clickhouse_db,
            "user": clickhouse_user,
            "secure": clickhouse_secure,
            "password_present": bool(os.getenv("CLICKHOUSE_PASSWORD")),
        },
        "env_file_loaded": env_path.exists(),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
    }


@app.get("/debug-schema/{table_name}")
async def debug_schema(table_name: str):
    """
    Show schema (column names/types) for key ClickHouse tables.
    This helps align queries when schemas differ across clients.
    """
    allowed = {
        "public_node_outputs",
        "public_runs",
        "public_sessions",
        "public_nodes",
    }
    if table_name not in allowed:
        raise HTTPException(status_code=400, detail=f"table_name must be one of: {sorted(list(allowed))}")
    try:
        return fetch_table_schema(table_name)
    except Exception as e:
        logger.exception("Error in debug_schema endpoint")
        raise HTTPException(status_code=500, detail=f"Error describing table {table_name}: {str(e)}")


@app.get("/debug-node-count")
async def debug_node_count(node_persistent_id: Optional[str] = None, days: int = 7):
    """
    Diagnostics: how many node outputs exist for this node id, and whether joins/org filtering are dropping rows.
    """
    try:
        node_id = node_persistent_id or os.getenv("BROKER_NODE_PERSISTENT_ID")
        if not node_id:
            raise HTTPException(status_code=400, detail="Missing node_persistent_id (param or BROKER_NODE_PERSISTENT_ID).")
        org_id = os.getenv("ORG_ID")
        return fetch_node_output_counts(node_id, org_id, days=days)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in debug_node_count endpoint")
        raise HTTPException(status_code=500, detail=f"Error computing node counts: {str(e)}")


@app.get("/debug-node-orgs")
async def debug_node_orgs(node_persistent_id: Optional[str] = None, days: int = 30):
    """
    Diagnostics: list the org_ids actually present for this node (via runs/sessions).
    """
    try:
        node_id = node_persistent_id or os.getenv("BROKER_NODE_PERSISTENT_ID")
        if not node_id:
            raise HTTPException(status_code=400, detail="Missing node_persistent_id (param or BROKER_NODE_PERSISTENT_ID).")
        return fetch_node_output_orgs(node_id, days=days)
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in debug_node_orgs endpoint")
        raise HTTPException(status_code=500, detail=f"Error computing node orgs: {str(e)}")


def _yesterday_range_iso(tz_name: str) -> tuple[str, str]:
    """
    Return (start, end) ISO timestamps for the *previous* calendar day in tz_name.
    End is exclusive.
    """
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    yday = (now - timedelta(days=1)).date()
    start_dt = datetime.combine(yday, time.min, tzinfo=tz)
    end_dt = start_dt + timedelta(days=1)
    # ClickHouse parseDateTime64BestEffort handles offsets well
    return start_dt.isoformat(), end_dt.isoformat()


def _day_range_iso(date_str: Optional[str], tz_name: str) -> tuple[str, str]:
    """
    Return (start, end) ISO timestamps for a calendar day in tz_name.
    If date_str is None, returns yesterday's range.
    """
    if not date_str:
        return _yesterday_range_iso(tz_name)
    tzinfo = ZoneInfo(tz_name)
    target = datetime.fromisoformat(date_str).date()  # expects YYYY-MM-DD
    start_dt = datetime.combine(target, time.min, tzinfo=tzinfo)
    end_dt = start_dt + timedelta(days=1)
    return start_dt.isoformat(), end_dt.isoformat()


@app.get("/daily-report")
async def get_live_daily_report(
    date: Optional[str] = None,
    tz: Optional[str] = None,
):
    """
    One-stop daily analytics report.

    - If `date` is omitted: returns yesterday (previous calendar day) in `tz`.
    - `date` format: YYYY-MM-DD
    """
    try:
        tz_name = tz or os.getenv("DEFAULT_TIMEZONE", "UTC")
        start_date, end_date = _day_range_iso(date, tz_name)

        # Pull core metrics (reuses existing ClickHouse-backed fetchers)
        call_stage = fetch_calls_ending_in_each_call_stage_stats(start_date, end_date)
        call_classification = fetch_call_classifcation_stats(start_date, end_date)
        load_status = fetch_load_status_stats(start_date, end_date)
        pricing = fetch_pricing_stats(start_date, end_date)
        carrier_end_state = fetch_carrier_end_state_stats(start_date, end_date)

        carrier_transfer_over_transfer_attempts = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
        carrier_transfer_over_call_attempts = fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date, end_date)
        successfully_transferred_for_booking = fetch_successfully_transferred_for_booking_stats(start_date, end_date)
        percent_non_convertible = fetch_percent_non_convertible_calls(start_date, end_date)
        non_convertible_with_cnq = fetch_non_convertible_calls_with_carrier_not_qualified(start_date, end_date)
        non_convertible_without_cnq = fetch_non_convertible_calls_without_carrier_not_qualified(start_date, end_date)
        carrier_not_qualified = fetch_carrier_not_qualified_stats(start_date, end_date)
        total_calls_and_duration = fetch_total_calls_and_total_duration(start_date, end_date)

        # Helper to compute success rate from call classification distribution
        success_rate = None
        if call_classification:
            total = sum(int(r.count) for r in call_classification if r is not None)
            success = sum(int(r.count) for r in call_classification if (r is not None and r.call_classification == "success"))
            success_rate = round((success / total) * 100.0, 2) if total else 0.0

        return {
            "date_range": {
                "tz": tz_name,
                "start_date": start_date,
                "end_date": end_date,
            },
            "kpis": {
                "total_calls": (total_calls_and_duration.total_calls if total_calls_and_duration else 0),
                "classified_calls": (non_convertible_with_cnq.total_calls if non_convertible_with_cnq else 0),
                "total_duration_hours": (round(total_calls_and_duration.total_duration / 3600.0, 2) if total_calls_and_duration else 0.0),
                "avg_minutes_per_call": (total_calls_and_duration.avg_minutes_per_call if total_calls_and_duration else 0.0),
                "success_rate_percent": success_rate,
                "non_convertible_calls_with_carrier_not_qualified": (
                    {
                        "count": non_convertible_with_cnq.non_convertible_calls_count,
                        "total_calls": non_convertible_with_cnq.total_calls,
                        "percentage": non_convertible_with_cnq.non_convertible_calls_percentage,
                    }
                    if non_convertible_with_cnq
                    else None
                ),
                "non_convertible_calls_without_carrier_not_qualified": (
                    {
                        "count": non_convertible_without_cnq.non_convertible_calls_count,
                        "total_calls": non_convertible_without_cnq.total_calls,
                        "percentage": non_convertible_without_cnq.non_convertible_calls_percentage,
                    }
                    if non_convertible_without_cnq
                    else None
                ),
                "carrier_not_qualified": (
                    {
                        "count": carrier_not_qualified.carrier_not_qualified_count,
                        "total_calls": carrier_not_qualified.total_calls,
                        "percentage": carrier_not_qualified.carrier_not_qualified_percentage,
                    }
                    if carrier_not_qualified
                    else None
                ),
                "carrier_transfer_over_total_transfer_attempts": (
                    {
                        "carrier_asked_count": carrier_transfer_over_transfer_attempts.carrier_asked_count,
                        "total_transfer_attempts": carrier_transfer_over_transfer_attempts.total_transfer_attempts,
                        "carrier_asked_percentage": carrier_transfer_over_transfer_attempts.carrier_asked_percentage,
                    }
                    if carrier_transfer_over_transfer_attempts
                    else None
                ),
                "carrier_transfer_over_total_call_attempts": (
                    {
                        "carrier_asked_count": carrier_transfer_over_call_attempts.carrier_asked_count,
                        "total_call_attempts": carrier_transfer_over_call_attempts.total_call_attempts,
                        "carrier_asked_percentage": carrier_transfer_over_call_attempts.carrier_asked_percentage,
                    }
                    if carrier_transfer_over_call_attempts
                    else None
                ),
                "successfully_transferred_for_booking": (
                    {
                        "successfully_transferred_for_booking_count": successfully_transferred_for_booking.successfully_transferred_for_booking_count,
                        "total_calls": successfully_transferred_for_booking.total_calls,
                        "successfully_transferred_for_booking_percentage": successfully_transferred_for_booking.successfully_transferred_for_booking_percentage,
                    }
                    if successfully_transferred_for_booking
                    else None
                ),
            },
            "breakdowns": {
                "call_stage": [{"call_stage": r.call_stage, "count": r.count, "percentage": r.percentage} for r in (call_stage or [])],
                "call_classification": [{"call_classification": r.call_classification, "count": r.count, "percentage": r.percentage} for r in (call_classification or [])],
                "load_status": [{"load_status": r.load_status, "count": r.count, "total_calls": r.total_calls, "load_status_percentage": r.load_status_percentage} for r in (load_status or [])],
                "pricing_notes": [{"pricing_notes": r.pricing_notes, "count": r.count, "percentage": r.percentage} for r in (pricing or [])],
                "carrier_end_state": [{"carrier_end_state": r.carrier_end_state, "count": r.count, "percentage": r.percentage} for r in (carrier_end_state or [])],
            },
        }
    except Exception as e:
        logger.exception("Error in get_daily_report endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching daily report: {str(e)}")


@app.get("/daily-node-outputs")
async def get_daily_node_outputs(
    node_persistent_id: Optional[str] = None,
    tz: Optional[str] = None,
    date: Optional[str] = None,
    limit: int = 200,
    include_flat_data: bool = True,
):
    """
    Return node outputs for a specific node persistent id for a single day.

    Defaults:
    - day = yesterday (in timezone `tz` or DEFAULT_TIMEZONE env var or UTC)
    - node_persistent_id = BROKER_NODE_PERSISTENT_ID env var (if set)
    """
    try:
        tz_name = tz or os.getenv("DEFAULT_TIMEZONE", "UTC")

        # Pick node id
        node_id = node_persistent_id or os.getenv("BROKER_NODE_PERSISTENT_ID")
        if not node_id:
            raise HTTPException(
                status_code=400,
                detail="Missing node_persistent_id. Provide query param `node_persistent_id` or set env var BROKER_NODE_PERSISTENT_ID.",
            )

        # Determine date range
        if date:
            tzinfo = ZoneInfo(tz_name)
            target = datetime.fromisoformat(date).date()  # accepts YYYY-MM-DD
            start_dt = datetime.combine(target, time.min, tzinfo=tzinfo)
            end_dt = start_dt + timedelta(days=1)
            start_date, end_date = start_dt.isoformat(), end_dt.isoformat()
        else:
            start_date, end_date = _yesterday_range_iso(tz_name)

        rows = fetch_daily_node_outputs(
            start_date=start_date,
            end_date=end_date,
            node_persistent_id=node_id,
            limit=limit,
            include_flat_data=include_flat_data,
        )

        # Convert dataclasses to JSON-friendly dicts
        return [
            {
                "run_id": r.run_id,
                "run_timestamp": r.run_timestamp,
                "node_persistent_id": r.node_persistent_id,
                "user_number": r.user_number,
                "duration_seconds": r.duration_seconds,
                "processing_timestamp": r.processing_timestamp,
                "call_classification": r.call_classification,
                "call_stage": r.call_stage,
                "call_notes": r.call_notes,
                "transfer_attempt": r.transfer_attempt,
                "transfer_reason": r.transfer_reason,
                "transfer_success": r.transfer_success,
                "load_status": r.load_status,
                "reference_number": r.reference_number,
                "carrier_name": r.carrier_name,
                "carrier_mc": r.carrier_mc,
                "carrier_qualification": r.carrier_qualification,
                "carrier_end_state": r.carrier_end_state,
                "pricing_notes": r.pricing_notes,
                "agreed_upon_rate": r.agreed_upon_rate,
                "flat_data": r.flat_data if include_flat_data else None,
            }
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_daily_node_outputs endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching daily node outputs: {str(e)}")

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
            raise HTTPException(status_code=500, detail="Error fetching load status stats")
        if not result:
            return []
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
    """Get percent non convertible calls stats (legacy)"""
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

@app.get("/non-convertible-calls-with-carrier-not-qualified-stats")
async def get_non_convertible_calls_with_carrier_not_qualified_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get non-convertible calls INCLUDING carrier_not_qualified"""
    try:
        result = fetch_non_convertible_calls_with_carrier_not_qualified(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No data found")
        return {
            "non_convertible_calls_count": result.non_convertible_calls_count,
            "total_calls": result.total_calls,
            "non_convertible_calls_percentage": result.non_convertible_calls_percentage
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in get_non_convertible_calls_with_carrier_not_qualified_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching non-convertible calls (with carrier_not_qualified) stats: {str(e)}")

@app.get("/non-convertible-calls-without-carrier-not-qualified-stats")
async def get_non_convertible_calls_without_carrier_not_qualified_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get non-convertible calls EXCLUDING carrier_not_qualified"""
    try:
        result = fetch_non_convertible_calls_without_carrier_not_qualified(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No data found")
        return {
            "non_convertible_calls_count": result.non_convertible_calls_count,
            "total_calls": result.total_calls,
            "non_convertible_calls_percentage": result.non_convertible_calls_percentage
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in get_non_convertible_calls_without_carrier_not_qualified_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching non-convertible calls (without carrier_not_qualified) stats: {str(e)}")

@app.get("/carrier-not-qualified-stats")
async def get_carrier_not_qualified_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get standalone carrier_not_qualified stats"""
    try:
        result = fetch_carrier_not_qualified_stats(start_date, end_date)
        if result is None:
            raise HTTPException(status_code=404, detail="No data found")
        return {
            "carrier_not_qualified_count": result.carrier_not_qualified_count,
            "total_calls": result.total_calls,
            "carrier_not_qualified_percentage": result.carrier_not_qualified_percentage
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in get_carrier_not_qualified_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching carrier_not_qualified stats: {str(e)}")

@app.get("/number-of-unique-loads-stats")
async def get_number_of_unique_loads_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get number of unique loads stats"""
    try:
        result = fetch_number_of_unique_loads(start_date, end_date)
        return {
            "number_of_unique_loads": result.number_of_unique_loads,
            "total_calls_count": result.total_calls,
            "number_of_unique_loads_percentage": result.calls_per_unique_load

        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_number_of_unique_loads_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching number of unique loads stats: {str(e)}")

@app.get("/list-of-unique-loads-stats")
async def get_list_of_unique_loads_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get list of unique loads stats"""
    try:
        result = fetch_list_of_unique_loads(start_date, end_date)
        if result:
            return {
                "list_of_unique_loads": result.list_of_unique_loads
            }
        else:
            return {
                "list_of_unique_loads": []
            }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_list_of_unique_loads_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching list of unique loads stats: {str(e)}")

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

@app.get("/calls-without-carrier-asked-for-transfer-stats")
async def get_calls_without_carrier_asked_for_transfer_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get calls without carrier asked for transfer stats"""
    try:
        result = fetch_calls_without_carrier_asked_for_transfer(start_date, end_date)
        return {
            "total_duration_no_carrier_asked_for_transfer": result.total_duration_no_carrier_asked_for_transfer / 3600,
            "total_calls_no_carrier_asked_for_transfer": result.total_calls_no_carrier_asked_for_transfer,

            "non_convertible_calls_count": result.non_convertible_calls_count,
            "non_convertible_calls_duration": result.non_convertible_calls_duration / 3600,
            "rate_too_high_calls_count": result.rate_too_high_calls_count,
            "rate_too_high_calls_duration": result.rate_too_high_calls_duration / 3600,
            "success_calls_count": result.success_calls_count,
            "success_calls_duration": result.success_calls_duration / 3600,
            "other_calls_count": result.other_calls_count,
            "other_calls_duration": result.other_calls_duration / 3600,
    

            "non_convertible_calls_percentage": result.non_convertible_calls_count / result.total_calls_no_carrier_asked_for_transfer,
            "rate_too_high_calls_percentage": result.rate_too_high_calls_count / result.total_calls_no_carrier_asked_for_transfer,
            "success_calls_percentage": result.success_calls_count / result.total_calls_no_carrier_asked_for_transfer,
            "other_calls_percentage": result.other_calls_count / result.total_calls_no_carrier_asked_for_transfer,

            "duration_non_convertible_calls_percentage": result.non_convertible_calls_duration / result.total_duration_no_carrier_asked_for_transfer,
            "duration_rate_too_high_calls_percentage": result.rate_too_high_calls_duration / result.total_duration_no_carrier_asked_for_transfer,
            "duration_success_calls_percentage": result.success_calls_duration / result.total_duration_no_carrier_asked_for_transfer,
            "duration_other_calls_percentage": result.other_calls_duration / result.total_duration_no_carrier_asked_for_transfer,

            "alternate_equipment_count": result.alternate_equipment_count,
            "caller_hung_up_no_explanation_count": result.caller_hung_up_no_explanation_count,
            "load_not_ready_count": result.load_not_ready_count,
            "load_past_due_count": result.load_past_due_count,
            "covered_count": result.covered_count,
            "carrier_not_qualified_count": result.carrier_not_qualified_count,
            "alternate_date_or_time_count": result.alternate_date_or_time_count,
            "user_declined_load_count": result.user_declined_load_count,
            "checking_with_driver_count": result.checking_with_driver_count,
            "carrier_cannot_see_reference_number_count": result.carrier_cannot_see_reference_number_count,
            "caller_put_on_hold_assistant_hung_up_count": result.caller_put_on_hold_assistant_hung_up_count,


        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_calls_without-carrier-asked-for-transfer-stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching calls without carrier asked for transfer stats: {str(e)}")


@app.get("/total-calls-and-total-duration-stats")
async def get_total_calls_and_total_duration_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get total calls and total duration stats"""
    try:
        result = fetch_total_calls_and_total_duration(start_date, end_date)
        if not result:
            return {"total_duration": 0.0, "total_calls": 0, "avg_minutes_per_call": 0.0}
        return {
            "total_duration": result.total_duration / 3600,
            "total_calls": result.total_calls,
            "avg_minutes_per_call": result.avg_minutes_per_call,
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_total_calls_and_total_duration_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching total calls and total duration stats: {str(e)}")

@app.get("/duration-carrier-asked-for-transfer-stats")
async def get_duration_carrier_asked_for_transfer_stats(start_date: Optional[str] = None, end_date: Optional[str] = None):
    """Get duration carrier asked for transfer stats"""
    try:
        result = fetch_duration_carrier_asked_for_transfer(start_date, end_date)
        return {
            "duration_carrier_asked_for_transfer": result.duration_carrier_asked_for_transfer / 3600,
        }
    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.exception("Error in get_duration_carrier_asked_for_transfer_stats endpoint")
        raise HTTPException(status_code=500, detail=f"Error fetching duration carrier asked for transfer stats: {str(e)}")


# =============================================================================
# Organizations API
# =============================================================================

class OrganizationCreate(BaseModel):
    """Request body for creating an organization."""
    org_id: str
    name: str
    node_persistent_id: str
    timezone: str = "UTC"


class OrganizationUpdate(BaseModel):
    """Request body for updating an organization."""
    name: Optional[str] = None
    node_persistent_id: Optional[str] = None
    timezone: Optional[str] = None
    is_active: Optional[bool] = None


@app.get("/api/orgs")
async def list_organizations(active_only: bool = True):
    """List all organizations."""
    try:
        orgs = get_all_organizations(active_only=active_only)
        return {
            "organizations": [
                {
                    "id": org.id,
                    "org_id": org.org_id,
                    "name": org.name,
                    "node_persistent_id": org.node_persistent_id,
                    "timezone": org.timezone,
                    "is_active": org.is_active,
                    "created_at": org.created_at,
                }
                for org in orgs
            ]
        }
    except Exception as e:
        logger.exception("Error listing organizations")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/orgs/{org_id}")
async def get_organization_by_id(org_id: str):
    """Get a specific organization."""
    try:
        org = get_organization(org_id)
        if not org:
            raise HTTPException(status_code=404, detail=f"Organization {org_id} not found")
        return {
            "id": org.id,
            "org_id": org.org_id,
            "name": org.name,
            "node_persistent_id": org.node_persistent_id,
            "timezone": org.timezone,
            "is_active": org.is_active,
            "created_at": org.created_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting organization")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/orgs")
async def create_new_organization(org_data: OrganizationCreate):
    """Create a new organization."""
    try:
        existing = get_organization(org_data.org_id)
        if existing:
            raise HTTPException(status_code=400, detail=f"Organization {org_data.org_id} already exists")

        org = Organization(
            id=None,
            org_id=org_data.org_id,
            name=org_data.name,
            node_persistent_id=org_data.node_persistent_id,
            timezone=org_data.timezone,
            is_active=True,
        )
        created = create_organization(org)
        return {
            "id": created.id,
            "org_id": created.org_id,
            "name": created.name,
            "node_persistent_id": created.node_persistent_id,
            "timezone": created.timezone,
            "is_active": created.is_active,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error creating organization")
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/api/orgs/{org_id}")
async def update_organization_by_id(org_id: str, org_data: OrganizationUpdate):
    """Update an organization."""
    try:
        existing = get_organization(org_id)
        if not existing:
            raise HTTPException(status_code=404, detail=f"Organization {org_id} not found")

        if org_data.name is not None:
            existing.name = org_data.name
        if org_data.node_persistent_id is not None:
            existing.node_persistent_id = org_data.node_persistent_id
        if org_data.timezone is not None:
            existing.timezone = org_data.timezone
        if org_data.is_active is not None:
            existing.is_active = org_data.is_active

        updated = update_organization(existing)
        return {
            "id": updated.id,
            "org_id": updated.org_id,
            "name": updated.name,
            "node_persistent_id": updated.node_persistent_id,
            "timezone": updated.timezone,
            "is_active": updated.is_active,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error updating organization")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Reports API
# =============================================================================

@app.get("/api/reports")
async def list_reports(
    org_id: Optional[str] = None,
    limit: int = 30,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    """
    List stored daily reports.

    - If org_id not provided, uses default from env
    - Returns most recent reports first
    """
    try:
        target_org_id = org_id or os.getenv("ORG_ID")
        if not target_org_id:
            raise HTTPException(status_code=400, detail="org_id required (param or ORG_ID env)")

        if start_date and end_date:
            reports = get_reports_in_range(target_org_id, start_date, end_date)
        else:
            reports = get_recent_reports(target_org_id, limit=limit)

        # Get org info
        org = get_organization(target_org_id)

        return {
            "org_id": target_org_id,
            "org_name": org.name if org else None,
            "count": len(reports),
            "reports": [
                {
                    "id": r.id,
                    "report_date": r.report_date,
                    "created_at": r.created_at,
                    # Include summary KPIs for list view
                    "total_calls": r.report_data.get("kpis", {}).get("total_calls", 0),
                    "success_rate_percent": r.report_data.get("kpis", {}).get("success_rate_percent"),
                    "non_convertible_percent": r.report_data.get("kpis", {}).get("non_convertible_calls_with_carrier_not_qualified", {}).get("percentage") if r.report_data.get("kpis", {}).get("non_convertible_calls_with_carrier_not_qualified") else None,
                }
                for r in reports
            ],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error listing reports")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/dates")
async def list_report_dates(org_id: Optional[str] = None):
    """Get all dates that have stored reports."""
    try:
        target_org_id = org_id or os.getenv("ORG_ID")
        if not target_org_id:
            raise HTTPException(status_code=400, detail="org_id required (param or ORG_ID env)")

        dates = get_all_report_dates(target_org_id)
        date_range = get_date_range(target_org_id)
        count = get_report_count(target_org_id)

        return {
            "org_id": target_org_id,
            "count": count,
            "date_range": date_range,
            "dates": dates,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error listing report dates")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/latest")
async def get_latest_stored_report(org_id: Optional[str] = None):
    """Get the most recent stored report."""
    try:
        target_org_id = org_id or os.getenv("ORG_ID")
        if not target_org_id:
            raise HTTPException(status_code=400, detail="org_id required (param or ORG_ID env)")

        report = get_latest_report(target_org_id)
        if not report:
            raise HTTPException(status_code=404, detail="No reports found")

        return {
            "id": report.id,
            "org_id": report.org_id,
            "report_date": report.report_date,
            "created_at": report.created_at,
            "data": report.report_data,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting latest report")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports/{report_date}")
async def get_stored_report(report_date: str, org_id: Optional[str] = None):
    """Get a specific stored report by date."""
    try:
        target_org_id = org_id or os.getenv("ORG_ID")
        if not target_org_id:
            raise HTTPException(status_code=400, detail="org_id required (param or ORG_ID env)")

        report = get_daily_report(target_org_id, report_date)
        if not report:
            raise HTTPException(status_code=404, detail=f"No report found for {report_date}")

        return {
            "id": report.id,
            "org_id": report.org_id,
            "report_date": report.report_date,
            "created_at": report.created_at,
            "data": report.report_data,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error getting report")
        raise HTTPException(status_code=500, detail=str(e))


# =============================================================================
# Report Generation API
# =============================================================================

class GenerateReportRequest(BaseModel):
    """Request body for generating reports."""
    org_id: Optional[str] = None
    date: Optional[str] = None  # YYYY-MM-DD format


class BackfillRequest(BaseModel):
    """Request body for backfilling reports."""
    org_id: str
    start_date: str  # YYYY-MM-DD format
    end_date: str    # YYYY-MM-DD format


@app.post("/api/reports/generate")
async def generate_report(request: GenerateReportRequest):
    """
    Manually trigger report generation.

    - If org_id not provided, generates for all active organizations
    - If date not provided, generates for yesterday
    """
    try:
        result = trigger_daily_report_now(
            org_id=request.org_id,
            target_date=request.date,
        )
        return result
    except Exception as e:
        logger.exception("Error generating report")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/reports/backfill")
async def backfill_historical_reports(request: BackfillRequest):
    """
    Backfill reports for a date range.

    Generates reports for each day in the range that doesn't already exist.
    """
    try:
        result = backfill_reports(
            org_id=request.org_id,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        return result
    except Exception as e:
        logger.exception("Error backfilling reports")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/scheduler/status")
async def get_scheduler_status():
    """Get the current scheduler status."""
    from scheduler import get_scheduler_health

    return get_scheduler_health()


@app.get("/api/scheduler/health")
async def get_scheduler_health_endpoint():
    """
    Comprehensive health check for scheduler and database.
    Use this for Railway health checks and monitoring.
    """
    from scheduler import get_scheduler_health
    from storage import get_database_info, get_recent_scheduler_runs

    scheduler_health = get_scheduler_health()
    db_info = get_database_info()
    recent_runs = get_recent_scheduler_runs(limit=3)

    # Determine overall health status
    is_healthy = True
    issues = []

    if not scheduler_health["running"] and scheduler_health["enabled"]:
        is_healthy = False
        issues.append("Scheduler enabled but not running")

    # Check if we've had recent failures
    if recent_runs:
        recent_failures = [r for r in recent_runs if r["status"] == "error"]
        if len(recent_failures) >= 2:
            is_healthy = False
            issues.append(f"{len(recent_failures)} recent scheduler failures")

    return {
        "healthy": is_healthy,
        "issues": issues,
        "scheduler": scheduler_health,
        "database": db_info,
        "recent_runs": recent_runs,
    }