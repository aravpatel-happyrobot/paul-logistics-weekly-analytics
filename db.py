# pepsi_metrics.py

from __future__ import annotations

import os
import sys
import logging
import json
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

# pip install clickhouse-connect python-dateutil pytz
import clickhouse_connect

# If you already have your own utilities, import them instead of these stubs:
# from timezone_utils import get_time_filter, format_timestamp_for_display
from datetime import datetime, timedelta, timezone

from queries import carrier_asked_transfer_over_total_transfer_attempt_stats_query, carrier_asked_transfer_over_total_call_attempts_stats_query, calls_ending_in_each_call_stage_stats_query, load_not_found_stats_query, load_status_stats_query, successfully_transferred_for_booking_stats_query, call_classifcation_stats_query, carrier_qualification_stats_query, pricing_stats_query, carrier_end_state_query, percent_non_convertible_calls_query, non_convertible_calls_with_carrier_not_qualified_query, non_convertible_calls_without_carrier_not_qualified_query, carrier_not_qualified_stats_query, number_of_unique_loads_query, list_of_unique_loads_query, number_of_unique_loads_query_broker_node, list_of_unique_loads_query_broker_node, calls_without_carrier_asked_for_transfer_query, total_calls_and_total_duration_query, duration_carrier_asked_for_transfer_query

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---- Config / Client ---------------------------------------------------------

def get_clickhouse_client():
    """
    Create a ClickHouse HTTP client from environment variables.
    Supports both naming conventions:
    - CLICKHOUSE_URL or CLICKHOUSE_HOST
    - CLICKHOUSE_USERNAME or CLICKHOUSE_USER
    - CLICKHOUSE_PASSWORD
    - CLICKHOUSE_DATABASE
    - CLICKHOUSE_SECURE (true/false for HTTPS)
    """
    from urllib.parse import urlparse
    
    # Support both CLICKHOUSE_URL and CLICKHOUSE_HOST
    host_raw = os.getenv("CLICKHOUSE_URL") or os.getenv("CLICKHOUSE_HOST", "localhost:8123")
    # Support both CLICKHOUSE_USERNAME and CLICKHOUSE_USER
    user = os.getenv("CLICKHOUSE_USERNAME") or os.getenv("CLICKHOUSE_USER", "default")
    password = os.getenv("CLICKHOUSE_PASSWORD", "")
    database = os.getenv("CLICKHOUSE_DATABASE", "default")
    # Check if secure connection is needed (for ClickHouse Cloud)
    secure_str = os.getenv("CLICKHOUSE_SECURE", "false").lower()
    is_secure = secure_str in ("true", "1", "yes")

    # Debug: Log what env vars were found
    has_url_env = os.getenv("CLICKHOUSE_URL") is not None
    has_host_env = os.getenv("CLICKHOUSE_HOST") is not None
    has_user_env = (os.getenv("CLICKHOUSE_USERNAME") is not None or 
                    os.getenv("CLICKHOUSE_USER") is not None)
    
    logger.info("ClickHouse config - URL/HOST='%s' (from env: %s)", 
                host_raw, "yes" if (has_url_env or has_host_env) else "no (using default)")
    logger.info("ClickHouse config - USER='%s' (from env: %s)", 
                user, "yes" if has_user_env else "no (using default)")
    logger.info("ClickHouse config - SECURE=%s", is_secure)
    
    # Also print to stderr for immediate visibility
    print(f"[DEBUG] CLICKHOUSE_URL/HOST from env: {has_url_env or has_host_env}, value: '{host_raw}'", file=sys.stderr)
    print(f"[DEBUG] CLICKHOUSE_USERNAME/USER from env: {has_user_env}, value: '{user}'", file=sys.stderr)
    print(f"[DEBUG] CLICKHOUSE_SECURE: {is_secure}", file=sys.stderr)

    # Parse host URL to extract hostname and port separately
    # Handle both formats: "http://localhost:8123" or "localhost:8123" or just "hostname"
    if "://" in host_raw:
        # Full URL format - parse it properly
        parsed = urlparse(host_raw)
        hostname = parsed.hostname or "localhost"
        port = parsed.port if parsed.port else (8443 if is_secure else 8123)
    else:
        # Already in hostname:port format or just hostname
        if ":" in host_raw:
            parts = host_raw.split(":")
            hostname = parts[0]
            try:
                port = int(parts[1])
            except (ValueError, IndexError):
                port = 8443 if is_secure else 8123
        else:
            hostname = host_raw
            # Default ports: 8443 for HTTPS, 8123 for HTTP
            port = 8443 if is_secure else 8123

    # Log final connection details
    logger.info("Connecting to ClickHouse host=%s port=%s secure=%s db=%s user=%s", 
                hostname, port, is_secure, database, user)

    return clickhouse_connect.get_client(
        host=hostname,
        port=port,
        username=user,
        password=password,
        database=database,
        secure=is_secure,
        connect_timeout=30,
        send_receive_timeout=120,
    )


# ---- Env helpers -------------------------------------------------------------

def get_org_id() -> Optional[str]:
    """
    Mirror the runtime env var check from the TS version.
    """
    org_id = os.getenv("ORG_ID")
    if not org_id:
        env_keys = [k for k in os.environ.keys() if ("ORG" in k or "CLICK" in k)]
        logger.error("❌ ORG_ID not found in os.environ. Available relevant env vars: %s", ", ".join(env_keys))
    else:
        logger.info("✓ ORG_ID found: %s...", org_id[:8])
    return org_id


# ---- Timezone utilities (stubs) ---------------------------------------------
# Replace these two with your real implementations if you already have them.

def get_time_filter(time_range: str, tz_name: str = "UTC") -> Tuple[str, str]:
    """
    Given a human range (e.g., 'last_30_days', 'today', 'yesterday', 'last_7_days'),
    return ISO time strings suitable for parseDateTime64BestEffort in ClickHouse.
    """
    tzinfo = timezone.utc  # Simplified; swap for zoneinfo if you need real TZ handling
    now = datetime.now(tzinfo)

    def iso(dt: datetime) -> str:
        # ISO without microseconds; ClickHouse parseDateTime64BestEffort handles offsets
        return dt.replace(microsecond=0).isoformat()

    tr = (time_range or "").lower()
    if tr in ("last_30_days", "30d", "last30"):
        start = now - timedelta(days=30)
        end = now
    elif tr in ("last_7_days", "7d", "last7"):
        start = now - timedelta(days=7)
        end = now
    elif tr in ("today",):
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
    elif tr in ("yesterday",):
        end = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=1)
    else:
        # Default: last 30 days
        start = now - timedelta(days=30)
        end = now

    return iso(start), iso(end)


def format_timestamp_for_display(ts: str, tz_name: str = "UTC") -> str:
    """
    Convert a ClickHouse timestamp string to a friendlier representation.
    This is a simple pass-through with standard ISO formatting—adjust as needed.
    """
    try:
        # Best-effort parsing for typical ClickHouse outputs
        dt = datetime.fromisoformat(ts.replace(" ", "T").replace("Z", "+00:00"))
        return dt.isoformat()
    except Exception:
        return ts


# ---- Constants ---------------------------------------------------------------

# Client configuration
#
# IMPORTANT: `main.py` loads `.env` at runtime, but `db.py` is imported before that.
# Therefore: anything that depends on `.env` must be read from `os.getenv(...)` at
# *call time* (not import time), otherwise it will fall back to defaults.
DEFAULT_BROKER_NODE_ID = "01999d78-d321-7db5-ae1f-ebfddc2bff11"  # legacy fallback


def get_broker_node_persistent_id() -> str:
    return os.getenv("BROKER_NODE_PERSISTENT_ID") or DEFAULT_BROKER_NODE_ID


def get_fbr_node_persistent_id() -> str:
    return os.getenv("FBR_NODE_PERSISTENT_ID") or get_broker_node_persistent_id()


def get_default_timezone() -> str:
    return os.getenv("DEFAULT_TIMEZONE") or "UTC"


def diagnostics_enabled() -> bool:
    return (os.getenv("ENABLE_DIAGNOSTICS", "false").lower() in ("true", "1", "yes"))


def excluded_user_numbers_sql(prefix: Optional[str] = None) -> str:
    """
    Build a ClickHouse SQL snippet to exclude configured user numbers.
    Returns an empty string if no exclusions are configured.
    """
    excluded_raw = os.getenv("EXCLUDED_USER_NUMBERS", "")
    excluded_nums = [n.strip() for n in excluded_raw.split(",") if n.strip()]
    if not excluded_nums:
        return ""
    excluded_list = ", ".join([f"'{n}'" for n in excluded_nums])
    col = f"{prefix}.user_number" if prefix else "user_number"
    return f"AND {col} NOT IN ({excluded_list})"

# ClickHouse query settings for large date ranges
CLICKHOUSE_QUERY_SETTINGS = {
    "max_execution_time": 180,  # Increased from 60 to 180 seconds
    "max_memory_usage": 10_000_000_000,  # Increased from 2GB to 10GB
    "max_threads": 16,  # Increased from 4 to 16 threads
}

# Cutoff date for switching between broker_node and FBR queries
# Dates BEFORE Nov 7, 2025 (i.e., Nov 6, 2025 and earlier) use broker_node queries
# Dates Nov 7, 2025 and AFTER use FBR (find by reference) queries
UNIQUE_LOADS_CUTOFF_DATE = "2025-11-07T00:00:00"

# ---- Data models -------------------------------------------------------------

@dataclass
class TransferStats:
    call_stage: str
    count: int
    percentage: float


@dataclass
class CarrierTransferStatsTotalTransferAttempts:
    carrier_asked_count: int
    total_transfer_attempts: int
    carrier_asked_percentage: float

@dataclass
class CarrierTransferStatsTotalCallAttempts:
    carrier_asked_count: int
    total_call_attempts: int
    carrier_asked_percentage: float

@dataclass
class LoadNotFoundStats:
    load_not_found_count: int
    total_calls: int
    load_not_found_percentage: float

@dataclass
class LoadStatusStats:
    load_status: str
    count: int
    total_calls: int
    load_status_percentage: float

@dataclass
class SuccessfullyTransferredForBooking:
    successfully_transferred_for_booking_count: int
    total_calls: int
    successfully_transferred_for_booking_percentage: float

@dataclass
class CallClassificationStats:
    call_classification: str
    count: int
    percentage: float

@dataclass
class CarrierQualificationStats:
    carrier_qualification: str
    count: int
    percentage: float


@dataclass
class PricingStats:
    pricing_notes: str
    count: int
    percentage: float

@dataclass
class CarrierEndStateStats:
    carrier_end_state: str
    count: int
    percentage: float

@dataclass
class PercentNonConvertibleCallsStats:
    non_convertible_calls_count: int
    total_calls_count: int
    non_convertible_calls_percentage: float

@dataclass
class NonConvertibleCallsWithCarrierNotQualifiedStats:
    non_convertible_calls_count: int
    total_calls: int
    non_convertible_calls_percentage: float

@dataclass
class NonConvertibleCallsWithoutCarrierNotQualifiedStats:
    non_convertible_calls_count: int
    total_calls: int
    non_convertible_calls_percentage: float

@dataclass
class CarrierNotQualifiedStats:
    carrier_not_qualified_count: int
    total_calls: int
    carrier_not_qualified_percentage: float

@dataclass
class NumberOfUniqueLoadsStats:
    number_of_unique_loads: int
    total_calls: int
    calls_per_unique_load: float

@dataclass
class ListOfUniqueLoadsStats:
    list_of_unique_loads: List[str]

@dataclass
class CallsWithoutCarrierAskedForTransferStats:
    non_convertible_calls_count: int
    non_convertible_calls_duration: int
    rate_too_high_calls_count: int
    rate_too_high_calls_duration: int
    success_calls_count: int
    success_calls_duration: int
    other_calls_count: int
    other_calls_duration: int
    total_duration_no_carrier_asked_for_transfer: int
    total_calls_no_carrier_asked_for_transfer: int
    alternate_equipment_count: int
    caller_hung_up_no_explanation_count: int
    load_not_ready_count: int
    load_past_due_count: int
    covered_count: int
    carrier_not_qualified_count: int
    alternate_date_or_time_count: int
    user_declined_load_count: int
    checking_with_driver_count: int
    carrier_cannot_see_reference_number_count: int
    caller_put_on_hold_assistant_hung_up_count: int

@dataclass
class TotalCallsAndTotalDurationStats:
    total_duration: int
    total_calls: int
    avg_minutes_per_call: float

@dataclass
class DurationCarrierAskedForTransferStats:
    duration_carrier_asked_for_transfer: int


@dataclass
class DailyNodeOutputRow:
    run_id: str
    run_timestamp: str
    node_persistent_id: str
    user_number: Optional[str]
    duration_seconds: Optional[int]
    processing_timestamp: Optional[str]

    call_classification: Optional[str]
    call_stage: Optional[str]
    call_notes: Optional[str]

    transfer_attempt: Optional[str]
    transfer_reason: Optional[str]
    transfer_success: Optional[str]

    load_status: Optional[str]
    reference_number: Optional[str]

    carrier_name: Optional[str]
    carrier_mc: Optional[str]
    carrier_qualification: Optional[str]
    carrier_end_state: Optional[str]

    pricing_notes: Optional[str]
    agreed_upon_rate: Optional[str]

    flat_data: Optional[Dict[str, Any]] = None


# ---- Queries ----------------------------------------------------------------

def _json_each_row(client, query: str, settings: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Run a query and return rows as list[dict], similar to JSONEachRow.
    clickhouse-connect already returns rows as python types; but to match the TS behavior,
    we'll get column names and recompose dicts.
    """
    rs = client.query(query, settings=settings or {})
    
    # Get column names - handle different clickhouse-connect API versions
    # The error suggests rs.result_set might be a list, so check that first
    cols = None
    
    # Method 1: Check if rs has column_names directly
    if hasattr(rs, 'column_names'):
        cols = rs.column_names
    # Method 2: Check if rs has columns_with_types directly  
    elif hasattr(rs, 'columns_with_types'):
        cols_data = rs.columns_with_types
        if isinstance(cols_data, list):
            cols = [c[0] if isinstance(c, (list, tuple)) else str(c) for c in cols_data]
        else:
            cols = [c[0] for c in cols_data]
    # Method 3: Check result_set, but handle the case where it's a list
    elif hasattr(rs, 'result_set'):
        result_set = rs.result_set
        if isinstance(result_set, list):
            # result_set is a list, not an object - try to get columns from rs itself
            # This might be the actual rows, so skip this path
            pass
        elif hasattr(result_set, 'column_names'):
            cols = result_set.column_names
        elif hasattr(result_set, 'columns_with_types'):
            cols_data = result_set.columns_with_types
            if isinstance(cols_data, list):
                cols = [c[0] if isinstance(c, (list, tuple)) else str(c) for c in cols_data]
            else:
                cols = [c[0] for c in cols_data]
    
    # Method 4: Try to get column names from query result metadata
    if cols is None and hasattr(rs, 'names'):
        cols = rs.names
    
    # Method 5: Last resort - infer from result_rows structure
    if cols is None:
        if rs.result_rows:
            num_cols = len(rs.result_rows[0]) if rs.result_rows else 0
            # Try to get column names from the query itself or use generic names
            cols = [f"column_{i}" for i in range(num_cols)]
            logger.warning(f"Could not determine column names, using generic names: {cols}")
        else:
            cols = []
    
    out = []
    for row in rs.result_rows:
        out.append({col: row[i] for i, col in enumerate(cols)})
    return out


def fetch_calls_ending_in_each_call_stage_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> List[TransferStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return []

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching call stage stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching call stage stats for last 30 days (no date range provided)")

        broker_node_id = get_broker_node_persistent_id()
        logger.info("Using ORG_ID: %s...", org_id[:8])
        logger.info("Using node_persistent_id: %s", broker_node_id)

        excluded_sql = excluded_user_numbers_sql()
        query = calls_ending_in_each_call_stage_stats_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()
        rows = _json_each_row(
            client,
            query,
            settings=CLICKHOUSE_QUERY_SETTINGS,
        )

        logger.info("Call stage stats query result: %d rows", len(rows))
        if diagnostics_enabled() and not rows:
            logger.info("⚠️ No call stage stats found. Running diagnostic query...")

            diag_query = f"""
                SELECT
                    COUNT(*) AS total_nodes,
                    COUNT(DISTINCT no.run_id) AS unique_runs
                FROM public_node_outputs no
                INNER JOIN public_nodes n ON no.node_id = n.id
                WHERE n.org_id = '{org_id}'
                  AND no.node_persistent_id = '{broker_node_id}'
                LIMIT 1
            """
            diag_rows = _json_each_row(client, diag_query)

            if diag_rows:
                dr = diag_rows[0]
                logger.info("📊 Diagnostic results:")
                logger.info("  - Total node outputs for this ORG_ID and node: %s", dr.get("total_nodes"))
                logger.info("  - Unique runs: %s", dr.get("unique_runs"))

                if int(dr.get("total_nodes", 0)) == 0:
                    logger.info("  ❌ No data found for this ORG_ID and node_persistent_id combination")
                    logger.info("  💡 Verify ORG_ID (%s...) and node_persistent_id (%s)", org_id[:8], broker_node_id)
                else:
                    logger.info("  ✓ Data exists! Checking date range and JSON structure...")

                    date_check_query = f"""
                        SELECT
                            MIN(r.timestamp) AS earliest_run,
                            MAX(r.timestamp) AS latest_run,
                            COUNT(*) AS run_count
                        FROM public_runs r
                        INNER JOIN public_node_outputs no ON no.run_id = r.id
                        INNER JOIN public_nodes n ON no.node_id = n.id
                        WHERE n.org_id = '{org_id}'
                          AND no.node_persistent_id = '{broker_node_id}'
                        LIMIT 1
                    """
                    try:
                        date_rows = _json_each_row(client, date_check_query)
                        if date_rows:
                            d = date_rows[0]
                            logger.info("  📅 Actual run dates:")
                            logger.info("     - Earliest: %s", d.get("earliest_run"))
                            logger.info("     - Latest:   %s", d.get("latest_run"))
                            logger.info("     - Query range: %s to %s", start_date, end_date)

                            if start_date and end_date and d.get("earliest_run") and d.get("latest_run"):
                                earliest = datetime.fromisoformat(str(d["earliest_run"]).replace(" ", "T"))
                                latest = datetime.fromisoformat(str(d["latest_run"]).replace(" ", "T"))
                                qstart = datetime.fromisoformat(start_date.replace(" ", "T"))
                                qend = datetime.fromisoformat(end_date.replace(" ", "T"))
                                if latest < qstart or earliest > qend:
                                    logger.info("  ❌ Date range mismatch! Data exists outside the query range.")
                    except Exception as e:
                        logger.exception("Error running date check: %s", e)

                    json_check_query = f"""
                        SELECT
                            JSONHas(flat_data, 'result', 'call', 'call_stage') AS has_nested_path,
                            JSONHas(flat_data, 'result.call.call_stage')       AS has_dot_path,
                            JSONExtractString(flat_data, 'result', 'call', 'call_stage') AS nested_value,
                            JSONExtractString(flat_data, 'result.call.call_stage')       AS dot_value
                        FROM public_node_outputs no
                        INNER JOIN public_nodes n ON no.node_id = n.id
                        WHERE n.org_id = '{org_id}'
                          AND no.node_persistent_id = '{broker_node_id}'
                          AND flat_data IS NOT NULL
                        LIMIT 5
                    """
                    try:
                        json_rows = _json_each_row(client, json_check_query)
                        if json_rows:
                            logger.info("  🔍 JSON structure check (sample of 5 rows):")
                            for idx, row in enumerate(json_rows, 1):
                                logger.info("     Row %d:", idx)
                                logger.info("       - Has nested path: %s", row.get("has_nested_path"))
                                logger.info("       - Has dot path:    %s", row.get("has_dot_path"))
                                nested_val = str(row.get("nested_value") or "")[:50] or "null"
                                dot_val = str(row.get("dot_value") or "")[:50] or "null"
                                logger.info("       - Nested value: %s", nested_val)
                                logger.info("       - Dot value:    %s", dot_val)
                    except Exception as e:
                        logger.exception("Error running JSON structure check: %s", e)

        return [
            TransferStats(
                call_stage=str(r.get("call_stage") or "Unknown"),
                count=int(r.get("count", 0)),
                percentage=float(r.get("percentage", 0.0)),
            )
            for r in rows
        ]
    except Exception as e:
        logger.exception("Error fetching call stage stats: %s", e)
        return []


def fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CarrierTransferStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching carrier transfer stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching carrier transfer stats for last 30 days (no date range provided)")

        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = carrier_asked_transfer_over_total_transfer_attempt_stats_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()
        
        
        # Optional: show all unique transfer_attempt values (setup diagnostics)
        values_query = f"""
            WITH recent_runs AS (
                SELECT id AS run_id
                FROM public_runs
                WHERE {date_filter}
            )
            SELECT
                JSONExtractString(no.flat_data, 'result.transfer.transfer_attempt') AS transfer_attempt,
                COUNT(*) AS count
            FROM public_node_outputs no
            INNER JOIN recent_runs rr ON no.run_id = rr.run_id
            INNER JOIN public_nodes n ON no.node_id = n.id
            WHERE n.org_id = '{org_id}'
              AND no.node_persistent_id = '{broker_node_id}'
              AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
            GROUP BY transfer_attempt
            ORDER BY count DESC
        """
        if diagnostics_enabled():
            try:
                values_rows = _json_each_row(client, values_query)
                logger.info("📊 transfer_attempt values (%d rows):", len(values_rows))
                for vr in values_rows:
                    attempt = vr.get("transfer_attempt", "NULL")
                    count = vr.get("count", 0)
                    logger.info("  - '%s': %s records", attempt, count)
            except Exception as values_err:
                logger.warning("Could not run transfer_attempt diagnostic query: %s", values_err)
        
        rows = _json_each_row(
            client,
            query,
            settings=CLICKHOUSE_QUERY_SETTINGS,
        )

        

        logger.info("Carrier transfer stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No carrier transfer stats found")
            return None

        r = rows[0]
        return CarrierTransferStatsTotalTransferAttempts(
            carrier_asked_count=int(r.get("carrier_asked_count", 0)),
            total_transfer_attempts=int(r.get("total_transfer_attempts", 0)),
            carrier_asked_percentage=float(r.get("carrier_asked_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching carrier transfer stats: %s", e)
        return None

def fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CarrierTransferStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching carrier transfer stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching carrier transfer stats for last 30 days (no date range provided)")

        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = carrier_asked_transfer_over_total_call_attempts_stats_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()

        rows = _json_each_row(
            client,
            query,
            settings=CLICKHOUSE_QUERY_SETTINGS,
        )

        logger.info("Carrier transfer stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No carrier transfer stats found")
            return None

        r = rows[0]
        return CarrierTransferStatsTotalCallAttempts(
            carrier_asked_count=int(r.get("carrier_asked_count", 0)),
            total_call_attempts=int(r.get("total_call_attempts", 0)),
            carrier_asked_percentage=float(r.get("carrier_asked_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching carrier transfer stats: %s", e)
        return None

def fetch_load_not_found_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[LoadNotFoundStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching load not found stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching load not found stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = load_not_found_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row(
            client,
            query,
            settings=CLICKHOUSE_QUERY_SETTINGS,
        )
        logger.info("Load not found stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No load not found stats found")
            return None

        r = rows[0]
        return LoadNotFoundStats(
            load_not_found_count=int(r.get("load_not_found_count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            load_not_found_percentage=float(r.get("load_not_found_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching load not found stats: %s", e)
        return None

def fetch_load_status_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[List[LoadStatusStats]]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching load status stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching load status stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = load_status_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Load status stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No load status stats found")
            return None
        return [LoadStatusStats(
            load_status=str(r.get("load_status") or "Unknown"),
            count=int(r.get("count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            load_status_percentage=float(r.get("load_status_percentage", 0.0)),
        ) for r in rows]
    except Exception as e:
        logger.exception("Error fetching load status stats: %s", e)
        return []

def fetch_successfully_transferred_for_booking_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[SuccessfullyTransferredForBooking]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching successfully transferred for booking stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching successfully transferred for booking stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = successfully_transferred_for_booking_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
        )
        logger.info("Successfully transferred for booking stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No successfully transferred for booking stats found")
            return None
        r = rows[0]
        return SuccessfullyTransferredForBooking(
            successfully_transferred_for_booking_count=int(r.get("successfully_transferred_for_booking_count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            successfully_transferred_for_booking_percentage=float(r.get("successfully_transferred_for_booking_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching successfully transferred for booking stats: %s", e)
        return None

def fetch_call_classifcation_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CallClassificationStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching call classification stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching call classification stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = call_classifcation_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Call classification stats query result: %d rows", len(rows))
        if not rows:
            logger.info
            ("No call classification stats found")
            return None
        return [CallClassificationStats(
            call_classification=str(r.get("call_classification") or "Unknown"),
            count=int(r.get("count", 0)),
            percentage=float(r.get("percentage", 0.0)),
        ) for r in rows]
    except Exception as e:
        logger.exception("Error fetching call classification stats: %s", e)
        return []

def fetch_carrier_qualification_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CarrierQualificationStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching carrier qualification stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching carrier qualification stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = carrier_qualification_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Carrier qualification stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No carrier qualification stats found")
            return None
        return [CarrierQualificationStats(
            carrier_qualification=str(r.get("carrier_qualification") or "Unknown"),
            count=int(r.get("count", 0)), 
            percentage=float(r.get("percentage", 0.0)),
        ) for r in rows]
    except Exception as e:
        logger.exception("Error fetching carrier qualification stats: %s", e)
        return []


def fetch_pricing_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[PricingStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching pricing stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching pricing stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = pricing_stats_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Pricing stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No pricing stats found")
            return None
        return [PricingStats(
            pricing_notes=str(r.get("pricing_notes") or "Unknown"),
            count=int(r.get("count", 0)),   
            percentage=float(r.get("percentage", 0.0)),
        ) for r in rows]
    except Exception as e:
        logger.exception("Error fetching pricing stats: %s", e)
        return []

def fetch_carrier_end_state_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CarrierEndStateStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching carrier end state stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching carrier end state stats for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = carrier_end_state_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Carrier end state stats query result: %d rows", len(rows))
        if not rows:
            logger.info("No carrier end state stats found")
            return None
        return [CarrierEndStateStats(
            carrier_end_state=str(r.get("carrier_end_state") or "Unknown"),
            count=int(r.get("count", 0)),
            percentage=float(r.get("percentage", 0.0)),
        ) for r in rows]
    except Exception as e:
        logger.exception("Error fetching carrier end state stats: %s", e)
        return []


def fetch_percent_non_convertible_calls(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[PercentNonConvertibleCallsStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        
        if start_date and end_date:
            logger.info("Fetching percent non convertible calls for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching percent non convertible calls for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = percent_non_convertible_calls_query(date_filter, org_id, broker_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings=CLICKHOUSE_QUERY_SETTINGS,
    )
        logger.info("Percent non convertible calls query result: %d rows", len(rows))
        if not rows:
            logger.info("No percent non convertible calls found")
            return None
        r = rows[0]
        return PercentNonConvertibleCallsStats(
            non_convertible_calls_count=int(r.get("non_convertible_calls_count", 0)),
            total_calls_count=int(r.get("total_calls", 0)),
            non_convertible_calls_percentage=float(r.get("non_convertible_calls_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching percent non convertible calls: %s", e)
        return None

def fetch_non_convertible_calls_with_carrier_not_qualified(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[NonConvertibleCallsWithCarrierNotQualifiedStats]:
    """
    Fetches non-convertible calls INCLUDING carrier_not_qualified.
    These are all calls that wouldn't have converted anyway (AI saved time).
    """
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching non-convertible calls (with carrier_not_qualified) for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching non-convertible calls (with carrier_not_qualified) for last 30 days")

        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = non_convertible_calls_with_carrier_not_qualified_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Non-convertible calls (with carrier_not_qualified) query result: %d rows", len(rows))

        if not rows:
            logger.info("No non-convertible calls (with carrier_not_qualified) found")
            return None

        r = rows[0]
        return NonConvertibleCallsWithCarrierNotQualifiedStats(
            non_convertible_calls_count=int(r.get("non_convertible_calls_count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            non_convertible_calls_percentage=float(r.get("non_convertible_calls_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching non-convertible calls (with carrier_not_qualified): %s", e)
        return None

def fetch_non_convertible_calls_without_carrier_not_qualified(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[NonConvertibleCallsWithoutCarrierNotQualifiedStats]:
    """
    Fetches non-convertible calls EXCLUDING carrier_not_qualified.
    These are calls with specific issues (equipment, timing, load status, etc).
    """
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching non-convertible calls (without carrier_not_qualified) for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching non-convertible calls (without carrier_not_qualified) for last 30 days")

        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = non_convertible_calls_without_carrier_not_qualified_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Non-convertible calls (without carrier_not_qualified) query result: %d rows", len(rows))

        if not rows:
            logger.info("No non-convertible calls (without carrier_not_qualified) found")
            return None

        r = rows[0]
        return NonConvertibleCallsWithoutCarrierNotQualifiedStats(
            non_convertible_calls_count=int(r.get("non_convertible_calls_count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            non_convertible_calls_percentage=float(r.get("non_convertible_calls_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching non-convertible calls (without carrier_not_qualified): %s", e)
        return None

def fetch_carrier_not_qualified_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CarrierNotQualifiedStats]:
    """
    Fetches standalone metric for carrier_not_qualified calls.
    These are carriers that didn't meet qualification requirements.
    """
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None

    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching carrier_not_qualified stats for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching carrier_not_qualified stats for last 30 days")

        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = carrier_not_qualified_stats_query(date_filter, org_id, broker_node_id, excluded_sql)

        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Carrier_not_qualified stats query result: %d rows", len(rows))

        if not rows:
            logger.info("No carrier_not_qualified stats found")
            return None

        r = rows[0]
        return CarrierNotQualifiedStats(
            carrier_not_qualified_count=int(r.get("carrier_not_qualified_count", 0)),
            total_calls=int(r.get("total_calls", 0)),
            carrier_not_qualified_percentage=float(r.get("carrier_not_qualified_percentage", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching carrier_not_qualified stats: %s", e)
        return None

def _split_date_range_for_unique_loads(start_date: Optional[str], end_date: Optional[str]) -> Tuple[Optional[Tuple[str, str]], Optional[Tuple[str, str]]]:
    """
    Split date range at Nov 7, 2025 cutoff.
    Dates before Nov 7, 2025 use broker_node, dates Nov 7, 2025 and after use FBR.
    Returns: (broker_node_range, fbr_range) where each range is (start, end) or None
    """
    if not start_date or not end_date:
        return None, None
    
    cutoff = UNIQUE_LOADS_CUTOFF_DATE
    
    try:
        start_dt = datetime.fromisoformat(start_date.replace(" ", "T"))
        end_dt = datetime.fromisoformat(end_date.replace(" ", "T"))
        cutoff_dt = datetime.fromisoformat(cutoff.replace(" ", "T"))
        
        broker_range = None
        fbr_range = None
        
        # If start_date is before Nov 7, 2025
        if start_dt < cutoff_dt:
            # End date for broker_range is the minimum of: end_date or Nov 7, 2025
            broker_end = min(end_dt, cutoff_dt)
            broker_range = (start_date, broker_end.strftime('%Y-%m-%dT%H:%M:%S'))
        
        # If end_date is Nov 7, 2025 or after
        if end_dt >= cutoff_dt:
            # Start date for FBR range is the maximum of: start_date or Nov 7, 2025
            fbr_start = max(start_dt, cutoff_dt)
            fbr_range = (fbr_start.strftime('%Y-%m-%dT%H:%M:%S'), end_date)

        logger.info(f"broker_range: {broker_range}, fbr_range: {fbr_range}")
        
        return broker_range, fbr_range
    except Exception as e:
        logger.warning(f"Error parsing dates for split: {e}, using single query")
        return None, None

def fetch_number_of_unique_loads(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[NumberOfUniqueLoadsStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        broker_range, fbr_range = _split_date_range_for_unique_loads(start_date, end_date)
        
        broker_node_id = get_broker_node_persistent_id()
        fbr_node_id = get_fbr_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()

        # If no date range provided, use default
        if not start_date or not end_date:
            date_filter = "timestamp >= now() - INTERVAL 30 DAY"
            logger.info("Fetching number of unique loads for last 30 days (no date range provided)")
            query = number_of_unique_loads_query(date_filter, org_id, fbr_node_id, excluded_sql)
            client = get_clickhouse_client()
            rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
            if not rows:
                return None
            r = rows[0]
            return NumberOfUniqueLoadsStats(
                number_of_unique_loads=int(r.get("number_of_unique_loads", 0)),
                total_calls=int(r.get("total_calls", 0)),
                calls_per_unique_load=float(r.get("calls_per_unique_load", 0.0)),
            )
        
        # If date range spans both periods, combine results
        if broker_range and fbr_range:
            logger.info("Fetching number of unique loads for split date range: broker_node %s-%s, FBR %s-%s", 
                       broker_range[0], broker_range[1], fbr_range[0], fbr_range[1])
            
            # Get broker_node results
            broker_filter = f"timestamp >= parseDateTime64BestEffort('{broker_range[0]}') AND timestamp < parseDateTime64BestEffort('{broker_range[1]}')"
            broker_query = number_of_unique_loads_query_broker_node(broker_filter, org_id, broker_node_id, excluded_sql)
            
            # Get FBR results
            fbr_filter = f"timestamp >= parseDateTime64BestEffort('{fbr_range[0]}') AND timestamp < parseDateTime64BestEffort('{fbr_range[1]}')"
            fbr_query = number_of_unique_loads_query(fbr_filter, org_id, fbr_node_id, excluded_sql)
            
            client = get_clickhouse_client()
            
            # Execute broker_node query
            broker_rows = _json_each_row(client, broker_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            broker_result = broker_rows[0] if broker_rows else {"number_of_unique_loads": 0, "total_calls": 0}
            
            # Execute FBR query
            fbr_rows = _json_each_row(client, fbr_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            fbr_result = fbr_rows[0] if fbr_rows else {"number_of_unique_loads": 0, "total_calls": 0}
            
            # Get lists to count unique combined
            broker_list_query = list_of_unique_loads_query_broker_node(broker_filter, org_id, broker_node_id, excluded_sql)
            fbr_list_query = list_of_unique_loads_query(fbr_filter, org_id, fbr_node_id, excluded_sql)
            
            broker_list_rows = _json_each_row(client, broker_list_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            fbr_list_rows = _json_each_row(client, fbr_list_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            
            broker_loads = {str(r.get("custom_load_id")) for r in broker_list_rows if r.get("custom_load_id")}
            fbr_loads = {str(r.get("custom_load_id")) for r in fbr_list_rows if r.get("custom_load_id")}
            
            # Combine and count unique
            combined_unique_loads = len(broker_loads | fbr_loads)
            combined_total_calls = int(broker_result.get("total_calls", 0)) + int(fbr_result.get("total_calls", 0))
            combined_calls_per_load = round(combined_total_calls / combined_unique_loads, 2) if combined_unique_loads > 0 else 0.0
            
            return NumberOfUniqueLoadsStats(
                number_of_unique_loads=combined_unique_loads,
                total_calls=combined_total_calls,
                calls_per_unique_load=combined_calls_per_load,
            )
        
        # Single period - determine which query to use
        elif broker_range:
            logger.info("Fetching number of unique loads for broker_node date range: %s to %s", broker_range[0], broker_range[1])
            date_filter = f"timestamp >= parseDateTime64BestEffort('{broker_range[0]}') AND timestamp < parseDateTime64BestEffort('{broker_range[1]}')"
            query = number_of_unique_loads_query_broker_node(date_filter, org_id, broker_node_id, excluded_sql)
        else:  # fbr_range
            logger.info("Fetching number of unique loads for FBR date range: %s to %s", fbr_range[0], fbr_range[1])
            date_filter = f"timestamp >= parseDateTime64BestEffort('{fbr_range[0]}') AND timestamp < parseDateTime64BestEffort('{fbr_range[1]}')"
            query = number_of_unique_loads_query(date_filter, org_id, fbr_node_id, excluded_sql)
        
        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Number of unique loads query result: %d rows", len(rows))
        if not rows:
            logger.info("No number of unique loads found")
            return None
        r = rows[0]
        return NumberOfUniqueLoadsStats(
            number_of_unique_loads=int(r.get("number_of_unique_loads", 0)),
            total_calls=int(r.get("total_calls", 0)),
            calls_per_unique_load=float(r.get("calls_per_unique_load", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching number of unique loads: %s", e)
        return None

def fetch_list_of_unique_loads(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[ListOfUniqueLoadsStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        broker_range, fbr_range = _split_date_range_for_unique_loads(start_date, end_date)
        logger.info(f"After split - broker_range: {broker_range}, fbr_range: {fbr_range}")
        
        broker_node_id = get_broker_node_persistent_id()
        fbr_node_id = get_fbr_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()

        # If no date range provided, use default
        if not start_date or not end_date:
            date_filter = "timestamp >= now() - INTERVAL 30 DAY"
            logger.info("Fetching list of unique loads for last 30 days (no date range provided)")
            query = list_of_unique_loads_query(date_filter, org_id, fbr_node_id, excluded_sql)
            client = get_clickhouse_client()
            rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
            rows = [str(r.get("custom_load_id")) for r in rows if r.get("custom_load_id")]
            return ListOfUniqueLoadsStats(list_of_unique_loads=rows)
        
        all_loads = set()
        client = get_clickhouse_client()
        
        # If date range spans both periods, combine results
        if broker_range and fbr_range:
            logger.info("Fetching list of unique loads for split date range: broker_node %s-%s, FBR %s-%s", 
                       broker_range[0], broker_range[1], fbr_range[0], fbr_range[1])
            
            # Get broker_node results
            broker_filter = f"timestamp >= parseDateTime64BestEffort('{broker_range[0]}') AND timestamp < parseDateTime64BestEffort('{broker_range[1]}')"
            print(f'broker_filter: {broker_filter}')
            broker_query = list_of_unique_loads_query_broker_node(broker_filter, org_id, broker_node_id, excluded_sql)
            broker_rows = _json_each_row(client, broker_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            broker_loads = {str(r.get("custom_load_id")) for r in broker_rows if r.get("custom_load_id")}
            all_loads.update(broker_loads)
            
            # Get FBR results
            fbr_filter = f"timestamp >= parseDateTime64BestEffort('{fbr_range[0]}') AND timestamp < parseDateTime64BestEffort('{fbr_range[1]}')"
            print(f'fbr_filter: {fbr_filter}')
            fbr_query = list_of_unique_loads_query(fbr_filter, org_id, fbr_node_id, excluded_sql)
            fbr_rows = _json_each_row(client, fbr_query, settings=CLICKHOUSE_QUERY_SETTINGS)
            # print(f'fbr_rows: {fbr_rows}')
            fbr_loads = {str(r.get("custom_load_id")) for r in fbr_rows if r.get("custom_load_id")}
            all_loads.update(fbr_loads)
            
            return ListOfUniqueLoadsStats(list_of_unique_loads=sorted(list(all_loads)))
        
        # Single period - determine which query to use
        elif broker_range:
            logger.info("Fetching list of unique loads for broker_node date range: %s to %s", broker_range[0], broker_range[1])
            date_filter = f"timestamp >= parseDateTime64BestEffort('{broker_range[0]}') AND timestamp < parseDateTime64BestEffort('{broker_range[1]}')"
            query = list_of_unique_loads_query_broker_node(date_filter, org_id, broker_node_id, excluded_sql)
            logger.info(f"Executing broker_node query for date filter: {date_filter}")
        elif fbr_range:
            logger.info("Fetching list of unique loads for FBR date range: %s to %s", fbr_range[0], fbr_range[1])
            date_filter = f"timestamp >= parseDateTime64BestEffort('{fbr_range[0]}') AND timestamp < parseDateTime64BestEffort('{fbr_range[1]}')"
            query = list_of_unique_loads_query(date_filter, org_id, fbr_node_id, excluded_sql)
            logger.info(f"Executing FBR query for date filter: {date_filter}")
        else:
            logger.warning("Neither broker_range nor fbr_range was set! This should not happen.")
            return ListOfUniqueLoadsStats(list_of_unique_loads=[])
        
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info(f"Query returned {len(rows)} rows")
        rows = [str(r.get("custom_load_id")) for r in rows if r.get("custom_load_id")]
        logger.info(f"After filtering, {len(rows)} rows with custom_load_id")
        if not rows:
            logger.info("No list of unique loads found")
            return ListOfUniqueLoadsStats(list_of_unique_loads=[])
        return ListOfUniqueLoadsStats(
            list_of_unique_loads=rows
        )
    except Exception as e:
        logger.exception("Error fetching list of unique loads: %s", e)
        return None

def fetch_calls_without_carrier_asked_for_transfer(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[CallsWithoutCarrierAskedForTransferStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        if start_date and end_date:
            logger.info("Fetching calls without carrier asked for transfer for date range: %s to %s", start_date, end_date)
        else:
            logger.info("Fetching calls without carrier asked for transfer for last 30 days (no date range provided)")
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = calls_without_carrier_asked_for_transfer_query(date_filter, org_id, broker_node_id, excluded_sql)
        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Calls without carrier asked for transfer query result: %d rows", len(rows))
        if not rows:
            logger.info("No calls without carrier asked for transfer found")
            return None
        r = rows[0]
        return CallsWithoutCarrierAskedForTransferStats(
            non_convertible_calls_count=int(r.get("non_convertible_calls_count", 0)),
            non_convertible_calls_duration=int(r.get("non_convertible_calls_duration", 0)),
            rate_too_high_calls_count=int(r.get("rate_too_high_calls_count", 0)),
            rate_too_high_calls_duration=int(r.get("rate_too_high_calls_duration", 0)),
            success_calls_count=int(r.get("success_calls_count", 0)),
            success_calls_duration=int(r.get("success_calls_duration", 0)),
            other_calls_count=int(r.get("other_calls_count", 0)),
            other_calls_duration=int(r.get("other_calls_duration", 0)),
            total_duration_no_carrier_asked_for_transfer=int(r.get("total_duration_no_carrier_asked_for_transfer", 0)),
            total_calls_no_carrier_asked_for_transfer=int(r.get("total_calls_no_carrier_asked_for_transfer", 0)),
            alternate_equipment_count=int(r.get("alternate_equipment_count", 0)),
            caller_hung_up_no_explanation_count=int(r.get("caller_hung_up_no_explanation_count", 0)),
            load_not_ready_count=int(r.get("load_not_ready_count", 0)),
            load_past_due_count=int(r.get("load_past_due_count", 0)),
            covered_count=int(r.get("covered_count", 0)),
            carrier_not_qualified_count=int(r.get("carrier_not_qualified_count", 0)),
            alternate_date_or_time_count=int(r.get("alternate_date_or_time_count", 0)),
            user_declined_load_count=int(r.get("user_declined_load_count", 0)),
            checking_with_driver_count=int(r.get("checking_with_driver_count", 0)),
            carrier_cannot_see_reference_number_count=int(r.get("carrier_cannot_see_reference_number_count", 0)),
            caller_put_on_hold_assistant_hung_up_count=int(r.get("caller_put_on_hold_assistant_hung_up_count", 0)),
        )
    except Exception as e:
        logger.exception("Error fetching calls without carrier asked for transfer: %s", e)
        return None

def fetch_total_calls_and_total_duration(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[TotalCallsAndTotalDurationStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        # Define calls as runs that reached this node within the date range, then join to sessions for duration.
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql_sessions = excluded_user_numbers_sql()

        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        query = f"""
            WITH recent_runs AS (
                SELECT id AS run_id
                FROM public_runs
                WHERE {date_filter}
                  AND org_id = '{org_id}'
            ),
            node_runs AS (
                SELECT DISTINCT no.run_id AS run_id
                FROM public_node_outputs no
                INNER JOIN recent_runs rr ON no.run_id = rr.run_id
                WHERE no.node_persistent_id = '{broker_node_id}'
            ),
            per_run AS (
                SELECT
                    nr.run_id AS run_id,
                    any(s.duration) AS duration
                FROM node_runs nr
                LEFT JOIN public_sessions s ON s.run_id = nr.run_id
                WHERE s.org_id = '{org_id}'
                {excluded_sql_sessions}
                GROUP BY nr.run_id
            )
            SELECT
                ifNull(sum(duration), 0) AS total_duration,
                count() AS total_calls,
                ifNull(round((sum(duration) / nullIf(count(), 0)) / 60, 2), 0) AS avg_minutes_per_call
            FROM per_run
        """

        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        if not rows:
            return None
        r = rows[0]
        return TotalCallsAndTotalDurationStats(
            total_duration=int(r.get("total_duration", 0)),
            total_calls=int(r.get("total_calls", 0)),
            avg_minutes_per_call=float(r.get("avg_minutes_per_call", 0.0)),
        )
    except Exception as e:
        logger.exception("Error fetching total calls and total duration: %s", e)
        return None

def fetch_duration_carrier_asked_for_transfer(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[DurationCarrierAskedForTransferStats]:
    org_id = get_org_id()
    if not org_id:
        logger.error("❌ ORG_ID not found in environment variables. Please check your .env and restart the app.")
        return None
    
    try:
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql_sessions = excluded_user_numbers_sql()

        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )

        query = f"""
            WITH recent_runs AS (
                SELECT id AS run_id
                FROM public_runs
                WHERE {date_filter}
                  AND org_id = '{org_id}'
            ),
            carrier_asked_runs AS (
                SELECT DISTINCT no.run_id AS run_id
                FROM public_node_outputs no
                INNER JOIN recent_runs rr ON no.run_id = rr.run_id
                WHERE no.node_persistent_id = '{broker_node_id}'
                  AND upper(JSONExtractString(no.flat_data, 'result.transfer.transfer_reason')) = 'CARRIER_ASKED_FOR_TRANSFER'
            ),
            per_run AS (
                SELECT
                    cr.run_id AS run_id,
                    any(s.duration) AS duration
                FROM carrier_asked_runs cr
                LEFT JOIN public_sessions s ON s.run_id = cr.run_id
                WHERE s.org_id = '{org_id}'
                {excluded_sql_sessions}
                GROUP BY cr.run_id
            )
            SELECT ifNull(sum(duration), 0) AS duration_carrier_asked_for_transfer
            FROM per_run
        """

        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        if not rows:
            return None
        r = rows[0]
        return DurationCarrierAskedForTransferStats(
            duration_carrier_asked_for_transfer=int(r.get("duration_carrier_asked_for_transfer", 0)),
        )
    except Exception as e:
        logger.exception("Error fetching duration carrier asked for transfer: %s", e)
        return None
def fetch_daily_node_outputs(
    start_date: str,
    end_date: str,
    node_persistent_id: str,
    limit: int = 500,
    include_flat_data: bool = True,
) -> List[DailyNodeOutputRow]:
    """
    Fetch raw-ish node outputs for a specific node persistent id over a date range.

    Intended as a “start here” endpoint for new clients: pull yesterday’s runs and inspect the
    `flat_data` payloads + core extracted fields.
    """
    org_id = os.getenv("ORG_ID")

    # Date filter compatible with existing query style (end_date is exclusive)
    date_filter = (
        f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
    )

    # Optional org filter: use runs/sessions org_id (NOT public_nodes org_id)
    org_filter_runs = f"AND org_id = '{org_id}'" if org_id else ""
    org_filter_sessions = f"AND org_id = '{org_id}'" if org_id else ""

    # Optional excluded test numbers (applied in sessions CTE if present)
    excluded_filter = excluded_user_numbers_sql()

    query = f"""
        WITH recent_runs AS (
            SELECT id AS run_id, timestamp
            FROM public_runs
            WHERE {date_filter}
            {org_filter_runs}
        ),
        sessions AS (
            SELECT run_id, user_number, duration
            FROM public_sessions
            WHERE {date_filter}
            {org_filter_sessions}
            {excluded_filter}
        )
        SELECT
            rr.run_id AS run_id,
            rr.timestamp AS run_timestamp,
            no.node_persistent_id AS node_persistent_id,
            s.user_number AS user_number,
            s.duration AS duration_seconds,
            JSONExtractString(no.flat_data, 'result.metadata.processing_timestamp') AS processing_timestamp,

            JSONExtractString(no.flat_data, 'result.call.call_classification') AS call_classification,
            JSONExtractString(no.flat_data, 'result.call.call_stage') AS call_stage,
            JSONExtractString(no.flat_data, 'result.call.notes') AS call_notes,

            JSONExtractString(no.flat_data, 'result.transfer.transfer_attempt') AS transfer_attempt,
            JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') AS transfer_reason,
            JSONExtractString(no.flat_data, 'result.transfer.transfer_success') AS transfer_success,

            JSONExtractString(no.flat_data, 'result.load.load_status') AS load_status,
            JSONExtractString(no.flat_data, 'result.load.reference_number') AS reference_number,

            JSONExtractString(no.flat_data, 'result.carrier.carrier_name') AS carrier_name,
            JSONExtractString(no.flat_data, 'result.carrier.carrier_mc') AS carrier_mc,
            JSONExtractString(no.flat_data, 'result.carrier.carrier_qualification') AS carrier_qualification,
            JSONExtractString(no.flat_data, 'result.carrier.carrier_end_state') AS carrier_end_state,

            JSONExtractString(no.flat_data, 'result.pricing.pricing_notes') AS pricing_notes,
            JSONExtractString(no.flat_data, 'result.pricing.agreed_upon_rate') AS agreed_upon_rate,

            no.flat_data AS flat_data
        FROM public_node_outputs no
        INNER JOIN recent_runs rr ON no.run_id = rr.run_id
        LEFT JOIN sessions s ON no.run_id = s.run_id
        WHERE no.node_persistent_id = '{node_persistent_id}'
        ORDER BY rr.timestamp DESC
        LIMIT {int(limit)}
    """

    client = get_clickhouse_client()
    rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)

    def _maybe_parse_flat_data(v: Any) -> Optional[Dict[str, Any]]:
        if not include_flat_data:
            return None
        if v is None:
            return None
        if isinstance(v, dict):
            return v
        if isinstance(v, str):
            try:
                return json.loads(v)
            except Exception:
                return {"_raw": v}
        return {"_raw": str(v)}

    out: List[DailyNodeOutputRow] = []
    for r in rows:
        out.append(
            DailyNodeOutputRow(
                run_id=str(r.get("run_id", "")),
                run_timestamp=str(r.get("run_timestamp", "")),
                node_persistent_id=str(r.get("node_persistent_id", "")),
                user_number=(str(r.get("user_number")) if r.get("user_number") is not None else None),
                duration_seconds=(int(r.get("duration_seconds")) if r.get("duration_seconds") is not None else None),
                processing_timestamp=(str(r.get("processing_timestamp")) if r.get("processing_timestamp") is not None else None),
                call_classification=(str(r.get("call_classification")) if r.get("call_classification") is not None else None),
                call_stage=(str(r.get("call_stage")) if r.get("call_stage") is not None else None),
                call_notes=(str(r.get("call_notes")) if r.get("call_notes") is not None else None),
                transfer_attempt=(str(r.get("transfer_attempt")) if r.get("transfer_attempt") is not None else None),
                transfer_reason=(str(r.get("transfer_reason")) if r.get("transfer_reason") is not None else None),
                transfer_success=(str(r.get("transfer_success")) if r.get("transfer_success") is not None else None),
                load_status=(str(r.get("load_status")) if r.get("load_status") is not None else None),
                reference_number=(str(r.get("reference_number")) if r.get("reference_number") is not None else None),
                carrier_name=(str(r.get("carrier_name")) if r.get("carrier_name") is not None else None),
                carrier_mc=(str(r.get("carrier_mc")) if r.get("carrier_mc") is not None else None),
                carrier_qualification=(str(r.get("carrier_qualification")) if r.get("carrier_qualification") is not None else None),
                carrier_end_state=(str(r.get("carrier_end_state")) if r.get("carrier_end_state") is not None else None),
                pricing_notes=(str(r.get("pricing_notes")) if r.get("pricing_notes") is not None else None),
                agreed_upon_rate=(str(r.get("agreed_upon_rate")) if r.get("agreed_upon_rate") is not None else None),
                flat_data=_maybe_parse_flat_data(r.get("flat_data")),
            )
        )
    return out


def fetch_table_schema(table_name: str) -> List[Dict[str, Any]]:
    """
    Return ClickHouse table schema via DESCRIBE TABLE.
    """
    client = get_clickhouse_client()
    query = f"DESCRIBE TABLE {table_name}"
    rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
    # Typical columns: name, type, default_type, default_expression, comment, codec_expression, ttl_expression
    return rows


def fetch_node_output_counts(
    node_persistent_id: str,
    org_id: Optional[str],
    days: int = 7,
) -> Dict[str, Any]:
    """
    Diagnostics: counts of node outputs and join coverage.
    Helps debug why a date-windowed query returns zero rows.
    """
    client = get_clickhouse_client()

    org_filter_nodes = f"AND n.org_id = '{org_id}'" if org_id else ""
    org_filter_runs = f"AND r.org_id = '{org_id}'" if org_id else ""
    org_filter_sessions = f"AND s.org_id = '{org_id}'" if org_id else ""
    days = int(days)

    query = f"""
        WITH
            node_all AS (
                SELECT
                    count() AS cnt,
                    min(timestamp) AS min_ts,
                    max(timestamp) AS max_ts
                FROM public_node_outputs
                WHERE node_persistent_id = '{node_persistent_id}'
            ),
            node_recent AS (
                SELECT
                    count() AS cnt,
                    min(timestamp) AS min_ts,
                    max(timestamp) AS max_ts
                FROM public_node_outputs
                WHERE node_persistent_id = '{node_persistent_id}'
                  AND timestamp >= now() - INTERVAL {days} DAY
            ),
            node_join_runs AS (
                SELECT
                    count() AS cnt
                FROM public_node_outputs no
                INNER JOIN public_runs r ON no.run_id = r.id
                WHERE no.node_persistent_id = '{node_persistent_id}'
                  AND r.timestamp >= now() - INTERVAL {days} DAY
            ),
            node_join_runs_run_org AS (
                SELECT
                    count() AS cnt
                FROM public_node_outputs no
                INNER JOIN public_runs r ON no.run_id = r.id
                WHERE no.node_persistent_id = '{node_persistent_id}'
                  AND r.timestamp >= now() - INTERVAL {days} DAY
                  {org_filter_runs}
            ),
            node_join_sessions_session_org AS (
                SELECT
                    count() AS cnt
                FROM public_node_outputs no
                INNER JOIN public_sessions s ON no.run_id = s.run_id
                WHERE no.node_persistent_id = '{node_persistent_id}'
                  AND s.timestamp >= now() - INTERVAL {days} DAY
                  {org_filter_sessions}
            ),
            node_join_runs_org AS (
                SELECT
                    count() AS cnt
                FROM public_node_outputs no
                INNER JOIN public_runs r ON no.run_id = r.id
                INNER JOIN public_nodes n ON no.node_id = n.id
                WHERE no.node_persistent_id = '{node_persistent_id}'
                  AND r.timestamp >= now() - INTERVAL {days} DAY
                  {org_filter_nodes}
            )
        SELECT
            (SELECT cnt FROM node_all) AS node_all_count,
            (SELECT min_ts FROM node_all) AS node_all_min_ts,
            (SELECT max_ts FROM node_all) AS node_all_max_ts,
            (SELECT cnt FROM node_recent) AS node_recent_count,
            (SELECT min_ts FROM node_recent) AS node_recent_min_ts,
            (SELECT max_ts FROM node_recent) AS node_recent_max_ts,
            (SELECT cnt FROM node_join_runs) AS node_join_runs_count,
            (SELECT cnt FROM node_join_runs_run_org) AS node_join_runs_run_org_count,
            (SELECT cnt FROM node_join_sessions_session_org) AS node_join_sessions_session_org_count,
            (SELECT cnt FROM node_join_runs_org) AS node_join_runs_org_count
    """
    rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
    return rows[0] if rows else {}


def fetch_node_output_orgs(node_persistent_id: str, days: int = 30) -> Dict[str, Any]:
    """
    Diagnostics: show which org_ids appear for this node (via runs and sessions).
    Returns the top org_ids by row count.
    """
    client = get_clickhouse_client()
    days = int(days)

    query = f"""
        WITH node_rows AS (
            SELECT run_id
            FROM public_node_outputs
            WHERE node_persistent_id = '{node_persistent_id}'
              AND timestamp >= now() - INTERVAL {days} DAY
        )
        SELECT
            'runs' AS source,
            toString(r.org_id) AS org_id,
            count() AS cnt
        FROM node_rows nr
        INNER JOIN public_runs r ON nr.run_id = r.id
        GROUP BY org_id
        ORDER BY cnt DESC
        LIMIT 20
    """
    runs_rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)

    query_sessions = f"""
        WITH node_rows AS (
            SELECT run_id
            FROM public_node_outputs
            WHERE node_persistent_id = '{node_persistent_id}'
              AND timestamp >= now() - INTERVAL {days} DAY
        )
        SELECT
            'sessions' AS source,
            toString(s.org_id) AS org_id,
            count() AS cnt
        FROM node_rows nr
        INNER JOIN public_sessions s ON nr.run_id = s.run_id
        GROUP BY org_id
        ORDER BY cnt DESC
        LIMIT 20
    """
    session_rows = _json_each_row(client, query_sessions, settings=CLICKHOUSE_QUERY_SETTINGS)

    return {
        "days": days,
        "node_persistent_id": node_persistent_id,
        "runs_orgs": runs_rows,
        "sessions_orgs": session_rows,
    }
    
    try:
        date_filter = (
            f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
            if start_date and end_date
            else "timestamp >= now() - INTERVAL 30 DAY"
        )
        broker_node_id = get_broker_node_persistent_id()
        excluded_sql = excluded_user_numbers_sql()
        query = duration_carrier_asked_for_transfer_query(date_filter, org_id, broker_node_id, excluded_sql)
        client = get_clickhouse_client()
        rows = _json_each_row(client, query, settings=CLICKHOUSE_QUERY_SETTINGS)
        logger.info("Duration carrier asked for transfer query result: %d rows", len(rows))
        if not rows:
            logger.info("No duration carrier asked for transfer found")
            return None
        r = rows[0]
        return DurationCarrierAskedForTransferStats(
            duration_carrier_asked_for_transfer=int(r.get("duration_carrier_asked_for_transfer", 0)),
        )
    except Exception as e:
        logger.exception("Error fetching duration carrier asked for transfer: %s", e)
        return None