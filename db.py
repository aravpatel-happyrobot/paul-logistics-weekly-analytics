# pepsi_metrics.py

from __future__ import annotations

import os
import sys
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple, Dict, Any

# pip install clickhouse-connect python-dateutil pytz
import clickhouse_connect

# If you already have your own utilities, import them instead of these stubs:
# from timezone_utils import get_time_filter, format_timestamp_for_display
from datetime import datetime, timedelta, timezone

from queries import carrier_asked_transfer_over_total_transfer_attempt_stats_query, carrier_asked_transfer_over_total_call_attempts_stats_query, calls_ending_in_each_call_stage_stats_query, load_not_found_stats_query, load_status_stats_query, successfully_transferred_for_booking_stats_query, call_classifcation_stats_query, carrier_qualification_stats_query, pricing_stats_query, carrier_end_state_query

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

# TODO: put your real persistent node id here
PEPSI_BROKER_NODE_ID = "01999d78-d321-7db5-ae1f-ebfddc2bff11"


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
class PepsiRecord:
    runId: str
    timestamp: str
    # TODO: Update these fields based on your actual data structure
    name: str = ""
    phoneNumber: str = ""
    status: str = "pending"  # 'pending' | 'completed' | 'error'
    outcome: Optional[Dict[str, Any]] = None  # {'result': 'successful' | 'unsuccessful', 'reason': str}


@dataclass
class PepsiData:
    totalRecords: int
    pendingRecords: int
    completedRecords: int
    errorRecords: int
    successfulRecords: int
    unsuccessfulRecords: int
    records: List[PepsiRecord] = field(default_factory=list)
    transferStats: Optional[List[TransferStats]] = None
    carrierTransferStats: Optional[CarrierTransferStatsTotalTransferAttempts] = None
    carrierCallAttemptsStats: Optional[CarrierTransferStatsTotalCallAttempts] = None
    loadNotFoundStats: Optional[LoadNotFoundStats] = None


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


def get_pepsi_data_optimized(start_date: str, end_date: str, timezone_name: str = "UTC") -> List[PepsiRecord]:
    """
    Placeholder fetch (mirrors the TS placeholder). Replace with your real query once your schema is set.
    """
    try:
        client = get_clickhouse_client()
        query = f"""
            SELECT
                run_id,
                timestamp
            FROM public_node_outputs_kv
            WHERE timestamp >= parseDateTime64BestEffort('{start_date}')
              AND timestamp <  parseDateTime64BestEffort('{end_date}')
            ORDER BY timestamp DESC
            LIMIT 1000
        """
        rows = _json_each_row(client, query)

        records = [
            PepsiRecord(
                runId=str(r.get("run_id", "")),
                timestamp=format_timestamp_for_display(str(r.get("timestamp", "")), timezone_name),
                name="",  # TODO map actual field
                phoneNumber="",  # TODO map actual field
                status="pending",
                outcome=None,
            )
            for r in rows
        ]
        return records
    except Exception as e:
        logger.exception("Error in Pepsi data query: %s", e)
        return []


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

        logger.info("Using ORG_ID: %s...", org_id[:8])
        logger.info("Using node_persistent_id: %s", PEPSI_BROKER_NODE_ID)

        query = calls_ending_in_each_call_stage_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)

        client = get_clickhouse_client()
        rows = _json_each_row(
            client,
            query,
            settings={
                "max_execution_time": 60,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
        )

        logger.info("Call stage stats query result: %d rows", len(rows))
        if not rows:
            logger.info("⚠️ No call stage stats found. Running diagnostic query...")

            diag_query = f"""
                SELECT
                    COUNT(*) AS total_nodes,
                    COUNT(DISTINCT no.run_id) AS unique_runs
                FROM public_node_outputs no
                INNER JOIN public_nodes n ON no.node_id = n.id
                WHERE n.org_id = '{org_id}'
                  AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
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
                    logger.info("  💡 Verify ORG_ID (%s...) and node_persistent_id (%s)", org_id[:8], PEPSI_BROKER_NODE_ID)
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
                          AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
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
                          AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
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

        query = carrier_asked_transfer_over_total_transfer_attempt_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)

        client = get_clickhouse_client()
        
        
        # Also show all unique transfer_attempt values
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
              AND no.node_persistent_id = '{PEPSI_BROKER_NODE_ID}'
              AND JSONHas(no.flat_data, 'result.transfer.transfer_reason') = 1
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != ''
              AND JSONExtractString(no.flat_data, 'result.transfer.transfer_reason') != 'null'
            GROUP BY transfer_attempt
            ORDER BY count DESC
        """
        try:
            print("[DEBUG] Running values query to check transfer_attempt values...", file=sys.stderr)
            values_rows = _json_each_row(client, values_query)
            print(f"[DEBUG] Values query returned {len(values_rows)} rows", file=sys.stderr)
            logger.info("📊 All transfer_attempt values:")
            print("📊 All transfer_attempt values:", file=sys.stderr)
            if values_rows:
                for vr in values_rows:
                    attempt = vr.get("transfer_attempt", "NULL")
                    count = vr.get("count", 0)
                    msg = f"  - '{attempt}': {count} records"
                    logger.info(msg)
                    print(msg, file=sys.stderr)
            else:
                logger.info("  No transfer_attempt values found")
                print("  No transfer_attempt values found", file=sys.stderr)
        except Exception as values_err:
            error_msg = f"Could not run values query: {values_err}"
            logger.warning(error_msg)
            logger.exception("Full traceback:")
            print(f"[ERROR] {error_msg}", file=sys.stderr)
            import traceback
            print(f"[ERROR] {traceback.format_exc()}", file=sys.stderr)
        
        rows = _json_each_row(
            client,
            query,
            settings={
                "max_execution_time": 60,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
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

        query = carrier_asked_transfer_over_total_call_attempts_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)

        client = get_clickhouse_client()

        rows = _json_each_row(
            client,
            query,
            settings={
                "max_execution_time": 60,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
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

def fetch_pepsi_data(time_range: str, timezone_name: str = "UTC") -> PepsiData:
    """
    Main fetch that mirrors the TS `fetchPepsiData` function.
    """
    try:
        start_date, end_date = get_time_filter(time_range, timezone_name)

        # Fetch rows
        records = get_pepsi_data_optimized(start_date, end_date, timezone_name)

        # Fetch stats in "parallel" (sequential here for simplicity)
        transfer_stats = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
        carrier_stats = fetch_carrier_asked_transfer_over_total_transfer_attempts_stats(start_date, end_date)
        carrier_call_attempts_stats = fetch_carrier_asked_transfer_over_total_call_attempts_stats(start_date, end_date)
        load_not_found_stats = fetch_load_not_found_stats(start_date, end_date)
        load_status_stats = fetch_load_status_stats(start_date, end_date)
        successfully_transferred_for_booking_stats = fetch_successfully_transferred_for_booking_stats(start_date, end_date)
        # Compute tallies
        total_records = len(records)
        pending_records = sum(1 for r in records if r.status == "pending")
        completed_records = sum(1 for r in records if r.status == "completed")
        error_records = sum(1 for r in records if r.status == "error")
        successful_records = sum(1 for r in records if (r.outcome or {}).get("result") == "successful")
        unsuccessful_records = sum(1 for r in records if (r.outcome or {}).get("result") == "unsuccessful")

        return PepsiData(
            totalRecords=total_records,
            pendingRecords=pending_records,
            completedRecords=completed_records,
            errorRecords=error_records,
            successfulRecords=successful_records,
            unsuccessfulRecords=unsuccessful_records,
            records=records,
            transferStats=transfer_stats or None,
            carrierTransferStats=carrier_stats or None,
            carrierCallAttemptsStats=carrier_call_attempts_stats or None,
            loadNotFoundStats=load_not_found_stats or None,
            loadStatusStats=load_status_stats or None,
            successfullyTransferredForBookingStats=successfully_transferred_for_booking_stats or None,
        )
    except Exception as e:
        logger.exception("Error in fetch_pepsi_data: %s", e)
        return PepsiData(
            totalRecords=0,
            pendingRecords=0,
            completedRecords=0,
            errorRecords=0,
            successfulRecords=0,
            unsuccessfulRecords=0,
            records=[],
        )

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
        query = load_not_found_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row(
            client,
            query,
            settings={
                "max_execution_time": 60,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
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

def fetch_load_status_stats(start_date: Optional[str] = None, end_date: Optional[str] = None) -> Optional[LoadStatusStats]:
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
        query = load_status_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
            "max_execution_time": 60,
            "max_memory_usage": 2_000_000_000,
            "max_threads": 4,
        },
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
        query = successfully_transferred_for_booking_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
                "max_execution_time": 60,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
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
        query = call_classifcation_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
            "max_execution_time": 60,
            "max_memory_usage": 2_000_000_000,
            "max_threads": 4,
        },
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
        query = carrier_qualification_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
            "max_execution_time": 60,
            "max_memory_usage": 2_000_000_000,
            "max_threads": 4,
        },
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
        query = pricing_stats_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
            "max_execution_time": 60,
            "max_memory_usage": 2_000_000_000,
            "max_threads": 4,
        },
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
        query = carrier_end_state_query(date_filter, org_id, PEPSI_BROKER_NODE_ID)
        
        client = get_clickhouse_client()
        rows = _json_each_row( client, query, settings={
            "max_execution_time": 60,
            "max_memory_usage": 2_000_000_000,
            "max_threads": 4,
        },
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

