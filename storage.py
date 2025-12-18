"""
Database storage layer for persisting daily reports and organization configs.

Supports:
- PostgreSQL (production - Railway, Heroku, etc.)
- SQLite (local development fallback)

The database backend is auto-detected from DATABASE_URL environment variable.
"""

import os
import json
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from contextlib import contextmanager
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

# Database URL - PostgreSQL in production, SQLite for local dev
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///analytics.db")

# Detect database type
IS_POSTGRES = DATABASE_URL.startswith("postgres")

if IS_POSTGRES:
    import psycopg2
    from psycopg2.extras import RealDictCursor
else:
    import sqlite3


@dataclass
class Organization:
    """Represents an organization/client configuration."""
    id: Optional[int]
    org_id: str
    name: str
    node_persistent_id: str
    timezone: str = "UTC"
    created_at: Optional[str] = None
    is_active: bool = True


@dataclass
class DailyReport:
    """Represents a stored daily report."""
    id: Optional[int]
    org_id: str
    report_date: str  # YYYY-MM-DD format
    report_data: Dict[str, Any]
    created_at: Optional[str] = None


def _parse_database_url(url: str) -> dict:
    """Parse DATABASE_URL into connection parameters."""
    if url.startswith("sqlite"):
        return {"type": "sqlite", "path": url.replace("sqlite:///", "")}

    # Handle postgres:// vs postgresql://
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)

    parsed = urlparse(url)
    return {
        "type": "postgres",
        "host": parsed.hostname,
        "port": parsed.port or 5432,
        "database": parsed.path[1:],  # Remove leading /
        "user": parsed.username,
        "password": parsed.password,
    }


@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    db_config = _parse_database_url(DATABASE_URL)

    if db_config["type"] == "sqlite":
        conn = sqlite3.connect(db_config["path"])
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
    else:
        conn = psycopg2.connect(
            host=db_config["host"],
            port=db_config["port"],
            database=db_config["database"],
            user=db_config["user"],
            password=db_config["password"],
            cursor_factory=RealDictCursor,
        )
        try:
            yield conn
        finally:
            conn.close()


def _execute(conn, query: str, params: tuple = (), fetch: str = None):
    """Execute a query with proper handling for both SQLite and PostgreSQL."""
    cursor = conn.cursor()

    # Convert ? placeholders to %s for PostgreSQL
    if IS_POSTGRES:
        query = query.replace("?", "%s")

    cursor.execute(query, params)

    if fetch == "one":
        return cursor.fetchone()
    elif fetch == "all":
        return cursor.fetchall()
    elif fetch == "lastrowid":
        if IS_POSTGRES:
            # PostgreSQL needs RETURNING clause, handled separately
            return cursor.fetchone()["id"] if cursor.description else None
        else:
            return cursor.lastrowid

    return cursor


def init_database():
    """Initialize the database schema."""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        if IS_POSTGRES:
            # PostgreSQL schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS organizations (
                    id SERIAL PRIMARY KEY,
                    org_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    node_persistent_id TEXT NOT NULL,
                    timezone TEXT DEFAULT 'UTC',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_reports (
                    id SERIAL PRIMARY KEY,
                    org_id TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    report_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, report_date)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_reports_org_date
                ON daily_reports(org_id, report_date DESC)
            """)

            # Scheduler health tracking table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduler_runs (
                    id SERIAL PRIMARY KEY,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    reports_generated INTEGER DEFAULT 0,
                    error_message TEXT
                )
            """)
        else:
            # SQLite schema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS organizations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id TEXT UNIQUE NOT NULL,
                    name TEXT NOT NULL,
                    node_persistent_id TEXT NOT NULL,
                    timezone TEXT DEFAULT 'UTC',
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS daily_reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    org_id TEXT NOT NULL,
                    report_date DATE NOT NULL,
                    report_data TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(org_id, report_date)
                )
            """)

            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_daily_reports_org_date
                ON daily_reports(org_id, report_date DESC)
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduler_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    reports_generated INTEGER DEFAULT 0,
                    error_message TEXT
                )
            """)

        conn.commit()
        logger.info("Database initialized (PostgreSQL=%s)", IS_POSTGRES)


def ensure_db_initialized():
    """Ensure database is initialized (call on app startup)."""
    init_database()


# =============================================================================
# Organization CRUD
# =============================================================================

def create_organization(org: Organization) -> Organization:
    """Create a new organization."""
    with get_db_connection() as conn:
        if IS_POSTGRES:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO organizations (org_id, name, node_persistent_id, timezone, is_active)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (org.org_id, org.name, org.node_persistent_id, org.timezone, org.is_active))
            org.id = cursor.fetchone()["id"]
        else:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO organizations (org_id, name, node_persistent_id, timezone, is_active)
                VALUES (?, ?, ?, ?, ?)
            """, (org.org_id, org.name, org.node_persistent_id, org.timezone, org.is_active))
            org.id = cursor.lastrowid

        conn.commit()
        logger.info("Created organization: %s (%s)", org.name, org.org_id)
        return org


def get_organization(org_id: str) -> Optional[Organization]:
    """Get an organization by org_id."""
    with get_db_connection() as conn:
        row = _execute(conn, "SELECT * FROM organizations WHERE org_id = ?", (org_id,), fetch="one")
        if row:
            return Organization(
                id=row["id"],
                org_id=row["org_id"],
                name=row["name"],
                node_persistent_id=row["node_persistent_id"],
                timezone=row["timezone"],
                is_active=bool(row["is_active"]),
                created_at=str(row["created_at"]) if row["created_at"] else None,
            )
        return None


def get_all_organizations(active_only: bool = True) -> List[Organization]:
    """Get all organizations."""
    with get_db_connection() as conn:
        if active_only:
            rows = _execute(conn, "SELECT * FROM organizations WHERE is_active = ? ORDER BY name", (True,), fetch="all")
        else:
            rows = _execute(conn, "SELECT * FROM organizations ORDER BY name", fetch="all")
        return [
            Organization(
                id=row["id"],
                org_id=row["org_id"],
                name=row["name"],
                node_persistent_id=row["node_persistent_id"],
                timezone=row["timezone"],
                is_active=bool(row["is_active"]),
                created_at=str(row["created_at"]) if row["created_at"] else None,
            )
            for row in rows
        ]


def update_organization(org: Organization) -> Organization:
    """Update an organization."""
    with get_db_connection() as conn:
        _execute(conn, """
            UPDATE organizations
            SET name = ?, node_persistent_id = ?, timezone = ?, is_active = ?
            WHERE org_id = ?
        """, (org.name, org.node_persistent_id, org.timezone, org.is_active, org.org_id))
        conn.commit()
        logger.info("Updated organization: %s", org.org_id)
        return org


def upsert_organization(org: Organization) -> Organization:
    """Create or update an organization."""
    existing = get_organization(org.org_id)
    if existing:
        org.id = existing.id
        return update_organization(org)
    else:
        return create_organization(org)


# =============================================================================
# Daily Report CRUD
# =============================================================================

def save_daily_report(report: DailyReport) -> DailyReport:
    """Save a daily report (upsert - replaces if exists for same org+date)."""
    with get_db_connection() as conn:
        report_json = json.dumps(report.report_data) if not IS_POSTGRES else report.report_data

        if IS_POSTGRES:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily_reports (org_id, report_date, report_data)
                VALUES (%s, %s, %s)
                ON CONFLICT(org_id, report_date) DO UPDATE SET
                    report_data = EXCLUDED.report_data,
                    created_at = CURRENT_TIMESTAMP
                RETURNING id
            """, (report.org_id, report.report_date, json.dumps(report.report_data)))
            result = cursor.fetchone()
            report.id = result["id"] if result else None
        else:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO daily_reports (org_id, report_date, report_data)
                VALUES (?, ?, ?)
                ON CONFLICT(org_id, report_date) DO UPDATE SET
                    report_data = excluded.report_data,
                    created_at = CURRENT_TIMESTAMP
            """, (report.org_id, report.report_date, report_json))
            report.id = cursor.lastrowid

        conn.commit()
        logger.info("Saved daily report for %s on %s", report.org_id, report.report_date)
        return report


def get_daily_report(org_id: str, report_date: str) -> Optional[DailyReport]:
    """Get a specific daily report."""
    with get_db_connection() as conn:
        row = _execute(conn, """
            SELECT * FROM daily_reports
            WHERE org_id = ? AND report_date = ?
        """, (org_id, report_date), fetch="one")

        if row:
            report_data = row["report_data"]
            if isinstance(report_data, str):
                report_data = json.loads(report_data)

            return DailyReport(
                id=row["id"],
                org_id=row["org_id"],
                report_date=str(row["report_date"]),
                report_data=report_data,
                created_at=str(row["created_at"]) if row["created_at"] else None,
            )
        return None


def get_latest_report(org_id: str) -> Optional[DailyReport]:
    """Get the most recent daily report for an organization."""
    with get_db_connection() as conn:
        row = _execute(conn, """
            SELECT * FROM daily_reports
            WHERE org_id = ?
            ORDER BY report_date DESC
            LIMIT 1
        """, (org_id,), fetch="one")

        if row:
            report_data = row["report_data"]
            if isinstance(report_data, str):
                report_data = json.loads(report_data)

            return DailyReport(
                id=row["id"],
                org_id=row["org_id"],
                report_date=str(row["report_date"]),
                report_data=report_data,
                created_at=str(row["created_at"]) if row["created_at"] else None,
            )
        return None


def get_reports_in_range(org_id: str, start_date: str, end_date: str) -> List[DailyReport]:
    """Get daily reports within a date range."""
    with get_db_connection() as conn:
        rows = _execute(conn, """
            SELECT * FROM daily_reports
            WHERE org_id = ? AND report_date >= ? AND report_date <= ?
            ORDER BY report_date DESC
        """, (org_id, start_date, end_date), fetch="all")

        reports = []
        for row in rows:
            report_data = row["report_data"]
            if isinstance(report_data, str):
                report_data = json.loads(report_data)
            reports.append(DailyReport(
                id=row["id"],
                org_id=row["org_id"],
                report_date=str(row["report_date"]),
                report_data=report_data,
                created_at=str(row["created_at"]) if row["created_at"] else None,
            ))
        return reports


def get_recent_reports(org_id: str, limit: int = 30) -> List[DailyReport]:
    """Get the most recent N daily reports for an organization."""
    with get_db_connection() as conn:
        rows = _execute(conn, """
            SELECT * FROM daily_reports
            WHERE org_id = ?
            ORDER BY report_date DESC
            LIMIT ?
        """, (org_id, limit), fetch="all")

        reports = []
        for row in rows:
            report_data = row["report_data"]
            if isinstance(report_data, str):
                report_data = json.loads(report_data)
            reports.append(DailyReport(
                id=row["id"],
                org_id=row["org_id"],
                report_date=str(row["report_date"]),
                report_data=report_data,
                created_at=str(row["created_at"]) if row["created_at"] else None,
            ))
        return reports


def get_all_report_dates(org_id: str) -> List[str]:
    """Get all dates that have reports for an organization."""
    with get_db_connection() as conn:
        rows = _execute(conn, """
            SELECT report_date FROM daily_reports
            WHERE org_id = ?
            ORDER BY report_date DESC
        """, (org_id,), fetch="all")
        return [str(row["report_date"]) for row in rows]


def get_report_count(org_id: str) -> int:
    """Get the total number of reports for an organization."""
    with get_db_connection() as conn:
        row = _execute(conn, """
            SELECT COUNT(*) as count FROM daily_reports WHERE org_id = ?
        """, (org_id,), fetch="one")
        return row["count"] if row else 0


def get_date_range(org_id: str) -> Optional[Dict[str, str]]:
    """Get the earliest and latest report dates for an organization."""
    with get_db_connection() as conn:
        row = _execute(conn, """
            SELECT MIN(report_date) as earliest, MAX(report_date) as latest
            FROM daily_reports WHERE org_id = ?
        """, (org_id,), fetch="one")
        if row and row["earliest"]:
            return {
                "earliest": str(row["earliest"]),
                "latest": str(row["latest"]),
            }
        return None


# =============================================================================
# Scheduler Health Tracking
# =============================================================================

def log_scheduler_run(run_type: str, status: str, reports_generated: int = 0, error_message: str = None) -> int:
    """Log a scheduler run for monitoring."""
    with get_db_connection() as conn:
        if IS_POSTGRES:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scheduler_runs (run_type, status, completed_at, reports_generated, error_message)
                VALUES (%s, %s, CURRENT_TIMESTAMP, %s, %s)
                RETURNING id
            """, (run_type, status, reports_generated, error_message))
            result = cursor.fetchone()
            run_id = result["id"] if result else None
        else:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO scheduler_runs (run_type, status, completed_at, reports_generated, error_message)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?)
            """, (run_type, status, reports_generated, error_message))
            run_id = cursor.lastrowid

        conn.commit()
        return run_id


def get_last_successful_run() -> Optional[Dict]:
    """Get the last successful scheduler run."""
    with get_db_connection() as conn:
        row = _execute(conn, """
            SELECT * FROM scheduler_runs
            WHERE status = 'success'
            ORDER BY completed_at DESC
            LIMIT 1
        """, fetch="one")

        if row:
            return {
                "id": row["id"],
                "run_type": row["run_type"],
                "status": row["status"],
                "started_at": str(row["started_at"]) if row["started_at"] else None,
                "completed_at": str(row["completed_at"]) if row["completed_at"] else None,
                "reports_generated": row["reports_generated"],
            }
        return None


def get_recent_scheduler_runs(limit: int = 10) -> List[Dict]:
    """Get recent scheduler runs for monitoring."""
    with get_db_connection() as conn:
        rows = _execute(conn, """
            SELECT * FROM scheduler_runs
            ORDER BY started_at DESC
            LIMIT ?
        """, (limit,), fetch="all")

        return [
            {
                "id": row["id"],
                "run_type": row["run_type"],
                "status": row["status"],
                "started_at": str(row["started_at"]) if row["started_at"] else None,
                "completed_at": str(row["completed_at"]) if row["completed_at"] else None,
                "reports_generated": row["reports_generated"],
                "error_message": row["error_message"],
            }
            for row in rows
        ]


def get_database_info() -> Dict:
    """Get information about the current database connection."""
    with get_db_connection() as conn:
        if IS_POSTGRES:
            row = _execute(conn, "SELECT version()", fetch="one")
            version = row["version"] if row else "Unknown"
            db_type = "PostgreSQL"
        else:
            row = _execute(conn, "SELECT sqlite_version()", fetch="one")
            version = list(row.values())[0] if row else "Unknown"
            db_type = "SQLite"

        # Count reports
        report_count = _execute(conn, "SELECT COUNT(*) as count FROM daily_reports", fetch="one")
        org_count = _execute(conn, "SELECT COUNT(*) as count FROM organizations", fetch="one")

        return {
            "type": db_type,
            "version": version,
            "is_postgres": IS_POSTGRES,
            "reports_count": report_count["count"] if report_count else 0,
            "organizations_count": org_count["count"] if org_count else 0,
        }


# =============================================================================
# Catch-up Logic
# =============================================================================

def get_missing_report_dates(org_id: str, days_back: int = 7, timezone: str = "America/Los_Angeles") -> List[str]:
    """
    Find dates in the last N days that don't have reports.
    Used for catch-up logic on startup.

    Important: Uses the organization's timezone to determine "today" so we only
    generate reports for completed days (yesterday and before in that timezone).
    """
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    # Use the org's timezone to determine what "today" is
    # This ensures we don't generate reports for days that haven't finished yet
    tz = ZoneInfo(timezone)
    today = datetime.now(tz).date()
    expected_dates = set()

    for i in range(1, days_back + 1):  # Start from 1 (yesterday) to days_back
        d = today - timedelta(days=i)
        expected_dates.add(d.isoformat())

    existing_dates = set(get_all_report_dates(org_id))
    missing = expected_dates - existing_dates

    return sorted(list(missing))  # Return in chronological order


# =============================================================================
# Utility functions
# =============================================================================

def seed_default_organization():
    """
    Seed the default organization from environment variables.
    Called on startup to ensure current org is in the database.
    """
    org_id = os.getenv("ORG_ID")
    node_id = os.getenv("BROKER_NODE_PERSISTENT_ID")
    client_name = os.getenv("CLIENT_NAME", "Default Organization")
    timezone = os.getenv("DEFAULT_TIMEZONE", "UTC")

    if not org_id or not node_id:
        logger.warning("ORG_ID or BROKER_NODE_PERSISTENT_ID not set, skipping seed")
        return None

    org = Organization(
        id=None,
        org_id=org_id,
        name=client_name,
        node_persistent_id=node_id,
        timezone=timezone,
        is_active=True,
    )

    return upsert_organization(org)


def get_database_info() -> Dict[str, Any]:
    """Get database connection info (for health checks)."""
    return {
        "type": "postgresql" if IS_POSTGRES else "sqlite",
        "url_configured": bool(os.getenv("DATABASE_URL")),
        "is_production": IS_POSTGRES,
    }
