# Backend Architecture

This document explains how the analytics backend works, from data flow to storage to scheduling.

---

## Overview

The backend is a **FastAPI** application that:

1. **Queries ClickHouse** for real-time call analytics
2. **Stores daily report snapshots** in SQLite for historical access
3. **Runs a scheduler** to automatically generate reports daily at 6 AM
4. **Serves a REST API** for the frontend dashboard

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Data Flow                                       │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ClickHouse (Source)          FastAPI Backend              SQLite (Cache)  │
│   ─────────────────           ───────────────               ──────────────  │
│                                                                              │
│   ┌─────────────────┐         ┌─────────────────┐         ┌──────────────┐  │
│   │ public_runs     │────────▶│                 │────────▶│ organizations│  │
│   │ public_sessions │         │   Scheduler     │         │ daily_reports│  │
│   │ public_node_    │────────▶│   (6 AM job)    │         └──────────────┘  │
│   │   outputs       │         │                 │                 │         │
│   │ public_nodes    │         └────────┬────────┘                 │         │
│   └─────────────────┘                  │                          │         │
│          │                             │                          │         │
│          │                    ┌────────▼────────┐                 │         │
│          │                    │   REST API      │◀────────────────┘         │
│          └───────────────────▶│   Endpoints     │                           │
│             (live queries)    └────────┬────────┘                           │
│                                        │                                    │
│                               ┌────────▼────────┐                           │
│                               │   Frontend      │                           │
│                               │   Dashboard     │                           │
│                               └─────────────────┘                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Files

### `main.py` - FastAPI Application

The main entry point. Defines all REST API endpoints.

**Key responsibilities:**
- Initialize FastAPI app with CORS middleware
- Define all API endpoints (analytics, reports, organizations)
- Handle request/response formatting
- Manage application lifespan (startup/shutdown)

**Lifespan events:**
```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    # STARTUP
    ensure_db_initialized()      # Create SQLite tables if needed
    seed_default_organization()  # Add Paul Logistics from .env
    start_scheduler()            # Start the 6 AM daily job

    yield

    # SHUTDOWN
    stop_scheduler()             # Gracefully stop scheduler
```

### `db.py` - ClickHouse Client

Handles all communication with ClickHouse.

**Key responsibilities:**
- Establish ClickHouse connections (using `clickhouse-connect`)
- Execute SQL queries and parse results
- Transform raw data into typed dataclasses
- Handle connection errors and retries

**How queries work:**
```python
def fetch_call_classifcation_stats(start_date, end_date):
    # 1. Build the SQL query
    query = call_classification_stats_query(date_filter, org_id, node_id, excluded_numbers)

    # 2. Execute against ClickHouse
    client = get_clickhouse_client()
    result = client.query(query, settings=CLICKHOUSE_QUERY_SETTINGS)

    # 3. Parse into dataclass
    return [CallClassificationStats(**row) for row in result.named_results()]
```

### `queries.py` - SQL Query Builders

Contains all SQL query templates as Python functions.

**Query pattern (CTE-based):**
```sql
WITH recent_runs AS (
    -- Filter runs by date range
    SELECT id AS run_id FROM public_runs
    WHERE timestamp >= '2025-12-15' AND timestamp < '2025-12-16'
),
sessions AS (
    -- Filter sessions by org, exclude test numbers
    SELECT run_id, user_number FROM public_sessions
    WHERE org_id = '01951f56-...'
    AND user_number NOT IN ('+19259898099')
),
aggregation AS (
    -- Main query logic: extract JSON fields, count, group
    SELECT
        JSONExtractString(no.flat_data, 'result.call.call_classification') AS classification,
        countDistinct(s.run_id) AS count
    FROM public_node_outputs no
    INNER JOIN sessions s ON no.run_id = s.run_id
    WHERE no.node_persistent_id = '019b099d-...'
    GROUP BY classification
)
SELECT * FROM aggregation
```

### `storage.py` - SQLite Storage Layer

Manages local persistence of daily reports and organization configs.

**Database schema:**
```sql
-- Organizations (multi-org support)
CREATE TABLE organizations (
    id INTEGER PRIMARY KEY,
    org_id TEXT UNIQUE NOT NULL,      -- e.g., '01951f56-8c6e-...'
    name TEXT NOT NULL,                -- e.g., 'Paul Logistics'
    node_persistent_id TEXT NOT NULL,  -- e.g., '019b099d-77fd-...'
    timezone TEXT DEFAULT 'UTC',
    is_active BOOLEAN DEFAULT 1,
    created_at TIMESTAMP
);

-- Daily report snapshots
CREATE TABLE daily_reports (
    id INTEGER PRIMARY KEY,
    org_id TEXT NOT NULL,
    report_date DATE NOT NULL,         -- e.g., '2025-12-15'
    report_data TEXT NOT NULL,         -- JSON blob with full report
    created_at TIMESTAMP,
    UNIQUE(org_id, report_date)        -- One report per org per day
);
```

**Key functions:**
- `save_daily_report()` - Upsert a report (replaces if exists)
- `get_daily_report()` - Get report by date
- `get_latest_report()` - Get most recent report
- `get_recent_reports()` - Get last N reports
- `upsert_organization()` - Create or update org config

### `scheduler.py` - Daily Report Automation

Runs background jobs using APScheduler.

**How it works:**

1. **On app startup:** `start_scheduler()` is called
2. **Creates a cron job:** Runs `run_daily_report_job()` at 6:00 AM
3. **Daily job logic:**
   ```python
   def run_daily_report_job():
       # For each active organization...
       for org in get_all_organizations(active_only=True):
           # Generate report for yesterday
           generate_daily_report_for_org(org)
   ```

**Report generation flow:**
```
generate_daily_report_for_org(org, date="2025-12-15")
│
├─► Check if report already exists → Skip if yes
│
├─► Calculate date range (full day in org's timezone)
│   └─► 2025-12-15T00:00:00-06:00 to 2025-12-16T00:00:00-06:00
│
├─► Fetch all metrics from ClickHouse:
│   ├─► call_stage_stats
│   ├─► call_classification_stats
│   ├─► load_status_stats
│   ├─► carrier_end_state_stats
│   ├─► non_convertible_calls (3 variants)
│   ├─► transfer_stats (2 variants)
│   └─► total_calls_and_duration
│
├─► Build report JSON structure
│
└─► Save to SQLite via save_daily_report()
```

---

## API Endpoints

### Live Analytics (from ClickHouse)

| Endpoint | Description |
|----------|-------------|
| `GET /daily-report` | Generate live report for any date |
| `GET /call-stage-stats` | Call stage distribution |
| `GET /call-classification-stats` | Call classification breakdown |
| `GET /load-status-stats` | Load status breakdown |
| `GET /carrier-end-state-stats` | Carrier end state breakdown |
| `GET /non-convertible-calls-*` | Non-convertible call metrics |
| `GET /total-calls-and-total-duration-stats` | Call volume and duration |

### Stored Reports (from SQLite)

| Endpoint | Description |
|----------|-------------|
| `GET /api/reports` | List stored reports (with summary KPIs) |
| `GET /api/reports/latest` | Get most recent stored report |
| `GET /api/reports/{date}` | Get specific stored report |
| `POST /api/reports/generate` | Manually trigger report generation |
| `POST /api/reports/backfill` | Generate reports for a date range |

### Organizations

| Endpoint | Description |
|----------|-------------|
| `GET /api/orgs` | List all organizations |
| `GET /api/orgs/{org_id}` | Get specific organization |
| `POST /api/orgs` | Create new organization |
| `PATCH /api/orgs/{org_id}` | Update organization |

### Scheduler

| Endpoint | Description |
|----------|-------------|
| `GET /api/scheduler/status` | Check scheduler status and next run time |

---

## Configuration

All configuration is via environment variables (see `.env`):

```bash
# Organization scoping
ORG_ID=01951f56-8c6e-7a93-a971-3d06a81a1ab0
BROKER_NODE_PERSISTENT_ID=019b099d-77fd-7e09-9d2b-cd4c476a0a19
CLIENT_NAME=Paul Logistics
DEFAULT_TIMEZONE=America/Chicago

# ClickHouse connection
CLICKHOUSE_URL=https://xxx.clickhouse.cloud:8443
CLICKHOUSE_USERNAME=fde_brokerage_pod
CLICKHOUSE_PASSWORD=***
CLICKHOUSE_DATABASE=default
CLICKHOUSE_SECURE=true

# Optional
SCHEDULER_ENABLED=true
SCHEDULER_HOUR=6
SCHEDULER_MINUTE=0
```

---

## Data Model

### ClickHouse Tables

| Table | Purpose |
|-------|---------|
| `public_runs` | Call session runs (timestamps, IDs) |
| `public_sessions` | Session metadata (org_id, user_number, duration) |
| `public_node_outputs` | Structured call outcomes in `flat_data` JSON |
| `public_nodes` | Node configuration |

### Key JSON Paths in `flat_data`

```javascript
{
  "result": {
    "call": {
      "call_classification": "success",       // Call outcome
      "call_stage": "TRANSFER_SUCCEEDED",     // How far the call got
      "notes": "..."
    },
    "transfer": {
      "transfer_attempt": "YES",              // Was transfer attempted?
      "transfer_reason": "CARRIER_ASKED_FOR_TRANSFER",
      "transfer_success": true
    },
    "load": {
      "load_status": "ACTIVE",
      "reference_number": "12345"
    },
    "carrier": {
      "carrier_qualification": "QUALIFIED",
      "carrier_end_state": "booking_started",
      "carrier_name": "ABC Trucking"
    },
    "pricing": {
      "pricing_notes": "AGREEMENT_REACHED_CARRIER_CONFIRMED",
      "agreed_upon_rate": 1500
    }
  }
}
```

---

## Report Structure

When a daily report is generated and stored, it has this structure:

```json
{
  "date_range": {
    "tz": "America/Chicago",
    "start_date": "2025-12-15T00:00:00-06:00",
    "end_date": "2025-12-16T00:00:00-06:00"
  },
  "kpis": {
    "total_calls": 271,
    "classified_calls": 211,
    "total_duration_hours": 10.23,
    "avg_minutes_per_call": 2.26,
    "success_rate_percent": 15.17,
    "non_convertible_calls_with_carrier_not_qualified": {
      "count": 136,
      "total_calls": 211,
      "percentage": 64.45
    },
    "carrier_not_qualified": {
      "count": 58,
      "total_calls": 211,
      "percentage": 27.49
    },
    "carrier_transfer_over_total_call_attempts": {
      "carrier_asked_count": 28,
      "total_call_attempts": 211,
      "carrier_asked_percentage": 13.27
    },
    "successfully_transferred_for_booking": {
      "successfully_transferred_for_booking_count": 27,
      "total_calls": 211,
      "successfully_transferred_for_booking_percentage": 12.8
    }
  },
  "breakdowns": {
    "call_stage": [...],
    "call_classification": [...],
    "load_status": [...],
    "pricing_notes": [...],
    "carrier_end_state": [...]
  },
  "metadata": {
    "org_id": "01951f56-...",
    "org_name": "Paul Logistics",
    "generated_at": "2025-12-16T06:00:00-06:00"
  }
}
```

---

## Multi-Org Support

The system is designed for multiple organizations:

1. **Each org has its own `node_persistent_id`** that identifies their workflow in ClickHouse
2. **Reports are stored per-org** with `(org_id, report_date)` as unique key
3. **Scheduler loops through all active orgs** and generates reports for each

To add a new organization:

```bash
# Via API
curl -X POST http://localhost:8000/api/orgs \
  -H "Content-Type: application/json" \
  -d '{
    "org_id": "new-org-uuid",
    "name": "New Company",
    "node_persistent_id": "their-node-uuid",
    "timezone": "America/New_York"
  }'
```

---

## Error Handling

- **ClickHouse timeouts:** Queries have a 180-second timeout and 10GB memory limit
- **Missing data:** Endpoints return `null` or empty arrays gracefully
- **Report already exists:** Scheduler skips if report for that date exists
- **Invalid dates:** Returns 400 Bad Request with helpful message

---

## Running the Backend

```bash
# Development
source venv/bin/activate
uvicorn main:app --reload --port 8000

# Production (Railway/Heroku)
uvicorn main:app --host 0.0.0.0 --port $PORT
```

**Health check:** `GET /health` returns `{"status": "healthy"}`

**Debug endpoints:**
- `GET /debug-config` - Show runtime configuration
- `GET /debug-node-count` - Check node output counts
- `GET /api/scheduler/status` - Check scheduler is running
