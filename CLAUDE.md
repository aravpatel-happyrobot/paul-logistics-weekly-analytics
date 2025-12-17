# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FastAPI-based analytics service for logistics/carrier call data. Originally built for PepsiCo but designed to be adaptable to other clients (currently being adapted for Paul Logistics). The service queries ClickHouse to provide analytics endpoints for call tracking, carrier transfers, load statuses, and various performance metrics.

## Architecture

### Core Components

- **main.py**: FastAPI application with CORS-enabled REST endpoints
- **db.py**: ClickHouse database client and data fetching logic (1340+ lines)
- **queries.py**: SQL query builders for ClickHouse analytics (1170+ lines)

### Data Model

The application uses dataclasses to represent analytics results. All queries pull data from ClickHouse tables:
- `public_runs` - call session runs
- `public_sessions` - session metadata with duration tracking
- `public_node_outputs` - structured JSON data in `flat_data` column
- `public_nodes` - node configuration

### Query Architecture

Queries use Common Table Expressions (CTEs) with these standard patterns:
1. `recent_runs` - filters runs by date range
2. `sessions` - filters sessions by org_id and excludes test number `+19259898099`
3. Main aggregation logic extracting JSON fields from `flat_data` using `JSONExtractString()`
4. Percentage calculations using `ROUND((count * 100.0) / total, 2)`

All queries use `countDistinct(run_id)` for accurate call counts.

### Environment Configuration

Required environment variables (loaded from `.env`):

**ClickHouse Connection:**
- `CLICKHOUSE_HOST` or `CLICKHOUSE_URL` - ClickHouse server address
- `CLICKHOUSE_USERNAME` or `CLICKHOUSE_USER` - database username
- `CLICKHOUSE_PASSWORD` - database password
- `CLICKHOUSE_DATABASE` - target database
- `CLICKHOUSE_SECURE` - set to "true" for HTTPS connections (ClickHouse Cloud)

**Client/Scoping Configuration:**
- `ORG_ID` - organization identifier for data filtering (use `/debug-node-orgs` to discover)
- `BROKER_NODE_PERSISTENT_ID` - primary node ID for analytics queries
- `FBR_NODE_PERSISTENT_ID` - (optional) secondary node ID for unique load queries
- `CLIENT_NAME` - label for API docs/branding (default: "Logistics")
- `EXCLUDED_USER_NUMBERS` - comma-separated list of phone numbers to exclude from analytics
- `DEFAULT_TIMEZONE` - timezone for daily reports (default: "UTC")

**CORS:**
- `ALLOWED_EMBED_ORIGINS` - CORS origins (comma-separated or "*")

Copy `env.example` to `.env` to get started. See `CLICKHOUSE_SETUP.md` for detailed setup instructions.

### Client Adaptability

The codebase is transitioning from PepsiCo-specific to multi-client support:
- Environment variables now control node IDs and excluded numbers (replacing hardcoded constants)
- For adapting to a new client, see `CLIENT_ADAPTATION.md` for a complete checklist
- Key adaptation points: ORG_ID, node persistent IDs, JSON extraction paths, excluded phone numbers

### Node IDs and Unique Loads Logic

The application queries `public_node_outputs` filtered by `node_persistent_id`. Different nodes may be used for different queries:
- Most queries use the node ID from `BROKER_NODE_PERSISTENT_ID` env var
- Unique loads queries may use a different node ID from `FBR_NODE_PERSISTENT_ID` (if set)
- Some implementations include date-based query switching (cutoff logic) for unique loads if the workflow changed over time

## Common Development Commands

### Running the Application

```bash
# Start development server
uvicorn main:app --reload

# Start production server (Heroku deployment)
uvicorn main:app --host 0.0.0.0 --port $PORT
```

### Testing

```bash
# Run test examples
python test_call_stage_stats.py

# Test with curl
bash curl_examples.sh

# Test specific endpoint
curl "http://localhost:8000/call-stage-stats?start_date=2024-01-01T00:00:00&end_date=2024-01-31T23:59:59"
```

### Installing Dependencies

```bash
pip install -r requirements.txt
```

## API Endpoints

### Core Endpoints
- `/` - welcome message
- `/health` - health check

### Debug/Discovery Endpoints
- `/debug-config` - show runtime configuration (no secrets)
- `/debug-schema/{table_name}` - show ClickHouse table schema (helps validate columns)
- `/debug-node-count` - diagnostics for node output counts and org filtering
- `/debug-node-orgs` - discover which org_ids exist for a given node

### Daily Reports
- `/daily-report` - one-stop daily analytics (defaults to yesterday in specified timezone)
- `/daily-node-outputs` - raw node outputs for a single day with extracted fields

### Metrics Endpoints

All metrics endpoints accept optional `start_date` and `end_date` query parameters in ISO format (e.g., `2024-01-01T00:00:00`). Defaults to last 30 days if not provided.

- `/call-stage-stats` - call stage distribution
- `/call-classification-stats` - call classification breakdown
- `/carrier-asked-transfer-over-total-transfer-attempts-stats` - transfer ratios
- `/carrier-asked-transfer-over-total-call-attempts-stats` - transfer over all calls
- `/load-status-stats` - load status breakdown
- `/load-not-found-stats` - load not found percentage
- `/successfully-transferred-for-booking-stats` - successful transfer percentage
- `/carrier-qualification-stats` - carrier qualification distribution
- `/carrier-end-state-stats` - carrier end state breakdown
- `/pricing-stats` - pricing notes distribution
- `/percent-non-convertible-calls-stats` - non-convertible call percentage
- `/number-of-unique-loads-stats` - unique load count and calls-per-load ratio
- `/list-of-unique-loads-stats` - list of all unique load IDs
- `/calls-without-carrier-asked-for-transfer-stats` - detailed breakdown of calls without carrier transfers
- `/total-calls-and-total-duration-stats` - total call volume and duration
- `/duration-carrier-asked-for-transfer-stats` - duration of carrier transfer calls
- `/all-stats` - aggregated view of all statistics with error handling

## Working with Queries

### Adding New Metrics

1. Create query builder function in `queries.py` following CTE pattern
2. Create dataclass in `db.py` for result structure
3. Add fetch function in `db.py` using `_json_each_row()`
4. Add endpoint in `main.py` with proper error handling
5. Use the excluded numbers from env var (loaded via `_get_excluded_user_numbers_sql()` in `db.py`)

### JSON Path Extraction

ClickHouse queries use nested paths in flat_data:
- Call data: `JSONExtractString(no.flat_data, 'result.call.call_stage')`
- Transfer data: `JSONExtractString(no.flat_data, 'result.transfer.transfer_reason')`
- Load data: `JSONExtractString(no.flat_data, 'result.load.load_status')`
- Carrier data: `JSONExtractString(no.flat_data, 'result.carrier.carrier_qualification')`
- Pricing data: `JSONExtractString(no.flat_data, 'result.pricing.pricing_notes')`

Always validate JSON fields exist: `JSONHas(no.flat_data, 'path') = 1`

### Query Performance Settings

Large date range queries use `CLICKHOUSE_QUERY_SETTINGS`:
- `max_execution_time: 180` seconds
- `max_memory_usage: 10_000_000_000` bytes (10GB)
- `max_threads: 16`

## Common Patterns

### Date Filtering

```python
date_filter = (
    f"timestamp >= parseDateTime64BestEffort('{start_date}') AND timestamp < parseDateTime64BestEffort('{end_date}')"
    if start_date and end_date
    else "timestamp >= now() - INTERVAL 30 DAY"
)
```

### Session Filtering

Always join sessions and filter test numbers (now using env var):
```sql
sessions AS (
    SELECT run_id, user_number FROM public_sessions
    WHERE {date_filter}
    AND org_id = '{org_id}'
    {excluded_user_numbers_sql}  -- Generated from EXCLUDED_USER_NUMBERS env var
)
```

### Percentage Calculations

```sql
ROUND((count * 100.0) / total, 2) AS percentage
```

Use `ifNull()` and `nullIf()` for safe division:
```sql
ifNull(round((count * 100.0) / nullIf(total, 0), 2), 0) AS percentage
```

## Additional Documentation

This repository includes supplementary documentation for specific use cases:

- **`README.md`** - Quick start guide and architecture overview
- **`CLICKHOUSE_SETUP.md`** - Step-by-step setup guide for non-SQL users
  - How to configure environment variables
  - How to discover the correct node IDs and org IDs
  - How to use the daily endpoints without writing SQL
  - What JSON fields are extracted and why
- **`CLIENT_ADAPTATION.md`** - Complete checklist for adapting from one client to another
  - Three critical configuration points (ORG_ID, node IDs, excluded numbers)
  - How to validate JSON schema differences
  - Unique loads cutoff logic explanation
  - Step-by-step porting checklist

**For setting up a new client:** start with `CLICKHOUSE_SETUP.md` then follow `CLIENT_ADAPTATION.md`.

## Discovery Workflow

When working with a new client or debugging missing data, follow this workflow:

1. **Verify ClickHouse connection**: `GET /health`
2. **Check runtime config**: `GET /debug-config` (shows what env vars were loaded)
3. **Discover node IDs**: Query ClickHouse for active node persistent IDs or use the UI
4. **Find correct org_id**: `GET /debug-node-orgs?node_persistent_id=<your-node-id>`
5. **Validate data exists**: `GET /debug-node-count?node_persistent_id=<your-node-id>`
6. **Inspect raw data**: `GET /daily-node-outputs?date=YYYY-MM-DD&limit=10`
7. **Validate JSON paths**: Check the extracted fields match your node's `flat_data` schema
8. **Test a metric endpoint**: Try `/call-stage-stats` with a known date range

## Deployment

This service is configured for Heroku deployment:
- `Procfile` specifies web dyno command
- Port assigned via `$PORT` environment variable
- All environment variables set through Heroku config vars
