# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FastAPI-based analytics service for PepsiCo logistics/carrier call data. The service queries ClickHouse to provide weekly analytics endpoints for call tracking, carrier transfers, load statuses, and various performance metrics.

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
- `CLICKHOUSE_HOST` or `CLICKHOUSE_URL` - ClickHouse server address
- `CLICKHOUSE_USERNAME` or `CLICKHOUSE_USER` - database username
- `CLICKHOUSE_PASSWORD` - database password
- `CLICKHOUSE_DATABASE` - target database
- `CLICKHOUSE_SECURE` - set to "true" for HTTPS connections
- `ORG_ID` - organization identifier for data filtering
- `ALLOWED_EMBED_ORIGINS` - CORS origins (comma-separated or "*")

### Node IDs

Two critical node identifiers used throughout:
- `PEPSI_BROKER_NODE_ID = "01999d78-d321-7db5-ae1f-ebfddc2bff11"` - used for most queries
- `PEPSI_FBR_NODE_ID = "0199f2f5-ec8f-73e4-898b-09a2286e240e"` - used for unique load queries (post Nov 7, 2025)

### Unique Loads Cutoff Logic

The `fetch_number_of_unique_loads` and `fetch_list_of_unique_loads` functions implement date-based query switching:
- Before Nov 7, 2025: use broker_node queries with `result.load.reference_number`
- Nov 7, 2025 and after: use FBR queries with `load.custom_load_id`
- Spanning both periods: merge results from both query types

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

All endpoints accept optional `start_date` and `end_date` query parameters in ISO format (e.g., `2024-01-01T00:00:00`). Defaults to last 30 days if not provided.

Key endpoints:
- `/call-stage-stats` - call stage distribution
- `/carrier-asked-transfer-over-total-transfer-attempts-stats` - transfer ratios
- `/load-status-stats` - load status breakdown
- `/percent-non-convertible-calls-stats` - non-convertible call percentage
- `/number-of-unique-loads-stats` - unique load count and calls-per-load ratio
- `/all-stats` - aggregated view of all statistics with error handling

## Working with Queries

### Adding New Metrics

1. Create query builder function in `queries.py` following CTE pattern
2. Create dataclass in `db.py` for result structure
3. Add fetch function in `db.py` using `_json_each_row()`
4. Add endpoint in `main.py` with proper error handling
5. Test number filter: always exclude `s.user_number != '+19259898099'`

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

Always join sessions and filter test numbers:
```sql
sessions AS (
    SELECT run_id, user_number FROM public_sessions
    WHERE {date_filter}
    AND org_id = '{org_id}'
    AND user_number != '+19259898099'
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

## Deployment

This service is configured for Heroku deployment:
- `Procfile` specifies web dyno command
- Port assigned via `$PORT` environment variable
- All environment variables set through Heroku config vars
