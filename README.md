## Paul Logistics Analytics Dashboard

Analytics dashboard and API service for Paul Logistics call data, built with **FastAPI** (backend) and **Next.js** (frontend), querying **ClickHouse** for real-time metrics.

### Features

- **Daily Reports**: Automated report generation at 6 AM PT with KPIs, breakdowns, and trends
- **Interactive Dashboard**: Modern Next.js frontend with charts, PDF export, and metric tooltips
- **Real-time Analytics**: Direct ClickHouse queries for live metrics
- **Scheduler**: APScheduler-based job runner with catch-up logic for missed reports

If you're adapting this for a different client, read [`CLIENT_ADAPTATION.md`](./CLIENT_ADAPTATION.md).
For initial setup, see [`CLICKHOUSE_SETUP.md`](./CLICKHOUSE_SETUP.md).

---

### Architecture (how the service is wired)

- **`main.py`**: FastAPI app + HTTP endpoints.
- **`db.py`**: ClickHouse connection + `fetch_*` functions returning dataclasses.
- **`queries.py`**: string-building helpers that generate ClickHouse SQL.

**Flow:** HTTP request → `main.py` endpoint → `db.py` `fetch_*` → `queries.py` query builder → ClickHouse → dataclass → JSON response.

The queries read from these ClickHouse tables:
- `public_runs`
- `public_sessions`
- `public_node_outputs` (JSON in `flat_data`)
- `public_nodes`

---

### Setup

#### 1) Create a virtualenv + install deps

```bash
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --upgrade pip
pip install -r requirements.txt
```

#### 2) Configure environment variables

The app loads a local `.env` file from the repo root (same directory as `main.py`). You can also set env vars in your shell / deployment.

Quickstart:

```bash
cp env.example .env
```

Required / commonly used env vars:
- **`CLICKHOUSE_URL`** or **`CLICKHOUSE_HOST`**: ClickHouse host (supports `host:port` or full URL)
- **`CLICKHOUSE_USERNAME`** or **`CLICKHOUSE_USER`**
- **`CLICKHOUSE_PASSWORD`**
- **`CLICKHOUSE_DATABASE`**
- **`CLICKHOUSE_SECURE`**: `true/false` (use `true` for ClickHouse Cloud)
- **`ORG_ID`**: the org id used to filter data for the client (use `GET /debug-node-orgs` to find the correct value for your node)
- **`ALLOWED_EMBED_ORIGINS`**: CORS allowlist (comma-separated) or `*`

---

### Run locally

```bash
source venv/bin/activate
uvicorn main:app --reload
```

Then open:
- `http://127.0.0.1:8000/health`

---

### API endpoints

All stats endpoints accept optional query params:
- **`start_date`** and **`end_date`**: ISO timestamps (example: `2024-01-01T00:00:00`).
- If omitted, most queries default to **last 30 days**.

#### Basics
- **`GET /`**: welcome message
- **`GET /health`**: health check

#### Metrics
- **`GET /call-stage-stats`**
- **`GET /carrier-asked-transfer-over-total-transfer-attempts-stats`**
- **`GET /carrier-asked-transfer-over-total-call-attempts-stats`**
- **`GET /load-not-found-stats`**
- **`GET /load-status-stats`**
- **`GET /successfully-transferred-for-booking-stats`**
- **`GET /call-classification-stats`**
- **`GET /carrier-qualification-stats`**
- **`GET /pricing-stats`**
- **`GET /carrier-end-state-stats`**
- **`GET /percent-non-convertible-calls-stats`**
- **`GET /number-of-unique-loads-stats`**
- **`GET /list-of-unique-loads-stats`**
- **`GET /calls-without-carrier-asked-for-transfer-stats`**
- **`GET /total-calls-and-total-duration-stats`**
- **`GET /duration-carrier-asked-for-transfer-stats`**
- **`GET /daily-node-outputs`** (convenience endpoint: pulls a single day’s raw rows + extracted fields; defaults to yesterday)

#### Aggregated
- **`GET /all-stats`**: returns a single JSON payload containing many of the stats above, plus an `errors` map if any sub-call fails.

---

### Query conventions (important when changing clients)

Most queries follow the same structure:
- `recent_runs`: runs filtered by date range
- `sessions`: sessions filtered by `org_id` + **excluding a hardcoded test number**
- join `public_node_outputs` → extract JSON fields from `flat_data` with `JSONExtractString()`
- use `countDistinct(run_id)` for call counts

**Client-specific configuration (via environment variables):**
- `ORG_ID` - Organization filter for data isolation
- `BROKER_NODE_PERSISTENT_ID` - Primary node for analytics queries
- `EXCLUDED_USER_NUMBERS` - Test phone numbers to filter out
- JSON paths inside `flat_data` (may differ by client/node)

See [`CLIENT_ADAPTATION.md`](./CLIENT_ADAPTATION.md) for the complete adaptation checklist.

---

### Adding a new metric (developer workflow)

1. **Add a query builder** in `queries.py` (CTEs + JSON extraction + safe division).
2. **Add a dataclass** in `db.py` for the result shape.
3. **Add a `fetch_*` function** in `db.py` that runs the query and maps rows → dataclass.
4. **Add a FastAPI endpoint** in `main.py` that calls the fetcher and serializes response JSON.
5. If you want it in the combined view, **wire it into `/all-stats`**.

---

### Tests / quick manual checks

There are a few script-style tests you can run directly:

```bash
source venv/bin/activate
python3 test_call_stage_stats.py
python3 test_percent_non_convertible_calls.py
```

There’s also `curl_examples.sh` as a reference for calling endpoints.

---

### Deployment

#### Railway (Recommended)

This project is configured for Railway deployment with Docker:

**Backend Service:**
1. Create a new project in Railway
2. Add a PostgreSQL database (Railway provides this)
3. Connect your GitHub repo and select the root directory
4. Railway will auto-detect the `Dockerfile` and `railway.toml`
5. Set environment variables:
   - `DATABASE_URL` - automatically set by Railway PostgreSQL
   - `CLICKHOUSE_URL`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`, `CLICKHOUSE_DATABASE`
   - `ORG_ID`, `BROKER_NODE_PERSISTENT_ID`, `CLIENT_NAME`
   - `SCHEDULER_ENABLED=true`, `SCHEDULER_HOUR=6`, `SCHEDULER_MINUTE=0`
   - `DEFAULT_TIMEZONE=America/Chicago`

**Frontend Service:**
1. Add another service in the same project
2. Connect the same repo but select the `frontend/` directory
3. Set environment variable:
   - `NEXT_PUBLIC_API_URL=https://your-backend-service.railway.app`

#### Docker Compose (Local Development)

```bash
docker compose up --build
```

This starts:
- Backend on `http://localhost:8000`
- Frontend on `http://localhost:3000`
- PostgreSQL on `localhost:5432`

#### Heroku (Legacy)

This repo includes a Heroku-style `Procfile`:

```
web: uvicorn main:app --host 0.0.0.0 --port $PORT
```

Set the same env vars (ClickHouse + `ORG_ID` + `DATABASE_URL`) in your deployment environment.

---

### Production Features

The system includes production-ready reliability features:

- **PostgreSQL/SQLite dual support**: Auto-detects `DATABASE_URL` - uses PostgreSQL in production, SQLite locally
- **Automated daily reports**: Scheduler runs at 6 AM (configurable) to generate and store reports
- **Catch-up logic**: On startup, automatically fills any missing reports from the last 7 days
- **Retry logic**: Failed report generation retries up to 3 times with 60-second delays
- **Health tracking**: All scheduler runs are logged to database for monitoring
- **Health endpoints**: `/api/scheduler/health` for comprehensive monitoring

See [`BACKEND_ARCHITECTURE.md`](./BACKEND_ARCHITECTURE.md) for detailed documentation.
