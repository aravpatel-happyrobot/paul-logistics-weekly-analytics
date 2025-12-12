## Paul Logistics Weekly Analytics (FastAPI + ClickHouse)

This repository is a **FastAPI** service that exposes REST endpoints for “weekly analytics” over call/session data stored in **ClickHouse**.

- **Current state**: the codebase is still branded and hardcoded for **PepsiCo** in several places (node IDs, excluded test phone number, unique-load cutoff logic).
- **Goal**: recreate these stats for **Paul Logistics** by swapping the client-specific identifiers and (if needed) adjusting JSON paths/logic.

If you’re adapting this for Paul Logistics, read [`CLIENT_ADAPTATION.md`](./CLIENT_ADAPTATION.md) first.

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
python -m pip install --upgrade pip
pip install -r requirements.txt
```

#### 2) Configure environment variables

The app loads a local `.env` file from the repo root (same directory as `main.py`). You can also set env vars in your shell / deployment.

Required / commonly used env vars:
- **`CLICKHOUSE_URL`** or **`CLICKHOUSE_HOST`**: ClickHouse host (supports `host:port` or full URL)
- **`CLICKHOUSE_USERNAME`** or **`CLICKHOUSE_USER`**
- **`CLICKHOUSE_PASSWORD`**
- **`CLICKHOUSE_DATABASE`**
- **`CLICKHOUSE_SECURE`**: `true/false` (use `true` for ClickHouse Cloud)
- **`ORG_ID`**: the org id used to filter data for the client
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

#### Aggregated
- **`GET /all-stats`**: returns a single JSON payload containing many of the stats above, plus an `errors` map if any sub-call fails.

---

### Query conventions (important when changing clients)

Most queries follow the same structure:
- `recent_runs`: runs filtered by date range
- `sessions`: sessions filtered by `org_id` + **excluding a hardcoded test number**
- join `public_node_outputs` → extract JSON fields from `flat_data` with `JSONExtractString()`
- use `countDistinct(run_id)` for call counts

**Client-specific parts to watch:**
- `ORG_ID` filter
- `node_persistent_id` filter (currently hardcoded as Pepsi node IDs)
- the excluded user/test phone number (currently `+19259898099`)
- JSON paths inside `flat_data` (may differ by client/node)

See [`CLIENT_ADAPTATION.md`](./CLIENT_ADAPTATION.md) for the concrete checklist.

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
python test_call_stage_stats.py
python test_percent_non_convertible_calls.py
```

There’s also `curl_examples.sh` as a reference for calling endpoints.

---

### Deployment

This repo includes a Heroku-style `Procfile`:

- `web: uvicorn main:app --host 0.0.0.0 --port $PORT`

Set the same env vars (ClickHouse + `ORG_ID`) in your deployment environment.
