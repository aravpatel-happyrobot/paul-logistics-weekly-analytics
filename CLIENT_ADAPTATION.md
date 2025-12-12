## Client adaptation guide (Pepsi ➜ Paul Logistics)

This codebase was originally built for **PepsiCo** analytics and is being adapted to **Paul Logistics**.

The main thing to understand is that almost everything is driven by:
- **which org we’re filtering to** (`ORG_ID`)
- **which node(s) produced the JSON outputs** (`node_persistent_id`)
- **what JSON schema the node outputs** (JSON paths inside `flat_data`)

---

### 1) The three client-specific knobs you must change

#### A) `ORG_ID`
All queries join into `public_sessions` / `public_nodes` and filter to a single org.

- Set **`ORG_ID`** in your `.env` / deployment env for Paul Logistics.
- If `ORG_ID` is missing, most `fetch_*` functions in `db.py` log an error and return `None`/`[]`.

#### B) Node persistent IDs (most important)
In `db.py` there are two hardcoded constants:
- `PEPSI_BROKER_NODE_ID`
- `PEPSI_FBR_NODE_ID`

These are used as `no.node_persistent_id = '...'` in virtually every query.

**For Paul Logistics you need the equivalent node persistent id(s)** for the Paul workflow that produces:
- `result.call.*`
- `result.transfer.*`
- `result.load.*`
- `result.carrier.*`
- `result.pricing.*`

Once you have them, the fastest path is to:
- replace the `PEPSI_*` constants in `db.py`
- update the app branding strings in `main.py`

**Recommended refactor (optional, but cleaner):** move these constants to environment variables, e.g.
- `BROKER_NODE_PERSISTENT_ID`
- `FBR_NODE_PERSISTENT_ID`

…and load them in `db.py` instead of hardcoding.

#### C) Excluded test phone number(s)
Many queries exclude a hardcoded test number:
- `s.user_number != '+19259898099'`

For Paul Logistics:
- either remove this filter
- or replace it with the correct Paul test number(s)

**Recommended refactor:** support an env var like `EXCLUDED_USER_NUMBERS` (comma-separated) and generate the `AND s.user_number NOT IN (...)` clause.

---

### 2) Confirm the JSON schema for Paul Logistics

All metrics depend on JSON paths extracted from `public_node_outputs.flat_data`.

Common paths used today (Pepsi):
- `result.call.call_stage`
- `result.call.call_classification`
- `result.transfer.transfer_reason`
- `result.transfer.transfer_attempt`
- `result.load.load_status`
- `result.load.reference_number` (older unique-load logic)
- `load.custom_load_id` (newer unique-load logic)
- `result.carrier.carrier_qualification`
- `result.carrier.carrier_end_state`
- `result.pricing.pricing_notes`
- `result.pricing.agreed_upon_rate`

If Paul’s node outputs have different paths, you must update the corresponding SQL in `queries.py`.

**Practical workflow to validate quickly:**
1. Pick a Paul node persistent id.
2. Run a small query in ClickHouse to inspect a few `flat_data` payloads for that node.
3. Update `JSONHas(...)` and `JSONExtractString(...)` paths in `queries.py` accordingly.

---

### 3) Unique loads: understand the cutoff logic before you port it

The repo currently has special logic for “unique loads”:
- In `db.py` there is `UNIQUE_LOADS_CUTOFF_DATE = '2025-11-07T00:00:00'`.
- For date ranges **before** the cutoff: the query uses the “broker node” and `result.load.reference_number`.
- For date ranges **after** the cutoff: the query uses the “FBR node” and `load.custom_load_id`.
- If a range spans both: it runs both and merges the unique IDs.

For Paul Logistics, you need to decide:
- Do you have **one** stable load identifier across time?
  - If yes: delete the cutoff complexity and use a single query.
- Did Paul’s workflow change at a known date?
  - If yes: replace the cutoff date and the two JSON paths/node IDs.

The relevant query builders are in `queries.py`:
- `number_of_unique_loads_query(...)` (FBR-style, uses `load.custom_load_id`)
- `number_of_unique_loads_query_broker_node(...)` (broker-style, uses `result.load.reference_number`)
- and the list variants.

---

### 4) What to change to rebrand from Pepsi to Paul

Places that are currently Pepsi-specific:
- **`main.py`**:
  - `title="Pepsi Weekly Analytics API"`
  - root endpoint message
- **`db.py`**:
  - constant names and comments (`PEPSI_*`, `PepsiData`, `PepsiRecord`)

You can keep the naming for now (functionality won’t break), but it’s worth renaming for clarity once Paul is working.

---

### 5) Where to implement Paul-specific metrics changes

- **If a metric is wrong because it’s pointing at the wrong node:** update the node persistent id constant(s) in `db.py`.
- **If a metric is wrong because it’s extracting the wrong JSON path:** update the SQL in `queries.py`.
- **If the endpoint output shape needs to change:** update `main.py` serialization.

---

### 6) Suggested “Paul Logistics port” checklist

1. Set `ORG_ID` for Paul Logistics.
2. Identify the Paul node persistent id(s) that correspond to the workflow.
3. Replace `PEPSI_BROKER_NODE_ID` / `PEPSI_FBR_NODE_ID` with Paul equivalents (or refactor to env vars).
4. Update the excluded test number(s).
5. Validate the JSON paths in `queries.py` against real Paul `flat_data`.
6. Run locally and hit `/health`, then `/call-stage-stats` for a small known date range.
7. Iterate metric-by-metric; only then revisit naming/cleanup.

---

### Notes / gotchas

- **Date filtering**: most queries use `parseDateTime64BestEffort(...)` and treat `end_date` as an exclusive upper bound.
- **Big ranges**: the ClickHouse client runs with `CLICKHOUSE_QUERY_SETTINGS` in `db.py` (higher time/memory/thread limits).
- **Duplicate counting**: many metrics use `countDistinct(run_id)` to avoid double-counting per-run multiple node outputs.
