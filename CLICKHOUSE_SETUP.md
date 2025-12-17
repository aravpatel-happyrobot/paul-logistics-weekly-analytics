## ClickHouse setup (for non-SQL users)

This doc is a practical checklist for getting this analytics service working against your own ClickHouse data.

---

### What you’re trying to do (business version)

You have runs happening in the platform. Each run produces a `public_node_outputs` row that contains:
- a `node_persistent_id` (which workflow/node produced it)
- a `flat_data` JSON payload (the structured outcome from the run)

You want:
- **raw visibility** into what’s happening (inspect `flat_data` rows)
- **daily rollups** (counts of success vs failure, transfers, load status, etc)

This repo gives you both:
- a “raw rows” endpoint: `GET /daily-node-outputs`
- a bunch of prebuilt metric endpoints (call stage/classification/etc) that you can run for any date range

---

### The environment variables you need

The easiest way to manage these locally is to copy `env.example` to `.env`:

```bash
cp env.example .env
```

`.env` is ignored by git (safe for local secrets).

#### 1) ClickHouse connection
These come from your ClickHouse provider (ClickHouse Cloud console, internal infra, or whoever manages it):

- **`CLICKHOUSE_URL`** (or `CLICKHOUSE_HOST`)
  - Example: `https://abc123.us-east-1.aws.clickhouse.cloud` (URL) or `abc123.us-east-1.aws.clickhouse.cloud:8443` (host:port)
- **`CLICKHOUSE_USERNAME`** (or `CLICKHOUSE_USER`)
- **`CLICKHOUSE_PASSWORD`**
- **`CLICKHOUSE_DATABASE`**
- **`CLICKHOUSE_SECURE`**
  - `true` for ClickHouse Cloud, otherwise usually `false`

Where to find these:
- **ClickHouse Cloud**: open your service → “Connect” / “Connection details” → copy host/user/password/db.
- **Self-hosted**: ask whoever provisioned ClickHouse for the HTTP endpoint, user, password, database.

#### 2) Client/scoping settings (this is the part you control)

- **`CLIENT_NAME`**: label for docs + OpenAPI title (example: `Paul Logistics`)
- **`BROKER_NODE_PERSISTENT_ID`**: which node/workflow you’re analyzing
  - For your screenshot/example: `019b099d-77fd-7e09-9d2b-cd4c476a0a19`
- **`ORG_ID`** (optional but recommended): limits queries to only that org’s data
  - If you don’t set this, the new daily endpoint will still work (it filters by node id), but if multiple orgs share the node id you might mix data.
  - Use `GET /debug-node-orgs` to discover the correct org_id for your node.
- **`EXCLUDED_USER_NUMBERS`** (optional): comma-separated phone numbers to exclude from analysis
  - Example: `+15551234567,+15559876543`
- **`DEFAULT_TIMEZONE`** (optional): used by the daily endpoint (default `UTC`)
  - Example: `America/Chicago`

---

### How to quickly verify you’ve got the right node id

You already have the node persistent id from the UI.

If you ever need to discover it again, the fastest “copy/paste” query is:

```sql
SELECT
  node_persistent_id,
  count() AS rows
FROM public_node_outputs
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY node_persistent_id
ORDER BY rows DESC
LIMIT 50;
```

Then pick the one that corresponds to the workflow you care about.

---

### How to get “yesterday’s data” without writing SQL

This repo now includes:

- **`GET /daily-node-outputs`**
  - defaults to **yesterday** (previous calendar day)
  - filters to `BROKER_NODE_PERSISTENT_ID` if set
  - returns extracted fields + optional `flat_data`

Example calls:

- Yesterday in UTC:

```bash
curl "http://127.0.0.1:8000/daily-node-outputs?limit=50"
```

- Yesterday in Chicago time:

```bash
curl "http://127.0.0.1:8000/daily-node-outputs?tz=America/Chicago&limit=50"
```

- A specific day (YYYY-MM-DD):

```bash
curl "http://127.0.0.1:8000/daily-node-outputs?date=2025-12-11&tz=America/Chicago&limit=200"
```

- Override node id explicitly:

```bash
curl "http://127.0.0.1:8000/daily-node-outputs?node_persistent_id=019b099d-77fd-7e09-9d2b-cd4c476a0a19&limit=50"
```

---

### What fields we extract (and why)

From your example `flat_data`, the most analytics-useful fields (high signal, low noise) are:

- **Call outcome**
  - `result.call.call_classification`
  - `result.call.call_stage`
  - `result.call.notes`

- **Transfer behavior**
  - `result.transfer.transfer_attempt`
  - `result.transfer.transfer_reason`
  - `result.transfer.transfer_success`

- **Load status**
  - `result.load.load_status`
  - `result.load.reference_number`

- **Carrier quality / disposition**
  - `result.carrier.carrier_qualification`
  - `result.carrier.carrier_end_state`
  - `result.carrier.carrier_name`
  - `result.carrier.carrier_mc`

- **Pricing outcome**
  - `result.pricing.pricing_notes`
  - `result.pricing.agreed_upon_rate`

- **Timing / ops**
  - `result.metadata.processing_timestamp`
  - session `duration` (if present in `public_sessions`)

These align with the existing metric endpoints in this repo (call stage, call classification, pricing notes, carrier end state, etc.)

---

### If the JSON keys look different for Paul Logistics

The whole analytics layer depends on these key strings. If Paul’s `flat_data` uses different keys, we update the query extraction to match.

The daily endpoint currently extracts keys like:
- `result.call.call_classification`
- `result.load.load_status`

If you paste 2–3 Paul `flat_data` examples, we can lock down the exact extraction list and then adjust the prebuilt stats endpoints to match.
