# Example curl requests for the call-stage-stats endpoint

# 1. Get call stage stats without dates (defaults to last 30 days)
curl "http://localhost:8000/call-stage-stats"

# 2. Get call stage stats with date range
curl "http://localhost:8000/call-stage-stats?start_date=2024-01-01T00:00:00&end_date=2024-01-31T23:59:59"

# 3. Get call stage stats for last 7 days (using ISO format dates)
# Note: Adjust dates based on your current date
curl "http://localhost:8000/call-stage-stats?start_date=2024-12-01T00:00:00&end_date=2024-12-08T00:00:00"

# 4. Pretty print JSON response (if you have jq installed)
curl "http://localhost:8000/call-stage-stats" | jq

# 5. With verbose output to see request details
curl -v "http://localhost:8000/call-stage-stats?start_date=2024-01-01T00:00:00&end_date=2024-01-31T23:59:59"

# Example response format:
# [
#   {
#     "call_stage": "connected",
#     "count": 150,
#     "percentage": 45.5
#   },
#   {
#     "call_stage": "voicemail",
#     "count": 80,
#     "percentage": 24.2
#   },
#   ...
# ]

# ------------------------------------------------------------
# Debug + raw daily data (recommended when setting up a new client)

# Show effective configuration (no secrets)
curl "http://localhost:8000/debug-config"

# Pull yesterday's raw node outputs (defaults to BROKER_NODE_PERSISTENT_ID if set)
curl "http://localhost:8000/daily-node-outputs?limit=50"

# Pull yesterday's data in a specific timezone (defines what \"yesterday\" means)
curl "http://localhost:8000/daily-node-outputs?tz=America/Chicago&limit=50"

# Pull a specific calendar day
curl "http://localhost:8000/daily-node-outputs?date=2025-12-11&tz=America/Chicago&limit=50"

