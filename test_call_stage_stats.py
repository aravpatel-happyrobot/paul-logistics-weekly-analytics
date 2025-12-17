"""
Example requests for the call-stage-stats endpoint
"""

import requests
from datetime import datetime, timedelta
import os

# Base URL - adjust if your server is running on a different host/port
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


def get_call_stage_stats_without_dates():
    """Get call stage stats for the last 30 days (default)"""
    url = f"{BASE_URL}/call-stage-stats"
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.json()


def get_call_stage_stats_with_dates(start_date: str, end_date: str):
    """Get call stage stats for a specific date range"""
    url = f"{BASE_URL}/call-stage-stats"
    params = {
        "start_date": start_date,
        "end_date": end_date
    }
    response = requests.get(url, params=params)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.json()


def get_call_stage_stats_last_7_days():
    """Get call stage stats for the last 7 days"""
    end_date = datetime.now().isoformat()
    start_date = (datetime.now() - timedelta(days=7)).isoformat()
    return get_call_stage_stats_with_dates(start_date, end_date)


def get_call_stage_stats_last_30_days():
    """Get call stage stats for the last 30 days"""
    end_date = datetime.now().isoformat()
    start_date = (datetime.now() - timedelta(days=30)).isoformat()
    return get_call_stage_stats_with_dates(start_date, end_date)


if __name__ == "__main__":
    print("=== Example 1: Get stats without dates (defaults to last 30 days) ===")
    get_call_stage_stats_without_dates()
    
    print("\n=== Example 2: Get stats for last 7 days ===")
    get_call_stage_stats_last_7_days()
    
    print("\n=== Example 3: Get stats for specific date range ===")
    # Example: last 24 hours (explicit range)
    end = datetime.now().isoformat()
    start = (datetime.now() - timedelta(days=1)).isoformat()
    get_call_stage_stats_with_dates(start_date=start, end_date=end)

