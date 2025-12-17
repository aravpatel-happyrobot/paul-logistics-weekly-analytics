"""
Example requests for the percent-non-convertible-calls-stats endpoint
"""

import os
import requests
from datetime import datetime, timedelta

# Base URL - adjust if your server is running on a different host/port
BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")


def get_percent_non_convertible_calls_without_dates():
    """Get percent non-convertible calls for the last 30 days (default)"""
    url = f"{BASE_URL}/percent-non-convertible-calls-stats"
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.json()


def get_percent_non_convertible_calls_with_dates(start_date: str, end_date: str):
    """Get percent non-convertible calls for a specific date range"""
    url = f"{BASE_URL}/percent-non-convertible-calls-stats"
    params = {"start_date": start_date, "end_date": end_date}
    response = requests.get(url, params=params)
    print(f"Status Code: {response.status_code}")
    print(f"Response: {response.json()}")
    return response.json()


def get_percent_non_convertible_calls_last_7_days():
    end_date = datetime.now().isoformat()
    start_date = (datetime.now() - timedelta(days=7)).isoformat()
    return get_percent_non_convertible_calls_with_dates(start_date, end_date)


if __name__ == "__main__":
    print("=== Example 1: Get stats without dates (defaults to last 30 days) ===")
    get_percent_non_convertible_calls_without_dates()

    print("\n=== Example 2: Get stats for last 7 days ===")
    get_percent_non_convertible_calls_last_7_days()
