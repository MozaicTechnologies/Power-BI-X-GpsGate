#!/usr/bin/env python
"""Simple backfill - 1 week per endpoint with error handling"""
import requests
import sys

BASE_URL = "http://localhost:5000"
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
GPSGATE_URL = "https://omantracking2.com"
TAG_ID = "39"

endpoints = {
    "trip": ("/trip-data", "1225", "1225"),
    "speeding": ("/speeding-data", "25", "18"),
    "idle": ("/idle-data", "25", "1328"),
    "awh": ("/awh-data", "25", "12"),
    "wh": ("/wh-data", "25", "13"),
    "ha": ("/ha-data", "25", "1327"),
    "hb": ("/hb-data", "25", "1326"),
    "wu": ("/wu-data", "25", "17"),
}

print("\n" + "=" * 80)
print("FAST BACKFILL - 1 WEEK PER ENDPOINT")
print("=" * 80 + "\n")

for name, (endpoint, report_id, event_id) in endpoints.items():
    payload = {
        "app_id": APP_ID,
        "token": TOKEN,
        "base_url": GPSGATE_URL,
        "report_id": report_id,
        "tag_id": TAG_ID,
    }
    if name != "trip":
        payload["event_id"] = event_id
    
    print(f"{name.upper():10s} ... ", end="", flush=True)
    
    try:
        r = requests.post(f"{BASE_URL}{endpoint}", json=payload, timeout=180)
        if r.status_code == 200:
            data = r.json()
            rows = data.get('total_rows', 0)
            db_stats = data.get('db_stats', {})
            inserted = db_stats.get('inserted', 0)
            failed = db_stats.get('failed', 0)
            if failed == 0:
                print(f"{rows:,} rows - {inserted} stored [OK]")
            else:
                print(f"{rows:,} rows - {inserted} stored, {failed} errors [WARN]")
        else:
            print(f"HTTP {r.status_code}")
    except Exception as e:
        print(f"ERROR: {str(e)[:50]}")

print("\n" + "=" * 80)
print("DONE!")
print("=" * 80 + "\n")
