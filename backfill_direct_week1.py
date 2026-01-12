#!/usr/bin/env python
"""
Direct backfill using Flask test client - no HTTP needed
"""
import os
import sys
from dotenv import load_dotenv

# Load .env first
load_dotenv()

# IMPORTANT: Set DATABASE_URL to use LIVE server BEFORE importing Flask/models
live_url = os.environ.get('DATABASE_LIVE_URL', '')
if not live_url:
    print("ERROR: DATABASE_LIVE_URL not found in .env")
    sys.exit(1)

os.environ['DATABASE_URL'] = live_url

print("=" * 80)
print("BACKFILL ALL 8 ENDPOINTS - WEEK 1 (JAN 1-7, 2025) - DIRECT")
print("=" * 80)
print(f"\nUsing Live Server Database: {os.environ['DATABASE_URL'][:60]}...\n")

from application import create_app
from datetime import datetime, timedelta
import json

app = create_app()

# Build weekly schedule
def build_weekly_schedule():
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 31)
    weeks = []
    current = start
    week_num = 1
    
    while current <= end:
        week_end = min(current + timedelta(days=6), end)
        weeks.append({
            "week": week_num,
            "start_date": current.strftime("%Y-%m-%d"),
            "end_date": week_end.strftime("%Y-%m-%d"),
            "week_start": int(current.timestamp()),
            "week_end": int((week_end + timedelta(days=1)).timestamp()),
        })
        current = week_end + timedelta(days=1)
        week_num += 1
    
    return weeks

weeks = build_weekly_schedule()[:1]  # Only week 1
week = weeks[0]

print(f"Processing Week 1: {week['start_date']} → {week['end_date']}\n")

# Endpoints configuration
ENDPOINTS = [
    {
        "name": "TRIP",
        "url": "/trip-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "1225",
            "tag_id": "39",
        }
    },
    {
        "name": "SPEEDING",
        "url": "/speeding-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "18",
        }
    },
    {
        "name": "IDLE",
        "url": "/idle-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "1328",
        }
    },
    {
        "name": "AWH",
        "url": "/awh-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "12",
        }
    },
    {
        "name": "WH",
        "url": "/wh-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "13",
        }
    },
    {
        "name": "HA",
        "url": "/ha-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "1327",
        }
    },
    {
        "name": "HB",
        "url": "/hb-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "1326",
        }
    },
    {
        "name": "WU",
        "url": "/wu-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "25",
            "tag_id": "39",
            "event_id": "17",
        }
    },
]

# Build payload for each endpoint
for i, endpoint in enumerate(ENDPOINTS, 1):
    print(f"[{i}/8] {endpoint['name']}")
    print("-" * 80)
    
    payload = {
        **endpoint['config'],
        "period_start": week['week_start'],
        "period_end": week['week_end'],
    }
    
    # Use test client to call endpoint
    with app.test_client() as client:
        response = client.post(
            endpoint['url'],
            data=json.dumps(payload),
            content_type='application/json'
        )
        
        if response.status_code == 200:
            result = response.get_json()
            rows = result.get(endpoint['name'].lower() + '_events', []) if isinstance(result, dict) else []
            inserted = result.get('db_stats', {}).get('inserted', 0) if isinstance(result, dict) else 0
            
            print(f"  Status: OK (HTTP 200)")
            print(f"  Rows fetched: {len(rows) if isinstance(rows, list) else 'unknown'}")
            if isinstance(result, dict):
                stats = result.get('db_stats', {})
                print(f"  Inserted: {stats.get('inserted', 0)}, Skipped: {stats.get('skipped', 0)}, Failed: {stats.get('failed', 0)}")
        else:
            print(f"  Status: FAIL (HTTP {response.status_code})")
            print(f"  Error: {response.get_json()}")
    
    print()

print("=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
