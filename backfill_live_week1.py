#!/usr/bin/env python
"""
Backfill all 8 endpoints for Week 1 of 2025 using LIVE SERVER DATABASE
Week 1: January 1-7, 2025
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
    print(f"Available: {list(os.environ.keys())}")
    sys.exit(1)

os.environ['DATABASE_URL'] = live_url

print("=" * 80)
print("BACKFILL ALL 8 ENDPOINTS - WEEK 1 (JAN 1-7, 2025)")
print("=" * 80)
print(f"\nUsing Live Server Database: {os.environ['DATABASE_URL'][:60]}...\n")

import requests
import json
from datetime import datetime

# Flask endpoints configuration
ENDPOINTS = [
    {
        "name": "TRIP",
        "url": "http://localhost:5000/trip-data",
        "config": {
            "app_id": "6",
            "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
            "base_url": "https://omantracking2.com",
            "report_id": "1225",
            "tag_id": "39",
            "event_id": "1225",
        }
    },
    {
        "name": "SPEEDING",
        "url": "http://localhost:5000/speeding-data",
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
        "url": "http://localhost:5000/idle-data",
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
        "url": "http://localhost:5000/awh-data",
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
        "url": "http://localhost:5000/wh-data",
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
        "url": "http://localhost:5000/ha-data",
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
        "url": "http://localhost:5000/hb-data",
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
        "url": "http://localhost:5000/wu-data",
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

print("Waiting for Flask server...\n")

# Try to connect to Flask
for attempt in range(5):
    try:
        requests.get("http://localhost:5000/", timeout=2)
        print("[OK] Flask server is running\n")
        break
    except:
        if attempt < 4:
            print(f"Waiting... ({attempt+1}/5)")
            import time
            time.sleep(2)
        else:
            print("[FAIL] Flask server not responding")
            sys.exit(1)

# Process all endpoints
total_stats = {
    "total_rows": 0,
    "total_inserted": 0,
    "total_failed": 0,
}

for i, endpoint in enumerate(ENDPOINTS, 1):
    print(f"\n[{i}/{len(ENDPOINTS)}] {endpoint['name']}")
    print("-" * 80)
    
    try:
        response = requests.post(
            endpoint['url'],
            json=endpoint['config'],
            timeout=300
        )
        
        if response.status_code == 200:
            data = response.json()
            rows = data.get('total_rows', 0)
            db_stats = data.get('db_stats', {})
            inserted = db_stats.get('inserted', 0)
            skipped = db_stats.get('skipped', 0)
            failed = db_stats.get('failed', 0)
            
            print(f"  Status: [OK]")
            print(f"  Rows fetched: {rows}")
            print(f"  Inserted: {inserted}, Skipped: {skipped}, Failed: {failed}")
            
            total_stats["total_rows"] += rows
            total_stats["total_inserted"] += inserted
            total_stats["total_failed"] += failed
        else:
            print(f"  Status: [HTTP {response.status_code}]")
            print(f"  Error: {response.text[:100]}")
    
    except Exception as e:
        print(f"  Status: [ERROR]")
        print(f"  Exception: {str(e)[:100]}")

# Summary
print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
print(f"Total Rows Fetched:   {total_stats['total_rows']}")
print(f"Total Inserted:       {total_stats['total_inserted']}")
print(f"Total Failed:         {total_stats['total_failed']}")
print("=" * 80)
