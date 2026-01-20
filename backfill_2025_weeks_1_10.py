#!/usr/bin/env python
"""
Backfill 2025 Weeks 1-10 (Jan 1 - Mar 11, 2025) for all 8 events
Uses cached render and result data from database
"""

import os
import sys
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

print("[BACKFILL_2025_10W] Script started")
print("[BACKFILL_2025_10W] Script started (stderr)", file=sys.stderr)
print(f"[BACKFILL_2025_10W] Python version: {sys.version}")
print(f"[BACKFILL_2025_10W] Python executable: {sys.executable}")

print("[BACKFILL_2025_10W] Set BACKFILL_MODE=false to use render/result cache")
os.environ['BACKFILL_MODE'] = 'false'

print("[BACKFILL_2025_10W] Set FETCH_CURRENT_WEEK=false to fetch historical data")
os.environ['FETCH_CURRENT_WEEK'] = 'false'

print("[BACKFILL_2025_10W] Loading .env file...")
load_dotenv()
print("[BACKFILL_2025_10W] .env loaded")

print("[BACKFILL_2025_10W] Creating Flask app...")
from application import create_app
from data_pipeline import process_event_data

app = create_app()
print("[CONFIG] Using PostgreSQL: " + str(os.environ.get('DATABASE_URL', ''))[:50] + "...")
print("[CONFIG] GDRIVE_FOLDER_ID: " + os.environ.get('GDRIVE_FOLDER_ID', 'NOT SET'))
print("[CONFIG] GOOGLE_APPLICATION_CREDENTIALS: " + ("Set" if os.environ.get('GOOGLE_APPLICATION_CREDENTIALS') else "NOT SET"))
print("[CONFIG] SECRET_KEY: " + ("Set" if os.environ.get('SECRET_KEY') else "NOT SET"))

print("[BACKFILL_2025_10W] Imports successful")
print("[BACKFILL_2025_10W] Flask app created")
print(f"[BACKFILL_2025_10W] FETCH_CURRENT_WEEK={os.environ.get('FETCH_CURRENT_WEEK')}")
print(f"[BACKFILL_2025_10W] Using historical schedule starting from 2025-01-01")
print(f"[BACKFILL_2025_10W] Database: localhost:5432/Fleetdb")
print()

# Endpoints configuration for 8 events
ENDPOINTS = [
    {
        "name": "Trip",
        "key": "trip_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "1225",
        "tag_id": "39",
        "event_id": None
    },
    {
        "name": "Speeding",
        "key": "speed_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "18"
    },
    {
        "name": "Idle",
        "key": "idle_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "1328"
    },
    {
        "name": "AWH",
        "key": "awh_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "12"
    },
    {
        "name": "WH",
        "key": "wh_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "13"
    },
    {
        "name": "HA",
        "key": "ha_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "1327"
    },
    {
        "name": "HB",
        "key": "hb_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "1326"
    },
    {
        "name": "WU",
        "key": "wu_events",
        "app_id": "6",
        "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
        "base_url": "https://omantracking2.com",
        "report_id": "25",
        "tag_id": "39",
        "event_id": "17"
    }
]

# Calculate date ranges for weeks 1-10
week_start = datetime(2025, 1, 1)  # Wednesday, Jan 1, 2025
week_end = datetime(2025, 3, 12)   # Wednesday, Mar 12, 2025

# Process weeks 1-10
total_start = datetime.now()
grand_totals = {
    'raw': 0,
    'internal_dupes': 0,
    'db_dupes': 0,
    'errors': 0,
    'inserted': 0,
}

print("=" * 80)
print("BACKFILL - 2025 WEEKS 1-10 (Jan 1 - Mar 11, 2025)")
print("=" * 80)
print()
print(f"Using start date: 2025-01-01")
print(f"Data will be fetched for: {week_start.date()} to {week_end.date()}")
print()
print(f"Processing {len(ENDPOINTS)} events across 10 weeks...")
print()

for idx, endpoint in enumerate(ENDPOINTS, 1):
    event_type = endpoint["name"]
    response_key = endpoint["key"]
    
    print("=" * 80)
    print(f"[{idx}/{len(ENDPOINTS)}] {event_type}")
    print("=" * 80)
    print()
    
    event_start = datetime.now()
    
    # Prepare payload for 10 weeks of data
    payload = {
        "app_id": endpoint["app_id"],
        "token": endpoint["token"],
        "base_url": endpoint["base_url"],
        "report_id": endpoint["report_id"],
        "tag_id": endpoint["tag_id"],
        "period_start": f"{week_start.isoformat()}Z",
        "period_end": f"{week_end.isoformat()}Z"
    }
    
    if endpoint["event_id"]:
        payload["event_id"] = endpoint["event_id"]
    
    print(f"REQUEST PAYLOAD:")
    print(json.dumps(payload, indent=2))
    print()
    
    # Call process_event_data within Flask request context
    with app.test_request_context(
        '/endpoint',
        method='POST',
        data=json.dumps(payload),
        content_type='application/json'
    ):
        try:
            result = process_event_data(event_type, response_key)
            
            if isinstance(result, tuple):
                response_data = result[0]
                status_code = result[1]
            else:
                response_data = result
                status_code = 200
            
            # Parse JSON if needed
            if hasattr(response_data, 'get_json'):
                response_dict = response_data.get_json()
            else:
                response_dict = response_data
            
            event_elapsed = (datetime.now() - event_start).total_seconds()
            
            if response_dict and 'stats' in response_dict:
                stats = response_dict.get('stats', {})
                print()
                print(f"  Status: {response_dict.get('status', 'OK')}")
                print(f"  Fetched (raw): {stats.get('raw', 0):,}")
                print(f"  Internal Dupes Removed: {stats.get('internal_dupes', 0):,}")
                print(f"  DB-Level Dupes Flagged: {stats.get('db_dupes', 0):,}")
                print(f"  DB Failed: {stats.get('errors', 0):,}")
                print(f"  Total Inserted: {stats.get('inserted', 0):,}")
                print(f"  Time: {event_elapsed:.1f}s")
                print()
                
                grand_totals['raw'] += stats.get('raw', 0)
                grand_totals['internal_dupes'] += stats.get('internal_dupes', 0)
                grand_totals['db_dupes'] += stats.get('db_dupes', 0)
                grand_totals['errors'] += stats.get('errors', 0)
                grand_totals['inserted'] += stats.get('inserted', 0)
            else:
                print(f"  ERROR: Unexpected response format")
                print(f"  Response: {response_dict}")
                print()
        
        except Exception as e:
            print(f"  ERROR: {str(e)}")
            print()
            import traceback
            traceback.print_exc()

total_elapsed = (datetime.now() - total_start).total_seconds()

print("=" * 80)
print("COMPLETE ROW ACCOUNTING ACROSS ALL 8 ENDPOINTS (10 WEEKS)")
print("=" * 80)
print(f"Total Raw Rows Fetched from API:              {grand_totals['raw']:,}")
print(f"  - Internal Duplicates Removed (CSV):   -    {grand_totals['internal_dupes']:,}")
print(f"  = Rows After Deduplication:                {grand_totals['raw'] - grand_totals['internal_dupes']:,}")
print(f"  - Database-Level Duplicates (flagged):  -  {grand_totals['db_dupes']:,}")
print(f"  - Failed DB Inserts:                    -   {grand_totals['errors']:,}")
print(f"  = TOTAL INSERTED TO DB:                    {grand_totals['inserted']:,}")
print("=" * 80)
print(f"Database Records: {grand_totals['inserted']:,} inserted, {grand_totals['errors']:,} errors")
print(f"Total Time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} minutes)")
print("=" * 80)
