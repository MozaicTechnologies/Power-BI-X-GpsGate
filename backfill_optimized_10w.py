#!/usr/bin/env python
"""
OPTIMIZED Backfill 2025 Weeks 1-10 (Jan 1 - Mar 11, 2025)
Fetches all 10 weeks in a single pass for each event type
Uses cached render and result data from database
"""

import os
import sys
import json
from datetime import datetime, timedelta
from dotenv import load_dotenv

print("[BACKFILL_OPT] Script started")
print("[BACKFILL_OPT] Set BACKFILL_MODE=false to use render/result cache")
os.environ['BACKFILL_MODE'] = 'false'

print("[BACKFILL_OPT] Set FETCH_CURRENT_WEEK=false to fetch historical data")
os.environ['FETCH_CURRENT_WEEK'] = 'false'

print("[BACKFILL_OPT] Loading .env file...")
load_dotenv()
print("[BACKFILL_OPT] .env loaded")

print("[BACKFILL_OPT] Creating Flask app...")
from application import create_app
from data_pipeline import process_event_data

app = create_app()
print("[CONFIG] Flask app created")
print(f"[BACKFILL_OPT] FETCH_CURRENT_WEEK={os.environ.get('FETCH_CURRENT_WEEK')}")
print(f"[BACKFILL_OPT] Database: localhost:5432/Fleetdb")
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

# Date range for all 10 weeks
week_start = datetime(2025, 1, 1)
week_end = datetime(2025, 3, 12)

total_start = datetime.now()
grand_totals = {
    'raw': 0,
    'internal_dupes': 0,
    'db_dupes': 0,
    'errors': 0,
    'inserted': 0,
}

print("=" * 80)
print("OPTIMIZED BACKFILL - 2025 WEEKS 1-10")
print("=" * 80)
print()
print(f"Date range: {week_start.date()} to {week_end.date()}")
print(f"Events: {len(ENDPOINTS)}")
print(f"Processing all 10 weeks in single pass per event...")
print()

for idx, endpoint in enumerate(ENDPOINTS, 1):
    event_type = endpoint["name"]
    response_key = endpoint["key"]
    
    print("=" * 80)
    print(f"[{idx}/{len(ENDPOINTS)}] {event_type}")
    print("=" * 80)
    print()
    
    event_start = datetime.now()
    
    # Prepare payload for 10 weeks
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
            else:
                response_data = result
            
            # Parse JSON if needed
            if hasattr(response_data, 'get_json'):
                response_dict = response_data.get_json()
            else:
                response_dict = response_data
            
            event_elapsed = (datetime.now() - event_start).total_seconds()
            
            if response_dict and 'stats' in response_dict:
                stats = response_dict.get('stats', {})
                print(f"Status: {response_dict.get('status', 'OK')}")
                print(f"Fetched (raw): {stats.get('raw', 0):,}")
                print(f"Internal Dupes Removed: {stats.get('internal_dupes', 0):,}")
                print(f"After Dedup: {stats.get('raw', 0) - stats.get('internal_dupes', 0):,}")
                print(f"DB Dupes Flagged: {stats.get('db_dupes', 0):,}")
                print(f"DB Errors: {stats.get('errors', 0):,}")
                print(f"Total Inserted: {stats.get('inserted', 0):,}")
                print(f"Time: {event_elapsed:.1f}s")
                print()
                
                grand_totals['raw'] += stats.get('raw', 0)
                grand_totals['internal_dupes'] += stats.get('internal_dupes', 0)
                grand_totals['db_dupes'] += stats.get('db_dupes', 0)
                grand_totals['errors'] += stats.get('errors', 0)
                grand_totals['inserted'] += stats.get('inserted', 0)
            else:
                print(f"ERROR: Unexpected response")
                print(f"Response: {response_dict}")
                print()
        
        except Exception as e:
            print(f"ERROR: {str(e)}")
            print()
            import traceback
            traceback.print_exc()

total_elapsed = (datetime.now() - total_start).total_seconds()

print("=" * 80)
print("COMPLETE ACCOUNTING - ALL 8 EVENTS, 10 WEEKS (JAN 1 - MAR 11, 2025)")
print("=" * 80)
total_after_dedup = grand_totals['raw'] - grand_totals['internal_dupes']
total_attempted = total_after_dedup
print(f"Total Raw Rows from API:              {grand_totals['raw']:,}")
print(f"  - CSV Internal Dupes Removed:   -  {grand_totals['internal_dupes']:,}")
print(f"  = Rows After CSV Dedup:            {total_after_dedup:,}")
print(f"  - Database Dupes Flagged:       -  {grand_totals['db_dupes']:,}")
print(f"  - DB Errors:                    -  {grand_totals['errors']:,}")
print(f"  = TOTAL INSERTED:                  {grand_totals['inserted']:,}")
print("=" * 80)
print()
print(f"Results:")
print(f"  Inserted: {grand_totals['inserted']:,}")
print(f"  Errors: {grand_totals['errors']:,}")
print(f"  Duplicates: {grand_totals['db_dupes']:,}")
print(f"  Total Time: {total_elapsed:.1f}s ({total_elapsed/60:.1f} min)")
print()
print("=" * 80)
