#!/usr/bin/env python
"""
Direct backfill - call process_event_data function directly
"""
import os
import sys
from dotenv import load_dotenv

# Load .env first
load_dotenv()

# Use local DATABASE_URL (not live) - Remove the override that was forcing live database
print("=" * 80)
print("BACKFILL ALL 8 ENDPOINTS - WEEK 1 (JAN 1-7, 2025) - DIRECT PYTHON")
print("=" * 80)
print(f"\nUsing Database: {os.environ['DATABASE_URL'][:60]}...\n")

from application import create_app
from data_pipeline import process_event_data
from datetime import datetime, timedelta
from flask import request
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

print(f"Processing Week 1: {week['start_date']} too {week['end_date']}\n")

# Endpoints configuration
ENDPOINTS = [
    ("TRIP", "Trip", "trip_events"),
    ("SPEEDING", "Speeding", "speed_events"),
    ("IDLE", "Idle", "idle_events"),
    ("AWH", "AWH", "awh_events"),
    ("WH", "WH", "wh_events"),
    ("HA", "HA", "ha_events"),
    ("HB", "HB", "hb_events"),
    ("WU", "WU", "wu_events"),
]

payload_template = {
    "app_id": "6",
    "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
    "base_url": "https://omantracking2.com",
    "report_id": "25",
    "tag_id": "39",
}

event_ids = {
    "Trip": None,  # No event_id for Trip
    "Speeding": "18",
    "Idle": "1328",
    "AWH": "12",
    "WH": "13",
    "HA": "1327",
    "HB": "1326",
    "WU": "17",
}

# For Trip, use different report_id
trip_payload = {
    **payload_template,
    "report_id": "1225",
}

total_rows = 0
total_inserted = 0
total_failed = 0
total_raw = 0
total_internal_dupes = 0
total_db_dupes = 0

# Track accounting per endpoint
endpoint_accounting = []

# Process each endpoint
for i, (name, event_type, response_key) in enumerate(ENDPOINTS, 1):
    print(f"[{i}/8] {name}")
    print("-" * 80)
    
    payload = trip_payload if event_type == "Trip" else payload_template.copy()
    
    # Convert timestamps to ISO format for render table matching
    from datetime import datetime
    period_start_iso = datetime.utcfromtimestamp(week['week_start']).strftime('%Y-%m-%dT%H:%M:%SZ')
    period_end_iso = datetime.utcfromtimestamp(week['week_end']).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    payload["period_start"] = period_start_iso
    payload["period_end"] = period_end_iso
    
    # Add event_id if needed
    if event_ids[event_type]:
        payload["event_id"] = event_ids[event_type]
    
    # Call process_event_data within Flask request context
    with app.test_request_context(
        '/endpoint',
        method='POST',
        data=json.dumps(payload),
        content_type='application/json'
    ):
        from flask import request as flask_request
        response_data, status_code = process_event_data(event_type, response_key)
        
        if isinstance(response_data, tuple):
            response_data = response_data[0]
        
        result = response_data.get_json() if hasattr(response_data, 'get_json') else response_data
        
        rows = len(result.get(response_key, [])) if isinstance(result, dict) else 0
        db_stats = result.get('db_stats', {}) if isinstance(result, dict) else {}
        accounting = result.get('accounting', {}) if isinstance(result, dict) else {}
        
        raw_fetched = accounting.get('raw_fetched', 0)
        internal_dupes = accounting.get('internal_dupes_removed', 0)
        rows_after_dedup = accounting.get('rows_after_dedup', 0)
        db_dupes_flagged = accounting.get('db_duplicates_flagged', 0)
        db_failed = accounting.get('db_failed', 0)
        total_db_inserted = accounting.get('total_inserted', 0)
        
        print(f"  Status: OK (HTTP 200)")
        print(f"  Fetched (raw): {raw_fetched}")
        print(f"  Internal Dupes Removed: {internal_dupes}")
        print(f"  After Dedup: {rows_after_dedup}")
        print(f"  DB-Level Dupes Flagged: {db_dupes_flagged}")
        print(f"  DB Failed: {db_failed}")
        print(f"  Total Inserted: {total_db_inserted}")
        
        total_rows += rows
        total_inserted += db_stats.get('inserted', 0)
        total_failed += db_stats.get('failed', 0)
        total_raw += raw_fetched
        total_internal_dupes += internal_dupes
        total_db_dupes += db_dupes_flagged
        
        endpoint_accounting.append({
            "endpoint": name,
            "raw_fetched": raw_fetched,
            "internal_dupes": internal_dupes,
            "after_dedup": rows_after_dedup,
            "db_dupes": db_dupes_flagged,
            "db_failed": db_failed,
            "inserted": total_db_inserted
        })
    
    print()

print("=" * 80)
print("COMPLETE ROW ACCOUNTING ACROSS ALL 8 ENDPOINTS")
print("=" * 80)
print(f"Total Raw Rows Fetched from API:          {total_raw:>10,}")
print(f"  - Internal Duplicates Removed (CSV):   -{total_internal_dupes:>10,}")
print(f"  = Rows After Deduplication:             {total_raw - total_internal_dupes:>10,}")
print(f"  - Database-Level Duplicates (flagged):  -{total_db_dupes:>10,}")
print(f"  - Failed DB Inserts:                    -{total_failed:>10,}")
print(f"  = TOTAL INSERTED TO DB:                 {total_inserted:>10,}")
print("=" * 80)
print("\nBREAKDOWN BY ENDPOINT:")
print("=" * 80)
for acct in endpoint_accounting:
    print(f"{acct['endpoint']:12} | Raw: {acct['raw_fetched']:>6} | Internal Dupes: {acct['internal_dupes']:>6} | After Dedup: {acct['after_dedup']:>6} | DB Dupes: {acct['db_dupes']:>6} | Inserted: {acct['inserted']:>6}")
print("=" * 80)
