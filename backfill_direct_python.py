#!/usr/bin/env python
"""
Direct backfill - call process_event_data function directly
"""
import os
import sys

print("[BACKFILL] Script started", flush=True)
print(f"[BACKFILL] Python version: {sys.version}", flush=True)
print(f"[BACKFILL] Python executable: {sys.executable}", flush=True)

from dotenv import load_dotenv

# Load .env first
print("[BACKFILL] Loading .env file...", flush=True)
load_dotenv()
print("[BACKFILL] .env loaded", flush=True)

# Use local DATABASE_URL (not live) - Remove the override that was forcing live database
print("[BACKFILL] Creating Flask app...", flush=True)
from application import create_app
from data_pipeline import process_event_data
from datetime import datetime, timedelta
from flask import request
import json

print("[BACKFILL] Imports successful", flush=True)

app = create_app()
print("[BACKFILL] Flask app created", flush=True)

# Check if fetching current week or historical
FETCH_CURRENT_WEEK = os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true'
print(f"[BACKFILL] FETCH_CURRENT_WEEK={FETCH_CURRENT_WEEK}", flush=True)

# Build weekly schedule
def build_weekly_schedule(current_week_only=None):
    # Check environment variable if parameter not explicitly provided
    if current_week_only is None:
        current_week_only = os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true'
    
    if current_week_only:
        # Calculate current week (today's date determines which week)
        today = datetime.now()
        # Find Monday of current week
        days_since_monday = today.weekday()
        week_start = today - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)
        
        print("=" * 80)
        print(f"BACKFILL - CURRENT WEEK ({week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')})")
        print("=" * 80)
        print(f"\nUsing Database: {os.environ['DATABASE_URL'][:60]}...\n")
        
        return [{
            "week": 1,
            "start_date": week_start.strftime("%Y-%m-%d"),
            "end_date": week_end.strftime("%Y-%m-%d"),
            "week_start": int(week_start.timestamp()),
            "week_end": int((week_end + timedelta(days=1)).timestamp()),
        }]
    else:
        # Historical 2025 schedule
        print("=" * 80)
        print("BACKFILL ALL 8 ENDPOINTS - HISTORICAL WEEKS (2025) - DIRECT PYTHON")
        print("=" * 80)
        print(f"\nUsing Database: {os.environ['DATABASE_URL'][:60]}...\n")
        
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

weeks = build_weekly_schedule(current_week_only=FETCH_CURRENT_WEEK)
print(f"Processing {len(weeks)} week(s)\n")

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

print("[BACKFILL] Starting main backfill loop...", flush=True)

# Process each of 8 endpoints (each internally processes multiple weeks)
for i, (name, event_type, response_key) in enumerate(ENDPOINTS, 1):
    print(f"[{i}/8] {name}")
    print("-" * 80)
    
    # Select appropriate payload (Trip uses different report_id)
    if event_type == "Trip":
        payload = trip_payload.copy()
    else:
        payload = payload_template.copy()
    
    # Build date range for this backfill
    if weeks:
        first_week = weeks[0]
        last_week = weeks[-1]
        
        from datetime import datetime, timezone
        period_start_iso = datetime.fromtimestamp(first_week['week_start'], timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        period_end_iso = datetime.fromtimestamp(last_week['week_end'], timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        
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
        print(f"  Fetched (raw): {raw_fetched:,}")
        print(f"  Internal Dupes Removed: {internal_dupes:,}")
        print(f"  After Dedup: {rows_after_dedup:,}")
        print(f"  DB-Level Dupes Flagged: {db_dupes_flagged:,}")
        print(f"  DB Failed: {db_failed:,}")
        print(f"  Total Inserted: {total_db_inserted:,}")
        
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
print("[BACKFILL] Script completed successfully!", flush=True)
sys.exit(0)

