# #!/usr/bin/env python
# """
# Backfill for 2025 Week 1 using cached render/result data
# Fetch all 8 events for January 1-7, 2025
# """
# import os
# import sys
# import traceback
# import json
# from datetime import datetime, timedelta

# try:
#     print("[BACKFILL_2025] Script started", flush=True)
#     sys.stderr.write("[BACKFILL_2025] Script started (stderr)\n")
#     sys.stderr.flush()
    
#     print(f"[BACKFILL_2025] Python version: {sys.version}", flush=True)
#     print(f"[BACKFILL_2025] Python executable: {sys.executable}", flush=True)

#     # IMPORTANT: Disable BACKFILL_MODE so we use /render -> /result -> download flow
#     # This allows us to use the cached render/result data
#     os.environ['BACKFILL_MODE'] = 'false'
#     print("[BACKFILL_2025] Set BACKFILL_MODE=false to use render/result cache", flush=True)
    
#     # Disable FETCH_CURRENT_WEEK so we use historical data (2025)
#     os.environ['FETCH_CURRENT_WEEK'] = 'false'
#     print("[BACKFILL_2025] Set FETCH_CURRENT_WEEK=false to fetch historical data", flush=True)

#     from dotenv import load_dotenv

#     # Load .env first
#     print("[BACKFILL_2025] Loading .env file...", flush=True)
#     load_dotenv()
#     print("[BACKFILL_2025] .env loaded", flush=True)

#     print("[BACKFILL_2025] Creating Flask app...", flush=True)
#     from application import create_app
#     from data_pipeline import process_event_data
#     from flask import request
    
#     print("[BACKFILL_2025] Imports successful", flush=True)

# except Exception as e:
#     error_msg = f"[BACKFILL_2025] FATAL ERROR during import: {str(e)}\n{traceback.format_exc()}"
#     print(error_msg, flush=True)
#     sys.stderr.write(error_msg + "\n")
#     sys.stderr.flush()
#     sys.exit(1)

# app = create_app()
# print("[BACKFILL_2025] Flask app created", flush=True)

# # Configuration
# print("[BACKFILL_2025] FETCH_CURRENT_WEEK=False")
# print("[BACKFILL_2025] Using historical schedule starting from 2025-01-01")

# # Check if we're using the correct database
# from config import Config
# print(f"[BACKFILL_2025] Database: {Config.SQLALCHEMY_DATABASE_URI.split('@')[1] if '@' in Config.SQLALCHEMY_DATABASE_URI else 'local'}")

# # Endpoints configuration matching the API
# ENDPOINTS = [
#     {
#         "name": "Trip",
#         "key": "trip_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "1225",
#         "tag_id": "39",
#         "event_id": None  # Trip doesn't have event_id
#     },
#     {
#         "name": "Speeding",
#         "key": "speed_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "18"
#     },
#     {
#         "name": "Idle",
#         "key": "idle_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "1328"
#     },
#     {
#         "name": "AWH",
#         "key": "awh_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "12"
#     },
#     {
#         "name": "WH",
#         "key": "wh_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "13"
#     },
#     {
#         "name": "HA",
#         "key": "ha_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "1327"
#     },
#     {
#         "name": "HB",
#         "key": "hb_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "1326"
#     },
#     {
#         "name": "WU",
#         "key": "wu_events",
#         "app_id": "6",
#         "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
#         "base_url": "https://omantracking2.com",
#         "report_id": "25",
#         "tag_id": "39",
#         "event_id": "17"
#     }
# ]

# print("\n" + "=" * 80)
# print("BACKFILL - 2025 WEEK 1 (Jan 1 - Jan 7, 2025)")
# print("=" * 80)


# # Allow week_start and week_end as command-line arguments
# import argparse
# parser = argparse.ArgumentParser(description="Backfill for a specific week in 2025")
# parser.add_argument('--week_start', type=str, default='2025-01-08', help='Start date (YYYY-MM-DD)')
# parser.add_argument('--week_end', type=str, default='2025-01-15', help='End date (YYYY-MM-DD, exclusive)')
# args = parser.parse_args()

# week_start = datetime.strptime(args.week_start, "%Y-%m-%d")
# week_end = datetime.strptime(args.week_end, "%Y-%m-%d")

# print(f"\nUsing start date: {week_start.date()}")
# print(f"Data will be fetched for: {week_start.date()} to {week_end.date()}")

# total_rows = 0
# total_inserted = 0
# total_failed = 0
# total_raw = 0
# total_internal_dupes = 0
# total_db_dupes = 0

# print(f"\nProcessing {len(ENDPOINTS)} events...\n")

# for i, endpoint in enumerate(ENDPOINTS, 1):
#     event_type = endpoint["name"]
#     response_key = endpoint["key"]
    
#     print(f"[{i}/8] {event_type}")
#     print("-" * 80)
    
#     # Prepare payload matching process_event_data signature
#     payload = {
#         "app_id": endpoint["app_id"],
#         "token": endpoint["token"],
#         "base_url": endpoint["base_url"],
#         "report_id": endpoint["report_id"],
#         "tag_id": endpoint["tag_id"],
#         "period_start": f"{week_start.isoformat()}Z",
#         "period_end": f"{week_end.isoformat()}Z"
#     }
    
#     if endpoint["event_id"]:
#         payload["event_id"] = endpoint["event_id"]
    
#     print(f"\nREQUEST PAYLOAD:")
#     print(json.dumps(payload, indent=2))
    
#     # Call process_event_data within Flask request context
#     with app.test_request_context(
#         '/endpoint',
#         method='POST',
#         data=json.dumps(payload),
#         content_type='application/json'
#     ):
#         try:
#             response_data, status_code = process_event_data(event_type, response_key)
            
#             if isinstance(response_data, tuple):
#                 response_data = response_data[0]
            
#             result = response_data.get_json() if hasattr(response_data, 'get_json') else response_data
            
#             rows = len(result.get(response_key, [])) if isinstance(result, dict) else 0
#             db_stats = result.get('db_stats', {}) if isinstance(result, dict) else {}
#             accounting = result.get('accounting', {}) if isinstance(result, dict) else {}
            
#             raw_fetched = accounting.get('raw_fetched', 0)
#             internal_dupes = accounting.get('internal_dupes_removed', 0)
#             rows_after_dedup = accounting.get('rows_after_dedup', 0)
#             db_dupes_flagged = accounting.get('db_duplicates_flagged', 0)
#             db_failed = accounting.get('db_failed', 0)
#             total_db_inserted = accounting.get('total_inserted', 0)
            
#             print(f"\n  Status: OK (HTTP {status_code})")
#             print(f"  Fetched (raw): {raw_fetched:,}")
#             print(f"  Internal Dupes Removed: {internal_dupes:,}")
#             print(f"  After Dedup: {rows_after_dedup:,}")
#             print(f"  DB-Level Dupes Flagged: {db_dupes_flagged:,}")
#             print(f"  DB Failed: {db_failed:,}")
#             print(f"  Total Inserted: {total_db_inserted:,}")
            
#             total_rows += rows
#             total_inserted += db_stats.get('inserted', 0)
#             total_failed += db_stats.get('failed', 0)
#             total_raw += raw_fetched
#             total_internal_dupes += internal_dupes
#             total_db_dupes += db_dupes_flagged
            
#         except Exception as e:
#             print(f"\n  ERROR: {e}")
#             import traceback
#             print(traceback.format_exc())
    
#     print()

# print("\n" + "=" * 80)
# print("COMPLETE ROW ACCOUNTING ACROSS ALL 8 ENDPOINTS")
# print("=" * 80)
# print(f"Total Raw Rows Fetched from API:           {total_raw:>10,}")
# print(f"  - Internal Duplicates Removed (CSV):   -{total_internal_dupes:>10,}")
# print(f"  = Rows After Deduplication:             {total_raw - total_internal_dupes:>10,}")
# print(f"  - Database-Level Duplicates (flagged):  -{total_db_dupes:>10,}")
# print(f"  = TOTAL INSERTED TO DB:                 {total_inserted:>10,}")
# print("=" * 80)
# print(f"Database Records: {total_inserted:,} inserted, {total_failed:,} errors")
# print("=" * 80)


#!/usr/bin/env python
"""
Backfill script with dynamic date support.
Fetches data ONLY for the provided date range.
"""

import os
import sys
import json
import argparse
import traceback
from datetime import datetime

# ------------------------------------------------------------------
# ENV CONFIG — force pipeline to use historical + render/result cache
# ------------------------------------------------------------------
os.environ["BACKFILL_MODE"] = "false"
os.environ["FETCH_CURRENT_WEEK"] = "false"

print("[BACKFILL] Starting backfill with dynamic dates", flush=True)
print(f"[BACKFILL] Python: {sys.version}", flush=True)

# ------------------------------------------------------------------
# LOAD ENV & APP
# ------------------------------------------------------------------
from dotenv import load_dotenv
load_dotenv()

from application import create_app
from data_pipeline import process_event_data

app = create_app()

# ------------------------------------------------------------------
# CLI ARGUMENTS (SINGLE SOURCE OF TRUTH)
# ------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Dynamic Backfill Script")

parser.add_argument(
    "--week_start",
    required=True,
    help="Start date (YYYY-MM-DD)"
)

parser.add_argument(
    "--week_end",
    required=True,
    help="End date (YYYY-MM-DD, exclusive)"
)

args = parser.parse_args()

week_start = datetime.strptime(args.week_start, "%Y-%m-%d")
week_end = datetime.strptime(args.week_end, "%Y-%m-%d")

print(f"[BACKFILL] Date range: {week_start.date()} → {week_end.date()}")

# ------------------------------------------------------------------
# ENDPOINT CONFIG (UNCHANGED)
# ------------------------------------------------------------------
ENDPOINTS = [
    {"name": "Trip", "key": "trip_events", "event_id": None},
    {"name": "Speeding", "key": "speed_events", "event_id": "18"},
    {"name": "Idle", "key": "idle_events", "event_id": "1328"},
    {"name": "AWH", "key": "awh_events", "event_id": "12"},
    {"name": "WH", "key": "wh_events", "event_id": "13"},
    {"name": "HA", "key": "ha_events", "event_id": "1327"},
    {"name": "HB", "key": "hb_events", "event_id": "1326"},
    {"name": "WU", "key": "wu_events", "event_id": "17"},
]

COMMON_CONFIG = {
    "app_id": "6",
    "token": "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA==",
    "base_url": "https://omantracking2.com",
    "report_id": "25",
    "tag_id": "39",
}

# ------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------
print("=" * 80)
print("BACKFILL EXECUTION STARTED")
print("=" * 80)

for idx, ep in enumerate(ENDPOINTS, start=1):
    print(f"\n[{idx}/8] Processing {ep['name']}")
    print("-" * 80)

    payload = {
        **COMMON_CONFIG,
        "period_start": f"{week_start.strftime('%Y-%m-%d')}T00:00:00Z",
        "period_end": f"{week_end.strftime('%Y-%m-%d')}T23:59:59Z",
    }

    if ep["event_id"]:
        payload["event_id"] = ep["event_id"]

    print("[REQUEST PAYLOAD]")
    print(json.dumps(payload, indent=2))

    with app.test_request_context(
        "/internal-backfill",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        try:
            response, status = process_event_data(ep["name"], ep["key"])

            if hasattr(response, "get_json"):
                result = response.get_json()
            else:
                result = response

            accounting = result.get("accounting", {})

            # print("[RESULT]")
            # print(f"  Raw fetched: {accounting.get('raw_fetched', 0)}")
            # print(f"  Internal dupes removed: {accounting.get('internal_dupes_removed', 0)}")
            # print(f"  DB duplicates flagged: {accounting.get('db_duplicates_flagged', 0)}")
            # print(f"  Inserted: {accounting.get('total_inserted', 0)}")

            print("[RESULT]")
            print(f"  Raw fetched: {accounting.get('raw', 0)}")
            print(f"  Inserted: {accounting.get('inserted', 0)}")
            print(f"  Skipped: {accounting.get('skipped', 0)}")
            print(f"  Failed: {accounting.get('failed', 0)}")


        except Exception as e:
            print("[ERROR] Backfill failed")
            print(traceback.format_exc())

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
