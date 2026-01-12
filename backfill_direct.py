"""
Direct backfill script - NO FLASK NEEDED
Calls data processing functions directly
Use: python backfill_direct.py trip
"""

import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

from application import create_app
from models import db
from db_storage import store_event_data_to_db
from gpsgate_api import render_endpoint, result_endpoint, download_csv_from_gdrive
from data_pipeline import clean_csv_data

# Create Flask app context
app = create_app()

# Configuration
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
GPSGATE_URL = "https://omantracking2.com"
TAG_ID = "39"

REPORT_IDS = {
    "trip": "1225",
    "speeding": "25",
    "idle": "25",
    "awh": "25",
    "wh": "25",
    "ha": "25",
    "hb": "25",
    "wu": "25",
}

EVENT_IDS = {
    "trip": "1225",
    "speeding": "18",
    "idle": "1328",
    "awh": "12",
    "wh": "13",
    "ha": "1327",
    "hb": "1326",
    "wu": "17",
}

if len(sys.argv) < 2:
    print("Usage: python backfill_direct.py <endpoint>")
    print("Available: trip, speeding, idle, awh, wh, ha, hb, wu")
    sys.exit(1)

event_type = sys.argv[1].lower()
valid_types = list(REPORT_IDS.keys())

if event_type not in valid_types:
    print(f"ERROR: Invalid endpoint '{event_type}'")
    print(f"Available: {', '.join(valid_types)}")
    sys.exit(1)

# Map to event name for database
event_name_map = {
    "trip": "Trip",
    "speeding": "Speeding",
    "idle": "Idle",
    "awh": "AWH",
    "wh": "WH",
    "ha": "HA",
    "hb": "HB",
    "wu": "WU",
}

event_name = event_name_map[event_type]
report_id = REPORT_IDS[event_type]
event_id = EVENT_IDS[event_type]

# Calculate weeks
START_DATE = datetime(2025, 1, 1)
TODAY = datetime.utcnow()
days_diff = (TODAY.date() - START_DATE.date()).days
total_weeks = days_diff // 7 + 1

print("=" * 80)
print(f"BACKFILL: {event_type.upper()} - DIRECT (No Flask)")
print("=" * 80)
print(f"Period: {START_DATE.strftime('%Y-%m-%d')} to {TODAY.strftime('%Y-%m-%d')}")
print(f"Total weeks: {total_weeks}")
print(f"Report ID: {report_id}, Event ID: {event_id}")
print("=" * 80)

total_inserted = 0
total_skipped = 0
total_failed = 0
total_rows = 0

with app.app_context():
    # Process each week
    for week_offset in range(0, -total_weeks, -1):
        week_num = -week_offset + 1
        
        # Calculate week dates
        week_start = START_DATE + timedelta(weeks=-week_offset)
        week_end = week_start + timedelta(days=7) - timedelta(seconds=1)
        
        week_start_str = week_start.strftime('%Y-%m-%d %H:%M:%S')
        week_end_str = week_end.strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"\n[{week_num:3d}/{total_weeks:3d}] {week_start.strftime('%Y-%m-%d')} → {week_end.strftime('%Y-%m-%d')}", end=" ", flush=True)
        
        try:
            # Step 1: Call render endpoint
            render_payload = {
                "app_id": APP_ID,
                "period_start": week_start_str,
                "period_end": week_end_str,
                "tag_id": TAG_ID,
                "token": TOKEN,
                "base_url": GPSGATE_URL,
                "report_id": report_id
            }
            
            # Add event_id for non-trip events
            if event_type != "trip":
                render_payload["event_id"] = event_id
            
            render_response = render_endpoint(render_payload)
            if render_response["status_code"] != 200:
                print(f"✗ Render failed")
                continue
            
            render_id = render_response["data"].get("render_id")
            if not render_id:
                print(f"✗ No render_id")
                continue
            
            # Step 2: Call result endpoint
            result_payload = {
                "app_id": APP_ID,
                "render_id": render_id,
                "token": TOKEN,
                "base_url": GPSGATE_URL,
                "report_id": report_id
            }
            
            result_response = result_endpoint(result_payload)
            if result_response["status_code"] != 200:
                print(f"✗ Result failed")
                continue
            
            gdrive_link = result_response["data"].get("gdrive_link")
            if not gdrive_link:
                print(f"✗ No gdrive_link")
                continue
            
            # Step 3: Download CSV
            csv_content = download_csv_from_gdrive(gdrive_link, TOKEN)
            if not csv_content:
                print(f"✗ Download failed")
                continue
            
            # Step 4: Clean CSV
            df = clean_csv_data(csv_content)
            if df is None or df.empty:
                print(f"✗ Empty data")
                continue
            
            # Step 5: Store to database
            db_stats = store_event_data_to_db(df, APP_ID, TAG_ID, event_name)
            
            inserted = db_stats.get("inserted", 0)
            skipped = db_stats.get("skipped", 0)
            failed = db_stats.get("failed", 0)
            rows = inserted + skipped + failed
            
            total_inserted += inserted
            total_skipped += skipped
            total_failed += failed
            total_rows += rows
            
            print(f"✓ {rows:5d} rows ({inserted} inserted, {skipped} dup, {failed} err)")
            
        except Exception as e:
            print(f"✗ Exception: {str(e)[:40]}")
            continue

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
print(f"Total rows: {total_rows:,}")
print(f"  Inserted: {total_inserted:,}")
print(f"  Duplicates: {total_skipped:,}")
print(f"  Errors: {total_failed:,}")
print("=" * 80)
