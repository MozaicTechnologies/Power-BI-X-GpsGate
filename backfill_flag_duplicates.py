#!/usr/bin/env python
"""Run backfill WITHOUT clearing (to see duplicate flagging in action)"""
from application import create_app
from data_pipeline import process_event_data
from datetime import datetime, timedelta
from flask import request

app = create_app()

print("=" * 80)
print("BACKFILL WITH DUPLICATE FLAGGING - Week 1 (JAN 1-7, 2025)")
print("=" * 80)
print("\nNote: Not clearing tables - will flag new duplicates with is_duplicate=True\n")

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

ENDPOINTS = [
    ("Trip", "tripdata", "1225"),
    ("Speeding", "speeding", "1226"),
    ("Idle", "idle_summary", "1227"),
    ("AWH", "awh_summary", "1228"),
    ("WH", "wh_summary", "1229"),
    ("HA", "ha_summary", "1230"),
    ("HB", "hb_summary", "1231"),
    ("WU", "wu_summary", "1232"),
]

weeks = build_weekly_schedule()[:1]  # Week 1 only
app_id = "6"
tag_id = "39"
base_url = "https://omantracking2.com"
token = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="

with app.test_request_context():
    total_inserted = 0
    total_skipped = 0
    total_failed = 0
    
    for i, (endpoint_name, endpoint_type, report_id) in enumerate(ENDPOINTS, 1):
        print(f"\n[{i}/8] {endpoint_name.upper()}")
        print("-" * 80)
        
        payload = {
            "app_id": app_id,
            "token": token,
            "base_url": base_url,
            "report_id": report_id,
            "tag_id": tag_id,
        }
        
        if endpoint_name != "Trip":
            payload["event_id"] = "123"
        
        payload["period_start"] = weeks[0]["week_start"]
        payload["period_end"] = weeks[0]["week_end"]
        
        with app.test_request_context(method='POST', json=payload):
            response, status_code = process_event_data(endpoint_name, endpoint_name.lower() + "s")
            
            if status_code == 200:
                data = response.get_json()
                db_stats = data.get("db_stats", {})
                inserted = db_stats.get("inserted", 0)
                skipped = db_stats.get("skipped", 0)
                failed = db_stats.get("failed", 0)
                
                total_inserted += inserted
                total_skipped += skipped
                total_failed += failed
                
                print(f"Status: OK - Inserted: {inserted}, Duplicates flagged: {skipped}, Failed: {failed}")

print("\n" + "=" * 80)
print("BACKFILL COMPLETE - Duplicates Flagged")
print("=" * 80)
print(f"Total Inserted:  {total_inserted}")
print(f"Total Flagged:   {total_skipped} (is_duplicate=True)")
print(f"Total Failed:    {total_failed}")
print("=" * 80)
