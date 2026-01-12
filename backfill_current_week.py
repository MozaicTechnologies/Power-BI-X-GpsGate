#!/usr/bin/env python
"""
Backfill current week data with UPSERT logic
Designed to run daily via cron/scheduler to pull and upsert current week's data
"""
import os
import sys
from dotenv import load_dotenv
from datetime import datetime, timedelta

# Load .env first
load_dotenv()

print("=" * 80)
print("BACKFILL CURRENT WEEK - DAILY UPSERT")
print("=" * 80)
print(f"\nUsing Database: {os.environ['DATABASE_URL'][:60]}...\n")

from application import create_app
from data_pipeline import process_event_data

app = create_app()

def get_current_week():
    """Get current week dates (Monday to Sunday)"""
    today = datetime.now()
    # Monday = 0, Sunday = 6
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday, sunday

def backfill_current_week():
    """Fetch and insert current week data for all 8 endpoints"""
    
    monday, sunday = get_current_week()
    print(f"Current Week: {monday.strftime('%Y-%m-%d')} to {sunday.strftime('%Y-%m-%d')}")
    print("=" * 80)
    
    # 8 endpoints to process
    endpoints = [
        ("Trip", "trip_data", "Trip"),
        ("Speeding", "speeding_data", "Speeding"),
        ("Idle", "idle_data", "Idle"),
        ("AWH", "awh_data", "AWH"),
        ("WH", "wh_data", "WH"),
        ("HA", "ha_data", "HA"),
        ("HB", "hb_data", "HB"),
        ("WU", "wu_data", "WU"),
    ]
    
    all_stats = {}
    
    with app.app_context():
        for label, response_key, event_name in endpoints:
            print(f"\n[{label}] Processing current week...")
            try:
                # Call process_event_data for current week
                result = process_event_data(event_name, response_key, week_start=monday, week_end=sunday)
                
                if result and 'db_stats' in result:
                    stats = result['db_stats']
                    inserted = stats.get('inserted', 0)
                    duplicates = stats.get('duplicates', 0)
                    errors = stats.get('errors', 0)
                    
                    all_stats[label] = {
                        'inserted': inserted,
                        'duplicates': duplicates,
                        'errors': errors
                    }
                    
                    print(f"[{label}] ✓ Inserted: {inserted:,} | Duplicates: {duplicates} | Errors: {errors}")
                else:
                    print(f"[{label}] ⚠ No data returned")
                    
            except Exception as e:
                print(f"[{label}] ✗ Error: {str(e)}")
                all_stats[label] = {'error': str(e)}
    
    # Summary
    print("\n" + "=" * 80)
    print("CURRENT WEEK BACKFILL SUMMARY")
    print("=" * 80)
    total_inserted = 0
    total_dupes = 0
    total_errors = 0
    
    for label, stats in all_stats.items():
        if 'error' in stats:
            print(f"{label:15} | Error: {stats['error']}")
        else:
            inserted = stats.get('inserted', 0)
            dupes = stats.get('duplicates', 0)
            errors = stats.get('errors', 0)
            total_inserted += inserted
            total_dupes += dupes
            total_errors += errors
            print(f"{label:15} | Inserted: {inserted:8,} | Dupes: {dupes:3} | Errors: {errors:5}")
    
    print("=" * 80)
    print(f"{'TOTALS':15} | Inserted: {total_inserted:8,} | Dupes: {total_dupes:3} | Errors: {total_errors:5}")
    print("=" * 80)
    
    return {
        'success': True,
        'week': f"{monday.strftime('%Y-%m-%d')} to {sunday.strftime('%Y-%m-%d')}",
        'total_inserted': total_inserted,
        'total_duplicates': total_dupes,
        'total_errors': total_errors,
        'stats_by_type': all_stats
    }

if __name__ == "__main__":
    result = backfill_current_week()
    sys.exit(0 if result['success'] else 1)
