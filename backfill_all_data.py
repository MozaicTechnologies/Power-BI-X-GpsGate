"""
Fetch ALL historical data week by week for all endpoints
This backfills the local database with all historical data
Run this ONCE to populate the database with all past weeks
"""

import subprocess
import sys
from datetime import datetime, timedelta

# Configuration
EVENT_TYPES = ["trip", "speeding", "idle", "awh", "wh", "ha", "hb", "wu"]
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.utcnow()

# Calculate number of weeks
days_diff = (END_DATE.date() - START_DATE.date()).days
total_weeks = days_diff // 7 + 1

print("=" * 80)
print("BACKFILL: Fetch all historical data week by week")
print("=" * 80)
print(f"\nPeriod: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
print(f"Total weeks: {total_weeks}")
print(f"Event types: {', '.join(EVENT_TYPES)}")
print(f"\nNote: This will make {total_weeks * len(EVENT_TYPES)} API calls")
print("Each call fetches 1 week of data to avoid timeouts")

response = input("\nContinue? (y/n): ")
if response.lower() != 'y':
    print("Cancelled")
    exit(0)

print("\n" + "=" * 80)
print("STARTING BACKFILL")
print("=" * 80)

successful = 0
failed = 0

# For each week going backwards from today
for week_offset in range(0, -total_weeks, -1):
    week_num = -week_offset
    print(f"\n[WEEK {week_num}/{total_weeks}]")
    
    # For each event type
    for event_type in EVENT_TYPES:
        try:
            result = subprocess.run(
                [sys.executable, "fetch_one_week.py", event_type, "--week", str(week_offset)],
                capture_output=True,
                text=True,
                timeout=240
            )
            
            if result.returncode == 0:
                successful += 1
                print(f"  ✓ {event_type}")
            else:
                failed += 1
                print(f"  ✗ {event_type}")
                
        except subprocess.TimeoutExpired:
            failed += 1
            print(f"  ✗ {event_type} (timeout)")
        except Exception as e:
            failed += 1
            print(f"  ✗ {event_type} ({str(e)[:50]})")

print("\n" + "=" * 80)
print("BACKFILL COMPLETED")
print("=" * 80)
print(f"\nResults:")
print(f"  Successful: {successful}")
print(f"  Failed: {failed}")
print(f"  Total: {successful + failed}")

if failed == 0:
    print(f"\n✓ All data successfully backfilled!")
else:
    print(f"\n⚠️  Some weeks failed. Run the script again to retry.")
