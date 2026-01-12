"""
Automatic backfill - fetch ALL historical data for all endpoints
Run this once to populate the database with all past weeks
No user interaction required
"""

import subprocess
import sys
import warnings
from datetime import datetime

# Suppress deprecation warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

# Configuration
EVENT_TYPES = ["trip", "speeding", "idle", "awh", "wh", "ha", "hb", "wu"]
REPORT_IDS = {
    "trip": "1225",      # Trip and Idle (Tag)-BI Format
    "speeding": "25",    # Event Rule detailed (Tag)
    "idle": "23",        # Trip & Idle detailed (Tag)
    "awh": "25",         # Event Rule detailed (Tag)
    "wh": "25",          # Event Rule detailed (Tag)
    "ha": "25",          # Event Rule detailed (Tag)
    "hb": "25",          # Event Rule detailed (Tag)
    "wu": "25",          # Event Rule detailed (Tag)
}
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.utcnow()

# Calculate number of weeks
days_diff = (END_DATE.date() - START_DATE.date()).days
total_weeks = days_diff // 7 + 1

print("=" * 80)
print("AUTO BACKFILL: Fetch all historical data week by week")
print("=" * 80)
print(f"\nPeriod: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
print(f"Total weeks: {total_weeks}")
print(f"Event types: {', '.join(EVENT_TYPES)}")
print(f"Total API calls: {total_weeks * len(EVENT_TYPES)}")
print("\nNote: Each call fetches 1 week of data to avoid timeouts")
print("Each call should take ~3 minutes")
print("Total estimated time: ~" + str(total_weeks * len(EVENT_TYPES) * 3 // 60) + " hours")

print("\n" + "=" * 80)
print("STARTING BACKFILL (WEEK 0 = current week)")
print("=" * 80)

successful = 0
failed = 0
failed_calls = []

# For each week (0 = current week, -1 = last week, etc)
for week_offset in range(0, -total_weeks, -1):
    week_num = -week_offset + 1
    print(f"\n{'='*80}")
    print(f"WEEK {week_num}/{total_weeks} (offset: {week_offset})")
    print(f"{'='*80}")
    
    # For each event type
    for event_type in EVENT_TYPES:
        try:
            print(f"\n  Fetching {event_type.upper()}...", end=" ", flush=True)
            result = subprocess.run(
                [sys.executable, "-W", "ignore", "fetch_one_week.py", event_type, "--week", str(week_offset)],
                capture_output=True,
                text=True,
                timeout=240  # 4 minute timeout
            )
            
            if result.returncode == 0:
                # Extract stats from output
                output = result.stdout
                if "Successfully fetched" in output:
                    # Parse the number of records
                    for line in output.split('\n'):
                        if "Total rows:" in line:
                            rows = line.split(":")[-1].strip()
                            print(f"✓ {rows} records")
                            successful += 1
                            break
                    else:
                        print(f"✓ Success")
                        successful += 1
                else:
                    print(f"❌ Failed")
                    failed += 1
                    failed_calls.append(f"Week {week_num}, {event_type}")
            else:
                print(f"❌ Error (code: {result.returncode})")
                failed += 1
                failed_calls.append(f"Week {week_num}, {event_type}")
                if result.stderr:
                    print(f"     Error: {result.stderr[:100]}")
        except subprocess.TimeoutExpired:
            print(f"❌ Timeout")
            failed += 1
            failed_calls.append(f"Week {week_num}, {event_type} (timeout)")
        except Exception as e:
            print(f"❌ Exception: {e}")
            failed += 1
            failed_calls.append(f"Week {week_num}, {event_type}")

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
print(f"\nSuccessful: {successful}")
print(f"Failed: {failed}")
print(f"Total: {successful + failed}")

if failed_calls:
    print(f"\nFailed calls:")
    for call in failed_calls[:10]:  # Show first 10
        print(f"  - {call}")
    if len(failed_calls) > 10:
        print(f"  ... and {len(failed_calls) - 10} more")

print("\nRun 'python db_summary.py' to check database statistics")
