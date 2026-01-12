"""
Simple backfill script - fetches all weeks for ONE endpoint at a time
Use: python backfill_simple.py <endpoint> (trip, speeding, idle, awh, wh, ha, hb, wu)
"""

import subprocess
import sys
import os
import warnings
from datetime import datetime

# Suppress deprecation warnings
warnings.filterwarnings('ignore', category=DeprecationWarning)

if len(sys.argv) < 2:
    print("Usage: python backfill_simple.py <endpoint>")
    print("Available endpoints: trip, speeding, idle, awh, wh, ha, hb, wu")
    sys.exit(1)

event_type = sys.argv[1].lower()
valid_types = ["trip", "speeding", "idle", "awh", "wh", "ha", "hb", "wu"]

if event_type not in valid_types:
    print(f"ERROR: Invalid endpoint '{event_type}'")
    print(f"Available: {', '.join(valid_types)}")
    sys.exit(1)

# Calculate number of weeks
START_DATE = datetime(2025, 1, 1)
END_DATE = datetime.utcnow()
days_diff = (END_DATE.date() - START_DATE.date()).days
total_weeks = days_diff // 7 + 1

print("=" * 80)
print(f"BACKFILL: {event_type.upper()}")
print("=" * 80)
print(f"\nPeriod: {START_DATE.strftime('%Y-%m-%d')} to {END_DATE.strftime('%Y-%m-%d')}")
print(f"Total weeks: {total_weeks}")
print(f"Total API calls: {total_weeks}")
print("\nNote: Each call fetches 1 week of data (~3 minutes per week)")
print(f"Estimated time: ~{total_weeks * 3 // 60} hours ({total_weeks * 3} minutes)")

print("\n" + "=" * 80)
print(f"STARTING BACKFILL FOR {event_type.upper()}")
print("=" * 80)

# First, start Flask server in background if not already running
import time
print("\nStarting Flask server...")
server_process = subprocess.Popen(
    [sys.executable, "main.py"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE
)
time.sleep(5)  # Wait for server to start

print("Flask server started. Beginning data fetch...\n")

successful = 0
failed = 0
total_rows = 0
failed_weeks = []

# For each week (0 = current week, -1 = last week, etc)
for week_offset in range(0, -total_weeks, -1):
    week_num = -week_offset + 1
    print(f"\n[{week_num:2d}/{total_weeks:2d}] Week {week_num} (offset: {week_offset:3d})... ", end="", flush=True)
    
    try:
        result = subprocess.run(
            [sys.executable, "-W", "ignore", "fetch_one_week.py", event_type, "--week", str(week_offset)],
            capture_output=True,
            text=True,
            timeout=240  # 4 minute timeout
        )
        
        if result.returncode == 0:
            output = result.stdout
            if "[OK] Successfully" in output:
                # Parse the number of records
                for line in output.split('\n'):
                    if "Total rows:" in line:
                        rows = int(line.split(":")[-1].strip())
                        total_rows += rows
                        print(f"OK - {rows} rows")
                        successful += 1
                        break
                else:
                    print(f"OK")
                    successful += 1
            elif "No data found" in output:
                print(f"OK - No data")
                successful += 1
            else:
                print(f"FAILED")
                failed += 1
                failed_weeks.append(week_num)
        else:
            print(f"ERROR (code: {result.returncode})")
            failed += 1
            failed_weeks.append(week_num)
    except subprocess.TimeoutExpired:
        print(f"TIMEOUT")
        failed += 1
        failed_weeks.append(week_num)
    except Exception as e:
        print(f"EXCEPTION: {e}")
        failed += 1
        failed_weeks.append(week_num)

print("\n" + "=" * 80)
print(f"BACKFILL COMPLETE FOR {event_type.upper()}")
print("=" * 80)
print(f"\nSuccessful: {successful}/{total_weeks}")
print(f"Failed: {failed}/{total_weeks}")
print(f"Total rows fetched: {total_rows}")

if failed_weeks:
    print(f"\nFailed weeks: {', '.join(map(str, failed_weeks[:10]))}", end="")
    if len(failed_weeks) > 10:
        print(f" ... and {len(failed_weeks)-10} more")
    else:
        print()

print("\nRun 'python db_summary.py' to check database statistics")
