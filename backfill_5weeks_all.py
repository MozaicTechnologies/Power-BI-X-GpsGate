#!/usr/bin/env python
"""
Backfill ALL 8 endpoints for just 5 weeks of data
Runs them sequentially
"""

import subprocess
import sys
from datetime import datetime

EVENT_TYPES = ["trip", "speeding", "idle", "awh", "wh", "ha", "hb", "wu"]

print("=" * 80)
print("BACKFILL ALL ENDPOINTS - 5 WEEKS ONLY")
print("=" * 80)

print("\nClearing database...")
result = subprocess.run(
    [sys.executable, "clear_fact_tables.py"],
    capture_output=True,
    text=True
)
if result.returncode != 0:
    print(f"Error clearing database: {result.stderr}")
    sys.exit(1)
print("[OK] Database cleared\n")

print("=" * 80)
print("STARTING BACKFILL FOR ALL ENDPOINTS (5 weeks each)")
print("=" * 80)

total_stats = {
    "inserted": 0,
    "skipped": 0,
    "failed": 0
}

for event_type in EVENT_TYPES:
    print(f"\n[{EVENT_TYPES.index(event_type) + 1}/{len(EVENT_TYPES)}] {event_type.upper()}")
    print("-" * 80)
    
    # Run fetch_one_week.py for the LAST 5 weeks (-52, -51, -50, -49, -48)
    # This fetches the 5 most recent weeks with actual data
    week_offsets = [-52, -51, -50, -49, -48]
    
    for i, week_offset in enumerate(week_offsets):
        week_num = i + 1
        
        print(f"  Week {week_num}/5 (offset: {week_offset:2d})... ", end="", flush=True)
        
        result = subprocess.run(
            [sys.executable, "fetch_one_week.py", event_type, "--week", str(week_offset)],
            capture_output=True,
            text=True,
            timeout=180
        )
        
        if result.returncode == 0:
            # Parse output to get stats
            output = result.stdout
            if "Inserted:" in output:
                # Extract stats from output
                lines = output.split('\n')
                for line in lines:
                    if "Inserted:" in line:
                        try:
                            inserted = int(line.split("Inserted:")[1].split()[0])
                            print(f"[OK] {inserted:,} rows")
                            total_stats["inserted"] += inserted
                        except:
                            print("[OK]")
                    elif "Duplicates" in line:
                        try:
                            skipped = int(line.split("Duplicates")[1].split()[0].replace("(skipped):", "").strip())
                            total_stats["skipped"] += skipped
                        except:
                            pass
                    elif "Failed:" in line:
                        try:
                            failed = int(line.split("Failed:")[1].split()[0])
                            total_stats["failed"] += failed
                        except:
                            pass
            else:
                print("[OK]")
        else:
            print(f"[FAIL]")
            print(result.stderr[:200])

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
print(f"Total Results:")
print(f"  Inserted: {total_stats['inserted']:,}")
print(f"  Duplicates: {total_stats['skipped']:,}")
print(f"  Errors: {total_stats['failed']:,}")
print("=" * 80)

# Show database summary
print("\nDatabase Summary:")
print("-" * 80)
result = subprocess.run(
    [sys.executable, "db_summary.py"],
    capture_output=True,
    text=True
)
print(result.stdout)
