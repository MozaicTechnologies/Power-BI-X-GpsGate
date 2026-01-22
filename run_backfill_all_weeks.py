#!/usr/bin/env python
"""
Week-first backfill runner
Executes backfill_2025_week1.py week-by-week until today
"""

import subprocess
import sys
from datetime import datetime, timedelta, date

# --------------------------------------------------
# CONFIG
# --------------------------------------------------

BACKFILL_SCRIPT = "backfill_2025_week1.py"

START_DATE = date(2025, 11, 5)
TODAY = datetime.utcnow().date()

PYTHON_BIN = sys.executable  # uses current venv python

# --------------------------------------------------
# EXECUTION
# --------------------------------------------------

print("=" * 80)
print("WEEK-FIRST BACKFILL RUNNER STARTED")
print("=" * 80)

current_start = START_DATE

while current_start < TODAY:
    week_start = current_start
    week_end = min(current_start + timedelta(days=7), TODAY)

    print("\n" + "=" * 80)
    print(f"Processing WEEK: {week_start} → {week_end}")
    print("=" * 80)

    cmd = [
        PYTHON_BIN,
        BACKFILL_SCRIPT,
        "--week_start", week_start.strftime("%Y-%m-%d"),
        "--week_end", week_end.strftime("%Y-%m-%d"),
    ]

    print("Running command:")
    print(" ".join(cmd))

    result = subprocess.run(cmd)

    if result.returncode != 0:
        print("\n❌ BACKFILL FAILED")
        print(f"Week: {week_start} → {week_end}")
        print("⛔ Stopping further execution to avoid partial corruption")
        sys.exit(1)

    print(f"✅ Week completed: {week_start} → {week_end}")

    current_start += timedelta(days=7)

print("\n" + "=" * 80)
print("✅ ALL WEEKS BACKFILLED SUCCESSFULLY")
print("=" * 80)
