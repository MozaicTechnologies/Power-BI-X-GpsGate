#!/usr/bin/env python
"""
Week-first backfill runner.

Executes backfill_2025_week1.py week-by-week until today.
If one week fails, the runner records the failure and continues with the next week.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import date, timedelta


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

BACKFILL_SCRIPT = "backfill_2025_week1.py"

START_DATE = date(2025, 4, 8)
# TODAY = date.today()
TODAY = date(2025, 4, 29)


PYTHON_BIN = sys.executable  # uses current venv python


# --------------------------------------------------
# EXECUTION
# --------------------------------------------------

print("=" * 80)
print("WEEK-FIRST BACKFILL RUNNER STARTED")
print("=" * 80)
print(f"Date range: {START_DATE} -> {TODAY}")

current_start = START_DATE
successful_weeks: list[tuple[date, date]] = []
failed_weeks: list[tuple[date, date, int]] = []

while current_start <= TODAY:
    week_start = current_start
    # Use inclusive 7-day windows to match the backfill script's inclusive end date.
    week_end = min(current_start + timedelta(days=6), TODAY)

    print("\n" + "=" * 80)
    print(f"Processing WEEK: {week_start} -> {week_end}")
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
        print("\nBACKFILL FAILED")
        print(f"Week: {week_start} -> {week_end}")
        print("Continuing to next week")
        failed_weeks.append((week_start, week_end, result.returncode))
    else:
        print(f"WEEK COMPLETED: {week_start} -> {week_end}")
        successful_weeks.append((week_start, week_end))

    current_start = week_end + timedelta(days=1)

print("\n" + "=" * 80)
print(f"Successful weeks: {len(successful_weeks)}")
print(f"Failed weeks: {len(failed_weeks)}")

if successful_weeks:
    print("\nSuccessful ranges:")
    for week_start, week_end in successful_weeks:
        print(f"  OK: {week_start} -> {week_end}")

if failed_weeks:
    print("\nFailed ranges:")
    for week_start, week_end, returncode in failed_weeks:
        print(f"  FAILED: {week_start} -> {week_end} (exit code {returncode})")
    print("=" * 80)
    sys.exit(1)

print("ALL WEEKS BACKFILLED SUCCESSFULLY")
print("=" * 80)
