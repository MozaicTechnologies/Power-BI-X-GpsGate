#!/usr/bin/env python
import os

print("STEP 1: Check before setting")
print(f"  FETCH_CURRENT_WEEK={os.environ.get('FETCH_CURRENT_WEEK')}")

os.environ['FETCH_CURRENT_WEEK'] = 'true'

print("\nSTEP 2: After setting environment variable")
print(f"  FETCH_CURRENT_WEEK={os.environ.get('FETCH_CURRENT_WEEK')}")
print(f"  Check:  {os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true'}")

print("\nSTEP 3: Now importing data_pipeline.build_weekly_schedule")
from data_pipeline import build_weekly_schedule

print("\nSTEP 4: Calling build_weekly_schedule()")
weeks = build_weekly_schedule()

print(f"\nSTEP 5: Results")
print(f"  Number of weeks: {len(weeks)}")
if len(weeks) ==1:
    print(f"  SUCCESS: Only 1 week!")
    print(f"  Week: {weeks[0]}")
else:
    print(f"  FAILED: Expected 1 week but got {len(weeks)}")
    print(f"  First 3 weeks:")
    for i in range(min(3, len(weeks))):
        print(f"    {weeks[i]}")
