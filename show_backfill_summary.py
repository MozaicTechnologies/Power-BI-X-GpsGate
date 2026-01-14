"""
Parse and display summary from backfill output
Shows what data was fetched for each event type
"""

import os
import re

output_file = "backfill_output.txt"

if not os.path.exists(output_file):
    print(f"❌ {output_file} not found")
    print("   Run: $env:FETCH_CURRENT_WEEK='true' ; python backfill_direct_python.py > backfill_output.txt 2>&1")
    exit(1)

with open(output_file, 'r', encoding='utf-8', errors='ignore') as f:
    content = f.read()

print("\n" + "="*70)
print("DATA FETCH SUMMARY")
print("="*70)

# Find all event processing markers
event_pattern = r'\[([0-9]/8)\]\s+(\w+)'
events = re.findall(event_pattern, content)

if events:
    print("\nEvent Processing Detected:")
    for num, name in events:
        print(f"  [{num}] {name}")
else:
    print("⚠ No event processing markers found yet")

# Find row counts
row_pattern = r'(\w+)\s+Week\s+\d+:\s+(\d+)\s+rows\s+fetched'
rows = re.findall(row_pattern, content)

if rows:
    print("\nRows Fetched per Event Type:")
    total = 0
    for event, count in rows:
        count_int = int(count)
        print(f"  {event:15} {count_int:8,} rows")
        total += count_int
    print(f"  {'TOTAL':15} {total:8,} rows")
else:
    print("⚠ No row count data found - still processing or not yet complete")

# Find COMPLETE ROW ACCOUNTING sections
if "COMPLETE ROW ACCOUNTING" in content:
    print("\n✓ Process Complete - Found Accounting Sections:")
    
    # Extract all accounting sections
    accounting_pattern = r'COMPLETE ROW ACCOUNTING[^\n]*EVENT\s+(\w+).*?Total Inserted:\s+(\d+)'
    matches = re.findall(accounting_pattern, content, re.DOTALL)
    
    if matches:
        total_inserted = 0
        for event, inserted in matches:
            count = int(inserted)
            total_inserted += count
            if count > 0:
                print(f"  ✓ {event:15} {count:8,} inserted")
            else:
                print(f"  - {event:15} {count:8,} (no new records)")
        print(f"  {'─'*40}")
        print(f"  {'TOTAL':15} {total_inserted:8,} inserted")
    else:
        # Try alternate pattern
        print("  (Parsing accounting data...)")
        for line in content.split('\n'):
            if 'Total Inserted' in line or 'Total Raw' in line or 'Internal Dupes' in line:
                print(f"  {line.strip()}")
else:
    # Check if still processing
    if "[DEBUG INT ERR]" in content:
        print("\n⏳ Still processing - database inserts in progress")
        
        # Count error lines
        error_count = content.count("[DEBUG INT ERR]")
        print(f"   Unique constraint violations so far: {error_count}")
    else:
        print("\n⏳ Process still running - check again in a moment")

# Show file size and last update
file_size = os.path.getsize(output_file)
print(f"\nOutput file: {file_size:,} bytes")

print("="*70)
print("\nTo see full details, run:")
print("  Get-Content backfill_output.txt | Select-String 'Trip|Speeding|Idle|AWH|WH|HA|HB|WU' -Context 1,1")
print("="*70 + "\n")
