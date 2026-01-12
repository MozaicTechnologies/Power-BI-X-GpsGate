#!/usr/bin/env python
"""Fast backfill with UPSERT logic - simply runs the proven backfill_direct_python"""
import subprocess
import sys

print("=" * 80)
print("FAST BACKFILL - All Endpoints (Week 1, 2025)")
print("=" * 80)
print("\nTables cleared. Running backfill_direct_python.py...\n")

# Run the existing working backfill script
result = subprocess.run([sys.executable, "backfill_direct_python.py"])
sys.exit(result.returncode)
