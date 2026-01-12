#!/usr/bin/env python
"""
Backfill FIRST 5 WEEKS of 2025 using data_pipeline endpoints
Week 1: Jan 1-7, 2025
Week 2: Jan 8-14, 2025
Week 3: Jan 15-21, 2025
Week 4: Jan 22-28, 2025
Week 5: Jan 29-Feb 4, 2025
"""

import requests
import sys
from datetime import datetime
import warnings
import time

warnings.filterwarnings('ignore')

# Configuration
BASE_URL = "http://localhost:5000"
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
GPSGATE_URL = "https://omantracking2.com"
TAG_ID = "39"

# First 5 weeks of 2025
WEEKS = [
    {"start": "2025-01-01 00:00:00", "end": "2025-01-07 23:59:59", "name": "Week 1 (Jan 1-7)"},
    {"start": "2025-01-08 00:00:00", "end": "2025-01-14 23:59:59", "name": "Week 2 (Jan 8-14)"},
    {"start": "2025-01-15 00:00:00", "end": "2025-01-21 23:59:59", "name": "Week 3 (Jan 15-21)"},
    {"start": "2025-01-22 00:00:00", "end": "2025-01-28 23:59:59", "name": "Week 4 (Jan 22-28)"},
    {"start": "2025-01-29 00:00:00", "end": "2025-02-04 23:59:59", "name": "Week 5 (Jan 29-Feb 4)"},
]

ENDPOINTS = {
    "trip": {
        "url": "/trip-data",
        "report_id": "1225",
        "event_id": "1225",  # Use event_id as report_id for trip
        "use_event_id": False
    },
    "speeding": {
        "url": "/speeding-data",
        "report_id": "25",
        "event_id": "18",
        "use_event_id": True
    },
    "idle": {
        "url": "/idle-data",
        "report_id": "25",
        "event_id": "1328",
        "use_event_id": True
    },
    "awh": {
        "url": "/awh-data",
        "report_id": "25",
        "event_id": "12",
        "use_event_id": True
    },
    "wh": {
        "url": "/wh-data",
        "report_id": "25",
        "event_id": "13",
        "use_event_id": True
    },
    "ha": {
        "url": "/ha-data",
        "report_id": "25",
        "event_id": "1327",
        "use_event_id": True
    },
    "hb": {
        "url": "/hb-data",
        "report_id": "25",
        "event_id": "1326",
        "use_event_id": True
    },
    "wu": {
        "url": "/wu-data",
        "report_id": "25",
        "event_id": "17",
        "use_event_id": True
    },
}

print("=" * 80)
print("BACKFILL FIRST 5 WEEKS OF 2025")
print("=" * 80)
print("\nWaiting for Flask server...")
time.sleep(2)

total_stats = {
    "inserted": 0,
    "skipped": 0,
    "failed": 0,
    "total_rows": 0
}

for event_name, endpoint_cfg in ENDPOINTS.items():
    print(f"\n[{list(ENDPOINTS.keys()).index(event_name) + 1}/8] {event_name.upper()}")
    print("-" * 80)
    
    for week_idx, week in enumerate(WEEKS):
        print(f"  {week['name']:25s}... ", end="", flush=True)
        
        try:
            payload = {
                "app_id": APP_ID,
                "token": TOKEN,
                "base_url": GPSGATE_URL,
                "report_id": endpoint_cfg["report_id"],
                "tag_id": TAG_ID,
            }
            
            # Add event_id if needed
            if endpoint_cfg["use_event_id"]:
                payload["event_id"] = endpoint_cfg["event_id"]
            
            response = requests.post(
                f"{BASE_URL}{endpoint_cfg['url']}",
                json=payload,
                timeout=300  # 5 minute timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                db_stats = data.get('db_stats', {})
                total_rows = data.get('total_rows', 0)
                inserted = db_stats.get('inserted', 0)
                skipped = db_stats.get('skipped', 0)
                failed = db_stats.get('failed', 0)
                
                total_stats['inserted'] += inserted
                total_stats['skipped'] += skipped
                total_stats['failed'] += failed
                total_stats['total_rows'] += total_rows
                
                if total_rows == 0:
                    print("0 rows")
                elif failed > 0:
                    print(f"{total_rows:,} rows ({inserted:,} OK, {failed:,} ERR)")
                else:
                    print(f"{total_rows:,} rows (OK)")
            elif response.text:
                try:
                    error_msg = response.json()
                    print(f"HTTP {response.status_code}: {error_msg.get('error', 'Unknown')[:40]}")
                except:
                    print(f"HTTP {response.status_code}: {response.text[:40]}")
            else:
                print(f"HTTP {response.status_code}: Empty response")
                
        except requests.exceptions.Timeout:
            print("TIMEOUT")
        except requests.exceptions.ConnectionError:
            print("NO SERVER - Start Flask with: python main.py")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: {str(e)[:40]}")

print("\n" + "=" * 80)
print("BACKFILL COMPLETE - FIRST 5 WEEKS OF 2025")
print("=" * 80)
print(f"Total Rows Processed: {total_stats['total_rows']:,}")
print(f"  Inserted: {total_stats['inserted']:,}")
print(f"  Duplicates: {total_stats['skipped']:,}")
print(f"  Errors: {total_stats['failed']:,}")
print("=" * 80)

# Show database summary
print("\nDatabase Summary:")
from dotenv import load_dotenv
load_dotenv()
from application import create_app
from models import db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU

app = create_app()
with app.app_context():
    models = [
        ('fact_trip', FactTrip),
        ('fact_speeding', FactSpeeding),
        ('fact_idle', FactIdle),
        ('fact_awh', FactAWH),
        ('fact_wh', FactWH),
        ('fact_ha', FactHA),
        ('fact_hb', FactHB),
        ('fact_wu', FactWU),
    ]
    print()
    for name, model in models:
        count = db.session.query(model).count()
        print(f"  {name:15s}: {count:,} records")
