#!/usr/bin/env python
"""
Fast backfill for 5 weeks of ALL endpoints
Uses direct Python calls (no subprocess overhead)
"""

import requests
import sys
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
BASE_URL = "http://localhost:5000"
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
GPSGATE_URL = "https://omantracking2.com"
TAG_ID = "39"

REPORT_IDS = {
    "trip": "1225", "speeding": "25", "idle": "25",
    "awh": "25", "wh": "25", "ha": "25", "hb": "25", "wu": "25",
}

EVENT_IDS = {
    "trip": "1225", "speeding": "18", "idle": "1328",
    "awh": "12", "wh": "13", "ha": "1327", "hb": "1326", "wu": "17",
}

ENDPOINTS = {
    "trip": "/trip-data",
    "speeding": "/speeding-data",
    "idle": "/idle-data",
    "awh": "/awh-data",
    "wh": "/wh-data",
    "ha": "/ha-data",
    "hb": "/hb-data",
    "wu": "/wu-data",
}

print("=" * 80)
print("BACKFILL ALL ENDPOINTS - 5 WEEKS (Fast Direct Method)")
print("=" * 80)

total_stats = {
    "inserted": 0,
    "skipped": 0,
    "failed": 0
}

event_types = list(REPORT_IDS.keys())
# Use positive offsets - just 1 week per endpoint to test
week_offsets = [0]  # Current week only for testing

for event_idx, event_type in enumerate(event_types):
    print(f"\n[{event_idx+1}/8] {event_type.upper()}")
    print("-" * 80)
    
    endpoint = ENDPOINTS[event_type]
    payload = {
        "app_id": APP_ID,
        "token": TOKEN,
        "base_url": GPSGATE_URL,
        "report_id": REPORT_IDS[event_type],
        "tag_id": TAG_ID,
    }
    
    # Add event_id for non-trip events
    if event_type != "trip":
        payload["event_id"] = EVENT_IDS[event_type]
    
    for week_idx, week_offset in enumerate(week_offsets):
        week_num = week_idx + 1
        print(f"  Week {week_num} (offset {week_offset:2d})... ", end="", flush=True)
        
        try:
            response = requests.post(
                f"{BASE_URL}{endpoint}",
                json=payload,
                timeout=180  # 3 minute timeout
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
                
                if total_rows == 0:
                    print("0 rows")
                elif failed > 0:
                    print(f"{total_rows} rows ({inserted} OK, {failed} ERR)")
                else:
                    print(f"{total_rows} rows (OK)")
            else:
                print(f"HTTP {response.status_code}")
        except requests.exceptions.Timeout:
            print("TIMEOUT")
        except requests.exceptions.ConnectionError:
            print("NO SERVER")
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: {str(e)[:40]}")

print("\n" + "=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)
print(f"Total Inserted: {total_stats['inserted']:,}")
print(f"Total Duplicates: {total_stats['skipped']:,}")
print(f"Total Errors: {total_stats['failed']:,}")
print("=" * 80)

# Show summary
print("\nDatabase Summary:")
from dotenv import load_dotenv
load_dotenv()
from application import create_app
from models import db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU

app = create_app()
with app.app_context():
    models = {
        'fact_trip': FactTrip,
        'fact_speeding': FactSpeeding,
        'fact_idle': FactIdle,
        'fact_awh': FactAWH,
        'fact_wh': FactWH,
        'fact_ha': FactHA,
        'fact_hb': FactHB,
        'fact_wu': FactWU,
    }
    for name, model in models.items():
        count = db.session.query(model).count()
        print(f"  {name:15s}: {count:,} records")
