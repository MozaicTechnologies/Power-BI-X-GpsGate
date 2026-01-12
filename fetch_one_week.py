"""
Fetch one week of data for any endpoint
Call this weekly via cron job to incrementally pull and store data
"""

import argparse
from datetime import datetime, timedelta
import requests
import json
from dotenv import load_dotenv

load_dotenv()

# Configuration
BASE_URL = "http://localhost:5000"
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
GPSGATE_URL = "https://omantracking2.com"

# Report IDs and Event IDs for each event type
REPORT_IDS = {
    "trip": "1225",           # Trip and Idle (Tag)-BI Format
    "speeding": "25",         # Event Rule detailed (Tag)
    "idle": "25",             # Trip & Idle detailed (Tag)
    "awh": "25",              # Event Rule detailed (Tag)
    "wh": "25",               # Event Rule detailed (Tag)
    "ha": "25",               # Event Rule detailed (Tag)
    "hb": "25",               # Event Rule detailed (Tag)
    "wu": "25",               # Event Rule detailed (Tag)
}

EVENT_IDS = {
    "trip": "1225",           # Trip
    "speeding": "18",         # Speeding
    "idle": "1328",           # Idle (30 min)
    "awh": "12",              # After Working Hours
    "wh": "13",               # Working Hours Usage
    "ha": "1327",             # Harsh Acceleration
    "hb": "1326",             # Harsh Braking
    "wu": "17",               # Weekend Usage
}

TAG_ID = "39"

def get_week_range(week_offset=0):
    """
    Get start and end dates for a specific week
    week_offset=0 is the current week, -1 is last week, etc.
    """
    today = datetime.utcnow()
    week_start = datetime(2025, 1, 1)
    
    # Calculate current week number
    days_since_start = (today.date() - week_start.date()).days
    current_week = days_since_start // 7
    
    # Calculate target week
    target_week = current_week + week_offset
    target_start = week_start + timedelta(weeks=target_week)
    target_end = target_start + timedelta(days=7) - timedelta(seconds=1)
    
    return target_start, target_end, target_week

def fetch_week_data(event_type, week_offset=0):
    """
    Fetch one week of data for a specific event type
    week_offset=0 is current week, -1 is last week, etc.
    """
    event_type = event_type.lower()
    
    if event_type not in REPORT_IDS:
        print(f"ERROR: Unknown event type: {event_type}")
        print(f"Available: {', '.join(REPORT_IDS.keys())}")
        return None
    
    # Get week dates
    week_start, week_end, week_num = get_week_range(week_offset)
    
    print(f"\n{'='*80}")
    print(f"FETCHING: {event_type.upper()} - Week {week_num}")
    print(f"Period: {week_start.strftime('%Y-%m-%d %H:%M')} to {week_end.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*80}")
    
    # Determine endpoint (must match Flask routes in data_pipeline.py)
    endpoint_map = {
        "trip": "/trip-data",
        "speeding": "/speeding-data",
        "idle": "/idle-data",
        "awh": "/awh-data",
        "wh": "/wh-data",
        "ha": "/ha-data",
        "hb": "/hb-data",
        "wu": "/wu-data",
    }
    endpoint = endpoint_map.get(event_type, f"/{event_type}-data")
    
    # Build payload
    payload = {
        "app_id": APP_ID,
        "token": TOKEN,
        "base_url": GPSGATE_URL,
        "report_id": REPORT_IDS[event_type],
        "tag_id": TAG_ID,
    }
    
    # Add event_id for all events (including trip)
    payload["event_id"] = EVENT_IDS.get(event_type, "1")
    
    try:
        print(f"\nSending request to {BASE_URL}{endpoint}...")
        response = requests.post(
            f"{BASE_URL}{endpoint}",
            json=payload,
            timeout=180  # 3 minute timeout for 1 week
        )
        
        print(f"Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            
            total_rows = data.get('total_rows', 0)
            weeks_processed = data.get('weeks_processed', 0)
            db_stats = data.get('db_stats', {})
            
            print(f"\nResults:")
            print(f"  Total rows: {total_rows}")
            print(f"  Weeks processed: {weeks_processed}")
            print(f"\nDatabase Storage:")
            print(f"  Inserted: {db_stats.get('inserted', 0)}")
            print(f"  Duplicates (skipped): {db_stats.get('skipped', 0)}")
            print(f"  Failed: {db_stats.get('failed', 0)}")
            
            if total_rows > 0:
                print(f"\n[OK] Successfully fetched and stored {total_rows} {event_type} records")
                return True
            else:
                print(f"\n[WARN] No data found for this week")
                return False
        else:
            print(f"ERROR: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            return False
            
    except requests.exceptions.Timeout:
        print(f"ERROR: Request timeout after 180 seconds")
        return False
    except requests.exceptions.ConnectionError:
        print(f"ERROR: Cannot connect to server at {BASE_URL}")
        print(f"Make sure Flask server is running: python main.py")
        return False
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch one week of data for a specific event type"
    )
    parser.add_argument(
        "event_type",
        choices=["trip", "speeding", "idle", "awh", "wh", "ha", "hb", "wu"],
        help="Event type to fetch"
    )
    parser.add_argument(
        "--week",
        type=int,
        default=0,
        help="Week offset (0=current, -1=last week, -2=2 weeks ago, etc.)"
    )
    
    args = parser.parse_args()
    
    success = fetch_week_data(args.event_type, args.week)
    exit(0 if success else 1)
