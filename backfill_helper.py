"""
Backfill helper for scheduled data pulling
Uses the same approach as backfill_direct_python.py
"""
import os
from datetime import datetime, timedelta
from flask import request as flask_request
import json

# Configuration from backfill_direct_python.py
APP_ID = "6"
TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
BASE_URL = "https://omantracking2.com"
REPORT_ID = "25"
TAG_ID = "39"

# Event IDs for each endpoint
EVENT_IDS = {
    "Trip": None,
    "Speeding": "18",
    "Idle": "1328",
    "AWH": "12",
    "WH": "13",
    "HA": "1327",
    "HB": "1326",
    "WU": "17",
}

# Trip uses different report_id
TRIP_REPORT_ID = "1225"


def backfill_current_week():
    """
    Fetch and store current week data for all 8 event types
    Uses process_event_data function with Flask request context
    
    Returns:
        dict with summary stats
    """
    from application import create_app, db
    from data_pipeline import process_event_data
    
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    
    endpoints = [
        ("Trip", "trip_data", "Trip"),
        ("Speeding", "speeding_data", "Speeding"),
        ("Idle", "idle_data", "Idle"),
        ("AWH", "awh_data", "AWH"),
        ("WH", "wh_data", "WH"),
        ("HA", "ha_data", "HA"),
        ("HB", "hb_data", "HB"),
        ("WU", "wu_data", "WU"),
    ]
    
    app = create_app()
    all_stats = {}
    total_inserted = 0
    total_duplicates = 0
    total_errors = 0
    
    with app.app_context():
        for label, response_key, event_name in endpoints:
            try:
                # Build payload for this event
                payload = {
                    "app_id": APP_ID,
                    "token": TOKEN,
                    "base_url": BASE_URL,
                    "report_id": TRIP_REPORT_ID if event_name == "Trip" else REPORT_ID,
                    "tag_id": TAG_ID,
                }
                
                # Add event_id if needed
                if EVENT_IDS.get(event_name):
                    payload["event_id"] = EVENT_IDS[event_name]
                
                # Create a mock request context
                with app.test_request_context(
                    method='POST',
                    data=json.dumps(payload),
                    content_type='application/json'
                ):
                    result = process_event_data(event_name, response_key)
                    
                    if result and isinstance(result, tuple):
                        # Response is (jsonify result, status_code)
                        response_data = result[0].get_json() if hasattr(result[0], 'get_json') else result[0]
                    else:
                        response_data = result
                    
                    if response_data and 'db_stats' in response_data:
                        stats = response_data['db_stats']
                        inserted = stats.get('inserted', 0)
                        duplicates = stats.get('duplicates', 0)
                        errors = stats.get('errors', 0)
                        
                        all_stats[label] = {
                            'inserted': inserted,
                            'duplicates': duplicates,
                            'errors': errors
                        }
                        
                        total_inserted += inserted
                        total_duplicates += duplicates
                        total_errors += errors
                    else:
                        all_stats[label] = {
                            'inserted': 0,
                            'duplicates': 0,
                            'errors': 0
                        }
                        
            except Exception as e:
                all_stats[label] = {
                    'error': str(e)[:200],
                    'inserted': 0,
                    'duplicates': 0,
                    'errors': 1
                }
                total_errors += 1
    
    return {
        'success': total_errors == 0,
        'week': f"{monday.strftime('%Y-%m-%d')} to {sunday.strftime('%Y-%m-%d')}",
        'total_inserted': total_inserted,
        'total_duplicates': total_duplicates,
        'total_errors': total_errors,
        'stats_by_type': all_stats,
        'timestamp': datetime.now().isoformat()
    }
