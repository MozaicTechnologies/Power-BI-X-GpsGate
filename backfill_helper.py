"""
Backfill helper for scheduled data pulling
Fetches from GpsGate API and stores to database
"""
import requests
from datetime import datetime, timedelta
from db_storage import store_event_data_to_db
from config import Config
import os

# Get configuration from environment
BASE_URL = "https://omantracking2.com"
APP_ID = os.getenv("GPSGATE_APP_ID", "")
TOKEN = os.getenv("GPSGATE_TOKEN", "")
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")


def get_gpsgate_session():
    """Create a requests session with auth headers"""
    session = requests.Session()
    if TOKEN:
        session.headers.update({
            'Authorization': f'Bearer {TOKEN}'
        })
    return session


def fetch_and_store_event(event_name, response_key, report_id, tag_id=None, event_id=None, 
                          week_start=None, week_end=None):
    """
    Fetch event data from GpsGate and store to database
    
    Args:
        event_name: Name of event (Trip, Speeding, Idle, etc)
        response_key: JSON response key (trip_data, speeding_data, etc)
        report_id: GpsGate report ID
        tag_id: GpsGate tag ID (required for most events)
        event_id: GpsGate event ID (required for some events)
        week_start: Start date (datetime, optional)
        week_end: End date (datetime, optional)
    
    Returns:
        dict with 'success', 'inserted', 'duplicates', 'errors'
    """
    try:
        if not APP_ID or not TOKEN:
            return {
                'success': False,
                'error': 'Missing GPSGATE_APP_ID or GPSGATE_TOKEN',
                'inserted': 0,
                'duplicates': 0,
                'errors': 1
            }
        
        # Build API URL
        api_path = f"comGpsGate/api/v.1/applications/{APP_ID}/reports/{report_id}/data"
        url = f"{BASE_URL}/{api_path}"
        
        # Build query parameters
        params = {}
        if week_start:
            params['from'] = week_start.isoformat()
        if week_end:
            params['to'] = week_end.isoformat()
        if tag_id:
            params['tag_id'] = tag_id
        if event_id:
            params['event_id'] = event_id
        
        # Fetch data
        session = get_gpsgate_session()
        response = response.get(url, params=params, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        
        # Extract event data from response
        if response_key not in data:
            return {
                'success': False,
                'error': f'Response key "{response_key}" not found',
                'inserted': 0,
                'duplicates': 0,
                'errors': 1
            }
        
        event_data = data[response_key]
        if not isinstance(event_data, list):
            event_data = [event_data] if event_data else []
        
        # Store to database
        result = store_event_data_to_db(event_name, event_data)
        
        return result
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e)[:200],
            'inserted': 0,
            'duplicates': 0,
            'errors': 1
        }


def backfill_current_week():
    """
    Fetch and store current week data for all 8 event types
    
    Returns:
        dict with summary stats
    """
    today = datetime.now()
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    
    endpoints = [
        {
            'event_name': 'Trip',
            'response_key': 'trip_data',
            'report_id': 'trip_report_id',
            'tag_id': True,
        },
        {
            'event_name': 'Speeding',
            'response_key': 'speeding_data',
            'report_id': 'speeding_report_id',
            'event_id': 'speeding_event_id',
        },
        {
            'event_name': 'Idle',
            'response_key': 'idle_data',
            'report_id': 'idle_report_id',
            'event_id': 'idle_event_id',
        },
        {
            'event_name': 'AWH',
            'response_key': 'awh_data',
            'report_id': 'awh_report_id',
            'event_id': 'awh_event_id',
        },
        {
            'event_name': 'WH',
            'response_key': 'wh_data',
            'report_id': 'wh_report_id',
            'tag_id': True,
        },
        {
            'event_name': 'HA',
            'response_key': 'ha_data',
            'report_id': 'ha_report_id',
            'event_id': 'ha_event_id',
        },
        {
            'event_name': 'HB',
            'response_key': 'hb_data',
            'report_id': 'hb_report_id',
            'event_id': 'hb_event_id',
        },
        {
            'event_name': 'WU',
            'response_key': 'wu_data',
            'report_id': 'wu_report_id',
            'event_id': 'wu_event_id',
        },
    ]
    
    all_stats = {}
    total_inserted = 0
    total_duplicates = 0
    total_errors = 0
    
    for ep in endpoints:
        try:
            # Get IDs from environment
            tag_id = os.getenv(f"GPSGATE_{ep['event_name'].upper()}_TAG_ID") if ep.get('tag_id') else None
            event_id = os.getenv(f"GPSGATE_{ep['event_name'].upper()}_EVENT_ID") if ep.get('event_id') else None
            report_id = os.getenv(f"GPSGATE_{ep['event_name'].upper()}_REPORT_ID", "")
            
            result = fetch_and_store_event(
                event_name=ep['event_name'],
                response_key=ep['response_key'],
                report_id=report_id,
                tag_id=tag_id,
                event_id=event_id,
                week_start=monday,
                week_end=sunday
            )
            
            all_stats[ep['event_name']] = {
                'inserted': result.get('inserted', 0),
                'duplicates': result.get('duplicates', 0),
                'errors': result.get('errors', 0)
            }
            
            total_inserted += result.get('inserted', 0)
            total_duplicates += result.get('duplicates', 0)
            total_errors += result.get('errors', 0)
            
        except Exception as e:
            all_stats[ep['event_name']] = {
                'error': str(e)[:100],
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
        'stats_by_type': all_stats
    }
