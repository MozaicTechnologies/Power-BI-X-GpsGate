"""
Data Pipeline for Fleet Dashboard
Handles: reports, event rules, weekly rendering, CSV download & cleaning
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, time
import requests
import pandas as pd
import io
from urllib.parse import urljoin
from models import db
import json
import numpy as np
import time as pytime

# Custom JSON encoder to handle pandas/numpy types
class PandasJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.isoformat()
        elif isinstance(obj, (pd.Series, pd.Index)):
            return obj.tolist()
        elif isinstance(obj, (np.integer, np.floating)):
            return obj.item()
        elif pd.isna(obj):
            return None
        return super().default(obj)

pipeline_bp = Blueprint('pipeline_bp', __name__)
MAX_EXECUTION_SECONDS = 140   # Power BI + Gunicorn safe window
# Differentiated week limits per endpoint
MAX_WEEKS_TRIP_WH = 2         # Trip & WH: 2 weeks (~65k rows) - Render stability
MAX_WEEKS_OTHER = 20          # Other endpoints: 20 weeks (~660k rows) - full historical data

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fetch_from_gpsgate_api(base_url, token, path):
    """Fetch data from GpsGate API using the /api endpoint"""
    form_body = {
        'method': 'GET',
        'token': token,
        'base_url': base_url,
        'path': path
    }
    
    try:
        response = requests.post(
            'https://powerbixgpsgatexgdriver.onrender.com/api',
            data=form_body,
            timeout=30
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching from GpsGate API: {str(e)}")
        return None


def download_csv_from_gdrive(gdrive_link, auth_token=None):
    """
    Download CSV from Google Drive or GpsGate
    Handles authorization header only for GpsGate
    """
    try:
        headers = {}
        
        # Only add Authorization header for GpsGate URLs
        if 'omantracking2.com' in gdrive_link:
            headers['Authorization'] = auth_token
        
        response = requests.get(gdrive_link, headers=headers, timeout=60)
        response.raise_for_status()
        
        return response.content
    except Exception as e:
        print(f"Error downloading CSV from {gdrive_link}: {str(e)}")
        return None


def clean_csv_data(csv_content):
    """
    Clean CSV data:
    - Skip first 8 rows to remove headers
    - Promote actual headers
    - Keep ALL columns and ALL records (no filtering)
    - Handle type conversions for JSON serialization
    """
    try:
        # Read CSV, skip first 8 rows
        df = pd.read_csv(io.BytesIO(csv_content), skiprows=8)
        
        print(f"  Original data: {len(df)} rows, {len(df.columns)} columns")
        
        # Replace NaN and NaT with None for JSON serialization
        # Handle both NaN (float) and NaT (datetime) values
        df = df.where(pd.notna(df), None)
        df = df.replace({np.nan: None, pd.NaT: None})
        
        # Convert datetime columns to ISO format strings for JSON serialization
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').where(pd.notna(df[col]), None)
        
        return df
    except Exception as e:
        print(f"Error cleaning CSV data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

def build_weekly_schedule(start_date_str="2025-01-01"):
    """
    Pure helper function (NO Flask context).
    Safe to call from routes and internal logic.
    """
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    today = datetime.utcnow().date()

    weeks = []
    current = start_date

    while current + timedelta(days=6) <= today:
        week_end = current + timedelta(days=6)
        weeks.append({
            'week_start': current.strftime('%Y-%m-%dT00:00:00Z'),
            'week_end': week_end.strftime('%Y-%m-%dT23:59:59Z'),
            'start_date': current.isoformat(),
            'end_date': week_end.isoformat()
        })
        current += timedelta(days=7)

    return weeks


# ============================================================================
# PIPELINE ENDPOINTS
# ============================================================================

@pipeline_bp.route('/reports', methods=['POST'])
def get_reports():
    """Fetch list of reports from GpsGate"""
    try:
        data = request.form or request.get_json()
        app_id = data.get('app_id')
        token = data.get('token')
        base_url = data.get('base_url')
        
        if not all([app_id, token, base_url]):
            return jsonify({"error": "Missing required parameters"}), 400
        
        path = f"comGpsGate/api/v.1/applications/{app_id}/reports"
        api_response = fetch_from_gpsgate_api(base_url, token, path)
        
        if not api_response or 'data' not in api_response:
            return jsonify({"error": "Failed to fetch reports"}), 500
        
        # Format response
        reports = []
        for report in api_response['data']:
            reports.append({
                'id': report.get('id'),
                'name': report.get('name'),
                'description': report.get('description')
            })
        
        print(f"Fetched {len(reports)} reports")
        return jsonify({'reports': reports}), 200
    
    except Exception as e:
        print(f"Error in /reports: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@pipeline_bp.route('/event-rules', methods=['POST'])
def get_event_rules():
    """Fetch list of event rules from GpsGate"""
    try:
        data = request.form or request.get_json()
        app_id = data.get('app_id')
        token = data.get('token')
        base_url = data.get('base_url')
        
        if not all([app_id, token, base_url]):
            return jsonify({"error": "Missing required parameters"}), 400
        
        path = f"comGpsGate/api/v.1/applications/{app_id}/eventrules"
        api_response = fetch_from_gpsgate_api(base_url, token, path)
        
        if not api_response or 'data' not in api_response:
            return jsonify({"error": "Failed to fetch event rules"}), 500
        
        # Format response
        event_rules = []
        for rule in api_response['data']:
            event_rules.append({
                'id': rule.get('id'),
                'name': rule.get('name'),
                'description': rule.get('description')
            })
        
        print(f"Fetched {len(event_rules)} event rules")
        return jsonify({'event_rules': event_rules}), 200
    
    except Exception as e:
        print(f"Error in /event-rules: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@pipeline_bp.route('/weekly-schedule', methods=['POST'])
def get_weekly_schedule():
    """Generate weekly schedule from start date to today"""
    try:
        data = request.form or request.get_json() or {}
        start_date_str = data.get('start_date', '2025-01-01')

        weeks = build_weekly_schedule(start_date_str)

        print(f"Generated {len(weeks)} weeks")
        return jsonify({'weeks': weeks}), 200

    except Exception as e:
        print(f"Error in /weekly-schedule: {str(e)}")
        return jsonify({"error": str(e)}), 500


# ============================================================================
# GENERIC EVENT DATA HANDLER
# ============================================================================

def process_event_data(event_name, response_key):
    """
    Generic handler for all event data endpoints
    Orchestrates: render → result → download → clean for any event
    
    Args:
        event_name: Human-readable name (e.g., "Working Hours Usage")
        response_key: JSON response key (e.g., "wh_events")
    """
    try:
        start_time = pytime.time()
        data = request.form or request.get_json() or {}
        
        # DEBUG: Print what was received
        print(f"DEBUG: Received data = {data}")
        print(f"DEBUG: request.form = {request.form}")
        print(f"DEBUG: request.get_json() = {request.get_json()}")
        
        app_id = data.get('app_id')
        token = data.get('token')
        base_url = data.get('base_url')
        report_id = data.get('report_id')
        # event_id = data.get('event_id')
        event_id = data.get('event_id')
        is_trip = (event_name == "Trip")
        tag_id = data.get('tag_id')
        
        # if not all([app_id, token, base_url, report_id, event_id, tag_id]):
        #     return jsonify({"error": "Missing required parameters"}), 400
        if is_trip:
            if not all([app_id, token, base_url, report_id, tag_id]):
                return jsonify({"error": "Missing required parameters for Trip"}), 400
        else:
            if not all([app_id, token, base_url, report_id, event_id, tag_id]):
                return jsonify({"error": "Missing required parameters"}), 400
        
        # # Get weekly schedule
        # schedule_response = get_weekly_schedule()
        # if schedule_response[1] != 200:
        #     return schedule_response
        
        # # weeks = schedule_response[0].json['weeks']
        # # print(f"Processing {len(weeks)} weeks for {event_name}...")
        # weeks = schedule_response[0].json['weeks'][:MAX_WEEKS_TO_PROCESS]
        # print(f"Processing {len(weeks)} weeks for {event_name} (capped)...")
        
        # all_dataframes = []

        # Build weekly schedule safely (NO Flask route call)
        # Use different limits for heavy endpoints vs others
        if event_name in ["Trip", "WH"]:
            max_weeks = MAX_WEEKS_TRIP_WH
        else:
            max_weeks = MAX_WEEKS_OTHER
        
        weeks = build_weekly_schedule()[:max_weeks]
        print(f"Processing {len(weeks)} weeks for {event_name} (capped at {max_weeks} weeks)...")

        all_dataframes = []


        
        # Process each week
        for i, week in enumerate(weeks):
            elapsed = pytime.time() - start_time
            if elapsed > MAX_EXECUTION_SECONDS:
                print(f"⏹ Stopping early after {elapsed:.1f}s to avoid Power BI timeout")
                break
            print(f"\nProcessing week {i+1}/{len(weeks)}: {week['start_date']} to {week['end_date']}")
            
            try:
                # Render
                # render_payload = {
                #     'app_id': app_id,
                #     'period_start': week['week_start'],
                #     'period_end': week['week_end'],
                #     'tag_id': tag_id,
                #     'token': token,
                #     'base_url': base_url,
                #     'report_id': report_id,
                #     'event_id': event_id
                # }
                render_payload = {
                    'app_id': app_id,
                    'period_start': week['week_start'],
                    'period_end': week['week_end'],
                    'tag_id': tag_id,
                    'token': token,
                    'base_url': base_url,
                    'report_id': report_id
                }
                if not is_trip:
                    render_payload['event_id'] = event_id
                
                render_resp = requests.post(
                    'https://powerbixgpsgatexgdriver.onrender.com/render',
                    data=render_payload,
                    timeout=20
                )
                
                if render_resp.status_code != 200:
                    print(f"  Warning: Render failed for week {i+1} ({render_resp.status_code})")
                    continue
                
                render_id = render_resp.json().get('render_id')
                if not render_id:
                    print(f"  Warning: No render_id for week {i+1}")
                    continue
                
                # Result
                result_payload = {
                    'app_id': app_id,
                    'render_id': render_id,
                    'token': token,
                    'base_url': base_url,
                    'report_id': report_id
                }
                
                result_resp = requests.post(
                    'https://powerbixgpsgatexgdriver.onrender.com/result',
                    data=result_payload,
                    timeout=40
                )
                
                if result_resp.status_code != 200:
                    print(f"  Warning: Result failed for week {i+1}")
                    continue
                
                gdrive_link = result_resp.json().get('gdrive_link')
                if not gdrive_link:
                    print(f"  Warning: No gdrive_link for week {i+1}")
                    continue
                
                # Download CSV
                csv_content = download_csv_from_gdrive(gdrive_link, token)
                if not csv_content:
                    print(f"  Warning: Failed to download CSV for week {i+1}")
                    continue
                
                # Clean and parse
                df = clean_csv_data(csv_content)
                if df is None or len(df) == 0:
                    print(f"  Warning: No data after cleaning for week {i+1}")
                    continue
                
                all_dataframes.append(df)
                print(f"  ✓ Week {i+1}: {len(df)} rows")
            
            except Exception as week_error:
                print(f"  Error week {i+1}: {str(week_error)}")
                continue
        
        # Combine
        if not all_dataframes:
            print(f"No data found for {event_name}")
            return jsonify({
                "message": "No data found",
                response_key: [],
                "total_rows": 0
            }), 200
        
        try:
            print(f"Combining {len(all_dataframes)} dataframes...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"Combined shape: {combined_df.shape} ({combined_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f}MB)")
            
            print("Replacing NaN values...")
            combined_df = combined_df.replace({np.nan: None, pd.NaT: None})
            print(f"After NaN replacement: {combined_df.memory_usage(deep=True).sum() / 1024 / 1024:.1f}MB")
            
            print("Converting to records...")
            # Convert to records with minimal memory usage
            events = combined_df.to_dict('records')
            print(f"Converted to {len(events)} records")
            
            # Clear dataframe to free memory immediately
            del combined_df
            del all_dataframes
            print("Memory freed")
            
            print(f"✓ {event_name}: {len(events)} total events")
            
            return jsonify({
                "message": "Success",
                response_key: events,
                "total_rows": len(events),
                "weeks_processed": len(events)
            }), 200
        
        except Exception as combine_error:
            print(f"Error combining data: {str(combine_error)}")
            import traceback
            print(traceback.format_exc())
            return jsonify({
                "message": f"Error combining data: {str(combine_error)}",
                response_key: [],
                "total_rows": 0
            }), 200
    
    except Exception as e:
        print(f"Critical error in {event_name}: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return jsonify({
            "message": f"Backend error: {str(e)}",
            response_key: [],
            "total_rows": 0
        }), 200 


# ============================================================================
# EVENT DATA ENDPOINTS
# ============================================================================

@pipeline_bp.route('/speeding-data', methods=['POST'])
def get_speeding_data():
    """Speeding events"""
    return process_event_data("Speeding", "speed_events")

@pipeline_bp.route('/idle-data', methods=['POST'])
def get_idle_data():
    """Idle events"""
    return process_event_data("Idle", "idle_events")

@pipeline_bp.route('/trip-data', methods=['POST'])
def get_trip_data():
    """Trip events"""
    return process_event_data("Trip", "trip_events")

@pipeline_bp.route('/awh-data', methods=['POST'])
def get_awh_data():
    """After Working Hours Usage events"""
    return process_event_data("AWH", "awh_events")

@pipeline_bp.route('/wh-data', methods=['POST'])
def get_wh_data():
    """Working Hours Usage events"""
    return process_event_data("WH", "wh_events")

@pipeline_bp.route('/ha-data', methods=['POST'])
def get_ha_data():
    """Harsh Acceleration events"""
    return process_event_data("HA", "ha_events")

@pipeline_bp.route('/hb-data', methods=['POST'])
def get_hb_data():
    """Harsh Braking events"""
    return process_event_data("HB", "hb_events")

@pipeline_bp.route('/wu-data', methods=['POST'])
def get_wu_data():
    """Weekend Usage events"""
    return process_event_data("WU", "wu_events")