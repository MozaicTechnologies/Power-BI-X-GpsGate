"""
Data Pipeline for Fleet Dashboard
Handles: reports, event rules, weekly rendering, CSV download & cleaning
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, time
import requests
import pandas as pd
import io
import json
import numpy as np
import time as pytime
import traceback
from models import db

# Import all models needed for insertion
from models import (
    FactIdle, FactSpeeding, FactAWH,
    FactHA, FactHB, FactWH, FactWU, FactTrip
)

pipeline_bp = Blueprint('pipeline_bp', __name__)

# --- Configuration ---
MAX_EXECUTION_SECONDS = 140
MAX_WEEKS_TRIP_WH = 999
MAX_WEEKS_OTHER = 999

# Centralized Base URL for your Microservices
BASE_SERVICE_URL = "https://fleetdashboard-ali-hxel.onrender.com"
RENDER_URL = f"{BASE_SERVICE_URL}/render"
RESULT_URL = f"{BASE_SERVICE_URL}/result"
API_PROXY_URL = f"{BASE_SERVICE_URL}/api"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fetch_from_gpsgate_api(base_url, token, path):
    form_body = {
        'method': 'GET',
        'token': token,
        'base_url': base_url,
        'path': path
    }
    try:
        response = requests.post(API_PROXY_URL, data=form_body, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching from GpsGate API: {e}")
        return None

def download_csv_from_gdrive(gdrive_link, auth_token=None):
    try:
        headers = {}
        if 'omantracking2.com' in gdrive_link:
            headers['Authorization'] = auth_token
        
        response = requests.get(gdrive_link, headers=headers, timeout=60)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return None

def clean_csv_data(csv_content):
    try:
        # GpsGate reports usually have 8 rows of header info before columns
        df = pd.read_csv(io.BytesIO(csv_content), skiprows=8)
        
        # Replace NaN/NaT with None for JSON/DB compatibility
        df = df.where(pd.notna(df), None)
        df = df.replace({np.nan: None, pd.NaT: None})
        
        # Format datetime columns if they exist
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S').where(pd.notna(df[col]), None)
        
        return df
    except Exception as e:
        print(f"CSV clean error: {e}")
        return None

def build_weekly_schedule(start_date_str="2025-01-01"):
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    except:
        start_date = datetime(2025, 1, 1).date()
        
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
# GENERIC EVENT HANDLER (The Core Logic)
# ============================================================================

def process_event_data(event_name, response_key):
    try:
        data = request.get_json(silent=True) or request.form or {}
        
        app_id = data.get('app_id')
        token = data.get('token')
        base_url = data.get('base_url')
        report_id = data.get('report_id')
        event_id = data.get('event_id')
        tag_id = data.get('tag_id')
        custom_start = data.get('start_date', '2025-01-01') 

        is_trip = (event_name == "Trip")
        # Build the schedule based on 2025
        weeks = build_weekly_schedule(custom_start)[:MAX_WEEKS_OTHER]
        
        MODEL_MAP = {
            "Idle": FactIdle, "Speeding": FactSpeeding, "AWH": FactAWH,
            "HA": FactHA, "HB": FactHB, "WH": FactWH, "WU": FactWU, "Trip": FactTrip
        }
        Model = MODEL_MAP.get(event_name)
        total_inserted = 0 

        for i, week in enumerate(weeks):
            print(f"⏳ Processing {event_name} - Week {i+1}: {week['start_date']}")
            
            # --- STEP 1: TRIGGER THE REPORT (RENDER) ---
            render_payload = {
                "app_id": app_id, "token": token, "base_url": base_url,
                "report_id": report_id, "tag_id": tag_id,
                "period_start": week['week_start'], "period_end": week['week_end']
            }
            # If it's an event (like Speeding), we need the event_id
            if event_id: render_payload["event_id"] = event_id

            render_r = requests.post(RENDER_URL, json=render_payload, timeout=60)
            if render_r.status_code != 200: continue
            
            # --- STEP 2: GET THE CSV LINK (RESULT) ---
            pytime.sleep(2) # Give GpsGate a moment to generate
            result_r = requests.get(f"{RESULT_URL}/{app_id}/{token}/{base_url.replace('https://','')}", timeout=60)
            if result_r.status_code != 200: continue
            
            csv_url = result_r.json().get('csv_url')
            if not csv_url: continue

            # --- STEP 3: DOWNLOAD AND CLEAN ---
            csv_content = download_csv_from_gdrive(csv_url, token)
            if not csv_content: continue
            
            df = clean_csv_data(csv_content)

            # --- STEP 4: INSERT DATA ---
            if df is not None and not df.empty:
                records = df.to_dict('records')
                week_inserted = 0
                
                for r in records:
                    raw_date = r.get("Start Date") or r.get("Date")
                    v_name = r.get("Vehicle")
                    
                    # Skip empty/summary rows
                    if not raw_date or not v_name or str(raw_date) == 'nan':
                        continue

                    # Clean the Date
                    try:
                        clean_date = datetime.strptime(str(raw_date), '%m/%d/%Y').date()
                    except:
                        try:
                            clean_date = datetime.strptime(str(raw_date), '%d/%m/%Y').date()
                        except:
                            continue

                    # Duplicate Check
                    s_time = r.get("Start Time") or r.get("Time")
                    exists = db.session.query(Model.id).filter(
                        Model.app_id == app_id,
                        Model.event_date == clean_date,
                        Model.start_time == s_time,
                        Model.vehicle == v_name
                    ).first()

                    if not exists:
                        obj = Model(
                            app_id=app_id,
                            tag_id=tag_id,
                            vehicle=v_name,
                            event_date=clean_date,
                            start_time=s_time,
                            duration=r.get("Duration"),
                            driver=r.get("Driver Name"),
                            location=r.get("Start Address") or r.get("Address"),
                            event_state=r.get("Event State")
                        )
                        if is_trip:
                            obj.stop_time = r.get("End Time")
                            obj.distance_gps = r.get("Distance (GPS)")

                        db.session.add(obj)
                        week_inserted += 1

                db.session.commit()
                total_inserted += week_inserted
                print(f"✅ Week {i+1} Done. Rows: {week_inserted}")

        return jsonify({"message": "Success", "rows_inserted": total_inserted}), 200

    except Exception as e:
        db.session.rollback()
        print(f"❌ ERROR: {traceback.format_exc()}")
        return jsonify({"status": "error", "message": str(e)}), 500

# ============================================================================
# ROUTES
# ============================================================================

@pipeline_bp.route('/reports', methods=['POST'])
def get_reports():
    data = request.get_json(silent=True) or request.form or {}
    app_id, token, base_url = data.get('app_id'), data.get('token'), data.get('base_url')
    
    if not all([app_id, token, base_url]):
        return jsonify({"error": "Missing parameters"}), 400
    
    path = f"comGpsGate/api/v.1/applications/{app_id}/reports"
    api_resp = fetch_from_gpsgate_api(base_url, token, path)
    
    if not api_resp or 'data' not in api_resp:
        return jsonify({"error": "Failed to fetch reports"}), 500
        
    return jsonify({'reports': api_resp['data']}), 200

@pipeline_bp.route('/event-rules', methods=['POST'])
def get_event_rules():
    data = request.get_json(silent=True) or request.form or {}
    app_id, token, base_url = data.get('app_id'), data.get('token'), data.get('base_url')
    
    if not all([app_id, token, base_url]):
        return jsonify({"error": "Missing parameters"}), 400
    
    path = f"comGpsGate/api/v.1/applications/{app_id}/eventrules"
    api_resp = fetch_from_gpsgate_api(base_url, token, path)
    
    if not api_resp or 'data' not in api_resp:
        return jsonify({"error": "Failed to fetch event rules"}), 500
        
    return jsonify({'event_rules': api_resp['data']}), 200

@pipeline_bp.route('/weekly-schedule', methods=['POST'])
def get_weekly_schedule():
    data = request.get_json(silent=True) or request.form or {}
    start_date = data.get('start_date', '2025-01-01')
    weeks = build_weekly_schedule(start_date)
    return jsonify({'weeks': weeks}), 200

# --- Event Specific Data Routes ---

@pipeline_bp.route('/speeding-data', methods=['POST'])
def get_speeding_data():
    return process_event_data("Speeding", "speed_events")

@pipeline_bp.route('/idle-data', methods=['POST'])
def get_idle_data():
    return process_event_data("Idle", "idle_events")

@pipeline_bp.route('/trip-data', methods=['POST'])
def get_trip_data():
    return process_event_data("Trip", "trip_events")

@pipeline_bp.route('/awh-data', methods=['POST'])
def get_awh_data():
    return process_event_data("AWH", "awh_events")

@pipeline_bp.route('/wh-data', methods=['POST'])
def get_wh_data():
    return process_event_data("WH", "wh_events")

@pipeline_bp.route('/ha-data', methods=['POST'])
def get_ha_data():
    return process_event_data("HA", "ha_events")

@pipeline_bp.route('/hb-data', methods=['POST'])
def get_hb_data():
    return process_event_data("HB", "hb_events")

@pipeline_bp.route('/wu-data', methods=['POST'])
def get_wu_data():
    return process_event_data("WU", "wu_events")