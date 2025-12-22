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

MAX_EXECUTION_SECONDS = 140
MAX_WEEKS_TRIP_WH = 2
MAX_WEEKS_OTHER = 20

RENDER_URL = "https://powerbixgpsgatexgdriver.onrender.com/render"
RESULT_URL = "https://powerbixgpsgatexgdriver.onrender.com/result"
API_PROXY_URL = "https://powerbixgpsgatexgdriver.onrender.com/api"

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def fetch_from_gpsgate_api(base_url, token, path):
    form_body = {
        "method": "GET",
        "token": token,
        "base_url": base_url,
        "path": path
    }

    try:
        resp = requests.post(API_PROXY_URL, data=form_body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Error fetching from GpsGate API: {e}")
        return None


def download_csv_from_gdrive(gdrive_link, auth_token=None):
    try:
        headers = {}
        if "omantracking2.com" in gdrive_link:
            headers["Authorization"] = auth_token

        resp = requests.get(gdrive_link, headers=headers, timeout=60)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        print(f"Error downloading CSV: {e}")
        return None


def clean_csv_data(csv_content):
    try:
        df = pd.read_csv(io.BytesIO(csv_content), skiprows=8)
        df = df.where(pd.notna(df), None)
        df = df.replace({np.nan: None, pd.NaT: None})

        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S").where(pd.notna(df[col]), None)

        return df
    except Exception as e:
        print(f"CSV clean error: {e}")
        return None


def build_weekly_schedule(start_date_str="2025-01-01"):
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    today = datetime.utcnow().date()

    weeks = []
    current = start_date

    while current + timedelta(days=6) <= today:
        end = current + timedelta(days=6)
        weeks.append({
            "week_start": f"{current}T00:00:00Z",
            "week_end": f"{end}T23:59:59Z",
            "start_date": current.isoformat(),
            "end_date": end.isoformat()
        })
        current += timedelta(days=7)

    return weeks


# ============================================================================
# GENERIC EVENT HANDLER
# ============================================================================

def process_event_data(event_name, response_key):
    try:
        start_time = pytime.time()

        # JSON first, then form
        data = request.get_json(silent=True) or request.form or {}

        app_id = data.get("app_id")
        token = data.get("token")
        base_url = data.get("base_url")
        report_id = data.get("report_id")
        tag_id = data.get("tag_id")
        event_id = data.get("event_id")

        is_trip = (event_name == "Trip")

        # ---------------- VALIDATION ----------------
        if is_trip:
            if not all([app_id, token, base_url, report_id, tag_id]):
                return jsonify({"error": "Missing required parameters for Trip"}), 400
        else:
            if not all([app_id, token, base_url, report_id, tag_id, event_id]):
                return jsonify({"error": "Missing required parameters"}), 400

        max_weeks = MAX_WEEKS_TRIP_WH if event_name in ["Trip", "WH"] else MAX_WEEKS_OTHER
        weeks = build_weekly_schedule()[:max_weeks]

        all_dataframes = []

        # ---------------- WEEK LOOP ----------------
        for week in weeks:
            if pytime.time() - start_time > MAX_EXECUTION_SECONDS:
                break

            try:
                render_payload = {
                    "app_id": app_id,
                    "period_start": week["week_start"],
                    "period_end": week["week_end"],
                    "tag_id": tag_id,
                    "token": token,
                    "base_url": base_url,
                    "report_id": report_id
                }

                # 🔒 CRITICAL FIX: Trip MUST NOT send event_id
                if not is_trip:
                    render_payload["event_id"] = event_id

                render_resp = requests.post(RENDER_URL, data=render_payload, timeout=20)
                if render_resp.status_code != 200:
                    continue

                render_id = render_resp.json().get("render_id")
                if not render_id:
                    continue

                result_payload = {
                    "app_id": app_id,
                    "render_id": render_id,
                    "token": token,
                    "base_url": base_url,
                    "report_id": report_id
                }

                result_resp = requests.post(RESULT_URL, data=result_payload, timeout=40)
                if result_resp.status_code != 200:
                    continue

                gdrive_link = result_resp.json().get("gdrive_link")
                if not gdrive_link:
                    continue

                csv_content = download_csv_from_gdrive(gdrive_link, token)
                if not csv_content:
                    continue

                df = clean_csv_data(csv_content)
                if df is not None and not df.empty:
                    all_dataframes.append(df)

            except Exception as e:
                print(f"Week processing error: {e}")
                continue

        if not all_dataframes:
            return jsonify({
                "message": "No data found",
                response_key: [],
                "total_rows": 0
            }), 200

        combined_df = pd.concat(all_dataframes, ignore_index=True)
        events = combined_df.to_dict("records")

        return jsonify({
            "message": "Success",
            response_key: events,
            "total_rows": len(events)
        }), 200

    except Exception as e:
        return jsonify({
            "message": f"Backend error: {e}",
            response_key: [],
            "total_rows": 0
        }), 200


# ============================================================================
# ROUTES
# ============================================================================

@pipeline_bp.route("/reports", methods=["POST"])
def get_reports():
    data = request.get_json(silent=True) or request.form or {}
    app_id = data.get("app_id")
    token = data.get("token")
    base_url = data.get("base_url")

    if not all([app_id, token, base_url]):
        return jsonify({"error": "Missing required parameters"}), 400

    path = f"comGpsGate/api/v.1/applications/{app_id}/reports"
    resp = fetch_from_gpsgate_api(base_url, token, path)

    if not resp or "data" not in resp:
        return jsonify({"error": "Failed to fetch reports"}), 500

    return jsonify({
        "reports": [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "description": r.get("description")
            } for r in resp["data"]
        ]
    }), 200


@pipeline_bp.route("/event-rules", methods=["POST"])
def get_event_rules():
    data = request.get_json(silent=True) or request.form or {}
    app_id = data.get("app_id")
    token = data.get("token")
    base_url = data.get("base_url")

    if not all([app_id, token, base_url]):
        return jsonify({"error": "Missing required parameters"}), 400

    path = f"comGpsGate/api/v.1/applications/{app_id}/eventrules"
    resp = fetch_from_gpsgate_api(base_url, token, path)

    if not resp or "data" not in resp:
        return jsonify({"error": "Failed to fetch event rules"}), 500

    return jsonify({
        "event_rules": [
            {
                "id": r.get("id"),
                "name": r.get("name"),
                "description": r.get("description")
            } for r in resp["data"]
        ]
    }), 200


@pipeline_bp.route("/weekly-schedule", methods=["POST"])
def get_weekly_schedule_route():
    data = request.get_json(silent=True) or request.form or {}
    start_date = data.get("start_date", "2025-01-01")
    return jsonify({"weeks": build_weekly_schedule(start_date)}), 200


# ============================================================================
# EVENT ENDPOINTS
# ============================================================================

@pipeline_bp.route("/speeding-data", methods=["POST"])
def speeding():
    return process_event_data("Speeding", "speed_events")

@pipeline_bp.route("/idle-data", methods=["POST"])
def idle():
    return process_event_data("Idle", "idle_events")

@pipeline_bp.route("/trip-data", methods=["POST"])
def trip():
    return process_event_data("Trip", "trip_events")

@pipeline_bp.route("/awh-data", methods=["POST"])
def awh():
    return process_event_data("AWH", "awh_events")

@pipeline_bp.route("/wh-data", methods=["POST"])
def wh():
    return process_event_data("WH", "wh_events")

@pipeline_bp.route("/ha-data", methods=["POST"])
def ha():
    return process_event_data("HA", "ha_events")

@pipeline_bp.route("/hb-data", methods=["POST"])
def hb():
    return process_event_data("HB", "hb_events")

@pipeline_bp.route("/wu-data", methods=["POST"])
def wu():
    return process_event_data("WU", "wu_events")
