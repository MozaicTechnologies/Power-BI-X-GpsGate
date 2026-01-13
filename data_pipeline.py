"""
Data Pipeline for Fleet Dashboard
Handles: reports, event rules, weekly rendering, CSV download & cleaning
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import io
from urllib.parse import urljoin
from models import db
from db_storage import store_event_data_to_db
import json
import numpy as np
import time as pytime
import os

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

# Calculate max weeks from 2025-01-01 to today
def get_max_weeks():
    """Calculate number of weeks from 2025-01-01 to today"""
    start = datetime(2025, 1, 1).date()
    today = datetime.utcnow().date()
    weeks = (today - start).days // 7 + 1
    return max(1, weeks)

MAX_WEEKS_TRIP_WH = 1  # Trip & WH: 1 week per call (incremental weekly pulls)
MAX_WEEKS_OTHER = 1    # Other events: 1 week per call (incremental weekly pulls)

# ============================================================================
# RESILIENT SESSION WITH RETRY LOGIC
# ============================================================================

def create_resilient_session():
    """Create requests session with automatic retries + backoff"""
    session = requests.Session()
    
    retry_strategy = Retry(
        total=3,                                    # Max 3 retries
        backoff_factor=2,                          # Wait 2s, 4s, 8s between retries
        status_forcelist=[429, 500, 502, 503, 504] # Retry on these HTTP codes
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

# Global session instance
RESILIENT_SESSION = create_resilient_session()

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
        # Use resilient session with timeout (connect=10s, read=60s)
        resp = RESILIENT_SESSION.post(
            API_PROXY_URL, 
            data=form_body, 
            timeout=(10, 60)
        )
        resp.raise_for_status()
        
        # Rate limit: small delay between requests
        pytime.sleep(0.3)
        
        return resp.json()
    except requests.exceptions.Timeout:
        print(f"Timeout fetching from GpsGate API after 3 retries")
        return None
    except Exception as e:
        print(f"Error fetching from GpsGate API: {e}")
        return None


def clean_csv_data(csv_content):
    try:
        df = pd.read_csv(io.BytesIO(csv_content) if isinstance(csv_content, bytes) else io.StringIO(csv_content), skiprows=8)
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

        # Print request body for Trip event
        if event_name == "Trip":
            print(f"\n{'='*70}")
            print(f"[TRIP] PROCESS_EVENT_DATA - TRIP REQUEST BODY")
            print(f"{'='*70}")
            print(f"Data: {json.dumps(data, indent=2)}")
            print(f"{'='*70}\n")

        app_id = data.get("app_id")
        token = data.get("token")
        base_url = data.get("base_url")
        report_id = data.get("report_id")
        tag_id = data.get("tag_id")
        event_id = data.get("event_id")

        is_trip = (event_name == "Trip")

        # Print validation info for Trip
        if event_name == "Trip":
            print(f"[OK] app_id: {app_id}")
            print(f"[OK] token: {'***' if token else 'MISSING'}")
            print(f"[OK] base_url: {base_url}")
            print(f"[OK] report_id: {report_id}")
            print(f"[OK] tag_id: {tag_id}\n")

        # ---------------- VALIDATION ----------------
        if is_trip:
            if not all([app_id, token, base_url, report_id, tag_id]):
                print(f"[ERROR] VALIDATION FAILED for Trip")
                return jsonify({"error": "Missing required parameters for Trip"}), 400
        else:
            if not all([app_id, token, base_url, report_id, tag_id, event_id]):
                print(f"[ERROR] VALIDATION FAILED for {event_name}")
                return jsonify({"error": "Missing required parameters"}), 400

        max_weeks = MAX_WEEKS_TRIP_WH if event_name in ["Trip", "WH"] else MAX_WEEKS_OTHER
        weeks = build_weekly_schedule()[:max_weeks]

        if event_name == "Trip":
            print(f"[OK] MAX_WEEKS: {max_weeks}")
            print(f"[OK] Total weeks available: {len(build_weekly_schedule())}")
            print(f"[OK] Processing {len(weeks)} weeks\n")

        all_dataframes = []

        # ---------------- WEEK LOOP ----------------
        weeks_processed = 0
        total_db_stats = {"inserted": 0, "skipped": 0, "failed": 0}
        total_internal_dupes = 0  # Track internal duplicates (CSV level)
        total_rows_raw = 0  # Track raw rows before dedup
        
        for i, week in enumerate(weeks):
            if pytime.time() - start_time > MAX_EXECUTION_SECONDS:
                print(f"[TIMEOUT] Exceeded {MAX_EXECUTION_SECONDS}s")
                break

            if event_name == "Trip":
                print(f"  Week {i+1}/{len(weeks)}: {week['start_date']} -> {week['end_date']}")

            try:
                # Build GpsGate render request parameters
                period_start = week["week_start"].replace("T", " ").replace("Z", "").split(".")[0] if "T" in week["week_start"] else week["week_start"]
                period_end = week["week_end"].replace("T", " ").replace("Z", "").split(".")[0] if "T" in week["week_end"] else week["week_end"]
                
                # Build GpsGate rendering URL and parameters
                url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"
                
                parameters = [
                    {
                        "parameterName": "Period",
                        "periodStart": period_start,
                        "periodEnd": period_end,
                        "value": "Custom",
                        "visible": False
                    },
                    {
                        "parameterName": "Tag" if event_id else "TagID",
                        "arrayValues": [tag_id]
                    }
                ]
                
                if event_id:
                    parameters.append({
                        "parameterName": "EventRule",
                        "arrayValues": [event_id]
                    })
                
                render_body = {
                    "parameters": parameters,
                    "reportFormatId": 2,
                    "reportId": int(report_id),
                    "sendEmail": False
                }
                
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": token
                }
                
                # Make render request to GpsGate
                render_resp = RESILIENT_SESSION.post(
                    url,
                    json=render_body,
                    headers=headers,
                    timeout=(10, 30)
                )
                
                if event_name == "Trip":
                    import sys
                    print(f"[DEBUG] Trip Week {i+1}: render_resp.status_code={render_resp.status_code}", file=sys.stderr)
                    print(f"[DEBUG] Trip Week {i+1}: render URL={url}", file=sys.stderr)
                    print(f"[DEBUG] Trip Week {i+1}: render body={render_body}", file=sys.stderr)
                    if render_resp.status_code != 200:
                        print(f"[DEBUG] Trip Week {i+1}: Render response={render_resp.text[:500]}", file=sys.stderr)
                
                if render_resp.status_code != 200:
                    if event_name == "Trip":
                        import sys; print(f"[DEBUG] Trip Week {i+1}: Render failed, skipping. Response: {render_resp.text[:200]}", file=sys.stderr)
                    continue

                render_data = render_resp.json()
                render_id = render_data.get("id") or render_data.get("render_id")
                if not render_id:
                    if event_name == "Trip":
                        import sys; print(f"[DEBUG] Trip Week {i+1}: No render_id, skipping", file=sys.stderr)
                    continue

                # Get result/download link from GpsGate
                result_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}/result"
                result_headers = {
                    "Authorization": token
                }
                
                result_resp = RESILIENT_SESSION.get(
                    result_url,
                    headers=result_headers,
                    timeout=(10, 120)
                )
                
                if event_name == "Trip":
                    import sys; print(f"[DEBUG] Trip Week {i+1}: result_resp.status_code={result_resp.status_code}", file=sys.stderr)
                
                if result_resp.status_code != 200:
                    if event_name == "Trip":
                        import sys; print(f"[DEBUG] Trip Week {i+1}: Result failed, skipping", file=sys.stderr)
                    continue

                result_data = result_resp.json()
                gdrive_link = result_data.get("gdrive_link") or result_data.get("link")
                if not gdrive_link:
                    if event_name == "Trip":
                        import sys; print(f"[DEBUG] Trip Week {i+1}: No gdrive_link, skipping", file=sys.stderr)
                    continue

                csv_headers = {}
                if "omantracking2.com" in gdrive_link:
                    csv_headers["Authorization"] = token
                
                csv_resp = RESILIENT_SESSION.get(
                    gdrive_link,
                    headers=csv_headers,
                    timeout=(10, 120)
                )
                
                if csv_resp.status_code != 200:
                    if event_name == "Trip":
                        import sys; print(f"[DEBUG] Trip Week {i+1}: CSV download failed, skipping", file=sys.stderr)
                    continue
                
                csv_content = csv_resp.content
                if not csv_content:
                    import sys; print(f"[{event_name}] Week {i+1}: No CSV content", file=sys.stderr)
                    if event_name == "Trip":
                        print(f"[DEBUG] Trip Week {i+1}: No CSV content from gdrive", file=sys.stderr)
                    continue

                df = clean_csv_data(csv_content)
                if df is not None and not df.empty:
                    df_original = len(df)
                    total_rows_raw += df_original
                    import sys; print(f"[SPEEDING] Week {i+1}: {df_original} rows fetched (raw)", file=sys.stderr)
                    
                    # DEDUPLICATE based on ALL columns (complete row duplicate)
                    # This removes rows where every column value is identical
                    df = df.drop_duplicates(keep='first')
                    df_after_dedup = len(df)
                    internal_dupes = df_original - df_after_dedup
                    total_internal_dupes += internal_dupes
                    if internal_dupes > 0:
                        print(f"[SPEEDING] Week {i+1}: {df_original} -> {df_after_dedup} after removing {internal_dupes} complete row duplicates (all columns identical)", file=sys.stderr)
                    else:
                        print(f"[SPEEDING] Week {i+1}: {df_after_dedup} rows (clean, no duplicates)", file=sys.stderr)
                    
                    all_dataframes.append(df)
                    
                    # ✓ Store data to database with incremental logic
                    db_stats = store_event_data_to_db(df, app_id, tag_id, event_name)
                    import sys; print(f"[SPEEDING] Week {i+1}: DB insert stats - Inserted: {db_stats.get('inserted')}, DB-level duplicates: {db_stats.get('skipped')}, Errors: {db_stats.get('failed')}", file=sys.stderr)
                    total_db_stats["inserted"] += db_stats.get("inserted", 0)
                    total_db_stats["skipped"] += db_stats.get("skipped", 0)
                    total_db_stats["failed"] += db_stats.get("failed", 0)
                    
                    # Week successfully processed
                    weeks_processed += 1

            except Exception as e:
                # Log exception for Trip
                if event_name == "Trip":
                    import sys; import traceback
                    print(f"[DEBUG ERROR] Trip Week {i+1}: Exception: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
                    print(f"[DEBUG] Traceback: {traceback.format_exc()[:500]}", file=sys.stderr)
                # Silently skip week errors - data already may have been partially processed
                continue

        # ============================================================================
        # COMPLETE ROW ACCOUNTING REPORT
        # ============================================================================
        rows_after_dedup = sum(len(df) for df in all_dataframes)
        db_inserted = total_db_stats.get("inserted", 0)
        db_duplicates = total_db_stats.get("skipped", 0)
        db_failed = total_db_stats.get("failed", 0)
        
        import sys
        print(f"\n{'='*80}", file=sys.stderr)
        print(f"COMPLETE ROW ACCOUNTING - {event_name}", file=sys.stderr)
        print(f"{'='*80}", file=sys.stderr)
        print(f"Total Raw Rows Fetched from API:          {total_rows_raw:>10,}", file=sys.stderr)
        print(f"  - Internal Duplicates Removed (CSV):   -{total_internal_dupes:>10,}", file=sys.stderr)
        print(f"  = Rows After Deduplication:             {rows_after_dedup:>10,}", file=sys.stderr)
        print(f"  - Database-Level Duplicates (flagged):  -{db_duplicates:>10,}", file=sys.stderr)
        print(f"  - Failed DB Inserts:                    -{db_failed:>10,}", file=sys.stderr)
        print(f"  = Total Inserted to DB:                 {db_inserted:>10,}", file=sys.stderr)
        print(f"{'='*80}", file=sys.stderr)
        print(f"Weeks Processed: {weeks_processed} | Time: {pytime.time() - start_time:.1f}s", file=sys.stderr)
        print(f"{'='*80}\n", file=sys.stderr)

        if not all_dataframes:
            return jsonify({
                "message": "No data found",
                response_key: [],
                "total_rows": 0,
                "weeks_processed": weeks_processed,
                "db_stats": total_db_stats,
                "accounting": {
                    "raw_fetched": total_rows_raw,
                    "internal_dupes_removed": total_internal_dupes,
                    "rows_after_dedup": rows_after_dedup,
                    "db_duplicates_flagged": db_duplicates,
                    "db_failed": db_failed,
                    "total_inserted": db_inserted
                }
            }), 200

        combined_df = pd.concat(all_dataframes, ignore_index=True)
        events = combined_df.to_dict("records")

        return jsonify({
            "message": "Success",
            response_key: events,
            "total_rows": len(events),
            "weeks_processed": weeks_processed,
            "db_stats": total_db_stats,
            "accounting": {
                "raw_fetched": total_rows_raw,
                "internal_dupes_removed": total_internal_dupes,
                "rows_after_dedup": rows_after_dedup,
                "db_duplicates_flagged": db_duplicates,
                "db_failed": db_failed,
                "total_inserted": db_inserted
            }
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
    # Print request body for debugging
    request_body = request.get_json(silent=True) or request.form or {}
    print(f"\n{'='*70}")
    print(f"[TRIP-DATA] TRIP-DATA ENDPOINT REQUEST")
    print(f"{'='*70}")
    print(f"Request Body: {json.dumps(request_body, indent=2)}")
    print(f"{'='*70}\n")
    
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
