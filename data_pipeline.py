"""
Data Pipeline for Fleet Dashboard (STANDARD VERSION)
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

# Timeout for processing - needs to be long for backfill with multiple weeks
# Each week can take 10-20 seconds, and we process multiple weeks per event type
MAX_EXECUTION_SECONDS = 600  # 10 minutes per event type

# Calculate max weeks from 2025-01-01 to today
def get_max_weeks():
    """Calculate number of weeks from 2025-01-01 to today"""
    start = datetime(2025, 1, 1).date()
    today = datetime.utcnow().date()
    weeks = (today - start).days // 7 + 1
    return max(1, weeks)

MAX_WEEKS_TRIP_WH = 1  # Trip & WH: 1 week per call (incremental weekly pulls)
MAX_WEEKS_OTHER = 1    # Other events: 1 week per call (incremental weekly pulls)

# Centralized Base URL for Microservices (local development default)
BASE_SERVICE_URL = os.getenv("BACKEND_HOST", "http://localhost:5000")
RENDER_URL = f"{BASE_SERVICE_URL}/render"
RESULT_URL = f"{BASE_SERVICE_URL}/result"
API_PROXY_URL = f"{BASE_SERVICE_URL}/api"

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
    import os
    from datetime import datetime, timedelta
    
    # Check if we should only fetch current week
    if os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true':
        # Return only current week
        today = datetime.now()
        days_since_monday = today.weekday()
        week_start = today - timedelta(days=days_since_monday)
        week_end = week_start + timedelta(days=6)
        
        return [{
            "week_start": f"{week_start.strftime('%Y-%m-%d')}T00:00:00Z",
            "week_end": f"{week_end.strftime('%Y-%m-%d')}T23:59:59Z",
            "start_date": week_start.strftime('%Y-%m-%d'),
            "end_date": week_end.strftime('%Y-%m-%d')
        }]
    
    # Historical schedule (default behavior)
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

        # Print request body for all events
        print(f"\n{'='*70}")
        print(f"[{event_name.upper()}] PROCESS_EVENT_DATA - {event_name} REQUEST BODY")
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

        # Print validation info for all events
        print(f"[{event_name}] Validation:")
        print(f"  app_id: {app_id}")
        print(f"  token: {'***' if token else 'MISSING'}")
        print(f"  base_url: {base_url}")
        print(f"  report_id: {report_id}")
        print(f"  tag_id: {tag_id}")
        if event_id:
            print(f"  event_id: {event_id}")
        print()

        if is_trip:
            if not all([app_id, token, base_url, report_id, tag_id]):
                print(f"[ERROR] VALIDATION FAILED for Trip")
                return jsonify({"error": "Missing required parameters for Trip"}), 400
        else:
            if not all([app_id, token, base_url, report_id, tag_id, event_id]):
                print(f"[ERROR] VALIDATION FAILED for {event_name}")
                return jsonify({"error": "Missing required parameters"}), 400

        max_weeks = MAX_WEEKS_TRIP_WH if event_name in ["Trip", "WH"] else MAX_WEEKS_OTHER
        
        # If fetching current week only, limit to 1 week
        import os
        if os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true':
            max_weeks = 1
        
        weeks = build_weekly_schedule()[:max_weeks]

        print(f"[{event_name}] Config:")
        print(f"  MAX_WEEKS: {max_weeks}")
        print(f"  Total weeks available: {len(build_weekly_schedule())}")
        print(f"  Processing {len(weeks)} weeks\n")

        all_dataframes = []
        
        # Check if we're in backfill mode (skip render/result calls for speed)
        BACKFILL_MODE = os.environ.get('BACKFILL_MODE', 'false').lower() == 'true'
        if BACKFILL_MODE:
            print(f"[{event_name}] ⚡ BACKFILL MODE ENABLED - Skipping render/result calls for speed", file=sys.stderr)

        # ----------- WEEK LOOP ----------------
        weeks_processed = 0
        total_db_stats = {"inserted": 0, "skipped": 0, "failed": 0}
        total_internal_dupes = 0  # Track internal duplicates (CSV level)
        total_rows_raw = 0  # Track raw rows before dedup
        
        for i, week in enumerate(weeks):
            print(f"[{event_name}] Week {i+1}/{len(weeks)}: {week['start_date']} -> {week['end_date']}")
            
            if pytime.time() - start_time > MAX_EXECUTION_SECONDS:
                print(f"[TIMEOUT] {event_name} exceeded {MAX_EXECUTION_SECONDS}s")
                break

            try:
                # BACKFILL MODE: Skip render/result calls for speed - just fetch and insert data
                if BACKFILL_MODE:
                    print(f"  [BACKFILL] Skipping /render and /result calls - fetching data directly from CSV")
                    # Directly fetch CSV without rendering
                    import requests as req_module
                    csv_resp = RESILIENT_SESSION.post(
                        f"{base_url}/api/v2/Reports/GenerateReport",
                        data={
                            "app_id": app_id,
                            "report_id": report_id,
                            "period_start": week["week_start"],
                            "period_end": week["week_end"],
                            "tag_id": tag_id,
                            "token": token,
                            **({"event_id": event_id} if not is_trip else {})
                        },
                        timeout=(10, 30)
                    )
                    
                    if csv_resp.status_code != 200:
                        print(f"  ✗ CSV fetch failed (status {csv_resp.status_code}), skipping week")
                        continue
                    
                    csv_data = csv_resp.content
                    print(f"  ✓ CSV fetched directly ({len(csv_data):,} bytes)")
                else:
                    # NORMAL MODE: Call /render endpoint to initiate rendering
                    render_payload = {
                        "app_id": app_id,
                        "period_start": week["week_start"],
                        "period_end": week["week_end"],
                        "tag_id": tag_id,
                        "token": token,
                        "base_url": base_url,
                        "report_id": report_id
                    }
                    
                    # ≡ƒöÆ CRITICAL FIX: Trip MUST NOT send event_id
                    if not is_trip:
                        render_payload["event_id"] = event_id

                    # Use resilient session with timeout
                    render_resp = RESILIENT_SESSION.post(
                        RENDER_URL,
                        data=render_payload, 
                        timeout=(10, 30)
                    )
                    print(f"  ✓ /render call (status: {render_resp.status_code})")

                    if render_resp.status_code != 200:
                        print(f"  ✗ Render failed, skipping week")
                        continue

                    render_id = render_resp.json().get("render_id")
                    if not render_id:
                        print(f"  ✗ No render_id returned, skipping week")
                        continue

                    result_payload = {
                        "app_id": app_id,
                        "render_id": render_id,
                        "token": token,
                        "base_url": base_url,
                        "report_id": report_id
                    }

                    # Use resilient session for result (can take longer for CSV download)
                    print(f"  → Calling /result endpoint (may take 10-120s for CSV download)...")
                    result_resp = RESILIENT_SESSION.post(
                        RESULT_URL,
                        data=result_payload,
                        timeout=(10, 120)
                    )
                    print(f"  ✓ /result call (status: {result_resp.status_code})")

                    if result_resp.status_code != 200:
                        print(f"  ✗ Result failed, skipping week")
                        continue

                    gdrive_link = result_resp.json().get("gdrive_link")
                    if not gdrive_link:
                        print(f"  ✗ No gdrive_link returned, skipping week")
                        continue

                    print(f"  → Downloading CSV from Google Drive...")
                    csv_headers = {}
                    if "omantracking2.com" in gdrive_link:
                        csv_headers["Authorization"] = token
                    
                    csv_resp = RESILIENT_SESSION.get(
                        gdrive_link,
                        headers=csv_headers,
                        timeout=(10, 120)
                    )
                    
                    if csv_resp.status_code != 200:
                        print(f"  ✗ CSV download failed (status: {csv_resp.status_code}), skipping week")
                        continue
                    
                    csv_data = csv_resp.content
                if not csv_content:
                    print(f"  ✗ No CSV content received, skipping week")
                    continue

                print(f"  → Cleaning and parsing CSV data...")
                df = clean_csv_data(csv_content)
                if df is not None and not df.empty:
                    df_original = len(df)
                    total_rows_raw += df_original
                    print(f"  ✓ CSV parsed: {df_original} rows (raw)")
                    
                    # DEDUPLICATE based on ALL columns (complete row duplicate)
                    # This removes rows where every column value is identical
                    df = df.drop_duplicates(keep='first')
                    df_after_dedup = len(df)
                    internal_dupes = df_original - df_after_dedup
                    total_internal_dupes += internal_dupes
                    if internal_dupes > 0:
                        print(f"  → Deduplication: {df_original} → {df_after_dedup} rows (removed {internal_dupes} duplicates)")
                    
                    all_dataframes.append(df)
                    
                    # ✓ Store data to database with incremental logic
                    print(f"  → Inserting into database...")
                    db_stats = store_event_data_to_db(df, app_id, tag_id, event_name)
                    print(f"  ✓ DB Stats: Inserted={db_stats.get('inserted', 0)}, Duplicates={db_stats.get('skipped', 0)}, Errors={db_stats.get('failed', 0)}")
                    total_db_stats["inserted"] += db_stats.get("inserted", 0)
                    total_db_stats["skipped"] += db_stats.get("skipped", 0)
                    total_db_stats["failed"] += db_stats.get("failed", 0)
                    
                    # Week successfully processed
                    weeks_processed += 1

            except Exception as e:
                import traceback
                print(f"  ✗ Exception during week processing: {type(e).__name__}: {str(e)[:100]}")
                print(f"    Traceback: {traceback.format_exc()[:300]}")
                # Silently skip week errors - data already may have been partially processed
                continue

        # ============================================================================
        # COMPLETE ROW ACCOUNTING REPORT
        # ============================================================================
        rows_after_dedup = sum(len(df) for df in all_dataframes)
        db_inserted = total_db_stats.get("inserted", 0)
        db_duplicates = total_db_stats.get("skipped", 0)
        db_failed = total_db_stats.get("failed", 0)
        
        print(f"\n{'='*80}")
        print(f"COMPLETE ROW ACCOUNTING - {event_name}")
        print(f"{'='*80}")
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
