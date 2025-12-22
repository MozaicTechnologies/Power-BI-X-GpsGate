"""
Trip Data Pipeline
Dedicated module for trip-specific data processing
Mirrors Power BI M query flow: Reports → TripReport → WeeklySchedule → Render → Result → Download → Clean
"""

import requests
import pandas as pd
import numpy as np
import json
import io
import csv
from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, date
import time as pytime

trip_bp = Blueprint('trip_bp', __name__)

# Configuration
MAX_EXECUTION_SECONDS = 140
GPSGATE_BASE = "https://omantracking2.com"
BACKEND_HOST = "https://powerbixgpsgatexgdriver.onrender.com"
TRIP_REPORT_NAME = "Trip and Idle (Tag)-BI Format"
WEEK_START_DATE = date(2025, 1, 1)

# CSV columns mapping (9 columns for Trip CSV)
TRIP_CSV_COLUMNS = {
    "Start Time": "datetime",
    "Duration": "time",
    "Vehicle": "string",
    "Distance (GPS)": "float",
    "Max Speed": "float",
    "Avg Speed": "float",
    "Trip/Idle*": "string"
}


# ============================================================================
# HELPER: Build Weekly Schedule (mirrors WeeklySchedule M query)
# ============================================================================

def build_weekly_schedule():
    """
    Generate weekly schedule from 2025-01-01 to today
    Mirrors Power BI WeeklySchedule table
    
    Returns:
        list: [{'WeekStart': '2025-01-01T00:00:00Z', 'WeekEnd': '2025-01-07T23:59:59Z'}, ...]
    """
    today = datetime.utcnow().date()
    weeks = []
    current = WEEK_START_DATE
    
    while current <= today:
        week_end = min(current + timedelta(days=6), today)
        weeks.append({
            'start_date': current,
            'end_date': week_end,
            'WeekStart': f"{current.isoformat()}T00:00:00Z",
            'WeekEnd': f"{week_end.isoformat()}T23:59:59Z"
        })
        current += timedelta(days=7)
    
    return weeks


# ============================================================================
# HELPER: Fetch from GpsGate API (mirrors /api endpoint call)
# ============================================================================

def fetch_from_gpsgate_api(base_url, token, path):
    """
    Call /api endpoint to fetch from GpsGate
    Used for: Reports, Tags, etc.
    
    Args:
        base_url: GpsGate base (https://omantracking2.com)
        token: Authorization token
        path: API path (e.g., "comGpsGate/api/v.1/applications/6/reports")
    
    Returns:
        dict: JSON response with 'data' key
    """
    form_body = {
        'method': 'GET',
        'token': token,
        'base_url': base_url,
        'path': path
    }
    
    try:
        response = requests.post(
            f"{BACKEND_HOST}/api",
            data=form_body,
            timeout=20
        )
        
        if response.status_code != 200:
            print(f"❌ API error ({response.status_code}): {response.text[:100]}")
            return None
        
        return response.json()
    
    except Exception as e:
        print(f"❌ Exception fetching from GpsGate API: {str(e)}")
        return None


# ============================================================================
# HELPER: Request Render (mirrors fnRequestRender M query)
# ============================================================================

def request_render(app_id, token, base_url, tag_id, report_id, period_start, period_end):
    """
    Call /render endpoint to generate report
    Mirrors fnRequestRender(PeriodStart, PeriodEnd, reportId) - NO event_id
    
    Args:
        app_id: Application ID
        token: Authorization token
        base_url: GpsGate base URL
        tag_id: Tag ID
        report_id: Report ID
        period_start: ISO format start (e.g., '2025-01-01T00:00:00Z')
        period_end: ISO format end (e.g., '2025-01-07T23:59:59Z')
    
    Returns:
        dict: {'report_id': '...', 'render_id': '...'} or None
    """
    payload = {
        'app_id': app_id,
        'period_start': period_start,
        'period_end': period_end,
        'tag_id': tag_id,
        'token': token,
        'base_url': base_url,
        'report_id': report_id
    }
    
    try:
        response = requests.post(
            f"{BACKEND_HOST}/render",
            data=payload,
            timeout=20
        )
        
        if response.status_code != 200:
            print(f"  ⚠️  Render error ({response.status_code}): {response.text[:100]}")
            return None
        
        return response.json()
    
    except Exception as e:
        print(f"  ⚠️  Exception in render: {str(e)}")
        return None


# ============================================================================
# HELPER: Request Result (mirrors fnRequestResult M query)
# ============================================================================

def request_result(app_id, token, base_url, report_id, render_id):
    """
    Call /result endpoint to get file path
    Mirrors fnRequestResult(reportId, renderId)
    
    Args:
        app_id: Application ID
        token: Authorization token
        base_url: GpsGate base URL
        report_id: Report ID
        render_id: Render ID from request_render()
    
    Returns:
        dict: {'filepath': '...', 'gdrive_link': '...'} or None
    """
    payload = {
        'app_id': app_id,
        'render_id': render_id,
        'token': token,
        'base_url': base_url,
        'report_id': report_id
    }
    
    try:
        response = requests.post(
            f"{BACKEND_HOST}/result",
            data=payload,
            timeout=40
        )
        
        if response.status_code != 200:
            print(f"  ⚠️  Result error ({response.status_code}): {response.text[:100]}")
            return None
        
        return response.json()
    
    except Exception as e:
        print(f"  ⚠️  Exception in result: {str(e)}")
        return None


# ============================================================================
# HELPER: Download CSV from GDrive or GpsGate (mirrors fnCSVTrip)
# ============================================================================

def download_csv_from_path(file_path, auth_token):
    """
    Download CSV from GDrive or GpsGate file path
    Mirrors fnDownloadAndCleanCsv logic (GDrive + GpsGate whitelist)
    
    Args:
        file_path: Full URL or relative path
        auth_token: Authorization token (for GpsGate only)
    
    Returns:
        str: CSV content or None
    """
    if not file_path or (isinstance(file_path, str) and file_path.strip() == ""):
        return None
    
    try:
        # Parse URL to determine host
        from urllib.parse import urlparse
        parsed = urlparse(file_path)
        host = parsed.netloc.lower() if parsed.netloc else ""
        
        # Whitelist hosts
        if host == "omantracking2.com" or host == "":
            # GpsGate - send auth header
            request_host = GPSGATE_BASE
            rel_path = file_path.lstrip("/") if file_path.startswith("/") else file_path
            headers = {"Authorization": auth_token}
        elif "drive.google.com" in host or "docs.google.com" in host:
            # Google Drive - no auth header
            request_host = "https://drive.google.com"
            rel_path = parsed.path.lstrip("/")
            headers = {}
        else:
            print(f"  ⚠️  Unsupported host: {host}")
            return None
        
        response = requests.get(file_path, headers=headers, timeout=30)
        
        if response.status_code != 200:
            print(f"  ⚠️  Download error ({response.status_code})")
            return None
        
        return response.text
    
    except Exception as e:
        print(f"  ⚠️  Exception downloading CSV: {str(e)}")
        return None


# ============================================================================
# HELPER: Clean CSV Data (mirrors fnCSVTrip)
# ============================================================================

def clean_csv_data(csv_content):
    """
    Clean and parse Trip CSV data
    Mirrors fnCSVTrip M query:
    - Skip first 8 rows
    - Parse 9 columns
    - Filter out empty Vehicle rows
    - Type conversion
    
    Args:
        csv_content: Raw CSV string
    
    Returns:
        pd.DataFrame or None
    """
    if not csv_content:
        return None
    
    try:
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        rows = list(csv_reader)
        
        if not rows:
            return None
        
        # Build dataframe
        df = pd.DataFrame(rows)
        
        # Select only Trip columns
        expected_cols = ["Start Time", "Duration", "Vehicle", "Distance (GPS)", "Max Speed", "Avg Speed", "Trip/Idle*"]
        available_cols = [col for col in expected_cols if col in df.columns]
        
        if not available_cols:
            print(f"  ⚠️  No expected columns found. Available: {df.columns.tolist()}")
            return None
        
        df = df[available_cols]
        
        # Filter: Vehicle not empty
        df = df[df['Vehicle'].notna()]
        df = df[df['Vehicle'].astype(str).str.strip() != ""]
        
        if len(df) == 0:
            return None
        
        # Type conversion
        try:
            if "Start Time" in df.columns:
                df["Start Time"] = pd.to_datetime(df["Start Time"], errors='coerce')
            
            for col in ["Distance (GPS)", "Max Speed", "Avg Speed"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Remove rows with NaT in Duration
            if "Duration" in df.columns:
                df = df.dropna(subset=["Duration"])
        
        except Exception as type_error:
            print(f"  ⚠️  Type conversion error: {str(type_error)}")
            pass
        
        return df if len(df) > 0 else None
    
    except Exception as e:
        print(f"  ⚠️  Exception cleaning CSV: {str(e)}")
        return None


# ============================================================================
# MAIN ENDPOINT: Trip Data
# ============================================================================

@trip_bp.route('/trip-data', methods=['POST'])
def get_trip_data():
    """
    Trip Data Endpoint
    
    Flow mirrors Power BI M queries:
    1. Fetch Reports
    2. Find "Trip and Idle (Tag)-BI Format" report
    3. Generate WeeklySchedule
    4. For each week: Render → Result → Download → Clean
    5. Combine all weeks into single table
    
    Required Parameters:
    - app_id: Application ID
    - token: Authorization token
    - base_url: GpsGate base URL
    - tag_id: Tag ID
    
    Note: NO event_id required for Trip endpoint
    
    Returns:
        JSON: {
            'trip_events': [list of trip records],
            'total_rows': int,
            'weeks_processed': int,
            'execution_time_seconds': float
        }
    """
    try:
        start_time = pytime.time()
        data = request.form or request.get_json() or {}
        
        # Extract parameters
        app_id = data.get('app_id')
        token = data.get('token')
        base_url = data.get('base_url')
        tag_id = data.get('tag_id')
        
        # Validate required parameters (NO event_id)
        if not all([app_id, token, base_url, tag_id]):
            return jsonify({
                "error": "Missing required parameters",
                "required": ["app_id", "token", "base_url", "tag_id"]
            }), 400
        
        print(f"\n{'='*70}")
        print(f"🚗 TRIP DATA PIPELINE START")
        print(f"{'='*70}")
        print(f"App ID: {app_id}")
        print(f"Tag ID: {tag_id}")
        print(f"Base URL: {base_url}")
        
        # =====================================================================
        # STEP 1: Fetch Reports (mirrors Reports M query)
        # =====================================================================
        print(f"\n📋 Step 1: Fetching reports...")
        reports_path = f"comGpsGate/api/v.1/applications/{app_id}/reports"
        reports_response = fetch_from_gpsgate_api(base_url, token, reports_path)
        
        if not reports_response or 'data' not in reports_response:
            return jsonify({"error": "Failed to fetch reports"}), 500
        
        reports_list = reports_response.get('data', [])
        print(f"  ✅ Found {len(reports_list)} reports")
        
        # =====================================================================
        # STEP 2: Find Trip Report (mirrors TripReport M query)
        # =====================================================================
        print(f"\n🔍 Step 2: Finding Trip Report '{TRIP_REPORT_NAME}'...")
        trip_report = next(
            (r for r in reports_list if r.get('name') == TRIP_REPORT_NAME),
            None
        )
        
        if not trip_report:
            available = [r.get('name') for r in reports_list]
            print(f"  ❌ Report not found. Available reports:")
            for name in available:
                print(f"     - {name}")
            return jsonify({
                "error": f"Report '{TRIP_REPORT_NAME}' not found",
                "available_reports": available
            }), 404
        
        report_id = trip_report.get('id')
        print(f"  ✅ Trip Report ID: {report_id}")
        
        # =====================================================================
        # STEP 3: Generate Weekly Schedule (mirrors WeeklySchedule M query)
        # =====================================================================
        print(f"\n📅 Step 3: Generating weekly schedule...")
        weeks = build_weekly_schedule()
        print(f"  ✅ Generated {len(weeks)} weeks (from 2025-01-01 to today)")
        
        all_dataframes = []
        weeks_processed = 0
        
        # =====================================================================
        # STEP 4: Process Each Week (mirrors WeeklyTripRender + WeeklyTripFilePath + Trip)
        # =====================================================================
        print(f"\n🔄 Step 4: Processing weeks...")
        
        for i, week in enumerate(weeks):
            elapsed = pytime.time() - start_time
            
            # Timeout check
            if elapsed > MAX_EXECUTION_SECONDS:
                print(f"\n⏹  Stopping after {elapsed:.1f}s to avoid Power BI timeout")
                break
            
            week_num = i + 1
            print(f"\n  Week {week_num}/{len(weeks)}: {week['start_date']} → {week['end_date']}")
            
            try:
                # 4a: Request Render (mirrors fnRequestRender)
                print(f"    📊 Requesting render...")
                render_result = request_render(
                    app_id=app_id,
                    token=token,
                    base_url=base_url,
                    tag_id=tag_id,
                    report_id=report_id,
                    period_start=week['WeekStart'],
                    period_end=week['WeekEnd']
                )
                
                if not render_result or 'render_id' not in render_result:
                    print(f"    ⚠️  Render failed")
                    continue
                
                render_id = render_result['render_id']
                print(f"    ✅ Render ID: {render_id}")
                
                # 4b: Request Result (mirrors fnRequestResult)
                print(f"    🔗 Requesting result...")
                result_result = request_result(
                    app_id=app_id,
                    token=token,
                    base_url=base_url,
                    report_id=report_id,
                    render_id=render_id
                )
                
                if not result_result or 'gdrive_link' not in result_result:
                    print(f"    ⚠️  Result not ready yet")
                    continue
                
                gdrive_link = result_result['gdrive_link']
                print(f"    ✅ Download link ready")
                
                # 4c: Download CSV (mirrors fnCSVTrip)
                print(f"    📥 Downloading CSV...")
                csv_content = download_csv_from_path(gdrive_link, token)
                
                if not csv_content:
                    print(f"    ⚠️  Download failed")
                    continue
                
                print(f"    ✅ Downloaded ({len(csv_content)} bytes)")
                
                # 4d: Clean CSV (mirrors fnCSVTrip cleaning)
                print(f"    🧹 Cleaning CSV...")
                df = clean_csv_data(csv_content)
                
                if df is None or len(df) == 0:
                    print(f"    ⚠️  No data after cleaning")
                    continue
                
                all_dataframes.append(df)
                weeks_processed += 1
                print(f"    ✅ Processed {len(df)} trips")
            
            except Exception as week_error:
                print(f"    ❌ Error: {str(week_error)}")
                continue
        
        # =====================================================================
        # STEP 5: Combine Results (mirrors Table.Combine in Trip M query)
        # =====================================================================
        print(f"\n📦 Step 5: Combining results...")
        
        if not all_dataframes:
            print(f"  ⚠️  No trip data found")
            return jsonify({
                "message": "No trip data found",
                "trip_events": [],
                "total_rows": 0,
                "weeks_processed": 0
            }), 200
        
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        combined_df = combined_df.replace({np.nan: None, pd.NaT: None})
        
        print(f"  ✅ Combined {len(combined_df)} total trips")
        
        # Convert to list of dicts
        trip_events = combined_df.to_dict('records')
        
        # Cleanup
        del combined_df
        del all_dataframes
        
        elapsed = pytime.time() - start_time
        
        # =====================================================================
        # Final Response
        # =====================================================================
        print(f"\n{'='*70}")
        print(f"✅ TRIP DATA PIPELINE COMPLETE")
        print(f"{'='*70}")
        print(f"📊 Total trips: {len(trip_events)}")
        print(f"📅 Weeks processed: {weeks_processed}/{len(weeks)}")
        print(f"⏱️  Execution time: {elapsed:.1f}s")
        print(f"{'='*70}\n")
        
        return jsonify({
            "message": "Success",
            "trip_events": trip_events,
            "total_rows": len(trip_events),
            "weeks_processed": weeks_processed,
            "execution_time_seconds": round(elapsed, 2)
        }), 200
    
    except Exception as e:
        elapsed = pytime.time() - start_time
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        
        return jsonify({
            "error": str(e),
            "trip_events": [],
            "total_rows": 0,
            "execution_time_seconds": round(elapsed, 2)
        }), 500