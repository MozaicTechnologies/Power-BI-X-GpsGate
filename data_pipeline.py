# """
# Data Pipeline for Fleet Dashboard (STANDARD VERSION)
# Handles: reports, event rules, weekly rendering, CSV download & cleaning
# """

# from flask import Blueprint, request, jsonify
# from datetime import datetime, timedelta, time
# import requests
# from requests.adapters import HTTPAdapter
# from urllib3.util.retry import Retry
# import pandas as pd
# import io
# import sys
# from urllib.parse import urljoin
# from models import db, Render, Result
# from db_storage import store_event_data_to_db
# import json
# import numpy as np
# import time as pytime
# import os

# # Custom JSON encoder to handle pandas/numpy types
# class PandasJSONEncoder(json.JSONEncoder):
#     def default(self, obj):
#         if isinstance(obj, (pd.Timestamp, datetime)):
#             return obj.isoformat()
#         elif isinstance(obj, time):
#             return obj.isoformat()
#         elif isinstance(obj, (pd.Series, pd.Index)):
#             return obj.tolist()
#         elif isinstance(obj, (np.integer, np.floating)):
#             return obj.item()
#         elif pd.isna(obj):
#             return None
#         return super().default(obj)

# pipeline_bp = Blueprint('pipeline_bp', __name__)

# # Timeout for processing - needs to be long for backfill with multiple weeks
# # Each week can take 10-20 seconds, and we process multiple weeks per event type
# MAX_EXECUTION_SECONDS = 600  # 10 minutes per event type

# # Calculate max weeks from 2025-01-01 to today
# def get_max_weeks():
#     """Calculate number of weeks from 2025-01-01 to today"""
#     start = datetime(2025, 1, 1).date()
#     today = datetime.utcnow().date()
#     weeks = (today - start).days // 7 + 1
#     return max(1, weeks)

# MAX_WEEKS_TRIP_WH = 1  # Trip & WH: 1 week per call (incremental weekly pulls)
# MAX_WEEKS_OTHER = 1    # Other events: 1 week per call (incremental weekly pulls)

# # Centralized Base URL for Microservices (local development default)
# BASE_SERVICE_URL = os.getenv("BACKEND_HOST", "http://localhost:5000")
# RENDER_URL = f"{BASE_SERVICE_URL}/render"
# RESULT_URL = f"{BASE_SERVICE_URL}/result"
# API_PROXY_URL = f"{BASE_SERVICE_URL}/api"

# # ============================================================================
# # RESILIENT SESSION WITH RETRY LOGIC
# # ============================================================================

# def create_resilient_session():
#     """Create requests session with automatic retries + backoff"""
#     session = requests.Session()
    
#     retry_strategy = Retry(
#         total=3,                                    # Max 3 retries
#         backoff_factor=2,                          # Wait 2s, 4s, 8s between retries
#         status_forcelist=[429, 500, 502, 503, 504] # Retry on these HTTP codes
#     )
    
#     adapter = HTTPAdapter(max_retries=retry_strategy)
#     session.mount("http://", adapter)
#     session.mount("https://", adapter)
    
#     return session

# # Global session instance
# RESILIENT_SESSION = create_resilient_session()

# # ============================================================================
# # HELPER FUNCTIONS
# # ============================================================================

# def fetch_from_gpsgate_api(base_url, token, path):
#     form_body = {
#         "method": "GET",
#         "token": token,
#         "base_url": base_url,
#         "path": path
#     }

#     try:
#         # Use resilient session with timeout (connect=10s, read=60s)
#         resp = RESILIENT_SESSION.post(
#             API_PROXY_URL, 
#             data=form_body, 
#             timeout=(10, 60)
#         )
#         resp.raise_for_status()
        
#         # Rate limit: small delay between requests
#         pytime.sleep(0.3)
        
#         return resp.json()
#     except requests.exceptions.Timeout:
#         print(f"Timeout fetching from GpsGate API after 3 retries")
#         return None
#     except Exception as e:
#         print(f"Error fetching from GpsGate API: {e}")
#         return None


# # def clean_csv_data(csv_content):
# #     try:
# #         df = pd.read_csv(io.BytesIO(csv_content) if isinstance(csv_content, bytes) else io.StringIO(csv_content), skiprows=8)
# #         df = df.where(pd.notna(df), None)
# #         df = df.replace({np.nan: None, pd.NaT: None})

# #         for col in df.columns:
# #             if pd.api.types.is_datetime64_any_dtype(df[col]):
# #                 df[col] = df[col].dt.strftime("%Y-%m-%d %H:%M:%S").where(pd.notna(df[col]), None)

# #         return df
# #     except Exception as e:
# #         print(f"CSV clean error: {e}")
# #         return None

# def clean_csv_data(file_bytes):
#     """
#     FINAL version (Power BI M Query compatible + pandas safe):
#     - Tolerates HTML/garbage rows
#     - Skips malformed lines (Power BI does this implicitly)
#     - Skips top 8 rows
#     - Promotes header
#     - Filters Vehicle <> ""
#     """

#     import io
#     import pandas as pd
#     import numpy as np

#     try:
#         text = file_bytes.decode("utf-8", errors="ignore")

#         # ----------------------------------------------------------
#         # 1️⃣ Read VERY defensively (key fix)
#         # ----------------------------------------------------------
#         df = pd.read_csv(
#             io.StringIO(text),
#             engine="python",
#             sep=",",
#             header=None,
#             skip_blank_lines=True,
#             on_bad_lines="skip"   # ⭐ CRITICAL FIX
#         )

#         print(f"[CSV_DEBUG] Raw read shape: {df.shape}")

#         # ----------------------------------------------------------
#         # 2️⃣ Skip top 8 junk rows (Power BI logic)
#         # ----------------------------------------------------------
#         if len(df) <= 9:
#             print("[CSV_DEBUG] Not enough rows after skip")
#             return None

#         df = df.iloc[8:].reset_index(drop=True)

#         # ----------------------------------------------------------
#         # 3️⃣ Promote first row as header
#         # ----------------------------------------------------------
#         df.columns = df.iloc[0]
#         df = df.iloc[1:].reset_index(drop=True)

#         # ----------------------------------------------------------
#         # 4️⃣ Normalize nulls
#         # ----------------------------------------------------------
#         df = df.replace({np.nan: None, "": None})

#         # ----------------------------------------------------------
#         # 5️⃣ Vehicle filter (CRITICAL)
#         # ----------------------------------------------------------
#         if "Vehicle" in df.columns:
#             before = len(df)
#             df = df[df["Vehicle"].notna()]
#             print(f"[CSV_DEBUG] Vehicle filter: {before} → {len(df)}")
#         else:
#             print("[CSV_DEBUG] Vehicle column not found")

#         print(f"[CSV_DEBUG] Final shape: {df.shape}")
#         return df

#     except Exception as e:
#         print(f"CSV clean error (FINAL): {e}")
#         return None




# def build_weekly_schedule(start_date_str="2025-01-01"):
#     import os
#     from datetime import datetime, timedelta
    
#     # Check if we should only fetch current week
#     if os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true':
#         # Return only current week
#         today = datetime.now()
#         days_since_monday = today.weekday()
#         week_start = today - timedelta(days=days_since_monday)
#         week_end = week_start + timedelta(days=6)
        
#         return [{
#             "week_start": f"{week_start.strftime('%Y-%m-%d')}T00:00:00Z",
#             "week_end": f"{week_end.strftime('%Y-%m-%d')}T23:59:59Z",
#             "start_date": week_start.strftime('%Y-%m-%d'),
#             "end_date": week_end.strftime('%Y-%m-%d')
#         }]
    
#     # Historical schedule (default behavior)
#     start_date = datetime.strptime(start_date_str, "%Y-%m-%d").date()
#     today = datetime.utcnow().date()

#     weeks = []
#     current = start_date

#     while current + timedelta(days=6) <= today:
#         end = current + timedelta(days=6)
#         weeks.append({
#             "week_start": f"{current}T00:00:00Z",
#             "week_end": f"{end}T23:59:59Z",
#             "start_date": current.isoformat(),
#             "end_date": end.isoformat()
#         })
#         current += timedelta(days=7)

#     return weeks


# def resolve_weeks_from_request(data, event_name, max_weeks):
#     """
#     Resolve week schedule based on request payload.
#     Priority:
#     1. Explicit backfill dates (period_start / period_end)
#     2. FETCH_CURRENT_WEEK env
#     3. Default historical mode (2025-01-01)
#     """

#     # 1️⃣ Explicit backfill mode (used by backfill scripts)
#     if data.get("period_start") and data.get("period_end"):
#         start_date = data["period_start"][:10]  # YYYY-MM-DD
#         print(f"[{event_name}] Backfill mode detected → start_date={start_date}")
#         return build_weekly_schedule(start_date)[:max_weeks]

#     # 2️⃣ Current week mode (live dashboard)
#     if os.environ.get("FETCH_CURRENT_WEEK", "false").lower() == "true":
#         today = datetime.utcnow()
#         week_start = today - timedelta(days=today.weekday())
#         start_date = week_start.strftime("%Y-%m-%d")
#         print(f"[{event_name}] FETCH_CURRENT_WEEK enabled → start_date={start_date}")
#         return build_weekly_schedule(start_date)[:1]

#     # 3️⃣ Default historical mode
#     print(f"[{event_name}] Historical mode → start_date=2025-01-01")
#     return build_weekly_schedule("2025-01-01")[:max_weeks]



# # ============================================================================
# # GENERIC EVENT HANDLER
# # ============================================================================

# def process_event_data(event_name, response_key):
#     print(f"[FUNCTION_START] process_event_data called for {event_name}")
#     try:
#         start_time = pytime.time()

#         # JSON first, then form
#         data = request.get_json(silent=True) or request.form or {}

#         # Print request body for all events
#         print(f"\n{'='*70}")
#         print(f"[{event_name.upper()}] PROCESS_EVENT_DATA - {event_name} REQUEST BODY")
#         print(f"{'='*70}")
#         print(f"Data: {json.dumps(data, indent=2)}")
#         print(f"{'='*70}\n")

#         app_id = data.get("app_id")
#         token = data.get("token")
#         base_url = data.get("base_url")
#         report_id = data.get("report_id")
#         tag_id = data.get("tag_id")
#         event_id = data.get("event_id")

#         is_trip = (event_name == "Trip")

#         # Print validation info for all events
#         print(f"[{event_name}] Validation:")
#         print(f"  app_id: {app_id}")
#         print(f"  token: {'***' if token else 'MISSING'}")
#         print(f"  base_url: {base_url}")
#         print(f"  report_id: {report_id}")
#         print(f"  tag_id: {tag_id}")
#         if event_id:
#             print(f"  event_id: {event_id}")
#         print()

#         if is_trip:
#             if not all([app_id, token, base_url, report_id, tag_id]):
#                 print(f"[ERROR] VALIDATION FAILED for Trip")
#                 return jsonify({"error": "Missing required parameters for Trip"}), 400
#         else:
#             if not all([app_id, token, base_url, report_id, tag_id, event_id]):
#                 print(f"[ERROR] VALIDATION FAILED for {event_name}")
#                 return jsonify({"error": "Missing required parameters"}), 400

#         max_weeks = MAX_WEEKS_TRIP_WH if event_name in ["Trip", "WH"] else MAX_WEEKS_OTHER
        
#         # RESTORE CURRENT WEEK FUNCTIONALITY
#         import os
#         from datetime import datetime
#         # if os.environ.get('FETCH_CURRENT_WEEK', 'false').lower() == 'true':
#         #     max_weeks = 1
#         #     # Calculate current week start (assuming it starts on Sunday)
#         #     current_date = datetime.now()
#         #     days_since_sunday = current_date.weekday() + 1  # Monday=0, so Sunday=6 -> 0
#         #     if days_since_sunday == 7:  # Sunday
#         #         days_since_sunday = 0
#         #     week_start_date = current_date - timedelta(days=days_since_sunday)
#         #     start_date_str = week_start_date.strftime('%Y-%m-%d')
#         #     print(f"[{event_name}] FETCH_CURRENT_WEEK enabled, using start_date: {start_date_str}")
#         #     weeks = build_weekly_schedule(start_date_str)[:max_weeks]
#         # else:
#         #     weeks = build_weekly_schedule()[:max_weeks]

#         weeks = resolve_weeks_from_request(data, event_name, max_weeks)


#         print(f"[{event_name}] Config:")
#         print(f"  MAX_WEEKS: {max_weeks}")
#         print(f"  Total weeks available: {len(weeks)}")
#         print(f"  Processing {len(weeks)} weeks")
#         if weeks:
#             print(f"  Week range: {weeks[0]['week_start'][:10]} to {weeks[-1]['week_end'][:10]}")
#         else:
#             print(f"  ERROR: No weeks returned from build_weekly_schedule!")
#         print()

#         all_dataframes = []
        
#         # Check if we're in backfill mode (skip render/result calls for speed)
#         BACKFILL_MODE = os.environ.get('BACKFILL_MODE', 'false').lower() == 'true'
#         if BACKFILL_MODE:
#             print(f"[{event_name}] BACKFILL MODE ENABLED - Skipping render/result calls for speed", file=sys.stderr)

#         # ----------- WEEK LOOP ----------------
#         weeks_processed = 0
#         total_db_stats = {"inserted": 0, "skipped": 0, "failed": 0}
#         total_internal_dupes = 0  # Track internal duplicates (CSV level)
#         total_rows_raw = 0  # Track raw rows before dedup
        
#         print(f"[{event_name}] About to start week loop with {len(weeks)} weeks...")
        
#         # Test imports before week loop
#         try:
#             from models import Render, Result
#             print(f"[{event_name}] Successfully imported Render and Result models")
#         except Exception as e:
#             print(f"[{event_name}] ERROR importing models: {e}")
#             return jsonify({"error": f"Model import failed: {e}"}), 500
        
#         for i, week in enumerate(weeks):
#             print(f"[{event_name}] Week {i+1}/{len(weeks)}: {week['start_date']} -> {week['end_date']}")
#             print(f"  [FLOW_DEBUG] Starting week processing loop...")
            
#             if pytime.time() - start_time > MAX_EXECUTION_SECONDS:
#                 print(f"[TIMEOUT] {event_name} exceeded {MAX_EXECUTION_SECONDS}s")
#                 break

#             try:
#                 # DATABASE-FIRST APPROACH: Check for existing render records before making API calls
#                 print(f"  [DEBUG] Checking database for existing render record...")
                
#                 # Look for existing render record matching this week and event type
#                 existing_render = Render.query.filter_by(
#                     app_id=str(app_id),
#                     period_start=week["week_start"],
#                     period_end=week["week_end"],
#                     tag_id=str(tag_id),
#                     report_id=str(report_id),
#                     event_id=str(event_id) if not is_trip else None
#                 ).first()
                
#                 if existing_render:
#                     print(f"  OK Found existing render record: render_id={existing_render.render_id}")
#                     render_id = existing_render.render_id
#                 else:
#                     print(f"  -> No existing render record, calling /render endpoint...")
                    
#                     # Prepare render API call
#                     render_payload = {
#                         "app_id": app_id,
#                         "period_start": week["week_start"],
#                         "period_end": week["week_end"],
#                         "tag_id": tag_id,
#                         "token": token,
#                         "base_url": base_url,
#                         "report_id": report_id
#                     }
#                     # Trip MUST NOT send event_id
#                     if not is_trip:
#                         render_payload["event_id"] = event_id
    
#                     # # Use resilient session with timeout
#                     # render_resp = RESILIENT_SESSION.post(
#                     #     RENDER_URL,
#                     #     data=render_payload, 
#                     #     timeout=(10, 30)
#                     # )
#                     # print(f"  OK /render call (status: {render_resp.status_code})")

#                     # if render_resp.status_code != 200:
#                     #     print(f"  ERROR Render failed (status {render_resp.status_code}), skipping week")
#                     #     continue
    
#                     # render_id = render_resp.json().get("render_id")

#                     render_id = None

#                     for attempt in range(3):
#                         render_resp = RESILIENT_SESSION.post(
#                             RENDER_URL,
#                             data=render_payload,
#                             timeout=(10, 60)
#                         )

#                         print(f"  /render attempt {attempt + 1} → status {render_resp.status_code}")

#                         if render_resp.status_code == 200:
#                             render_id = render_resp.json().get("render_id")
#                             if render_id:
#                                 break

#                         pytime.sleep(5)

#                     if not render_id:
#                         print("  ERROR Render never stabilized after retries, skipping week")
#                         continue

#                     print(f"  OK New render created: render_id={render_id}")


#                     if not render_id:
#                         print(f"  ERROR No render_id returned, skipping week")
#                         continue
                        
#                     print(f"  OK New render created: render_id={render_id}")

#                 # DATABASE-FIRST: Check for existing result record
#                 print(f"  [DEBUG] Checking database for existing result record...")
#                 existing_result = Result.query.filter_by(render_id=str(render_id)).first()
                
#                 if existing_result and existing_result.gdrive_link:
#                     print(f"  OK Found existing result with gdrive_link")
#                     gdrive_link = existing_result.gdrive_link
#                 else:
#                     print(f"  -> No cached result, calling /result endpoint...")
                    
#                     result_payload = {
#                         "app_id": app_id,
#                         "render_id": render_id,
#                         "token": token,
#                         "base_url": base_url,
#                         "report_id": report_id
#                     }
    
#                     # Use resilient session for result (can take longer for CSV download)
#                     result_resp = RESILIENT_SESSION.post(
#                         RESULT_URL,
#                         data=result_payload,
#                         timeout=(10, 120)
#                     )
#                     print(f"  OK /result call (status: {result_resp.status_code})")
    
#                     if result_resp.status_code != 200:
#                         print(f"  ERROR Result failed (status {result_resp.status_code}), skipping week")
#                         continue
    
#                     gdrive_link = result_resp.json().get("gdrive_link")
#                     if not gdrive_link:
#                         print(f"  ERROR No gdrive_link returned, skipping week")
#                         continue
                        
#                     print(f"  OK Got gdrive_link from API: {gdrive_link[:60]}...")

#                 print(f"  -> Downloading CSV from Google Drive...")
#                 csv_headers = {}
#                 if "omantracking2.com" in gdrive_link:
#                     csv_headers["Authorization"] = token
                
#                 # csv_resp = RESILIENT_SESSION.get(
#                 #     gdrive_link,
#                 #     headers=csv_headers,
#                 #     timeout=(10, 120)
#                 # )
                
#                 # if csv_resp.status_code != 200:
#                 #     print(f"  ERROR CSV download failed (status: {csv_resp.status_code}), skipping week")
#                 #     continue
                
#                 # csv_data = csv_resp.content

#                 # print(f"  OK CSV data retrieved ({len(csv_data):,} bytes)", file=sys.stderr)

#                 csv_resp = RESILIENT_SESSION.get(
#                     gdrive_link,
#                     headers=csv_headers,
#                     stream=True,
#                     timeout=(10, 300)
#                 )

#                 if csv_resp.status_code != 200:
#                     print(f"  ERROR CSV download failed (status: {csv_resp.status_code})")
#                     continue

#                 chunks = []
#                 for chunk in csv_resp.iter_content(chunk_size=1024 * 1024):  # 1MB chunks
#                     if chunk:
#                         chunks.append(chunk)

#                 csv_data = b"".join(chunks)
#                 print(f"  OK CSV data retrieved ({len(csv_data):,} bytes)", file=sys.stderr)
                
#                 # FIX: Check csv_data (not undefined csv_content) after both BACKFILL and NORMAL modes
#                 if not csv_data:
#                     print(f"  ERROR No CSV content received, skipping week")
#                     continue

#                 print(f"  -> Cleaning and parsing CSV data...")
#                 df = clean_csv_data(csv_data)
                
#                 # DEBUG: Log DataFrame details
#                 print(f"  [DEBUG] clean_csv_data returned: type={type(df)}, is_none={df is None}, is_empty={df.empty if df is not None else 'N/A'}", file=sys.stderr)
#                 if df is not None and not df.empty:
#                     print(f"  [DEBUG] DataFrame shape: {df.shape}, columns: {df.columns.tolist()}", file=sys.stderr)
#                     print(f"  [DEBUG] First 2 rows:\n{df.head(2).to_string()}", file=sys.stderr)
                
#                 if df is not None and not df.empty:
#                     df_original = len(df)
#                     total_rows_raw += df_original
#                     print(f"  OK CSV parsed: {df_original} rows (raw)")
                    
#                     # DEDUPLICATE based on ALL columns (complete row duplicate)
#                     # This removes rows where every column value is identical
#                     df = df.drop_duplicates(keep='first')
#                     df_after_dedup = len(df)
#                     internal_dupes = df_original - df_after_dedup
#                     total_internal_dupes += internal_dupes
#                     if internal_dupes > 0:
#                         print(f"  -> Deduplication: {df_original} -> {df_after_dedup} rows (removed {internal_dupes} duplicates)")
                    
#                     all_dataframes.append(df)
                    
#                     # Store data to database with incremental logic
#                     print(f"  -> Inserting into database...")
#                     db_stats = store_event_data_to_db(df, app_id, tag_id, event_name)
#                     print(f"  OK DB Stats: Inserted={db_stats.get('inserted', 0)}, Duplicates={db_stats.get('skipped', 0)}, Errors={db_stats.get('failed', 0)}")
#                     total_db_stats["inserted"] += db_stats.get("inserted", 0)
#                     total_db_stats["skipped"] += db_stats.get("skipped", 0)
#                     total_db_stats["failed"] += db_stats.get("failed", 0)
                    
#                     # Week successfully processed
#                     weeks_processed += 1

#             except Exception as e:
#                 import traceback
#                 tb_str = traceback.format_exc()
#                 print(f"  ERROR Exception during week processing: {type(e).__name__}: {str(e)}")
#                 print(f"  Full Traceback:\n{tb_str}")
#                 # Silently skip week errors - data already may have been partially processed
#                 continue

#         # ============================================================================
#         # COMPLETE ROW ACCOUNTING REPORT
#         # ============================================================================
#         rows_after_dedup = sum(len(df) for df in all_dataframes)
#         db_inserted = total_db_stats.get("inserted", 0)
#         db_duplicates = total_db_stats.get("skipped", 0)
#         db_failed = total_db_stats.get("failed", 0)
        
#         print(f"\n{'='*80}")
#         print(f"COMPLETE ROW ACCOUNTING - {event_name}")
#         print(f"{'='*80}")
#         print(f"Total Raw Rows Fetched from API:          {total_rows_raw:>10,}", file=sys.stderr)
#         print(f"  - Internal Duplicates Removed (CSV):   -{total_internal_dupes:>10,}", file=sys.stderr)
#         print(f"  = Rows After Deduplication:             {rows_after_dedup:>10,}", file=sys.stderr)
#         print(f"  - Database-Level Duplicates (flagged):  -{db_duplicates:>10,}", file=sys.stderr)
#         print(f"  - Failed DB Inserts:                    -{db_failed:>10,}", file=sys.stderr)
#         print(f"  = Total Inserted to DB:                 {db_inserted:>10,}", file=sys.stderr)
#         print(f"{'='*80}", file=sys.stderr)
#         print(f"Weeks Processed: {weeks_processed} | Time: {pytime.time() - start_time:.1f}s", file=sys.stderr)
#         print(f"{'='*80}\n", file=sys.stderr)

#         if not all_dataframes:
#             return jsonify({
#                 "message": "No data found",
#                 response_key: [],
#                 "total_rows": 0,
#                 "weeks_processed": weeks_processed,
#                 "db_stats": total_db_stats,
#                 "accounting": {
#                     "raw_fetched": total_rows_raw,
#                     "internal_dupes_removed": total_internal_dupes,
#                     "rows_after_dedup": rows_after_dedup,
#                     "db_duplicates_flagged": db_duplicates,
#                     "db_failed": db_failed,
#                     "total_inserted": db_inserted
#                 }
#             }), 200

#         combined_df = pd.concat(all_dataframes, ignore_index=True)
#         events = combined_df.to_dict("records")

#         return jsonify({
#             "message": "Success",
#             response_key: events,
#             "total_rows": len(events),
#             "weeks_processed": weeks_processed,
#             "db_stats": total_db_stats,
#             "accounting": {
#                 "raw_fetched": total_rows_raw,
#                 "internal_dupes_removed": total_internal_dupes,
#                 "rows_after_dedup": rows_after_dedup,
#                 "db_duplicates_flagged": db_duplicates,
#                 "db_failed": db_failed,
#                 "total_inserted": db_inserted
#             }
#         }), 200

#     except Exception as e:
#         return jsonify({
#             "message": f"Backend error: {e}",
#             response_key: [],
#             "total_rows": 0
#         }), 200


# # ============================================================================
# # ROUTES
# # ============================================================================

# @pipeline_bp.route("/reports", methods=["POST"])
# def get_reports():
#     data = request.get_json(silent=True) or request.form or {}
#     app_id = data.get("app_id")
#     token = data.get("token")
#     base_url = data.get("base_url")

#     if not all([app_id, token, base_url]):
#         return jsonify({"error": "Missing required parameters"}), 400

#     path = f"comGpsGate/api/v.1/applications/{app_id}/reports"
#     resp = fetch_from_gpsgate_api(base_url, token, path)

#     if not resp or "data" not in resp:
#         return jsonify({"error": "Failed to fetch reports"}), 500

#     return jsonify({
#         "reports": [
#             {
#                 "id": r.get("id"),
#                 "name": r.get("name"),
#                 "description": r.get("description")
#             } for r in resp["data"]
#         ]
#     }), 200


# @pipeline_bp.route("/event-rules", methods=["POST"])
# def get_event_rules():
#     data = request.get_json(silent=True) or request.form or {}
#     app_id = data.get("app_id")
#     token = data.get("token")
#     base_url = data.get("base_url")

#     if not all([app_id, token, base_url]):
#         return jsonify({"error": "Missing required parameters"}), 400

#     path = f"comGpsGate/api/v.1/applications/{app_id}/eventrules"
#     resp = fetch_from_gpsgate_api(base_url, token, path)

#     if not resp or "data" not in resp:
#         return jsonify({"error": "Failed to fetch event rules"}), 500

#     return jsonify({
#         "event_rules": [
#             {
#                 "id": r.get("id"),
#                 "name": r.get("name"),
#                 "description": r.get("description")
#             } for r in resp["data"]
#         ]
#     }), 200


# @pipeline_bp.route("/weekly-schedule", methods=["POST"])
# def get_weekly_schedule_route():
#     data = request.get_json(silent=True) or request.form or {}
#     start_date = data.get("start_date", "2025-01-01")
#     return jsonify({"weeks": build_weekly_schedule(start_date)}), 200


# # ============================================================================
# # EVENT ENDPOINTS
# # ============================================================================

# @pipeline_bp.route("/speeding-data", methods=["POST"])
# def speeding():
#     return process_event_data("Speeding", "speed_events")

# @pipeline_bp.route("/idle-data", methods=["POST"])
# def idle():
#     return process_event_data("Idle", "idle_events")

# @pipeline_bp.route("/trip-data", methods=["POST"])
# def trip():
#     # Print request body for debugging
#     request_body = request.get_json(silent=True) or request.form or {}
#     print(f"\n{'='*70}")
#     print(f"[TRIP-DATA] TRIP-DATA ENDPOINT REQUEST")
#     print(f"{'='*70}")
#     print(f"Request Body: {json.dumps(request_body, indent=2)}")
#     print(f"{'='*70}\n")
    
#     return process_event_data("Trip", "trip_events")

# @pipeline_bp.route("/awh-data", methods=["POST"])
# def awh():
#     return process_event_data("AWH", "awh_events")

# @pipeline_bp.route("/wh-data", methods=["POST"])
# def wh():
#     return process_event_data("WH", "wh_events")

# @pipeline_bp.route("/ha-data", methods=["POST"])
# def ha():
#     return process_event_data("HA", "ha_events")

# @pipeline_bp.route("/hb-data", methods=["POST"])
# def hb():
#     return process_event_data("HB", "hb_events")

# @pipeline_bp.route("/wu-data", methods=["POST"])
# def wu():
#     return process_event_data("WU", "wu_events")


"""
Data Pipeline for Fleet Dashboard (FINAL STABLE VERSION)
Handles: reports, event rules, weekly rendering, CSV parsing,
retry-safe downloads, and DB insertion.
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import json
import numpy as np
import time as pytime
import os
import io
import logging
from logging.handlers import RotatingFileHandler

from models import db, Render, Result
from db_storage import store_event_data_to_db

# ------------------------------------------------------------------------------
# LOGGING SETUP (ADDED)
# ------------------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "pipeline_full.log")

logger = logging.getLogger("DATA_PIPELINE")
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=10 * 1024 * 1024,
    backupCount=5,
    encoding="utf-8"   # 🔑 REQUIRED
    )

    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

# ------------------------------------------------------------------------------
# BLUEPRINT
# ------------------------------------------------------------------------------

pipeline_bp = Blueprint("pipeline_bp", __name__)

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

MAX_EXECUTION_SECONDS = 600

BASE_SERVICE_URL = os.getenv("BACKEND_HOST", "http://localhost:5000")
RENDER_URL = f"{BASE_SERVICE_URL}/render"
RESULT_URL = f"{BASE_SERVICE_URL}/result"

MAX_WEEKS_TRIP_WH = 1
MAX_WEEKS_OTHER = 1

# ------------------------------------------------------------------------------
# RESILIENT SESSION
# ------------------------------------------------------------------------------

def create_resilient_session():
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

RESILIENT_SESSION = create_resilient_session()

# ------------------------------------------------------------------------------
# CSV CLEANER
# ------------------------------------------------------------------------------

def clean_csv_data(file_bytes):
    try:
        df = pd.read_csv(
            io.BytesIO(file_bytes),
            delimiter=",",
            encoding="utf-8",
            skiprows=8,
            dtype=str,
            engine="python",
            on_bad_lines="skip"
        )

        df.columns = [c.strip() for c in df.columns]

        if "Vehicle" not in df.columns:
            logger.warning("CSV missing Vehicle column")
            return None

        df = df[df["Vehicle"].notna() & (df["Vehicle"].str.strip() != "")]
        df = df.reset_index(drop=True)

        logger.info(f"CSV parsed rows={len(df)} cols={list(df.columns)}")
        if not df.empty:
            logger.debug("CSV sample row logged (content suppressed due to unicode)")


        return df

    except Exception as e:
        logger.exception(f"CSV clean failed: {e}")
        return None

# ------------------------------------------------------------------------------
# WEEK RESOLUTION
# ------------------------------------------------------------------------------

def build_weekly_schedule(start_date_str):
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    today = datetime.utcnow().date()
    weeks = []
    cur = start

    while cur + timedelta(days=6) <= today:
        end = cur + timedelta(days=6)
        weeks.append({
            "week_start": f"{cur}T00:00:00Z",
            "week_end": f"{end}T23:59:59Z",
        })
        cur += timedelta(days=7)

    return weeks

def resolve_weeks(data, max_weeks):
    if data.get("period_start") and data.get("period_end"):
        start = data["period_start"][:10]
        return build_weekly_schedule(start)[:max_weeks]
    return build_weekly_schedule("2025-01-01")[:max_weeks]

# ------------------------------------------------------------------------------
# DOWNLOAD WITH RETRY
# ------------------------------------------------------------------------------

def download_with_retry(url, headers, max_attempts=3):
    for attempt in range(1, max_attempts + 1):
        try:
            resp = RESILIENT_SESSION.get(
                url, headers=headers, stream=True, timeout=(10, 300)
            )
            resp.raise_for_status()
            content = b"".join(resp.iter_content(chunk_size=512 * 1024))
            logger.info(f"Downloaded bytes={len(content)} url={url}")
            return content
        except Exception as e:
            logger.warning(f"Download attempt {attempt} failed: {e}")
            pytime.sleep(5)
    raise RuntimeError("CSV download failed")

# ------------------------------------------------------------------------------
# CORE PROCESSOR
# ------------------------------------------------------------------------------

def process_event_data(event_name, response_key):
    start_time = pytime.time()
    data = request.get_json(silent=True) or request.form or {}

    logger.info(f"START event={event_name}")
    logger.debug(f"Payload={json.dumps(data, default=str)}")

    app_id = data.get("app_id")
    token = data.get("token")
    base_url = data.get("base_url")
    tag_id = data.get("tag_id")
    event_id = data.get("event_id")

    report_id = "1225" if event_name == "Trip" else "25"

    if not all([app_id, token, base_url, tag_id]):
        logger.error("Missing required parameters")
        return jsonify({"error": "Missing required parameters"}), 400

    weeks = resolve_weeks(data, 1)
    totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}
    weeks_processed = 0

    for week in weeks:
        try:
            # ---------------- RENDER ----------------
            render = Render.query.filter_by(
                app_id=str(app_id),
                period_start=week["week_start"],
                period_end=week["week_end"],
                tag_id=str(tag_id),
                report_id=str(report_id),
                event_id=str(event_id) if event_name != "Trip" else None
            ).first()

            if render:
                render_id = render.render_id
                logger.debug(f"Using cached render_id={render_id}")
            else:
                payload = {
                    "app_id": app_id,
                    "period_start": week["week_start"],
                    "period_end": week["week_end"],
                    "tag_id": tag_id,
                    "token": token,
                    "base_url": base_url,
                    "report_id": report_id,
                }
                if event_name != "Trip":
                    payload["event_id"] = event_id

                render_id = None
                for _ in range(3):
                    r = RESILIENT_SESSION.post(RENDER_URL, data=payload, timeout=(10, 60))
                    if r.status_code == 200:
                        render_id = r.json().get("render_id")
                        if render_id:
                            break
                    pytime.sleep(5)

                if not render_id:
                    logger.error("Render failed")
                    continue

            # ---------------- RESULT ----------------
            result = Result.query.filter_by(render_id=str(render_id)).first()
            if result and result.gdrive_link:
                gdrive_link = result.gdrive_link
            else:
                payload = {
                    "app_id": app_id,
                    "render_id": render_id,
                    "token": token,
                    "base_url": base_url,
                    "report_id": report_id,
                }
                gdrive_link = None
                for _ in range(3):
                    res = RESILIENT_SESSION.post(RESULT_URL, data=payload, timeout=(10, 120))
                    if res.status_code == 200:
                        gdrive_link = res.json().get("gdrive_link")
                        if gdrive_link:
                            break
                    pytime.sleep(5)

                if not gdrive_link:
                    logger.error("Result fetch failed")
                    continue

            # ---------------- DOWNLOAD ----------------
            headers = {"Authorization": token} if "omantracking2.com" in gdrive_link else {}
            csv_bytes = download_with_retry(gdrive_link, headers)

            # ---------------- CLEAN ----------------
            raw_df = clean_csv_data(csv_bytes)
            if raw_df is None or raw_df.empty:
                logger.warning("Empty CSV after clean")
                continue

            totals["raw"] += len(raw_df)

            # ---------------- STORE ----------------
            stats = store_event_data_to_db(raw_df, app_id, tag_id, event_name)
            totals["inserted"] += stats["inserted"]
            totals["skipped"] += stats["skipped"]
            totals["failed"] += stats["failed"]

            logger.info(
                f"DB RESULT event={event_name} "
                f"inserted={stats['inserted']} skipped={stats['skipped']} failed={stats['failed']}"
            )

            weeks_processed += 1

        except Exception as e:
            logger.exception(f"{event_name} week failed: {e}")

    logger.info(f"END event={event_name} totals={totals}")

    return jsonify({
        "message": "Success",
        "weeks_processed": weeks_processed,
        "accounting": totals,
    }), 200

# ------------------------------------------------------------------------------
# ROUTES
# ------------------------------------------------------------------------------

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
