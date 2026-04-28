"""
Data Pipeline for Fleet Dashboard (FINAL STABLE VERSION)
Handles: reports, event rules, weekly rendering, CSV parsing,
retry-safe downloads, and DB insertion.
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, timezone
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

BASE_SERVICE_URL = os.getenv("BACKEND_HOST", "http://127.0.0.1:5000")
RENDER_URL = f"{BASE_SERVICE_URL}/render"
RESULT_URL = f"{BASE_SERVICE_URL}/result"
RESULT_TIMEOUT = (10, 360)

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


import io
import pandas as pd

def clean_csv_data(file_bytes):
    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin1")

    def _read_csv(enc, skip):
        return pd.read_csv(
            io.BytesIO(file_bytes),
            delimiter=",",
            encoding=enc,
            skiprows=skip,
            dtype=str,
            engine="python",
            on_bad_lines="skip"
        )

    for enc in encodings:
        try:
            # 1) First attempt (your current behavior)
            df = _read_csv(enc, skip=8)

            # If skiprows=8 caused header issues (common in messy exports), retry
            if df is None or df.empty or len(df.columns) <= 1:
                logger.warning(f"CSV looks empty/invalid after skiprows=8, retrying skiprows=0 (encoding={enc})")
                df = _read_csv(enc, skip=0)

            # ---- Clean / normalize columns
            df.columns = [
                " ".join(str(c).strip().split())  # strip + collapse multiple spaces
                for c in df.columns
            ]

            # Normalize Vehicle column variants -> "Vehicle"
            rename_map = {}
            for c in df.columns:
                if str(c).strip().lower() == "vehicle":
                    rename_map[c] = "Vehicle"
            if rename_map:
                df = df.rename(columns=rename_map)

            # ---- Log columns (this is what you said you were seeing earlier)
            logger.info(f"CSV parsed rows={len(df)} encoding={enc}")
            logger.info(f"CSV columns count={len(df.columns)} encoding={enc}")
            logger.info(f"CSV columns (first 12)={', '.join(list(df.columns)[:12])} encoding={enc}")

            logger.info(f"CSV columns={list(df.columns)} encoding={enc}")

            # ---- Validate Vehicle
            if "Vehicle" not in df.columns:
                logger.warning(
                    f"CSV missing Vehicle column (encoding tried: {enc}). "
                    f"Available columns: {list(df.columns)}"
                )
                return None

            # ---- Clean rows with missing vehicle
            df["Vehicle"] = df["Vehicle"].astype(str)
            df = df[df["Vehicle"].notna() & (df["Vehicle"].str.strip() != "")]
            df = df.reset_index(drop=True)

            return df

        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.exception(f"CSV clean failed (encoding={enc}): {e}")
            return None

    logger.error("CSV decode failed for all encodings tried")
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
    """
    Resolve processing windows.

    If caller provides an explicit period_start/period_end (daily/weekly jobs do),
    process that exact range as a single window.
    """
    period_start = data.get("period_start")
    period_end = data.get("period_end")

    if period_start and period_end:
        return [{
            "week_start": period_start,
            "week_end": period_end,
        }]

    windows = build_weekly_schedule("2025-01-01")
    if max_weeks and max_weeks > 0:
        # Use most recent windows when no explicit range is supplied.
        return windows[-max_weeks:]
    return windows

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

    report_id = data.get("report_id")
    
    # Fallback report IDs - only use VERIFIED IDs from your system
    fallback_report_ids = {
        "Trip": ["1225", "25"],  # Try 1225 first, then fall back to 25 
        "default": ["25"]        # All events work with 25
    }

    if not all([app_id, token, base_url, tag_id]):
        logger.error("Missing required parameters")
        return jsonify({"error": "Missing required parameters"}), 400

    weeks = resolve_weeks(data, 1)
    totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}
    weeks_processed = 0

    for week in weeks:
        try:
            render_id = None
            successful_report_id = None
            
            # Prefer the caller's configured report_id. Only use hardcoded fallbacks when none was supplied.
            if report_id:
                report_ids_to_try = [str(report_id)]
            else:
                report_ids_to_try = fallback_report_ids.get(event_name, fallback_report_ids["default"])
            
            for try_report_id in report_ids_to_try:
                logger.info(f"Trying report_id={try_report_id} for event={event_name}")
                
                # ---------------- RENDER ----------------
                render = Render.query.filter_by(
                    app_id=str(app_id),
                    period_start=week["week_start"],
                    period_end=week["week_end"],
                    tag_id=str(tag_id),
                    report_id=str(try_report_id),
                    event_id=str(event_id) if event_name != "Trip" else None
                ).first()

                if render:
                    cached_result = Result.query.filter_by(render_id=str(render.render_id)).first()
                    if cached_result and cached_result.gdrive_link:
                        render_id = render.render_id
                        successful_report_id = try_report_id
                        logger.debug(
                            f"Using cached render_id={render_id} with cached result for report_id={try_report_id}"
                        )
                        break

                    logger.info(
                        f"Cached render_id={render.render_id} for event={event_name} "
                        f"has no cached result; requesting a fresh render"
                    )
                    render = None

                if not render:
                    payload = {
                        "app_id": app_id,
                        "period_start": week["week_start"],
                        "period_end": week["week_end"],
                        "tag_id": tag_id,
                        "token": token,
                        "base_url": base_url,
                        "report_id": try_report_id,
                    }
                    if event_name != "Trip":
                        payload["event_id"] = event_id

                    for attempt in range(2):  # Reduced attempts per report_id
                        r = RESILIENT_SESSION.post(RENDER_URL, data=payload, timeout=(10, 60))
                        if r.status_code == 200:
                            render_id = r.json().get("render_id")
                            if render_id:
                                successful_report_id = try_report_id
                                logger.info(f"Render succeeded with report_id={try_report_id}")
                                break
                        else:
                            logger.warning(f"Render failed with report_id={try_report_id}, status={r.status_code}")
                        pytime.sleep(2)

                    if render_id:
                        break
                        
            if not render_id:
                logger.error("Render failed for all report IDs")
                continue

            # Store the successful render record if it's not cached
            if successful_report_id and not render:
                new_render = Render(
                    app_id=str(app_id),
                    period_start=week["week_start"],
                    period_end=week["week_end"],
                    tag_id=str(tag_id),
                    report_id=str(successful_report_id),
                    render_id=render_id,
                    event_id=str(event_id) if event_name != "Trip" else None,
                    created_at=datetime.now(timezone.utc),
                )
                db.session.add(new_render)
                db.session.commit()
                logger.info(f"Stored new render record with report_id={successful_report_id}")

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
                    "report_id": successful_report_id or report_id,
                }
                gdrive_link = None
                result_started_at = pytime.time()
                logger.info(
                    f"Calling /result event={event_name} render_id={render_id} "
                    f"timeout={RESULT_TIMEOUT[1]}s max_attempts=3"
                )
                for attempt in range(1, 4):
                    logger.info(
                        f"/result attempt={attempt}/3 event={event_name} render_id={render_id}"
                    )
                    res = requests.post(RESULT_URL, data=payload, timeout=RESULT_TIMEOUT)
                    logger.info(
                        f"/result response attempt={attempt}/3 event={event_name} "
                        f"render_id={render_id} status={res.status_code}"
                    )
                    if res.status_code == 200:
                        gdrive_link = res.json().get("gdrive_link")
                        if gdrive_link:
                            elapsed = pytime.time() - result_started_at
                            logger.info(
                                f"/result completed event={event_name} render_id={render_id} "
                                f"elapsed={elapsed:.1f}s"
                            )
                            break
                    pytime.sleep(5)

                if not gdrive_link:
                    elapsed = pytime.time() - result_started_at
                    logger.error(
                        f"Result fetch failed event={event_name} render_id={render_id} "
                        f"elapsed={elapsed:.1f}s"
                    )
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
                f"inserted={stats['inserted']} "
                f"skipped={stats['skipped']} "
                f"invalid_rows_skipped={stats.get('invalid_rows_skipped', 0)} "
                f"duplicate_rows_skipped={stats.get('duplicate_rows_skipped', 0)} "
                f"failed={stats['failed']}"
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

@pipeline_bp.route("/test", methods=["GET"])
def test():
    return jsonify({"message": "Pipeline blueprint is working!"}), 200
