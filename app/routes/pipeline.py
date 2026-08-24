"""
Data Pipeline for Fleet Dashboard (FINAL STABLE VERSION)
Handles: reports, event rules, weekly rendering, CSV parsing,
retry-safe downloads, and DB insertion.
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlsplit
import pandas as pd
import numpy as np
import time as pytime
import uuid
from app.utils.logger import setup_dedicated_file_logger, setup_logger

from app.models import db, Render, Result, GpsGateApplication
from app.services.db_storage import store_event_data_to_db
from app.services.gpsgate_reports import create_report_render, wait_for_report_result

logger = setup_logger("DATA_PIPELINE")
trip_logger = setup_dedicated_file_logger("TRIP_PIPELINE", "trip_pipeline")

# ------------------------------------------------------------------------------
# BLUEPRINT
# ------------------------------------------------------------------------------

pipeline_bp = Blueprint("pipeline_bp", __name__)

# ------------------------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------------------------

MAX_EXECUTION_SECONDS = 600

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


def clean_csv_data(file_bytes, event_name=None, trace_id=None):
    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin1")
    diagnostic = trip_logger if event_name == "Trip" else None

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

            if diagnostic:
                diagnostic.info(
                    "trace=%s stage=csv_parse encoding=%s skiprows=8 rows=%d columns=%s",
                    trace_id, enc, len(df), list(df.columns),
                )

            # If skiprows=8 caused header issues (common in messy exports), retry
            if df is None or df.empty or len(df.columns) <= 1:
                logger.warning(f"CSV looks empty/invalid after skiprows=8, retrying skiprows=0 (encoding={enc})")
                df = _read_csv(enc, skip=0)
                if diagnostic:
                    diagnostic.warning(
                        "trace=%s stage=csv_parse retry=skiprows_0 encoding=%s rows=%d columns=%s",
                        trace_id, enc, len(df), list(df.columns),
                    )

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
                if diagnostic:
                    diagnostic.error(
                        "trace=%s stage=csv_validate reason=missing_vehicle columns=%s",
                        trace_id, list(df.columns),
                    )
                return None

            if diagnostic:
                expected = ["Vehicle", "Start Time", "Stop Time", "Duration (s)", "Distance (GPS)"]
                missing = [column for column in expected if column not in df.columns]
                diagnostic.log(
                    30 if "Start Time" in missing else 20,
                    "trace=%s stage=csv_validate rows=%d missing_expected=%s columns=%s",
                    trace_id, len(df), missing, list(df.columns),
                )

            # ---- Clean rows with missing vehicle
            df["Vehicle"] = df["Vehicle"].astype(str)
            df = df[df["Vehicle"].notna() & (df["Vehicle"].str.strip() != "")]
            df = df.reset_index(drop=True)

            if diagnostic:
                diagnostic.info(
                    "trace=%s stage=csv_clean rows_after_vehicle_filter=%d vehicle_blank=%d start_time_blank=%s",
                    trace_id,
                    len(df),
                    int((df["Vehicle"].str.strip() == "").sum()),
                    int(df["Start Time"].fillna("").astype(str).str.strip().eq("").sum())
                    if "Start Time" in df.columns else "column_missing",
                )

            return df

        except UnicodeDecodeError:
            if diagnostic:
                diagnostic.info("trace=%s stage=csv_decode encoding=%s result=unicode_error", trace_id, enc)
            continue
        except Exception as e:
            logger.exception(f"CSV clean failed (encoding={enc}): {e}")
            if diagnostic:
                diagnostic.exception(
                    "trace=%s stage=csv_parse encoding=%s result=exception", trace_id, enc
                )
            return None

    logger.error("CSV decode failed for all encodings tried")
    if diagnostic:
        diagnostic.error("trace=%s stage=csv_decode result=all_encodings_failed", trace_id)
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
            parsed_url = urlsplit(str(url))
            logger.info(
                "Downloaded bytes=%d source=%s%s",
                len(content), parsed_url.netloc, parsed_url.path,
            )
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
    trace_id = uuid.uuid4().hex[:12] if event_name == "Trip" else None

    logger.info(f"START event={event_name}")
    safe_data = dict(data)
    if safe_data.get("token"):
        safe_data["token"] = "<redacted>"
    logger.debug(f"Payload={json.dumps(safe_data, default=str)}")

    app_id = data.get("app_id")
    token = data.get("token")
    base_url = data.get("base_url")
    tag_id = data.get("tag_id")
    event_id = data.get("event_id")

    report_id = data.get("report_id")

    if event_name == "Trip":
        trip_logger.info(
            "trace=%s stage=start app_id=%s report_id=%s tag_id=%s period_start=%s period_end=%s base_host=%s",
            trace_id, app_id, report_id, tag_id, data.get("period_start"), data.get("period_end"),
            urlsplit(str(base_url or "")).netloc,
        )
    
    # Fallback report IDs - only use VERIFIED IDs from your system
    fallback_report_ids = {
        "Trip": ["1225", "25"],  # Try 1225 first, then fall back to 25 
        "default": ["25"]        # All events work with 25
    }

    if not all([app_id, token, base_url, tag_id]):
        logger.error("Missing required parameters")
        if event_name == "Trip":
            missing = [key for key, value in {
                "app_id": app_id, "token": token, "base_url": base_url, "tag_id": tag_id,
            }.items() if not value]
            trip_logger.error("trace=%s stage=validate reason=missing_parameters fields=%s", trace_id, missing)
        return jsonify({"error": "Missing required parameters"}), 400

    gpsgate_app = GpsGateApplication.query.filter_by(application_id=int(app_id or 0)).first()
    if not gpsgate_app:
        logger.error(f"No gpsgate_application found for app_id={app_id}")
        if event_name == "Trip":
            trip_logger.error(
                "trace=%s stage=config reason=application_not_found app_id=%s", trace_id, app_id
            )
        return jsonify({"error": f"No gpsgate_application found for app_id={app_id}"}), 404
    gpsgate_application_id = gpsgate_app.id

    if event_name == "Trip":
        trip_logger.info(
            "trace=%s stage=config db_pk=%s configured_report_name=%r configured_report_id=%s configured_tag_name=%r configured_tag_id=%s caller_matches_config=%s",
            trace_id, gpsgate_application_id, gpsgate_app.trip_report_name,
            gpsgate_app.trip_report_id, gpsgate_app.tag_name, gpsgate_app.tag_id,
            str(report_id) == str(gpsgate_app.trip_report_id) and str(tag_id) == str(gpsgate_app.tag_id),
        )

    weeks = resolve_weeks(data, 1)
    totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}
    weeks_processed = 0

    for week in weeks:
        try:
            render_id = None
            successful_report_id = None
            if event_name == "Trip":
                trip_logger.info(
                    "trace=%s stage=window period_start=%s period_end=%s",
                    trace_id, week["week_start"], week["week_end"],
                )
            
            # Prefer the caller's configured report_id. Only use hardcoded fallbacks when none was supplied.
            if report_id:
                report_ids_to_try = [str(report_id)]
            else:
                report_ids_to_try = fallback_report_ids.get(event_name, fallback_report_ids["default"])
            
            for try_report_id in report_ids_to_try:
                logger.info(f"Trying report_id={try_report_id} for event={event_name}")
                
                # ---------------- RENDER ----------------
                render = Render.query.filter_by(
                    gpsgate_application_id=gpsgate_application_id,
                    period_start=week["week_start"],
                    period_end=week["week_end"],
                    tag_id=str(tag_id),
                    report_id=str(try_report_id),
                    event_id=str(event_id) if event_name != "Trip" else None
                ).first()

                if event_name == "Trip":
                    trip_logger.info(
                        "trace=%s stage=render_lookup report_id=%s cached=%s cached_render_id=%s",
                        trace_id, try_report_id, bool(render), render.render_id if render else None,
                    )

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
                    logger.debug("[PAYLOAD] event=%s app=%s payload=%s", event_name, app_id, payload)
                    if event_name != "Trip":
                        payload["event_id"] = event_id

                    for attempt in range(2):
                        render_data, render_status = create_report_render(payload)
                        if event_name == "Trip":
                            trip_logger.log(
                                20 if render_status == 200 else 30,
                                "trace=%s stage=render_create attempt=%d status=%s response_keys=%s error=%r",
                                trace_id, attempt + 1, render_status,
                                sorted(render_data.keys()) if isinstance(render_data, dict) else [],
                                render_data.get("error") if isinstance(render_data, dict) else None,
                            )
                        if render_status == 200:
                            render_id = render_data.get("render_id")
                            if render_id:
                                successful_report_id = try_report_id
                                logger.info(f"Render succeeded with report_id={try_report_id}")
                                break
                        else:
                            logger.warning(f"Render failed with report_id={try_report_id}, status={render_status}")
                        pytime.sleep(2)

                    if render_id:
                        break
                        
            if not render_id:
                logger.error("Render failed for all report IDs")
                if event_name == "Trip":
                    trip_logger.error(
                        "trace=%s stage=render result=failed report_ids=%s", trace_id, report_ids_to_try
                    )
                continue

            # Store the successful render record if it's not cached
            if successful_report_id and not render:
                new_render = Render(
                    app_id=str(app_id),
                    gpsgate_application_id=gpsgate_application_id,
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
                if event_name == "Trip":
                    trip_logger.info("trace=%s stage=result source=cache render_id=%s", trace_id, render_id)
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
                    f"Polling GpsGate result directly event={event_name} "
                    f"render_id={render_id} max_wait=300s max_attempts=3"
                )
                for attempt in range(1, 4):
                    logger.info(
                        f"Result poll attempt={attempt}/3 event={event_name} render_id={render_id}"
                    )
                    result_data, result_status = wait_for_report_result(payload)
                    if event_name == "Trip":
                        trip_logger.log(
                            20 if result_status == 200 else 30,
                            "trace=%s stage=result_poll attempt=%d status=%s ready_link=%s error=%r",
                            trace_id, attempt, result_status,
                            bool(result_data.get("gdrive_link")) if isinstance(result_data, dict) else False,
                            result_data.get("error") if isinstance(result_data, dict) else None,
                        )
                    logger.info(
                        f"Result poll response attempt={attempt}/3 event={event_name} "
                        f"render_id={render_id} status={result_status}"
                    )
                    if result_status == 200:
                        gdrive_link = result_data.get("gdrive_link")
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
                    if event_name == "Trip":
                        trip_logger.error(
                            "trace=%s stage=result result=no_output_link render_id=%s elapsed=%.1f",
                            trace_id, render_id, elapsed,
                        )
                    continue

            # ---------------- DOWNLOAD ----------------
            headers = {"Authorization": token} if "omantracking2.com" in gdrive_link else {}
            csv_bytes = download_with_retry(gdrive_link, headers)
            if event_name == "Trip":
                trip_logger.info(
                    "trace=%s stage=download bytes=%d sha256=%s source_host=%s",
                    trace_id, len(csv_bytes), hashlib.sha256(csv_bytes).hexdigest(),
                    urlsplit(str(gdrive_link)).netloc,
                )

            # ---------------- CLEAN ----------------
            raw_df = clean_csv_data(csv_bytes, event_name=event_name, trace_id=trace_id)
            if raw_df is None or raw_df.empty:
                logger.warning("Empty CSV after clean")
                if event_name == "Trip":
                    trip_logger.error("trace=%s stage=csv_clean result=empty", trace_id)
                continue

            totals["raw"] += len(raw_df)

            # ---------------- STORE ----------------
            stats = store_event_data_to_db(
                raw_df, app_id, tag_id, event_name, gpsgate_application_id, trace_id=trace_id
            )
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
            if event_name == "Trip":
                trip_logger.info(
                    "trace=%s stage=store result=complete raw=%d inserted=%d skipped=%d invalid=%d duplicates=%d failed=%d",
                    trace_id, len(raw_df), stats["inserted"], stats["skipped"],
                    stats.get("invalid_rows_skipped", 0),
                    stats.get("duplicate_rows_skipped", 0), stats["failed"],
                )

            weeks_processed += 1

        except Exception as e:
            logger.exception(f"{event_name} week failed: {e}")
            if event_name == "Trip":
                trip_logger.exception(
                    "trace=%s stage=window result=unhandled_exception period_start=%s period_end=%s",
                    trace_id, week.get("week_start"), week.get("week_end"),
                )

    logger.info(f"END event={event_name} totals={totals}")
    if event_name == "Trip":
        trip_logger.info(
            "trace=%s stage=end weeks_processed=%d totals=%s elapsed=%.1f",
            trace_id, weeks_processed, totals, pytime.time() - start_time,
        )

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
