import pandas as pd
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import ResourceClosedError
import logging
from models import db

logger = logging.getLogger("DATA_PIPELINE")


def _safe_int(v):
    try:
        return int(v) if pd.notna(v) else None
    except Exception:
        return None


def _safe_float(v):
    try:
        return float(v) if pd.notna(v) else None
    except Exception:
        return None


def store_event_data_to_db(df, app_id, tag_id, event_name):
    logger.info("[DB_STORAGE] FAST MODE ENABLED")

    if df is None or df.empty:
        return {
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
            "invalid_rows_skipped": 0,
            "duplicate_rows_skipped": 0,
        }

    table = {
        "Trip": "fact_trip",
        "Speeding": "fact_speeding",
        "Idle": "fact_idle",
        "AWH": "fact_awh",
        "WH": "fact_wh",
        "HA": "fact_ha",
        "HB": "fact_hb",
        "WU": "fact_wu",
    }.get(event_name)

    if not table:
        return {
            "inserted": 0,
            "skipped": 0,
            "failed": 0,
            "invalid_rows_skipped": 0,
            "duplicate_rows_skipped": 0,
        }

    records = []
    invalid_rows_skipped = failed = 0

    for _, row in df.iterrows():
        try:
            vehicle = str(row.get("Vehicle", "")).strip()
            if not vehicle:
                invalid_rows_skipped += 1
                continue

            driver = (
                str(row.get("Driver", "")).strip()
                or str(row.get("Driver Name", "")).strip()
            )

            address = (
                str(row.get("Address", "")).strip()
                or str(row.get("Start Address", "")).strip()
            )

            # ---------------- TRIP ----------------
            if event_name == "Trip":
                start_dt = pd.to_datetime(row.get("Start Time"), errors="coerce")
                stop_dt = pd.to_datetime(row.get("Stop Time"), errors="coerce")

                if pd.isna(start_dt):
                    invalid_rows_skipped += 1
                    continue

                base = {
                    "app_id": app_id,
                    "tag_id": tag_id,
                    "event_date": start_dt,
                    "start_time": start_dt.time(),
                    "stop_time": stop_dt,
                    "vehicle": vehicle,
                    "driver": driver,
                    "address": address,
                    "duration": row.get("Duration"),
                    "duration_s": _safe_int(row.get("Duration (s)")),
                    "distance_gps": _safe_float(row.get("Distance (GPS)")),
                    "max_speed": _safe_float(row.get("Max Speed")),
                    "avg_speed": _safe_float(row.get("Avg Speed")),
                    "event_state": row.get("Trip/Idle*"),
                    "created_at": datetime.utcnow(),
                }

            # ---------------- NON-TRIP ----------------
            else:
                start_date = pd.to_datetime(row.get("Start Date"), errors="coerce")
                start_time = pd.to_datetime(row.get("Start Time"), errors="coerce")

                if pd.isna(start_date) or pd.isna(start_time):
                    invalid_rows_skipped += 1
                    continue

                event_time = datetime.combine(
                    start_date.date(),
                    start_time.time()
                )

                base = {
                    "app_id": app_id,
                    "tag_id": tag_id,
                    "event_date": start_date.date(),
                    "start_time": start_time.time(),
                    "event_time": event_time,
                    "vehicle": vehicle,
                    "driver": driver,
                    "address": address,
                    "duration": row.get("Duration"),
                    "duration_s": _safe_int(row.get("Duration (s)")),
                    "created_at": datetime.utcnow(),
                }

                if event_name == "Speeding":
                    base.update({
                        "speed": _safe_float(row.get("Speed")),
                        "speed_limit": _safe_float(row.get("Speed Limit")),
                        "over_limit": _safe_float(row.get("Over Limit")),
                        "max_speed": _safe_float(row.get("Max Speed")),
                    })

                elif event_name in {"HA", "HB"}:
                    base["severity"] = row.get("Acceleration") or row.get("Braking")

                elif event_name == "WU":
                    base["violation_type"] = row.get("Event State")

            records.append(base)

        except Exception:
            failed += 1

    if not records:
        return {
            "inserted": 0,
            "skipped": invalid_rows_skipped,
            "failed": failed,
            "invalid_rows_skipped": invalid_rows_skipped,
            "duplicate_rows_skipped": 0,
        }

    cols = records[0].keys()
    sql = f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES ({", ".join(f":{c}" for c in cols)})
        ON CONFLICT DO NOTHING
        RETURNING 1
    """

    try:
        result = db.session.execute(text(sql), records)
        # Some SQLAlchemy/driver executemany paths close RETURNING results automatically.
        try:
            inserted = len(result.fetchall())
        except ResourceClosedError:
            inserted = result.rowcount if result.rowcount and result.rowcount > 0 else 0
        duplicate_skipped = max(0, len(records) - inserted)
        total_skipped = invalid_rows_skipped + duplicate_skipped
        db.session.commit()
        return {
            "inserted": inserted,
            "skipped": total_skipped,
            "failed": failed,
            "invalid_rows_skipped": invalid_rows_skipped,
            "duplicate_rows_skipped": duplicate_skipped,
        }
    except Exception:
        db.session.rollback()
        logger.exception(f"[DB_STORAGE] {event_name} insert failed")
        return {
            "inserted": 0,
            "skipped": invalid_rows_skipped,
            "failed": len(records),
            "invalid_rows_skipped": invalid_rows_skipped,
            "duplicate_rows_skipped": 0,
        }
