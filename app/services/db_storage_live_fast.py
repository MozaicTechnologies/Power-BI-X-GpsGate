import pandas as pd
from datetime import datetime
from sqlalchemy import text
from sqlalchemy.exc import ResourceClosedError
from app.utils.logger import setup_logger
from app.models import db

logger = setup_logger("DATA_PIPELINE")

# Change 2: number of rows per INSERT batch
CHUNK_SIZE = 5_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe(val, cast=None):
    """Return None for NaN/NaT, optionally cast to int or float."""
    try:
        if pd.isnull(val):
            return None
    except (TypeError, ValueError):
        pass
    if cast is None:
        return val
    try:
        return cast(val)
    except (ValueError, TypeError):
        return None


def _col(df, *names):
    """Return first matching column as stripped-string Series; empty strings if missing."""
    for name in names:
        if name in df.columns:
            return df[name].fillna("").astype(str).str.strip()
    return pd.Series([""] * len(df), index=df.index)


def _to_numeric(df, col):
    return pd.to_numeric(df[col] if col in df.columns else None, errors="coerce")


def _to_dt(df, col):
    return pd.to_datetime(df[col] if col in df.columns else None, errors="coerce")


# ---------------------------------------------------------------------------
# Change 1: vectorized record building (itertuples >> iterrows)
# ---------------------------------------------------------------------------

def _build_records(df, app_id, tag_id, event_name, now):
    """
    Build a list of insert-ready dicts using vectorized pandas ops + itertuples.
    itertuples is ~50x faster than iterrows for large DataFrames.
    Returns (records, invalid_rows_skipped).
    """
    df = df.copy()
    invalid = 0

    # --- Validate Vehicle (all event types) ---
    df["v_vehicle"] = _col(df, "Vehicle")
    bad = df["v_vehicle"] == ""
    invalid += int(bad.sum())
    df = df[~bad].reset_index(drop=True)
    if df.empty:
        return [], invalid

    # --- Common string columns ---
    df["v_addr"] = _col(df, "Address", "Start Address")
    df["v_dur"]  = _col(df, "Duration")

    # ---------------------------------------------------------------
    # TRIP
    # ---------------------------------------------------------------
    if event_name == "Trip":
        df["v_start"] = _to_dt(df, "Start Time")
        invalid += int(df["v_start"].isna().sum())
        df = df[df["v_start"].notna()].reset_index(drop=True)
        if df.empty:
            return [], invalid

        df["v_stop"]   = _to_dt(df, "Stop Time")
        df["v_dur_s"]  = _to_numeric(df, "Duration (s)")
        df["v_dist"]   = _to_numeric(df, "Distance (GPS)")
        df["v_maxspd"] = _to_numeric(df, "Max Speed")
        df["v_avgspd"] = _to_numeric(df, "Avg Speed")
        df["v_state"]  = _col(df, "Trip/Idle*")

        records = [
            {
                "app_id":       app_id,
                "tag_id":       tag_id,
                "event_date":   r.v_start,
                "start_time":   r.v_start.time(),
                "stop_time":    _safe(r.v_stop),
                "vehicle":      r.v_vehicle,
                "address":      r.v_addr  or None,
                "duration":     r.v_dur   or None,
                "duration_s":   _safe(r.v_dur_s,  int),
                "distance_gps": _safe(r.v_dist,   float),
                "max_speed":    _safe(r.v_maxspd, float),
                "avg_speed":    _safe(r.v_avgspd, float),
                "event_state":  r.v_state or None,
                "created_at":   now,
            }
            for r in df.itertuples(index=False)
        ]
        return records, invalid

    # ---------------------------------------------------------------
    # NON-TRIP (Speeding / Idle / AWH / WH / HA / HB / WU)
    # ---------------------------------------------------------------
    df["v_driver"] = _col(df, "Driver", "Driver Name")
    df["v_date"]   = _to_dt(df, "Start Date")
    df["v_time"]   = _to_dt(df, "Start Time")

    bad_dt = df["v_date"].isna() | df["v_time"].isna()
    invalid += int(bad_dt.sum())
    df = df[~bad_dt].reset_index(drop=True)
    if df.empty:
        return [], invalid

    df["v_dur_s"] = _to_numeric(df, "Duration (s)")

    # Event-specific numeric/string columns (vectorized)
    if event_name == "Speeding":
        df["v_speed"]  = _to_numeric(df, "Speed")
        df["v_slimit"] = _to_numeric(df, "Speed Limit")
        df["v_over"]   = _to_numeric(df, "Over Limit")
    elif event_name in {"HA", "HB"}:
        df["v_severity"] = _col(df, "Acceleration", "Braking")
    elif event_name == "WU":
        df["v_vtype"] = _col(df, "Event State")

    records = []
    for r in df.itertuples(index=False):
        rec = {
            "app_id":     app_id,
            "tag_id":     tag_id,
            "event_date": r.v_date.date(),
            "start_time": r.v_time.time(),
            "vehicle":    r.v_vehicle,
            "driver":     r.v_driver or None,
            "address":    r.v_addr   or None,
            "duration":   r.v_dur    or None,
            "duration_s": _safe(r.v_dur_s, int),
            "created_at": now,
        }
        if event_name == "Speeding":
            rec["speed"]       = _safe(r.v_speed,  float)
            rec["speed_limit"] = _safe(r.v_slimit, float)
            rec["over_limit"]  = _safe(r.v_over,   float)
        elif event_name in {"HA", "HB"}:
            rec["severity"] = r.v_severity or None
        elif event_name == "WU":
            rec["violation_type"] = r.v_vtype or None

        records.append(rec)

    return records, invalid


# ---------------------------------------------------------------------------
# Change 2: chunked INSERT (commit every CHUNK_SIZE rows)
# ---------------------------------------------------------------------------

def _chunked_insert(records, table, invalid_rows_skipped, event_name):
    cols = list(records[0].keys())
    sql = text(f"""
        INSERT INTO {table} ({", ".join(cols)})
        VALUES ({", ".join(f":{c}" for c in cols)})
        ON CONFLICT DO NOTHING
        RETURNING 1
    """)

    total_inserted = 0
    total_failed   = 0

    for offset in range(0, len(records), CHUNK_SIZE):
        chunk = records[offset : offset + CHUNK_SIZE]
        try:
            result = db.session.execute(sql, chunk)
            try:
                inserted = len(result.fetchall())
            except ResourceClosedError:
                inserted = result.rowcount if result.rowcount and result.rowcount > 0 else 0
            total_inserted += inserted
            db.session.commit()
            logger.debug(
                f"[DB_STORAGE] {event_name} chunk offset={offset} "
                f"size={len(chunk)} inserted={inserted}"
            )
        except Exception:
            db.session.rollback()
            logger.exception(
                f"[DB_STORAGE] {event_name} chunk at offset={offset} failed"
            )
            total_failed += len(chunk)

    duplicate_skipped = max(0, len(records) - total_inserted)
    total_skipped     = invalid_rows_skipped + duplicate_skipped
    return {
        "inserted":              total_inserted,
        "skipped":               total_skipped,
        "failed":                total_failed,
        "invalid_rows_skipped":  invalid_rows_skipped,
        "duplicate_rows_skipped": duplicate_skipped,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def store_event_data_to_db(df, app_id, tag_id, event_name):
    logger.info(f"[DB_STORAGE] {event_name} rows={len(df)} chunk_size={CHUNK_SIZE}")

    if df is None or df.empty:
        return {
            "inserted": 0, "skipped": 0, "failed": 0,
            "invalid_rows_skipped": 0, "duplicate_rows_skipped": 0,
        }

    table = {
        "Trip":     "fact_trip",
        "Speeding": "fact_speeding",
        "Idle":     "fact_idle",
        "AWH":      "fact_awh",
        "WH":       "fact_wh",
        "HA":       "fact_ha",
        "HB":       "fact_hb",
        "WU":       "fact_wu",
    }.get(event_name)

    if not table:
        logger.warning(f"[DB_STORAGE] Unknown event_name={event_name}")
        return {
            "inserted": 0, "skipped": 0, "failed": 0,
            "invalid_rows_skipped": 0, "duplicate_rows_skipped": 0,
        }

    now = datetime.utcnow()
    records, invalid_rows_skipped = _build_records(df, app_id, tag_id, event_name, now)

    logger.info(
        f"[DB_STORAGE] {event_name} valid={len(records)} "
        f"invalid_skipped={invalid_rows_skipped}"
    )

    if not records:
        return {
            "inserted": 0, "skipped": invalid_rows_skipped, "failed": 0,
            "invalid_rows_skipped": invalid_rows_skipped, "duplicate_rows_skipped": 0,
        }

    return _chunked_insert(records, table, invalid_rows_skipped, event_name)
