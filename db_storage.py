"""
Database storage functions for all event types with incremental per-week logic.
Handles deduplication via unique constraints and tracks insertion statistics.
"""

import pandas as pd
from datetime import datetime
from sqlalchemy.exc import IntegrityError
import logging
import os
import sys

# Setup logging to file for debugging
log_dir = os.path.dirname(os.path.abspath(__file__))
logging.basicConfig(
    filename=os.path.join(log_dir, 'db_storage.log'),
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

from models import (
    db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
)

# Detect if using live database (check for localhost = local, anything else = live)
db_url = os.environ.get('DATABASE_URL', '')
USE_LIVE_DATABASE = 'localhost' not in db_url and '127.0.0.1' not in db_url and 'singapore-postgres.render.com' in db_url
print(f"[DEBUG] DATABASE_URL: {db_url[:50] if db_url else 'NOT SET'}...", file=sys.stderr)
print(f"[DEBUG] USE_LIVE_DATABASE: {USE_LIVE_DATABASE}", file=sys.stderr)


# Mapping event types to database models and key field configurations
if USE_LIVE_DATABASE:
    # ===== LIVE SERVER SCHEMA =====
    # NOTE: Live database uses different column names and types!
    # - Uses "location" instead of "address"
    # - Uses "start_time" (TIME type) instead of "event_time" (TIMESTAMP)
    EVENT_MODELS = {
        "Trip": {
            "model": FactTrip,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "secondary_time_col": "stop_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
                "Distance (GPS)": "distance_gps",
                "Max Speed": "max_speed",
                "Avg Speed": "avg_speed",
                "Event State": "event_state",
                "Trip/Idle*": "event_state",
            },
        },
        "Speeding": {
            "model": FactSpeeding,
            "date_col": "event_date",
            "primary_time_col": "start_time",  # <-- LIVE uses start_time, not event_time
            "vehicle_col": "vehicle",
            "address_col": "address",  # <-- Still called "address" in our model, but live DB uses "location"
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
                "Speed": "speed",
                "Speed Limit": "speed_limit",
                "Over Limit": "over_limit",
            },
        },
        "Idle": {
            "model": FactIdle,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "AWH": {
            "model": FactAWH,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "WH": {
            "model": FactWH,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "HA": {
            "model": FactHA,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Severity": "severity",
            },
        },
        "HB": {
            "model": FactHB,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Severity": "severity",
            },
        },
        "WU": {
            "model": FactWU,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Violation Type": "violation_type",
            },
        },
    }
else:
    # ===== LOCAL TEST SCHEMA =====
    EVENT_MODELS = {
        "Trip": {
            "model": FactTrip,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "secondary_time_col": "stop_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
                "Distance (GPS)": "distance_gps",
                "Max Speed": "max_speed",
                "Avg Speed": "avg_speed",
                "Event State": "event_state",
                "Trip/Idle*": "event_state",
            },
        },
        "Speeding": {
            "model": FactSpeeding,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
                "Speed": "speed",
                "Speed Limit": "speed_limit",
                "Over Limit": "over_limit",
            },
        },
        "Idle": {
            "model": FactIdle,
            "date_col": "event_date",
            "primary_time_col": "start_time",
            "secondary_time_col": "stop_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "AWH": {
            "model": FactAWH,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "WH": {
            "model": FactWH,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Duration": "duration",
                "Vehicle": "vehicle",
                "Address": "address",
            },
        },
        "HA": {
            "model": FactHA,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Severity": "severity",
            },
        },
        "HB": {
            "model": FactHB,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Severity": "severity",
            },
        },
        "WU": {
            "model": FactWU,
            "date_col": "event_date",
            "primary_time_col": "event_time",
            "vehicle_col": "vehicle",
            "column_mapping": {
                "Vehicle": "vehicle",
                "Address": "address",
                "Violation Type": "violation_type",
            },
        },
    }


def normalize_datetime_string(dt_string):
    """Convert various datetime formats to datetime object"""
    if pd.isna(dt_string) or dt_string is None or dt_string == "":
        return None
    
    if isinstance(dt_string, datetime):
        return dt_string
    
    dt_string = str(dt_string).strip()
    
    try:
        # Try common formats - ordered by specificity
        formats_to_try = [
            "%m/%d/%Y %H:%M:%S",   # 01/01/2025 05:01:00
            "%m/%d/%Y %H:%M",      # 01/01/2025 05:01
            "%Y-%m-%d %H:%M:%S",   # 2025-01-01 05:01:00
            "%Y-%m-%d %H:%M",      # 2025-01-01 05:01
            "%d/%m/%Y %H:%M:%S",   # 01/01/2025 05:01:00
            "%d/%m/%Y %H:%M",      # 01/01/2025 05:01
        ]
        
        for fmt in formats_to_try:
            try:
                result = datetime.strptime(dt_string, fmt)
                return result
            except ValueError:
                continue
        
        # Fallback: try pandas parsing (handles many formats)
        parsed = pd.to_datetime(dt_string, errors='coerce')
        if pd.notna(parsed):
            return parsed.to_pydatetime()
        
        return None
    except:
        return None


def store_event_data_to_db(df, app_id, tag_id, event_name):
    """
    Store event data to appropriate database table with incremental logic.
    Maps CSV column names to database column names.
    
    Args:
        df (DataFrame): Cleaned event data from GpsGate CSV
        app_id (str): Application ID
        tag_id (str): Tag ID
        event_name (str): Event type ("Trip", "Speeding", "Idle", "AWH", "WH", "HA", "HB", "WU")
    
    Returns:
        dict: {
            "inserted": int,
            "skipped": int,  # Due to duplicate unique constraint
            "failed": int,   # Other database errors
            "event_type": str
        }
    """
    
    # Use raw SQL storage for ALL events on LIVE database (ORM models may have schema mismatch)
    if USE_LIVE_DATABASE:
        import sys
        print(f"[DB_STORAGE] Using db_storage_live_fast.py for {event_name} (LIVE database with BATCH raw SQL)", file=sys.stderr)
        from db_storage_live_fast import store_to_live_db_fast
        return store_to_live_db_fast(df, app_id, tag_id, event_name, db)
    
    if event_name not in EVENT_MODELS:
        print(f"❌ Unknown event type: {event_name}")
        return {"inserted": 0, "skipped": 0, "failed": 0, "event_type": event_name}
    
    if df is None or df.empty:
        print(f"[SKIP] No data to store for {event_name}")
        return {"inserted": 0, "skipped": 0, "failed": 0, "event_type": event_name}
    
    config = EVENT_MODELS[event_name]
    model = config["model"]
    
    # Get model-specific column mapping
    column_mapping = config.get("column_mapping", {})
    
    inserted = 0
    skipped = 0
    failed = 0
    first_error_printed = False
    
    import sys
    if len(df) > 0 and event_name == "Trip":
        print(f"[DEBUG] {event_name}: Processing {len(df)} rows", file=sys.stderr)
        print(f"[DEBUG] {event_name}: Available columns: {df.columns.tolist()}", file=sys.stderr)
        print(f"[DEBUG] {event_name}: First row: {df.iloc[0].to_dict()}", file=sys.stderr)
    
    logging.info(f"\n{'='*70}")
    logging.info(f"STORING: {event_name} - {len(df)} rows")
    logging.info(f"Columns: {list(df.columns)}")
    logging.info(f"{'='*70}")
    
    try:
        # Use PostgreSQL's ON CONFLICT DO UPDATE for upsert behavior
        for idx, row in df.iterrows():
            try:
                # Prepare record data
                record = {
                    "app_id": app_id,
                    "tag_id": tag_id,
                }
                
                # ============================================
                # EXTRACT AND SET EVENT_DATE (REQUIRED)
                # ============================================
                event_date = None
                
                # Strategy 1: Use "Start Date" column (don't use "Start Time" alone - it defaults to today!)
                if "Start Date" in df.columns and pd.notna(row.get("Start Date")):
                    dt = normalize_datetime_string(row["Start Date"])
                    if dt:
                        event_date = dt
                
                # Strategy 2: Look for "Event Date" column
                if event_date is None and "Event Date" in df.columns and pd.notna(row.get("Event Date")):
                    dt = normalize_datetime_string(row["Event Date"])
                    if dt:
                        event_date = dt
                
                # Strategy 3: Extract date from "Start Time" column (Trip events have date embedded)
                if event_date is None and "Start Time" in df.columns and pd.notna(row.get("Start Time")):
                    dt = normalize_datetime_string(row["Start Time"])
                    if dt:
                        event_date = dt
                
                # Fallback: Skip this row if we can't get event_date
                if event_date is None:
                    skipped += 1
                    continue
                
                record["event_date"] = event_date
                
                # ============================================
                # EXTRACT AND SET PRIMARY TIME COLUMN
                # ============================================
                primary_time_col = config.get("primary_time_col")
                
                if primary_time_col == "start_time":
                    # For Trip, Idle: Use Start Time or Start Date + Start Time
                    start_time_value = None
                    
                    # Try combining Start Date + Start Time
                    if "Start Date" in df.columns and "Start Time" in df.columns:
                        sd = row.get("Start Date")
                        st = row.get("Start Time")
                        if pd.notna(sd) and pd.notna(st):
                            combined = f"{str(sd).strip()} {str(st).strip()}"
                            dt = normalize_datetime_string(combined)
                            if dt:
                                start_time_value = dt
                    
                    # Fallback: Just use Start Time
                    if start_time_value is None and "Start Time" in df.columns:
                        dt = normalize_datetime_string(row.get("Start Time"))
                        if dt:
                            start_time_value = dt
                    
                    if start_time_value:
                        record["start_time"] = start_time_value
                    else:
                        if event_name == "Trip" and skipped < 3:
                            print(f"[DEBUG SKIP] {event_name} row {idx}: Missing start_time", file=sys.stderr)
                        skipped += 1
                        continue
                
                elif primary_time_col == "event_time":
                    # For Speeding, AWH, WH, HA, HB, WU: Use event_time
                    event_time_value = None
                    
                    # Try combining Start Date + Start Time first
                    if "Start Date" in df.columns and "Start Time" in df.columns:
                        sd = row.get("Start Date")
                        st = row.get("Start Time")
                        if pd.notna(sd) and pd.notna(st):
                            combined = f"{str(sd).strip()} {str(st).strip()}"
                            dt = normalize_datetime_string(combined)
                            if dt:
                                event_time_value = dt
                    
                    # Try "Event Time" column
                    if event_time_value is None and "Event Time" in df.columns:
                        dt = normalize_datetime_string(row.get("Event Time"))
                        if dt:
                            event_time_value = dt
                    
                    # Try "Start Time" column
                    if event_time_value is None and "Start Time" in df.columns:
                        dt = normalize_datetime_string(row.get("Start Time"))
                        if dt:
                            event_time_value = dt
                    
                    if event_time_value:
                        record["event_time"] = event_time_value
                    else:
                        # event_time is NOT NULL for these models
                        skipped += 1
                        continue
                
                # ============================================
                # EXTRACT SECONDARY TIME COLUMN (if needed)
                # ============================================
                if "secondary_time_col" in config:
                    secondary_col = config["secondary_time_col"]
                    if secondary_col == "stop_time" and "Stop Time" in df.columns:
                        dt = normalize_datetime_string(row.get("Stop Time"))
                        if dt:
                            record["stop_time"] = dt
                
                # ============================================
                # MAP ALL OTHER COLUMNS
                # ============================================
                for csv_col, db_col in column_mapping.items():
                    # Skip if already set above
                    if db_col in record:
                        continue
                    
                    # Skip if column not in dataframe
                    if csv_col not in df.columns:
                        continue
                    
                    # Skip if value is null
                    if pd.isna(row.get(csv_col)):
                        continue
                    
                    value = row[csv_col]
                    
                    # Handle numeric conversions for speed/distance fields
                    if db_col in ["distance_gps", "max_speed", "avg_speed", "speed", "speed_limit", "over_limit"]:
                        try:
                            value = float(value) if value else None
                            if value is None:
                                continue
                        except:
                            continue
                    
                    if value is not None:
                        record[db_col] = value
                
                # ============================================
                # UPSERT RECORD OR FLAG AS DUPLICATE
                # ============================================
                try:
                    # First attempt: insert as new record with is_duplicate=False
                    record["is_duplicate"] = False
                    db_record = model(**record)
                    db.session.add(db_record)
                    db.session.flush()  # Flush to detect duplicates immediately
                    inserted += 1
                except IntegrityError as ie:
                    # Duplicate detected OR other integrity issue - update existing record to flag it
                    db.session.rollback()
                    
                    # Build WHERE clause for the duplicate record
                    config = EVENT_MODELS[event_name]
                    primary_key_cols = ['app_id', 'tag_id', 'event_date']
                    primary_time_col = config.get("primary_time_col")
                    
                    if primary_time_col and primary_time_col in record:
                        primary_key_cols.append(primary_time_col)
                    
                    # Add vehicle if present
                    if "vehicle" in record:
                        primary_key_cols.append("vehicle")
                    
                    # Build update query
                    try:
                        from sqlalchemy import update
                        stmt = update(model)
                        
                        for col_name in primary_key_cols:
                            if col_name in record:
                                stmt = stmt.where(getattr(model, col_name) == record[col_name])
                        
                        # Flag as duplicate
                        stmt = stmt.values(is_duplicate=True)
                        result = db.session.execute(stmt)
                        db.session.commit()
                        
                        # Only count as skipped if a record was actually updated
                        if result.rowcount > 0:
                            skipped += 1
                        else:
                            # IntegrityError but no record found to update - might be a real error
                            failed += 1
                    except Exception as update_err:
                        db.session.rollback()
                        failed += 1
                
                except Exception as ie:
                    # Other database errors
                    db.session.rollback()
                    failed += 1
                    if not first_error_printed:
                        print(f"\n❌ ERROR creating {event_name} model at row {idx}:", file=sys.stderr)
                        print(f"  Exception: {type(ie).__name__}: {str(ie)[:100]}", file=sys.stderr)
                        first_error_printed = True
                    
            except Exception as e:
                # Outer exception handler for row iteration errors
                failed += 1
                if not first_error_printed:
                    print(f"\n❌ ERROR at row {idx}:", file=sys.stderr)
                    print(f"  Exception: {type(e).__name__}: {str(e)[:100]}", file=sys.stderr)
                    first_error_printed = True
        
        # ============================================
        # COMMIT ALL SUCCESSFUL RECORDS
        # ============================================
        if inserted > 0:
            try:
                import sys
                print(f"[DB_STORAGE] About to commit {inserted} records for {event_name}...", file=sys.stderr)
                db.session.commit()
                print(f"[DB_STORAGE] Successfully committed {inserted} records for {event_name}", file=sys.stderr)
            except Exception as e:
                # Final commit error
                import sys
                db.session.rollback()
                print(f"\n❌ Error committing {event_name}:", file=sys.stderr)
                print(f"  {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
                print(f"[DB_STORAGE] Rolled back {inserted} pending records due to commit error", file=sys.stderr)
                failed += inserted
                inserted = 0
        else:
            # No inserts, but still verify session is clean
            try:
                db.session.rollback()
            except:
                pass
        
    except Exception as e:
        print(f"❌ Unexpected error during processing for {event_name}: {type(e).__name__}: {str(e)}", file=sys.stderr)
        db.session.rollback()

    stats = {
        "inserted": inserted,
        "skipped": skipped,
        "failed": failed,
        "event_type": event_name
    }
    
    if inserted > 0:
        print(f"[OK] {event_name}: {inserted} inserted, {skipped} duplicates, {failed} errors")
    elif skipped > 0:
        print(f"[SKIP] {event_name}: {skipped} duplicates, {failed} errors")
    else:
        print(f"[FAILED] {event_name}: {failed} errors, {skipped} duplicates")
    
    return stats


def get_event_model(event_name):
    """Get the database model for an event type"""
    if event_name in EVENT_MODELS:
        return EVENT_MODELS[event_name]["model"]
    return None


def get_stored_event_count(event_name, app_id=None, tag_id=None):
    """Get count of records stored for an event type"""
    model = get_event_model(event_name)
    if not model:
        return 0
    
    query = db.session.query(model)
    if app_id:
        query = query.filter_by(app_id=app_id)
    if tag_id:
        query = query.filter_by(tag_id=tag_id)
    
    return query.count()
