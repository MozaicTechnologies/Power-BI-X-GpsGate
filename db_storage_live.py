"""
Storage function for LIVE DATABASE using raw SQL
The live database has a different schema than our local test database
"""

import pandas as pd
from datetime import datetime, time
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
import sys

def store_to_live_db(df, app_id, tag_id, event_name, db):
    """
    Store event data directly to LIVE database using raw SQL
    Handles the live database schema differences
    """
    
    if df is None or df.empty:
        print(f"[SKIP] No data to store for {event_name}")
        return {"inserted": 0, "skipped": 0, "failed": 0, "event_type": event_name}
    
    inserted = 0
    skipped = 0
    failed = 0
    first_error_printed = False
    
    table_name = {
        "Trip": "fact_trip",
        "Speeding": "fact_speeding",
        "Idle": "fact_idle",
        "AWH": "fact_awh",
        "WH": "fact_wh",
        "HA": "fact_ha",
        "HB": "fact_hb",
        "WU": "fact_wu",
    }.get(event_name, "")
    
    if not table_name:
        print(f"❌ Unknown event type: {event_name}")
        return {"inserted": 0, "skipped": 0, "failed": 0, "event_type": event_name}
    
    # Column mapping for live database (different from local DB!)
    columns_mapping = {
        "Trip": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "stop_time": "stop_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "address": "address",
            "distance_gps": "distance_gps",
            "max_speed": "max_speed",
            "avg_speed": "avg_speed",
            "event_state": "event_state",
        },
        "Speeding": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",  # TIME type, but we'll insert as time
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",  # Map "address" from API to "location" in live DB
            "max_speed": "speed",   # Use max_speed column for Speed data
        },
        "Idle": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
        "AWH": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
        "WH": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
        "HA": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
        "HB": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
        "WU": {
            "app_id": "app_id",
            "tag_id": "tag_id",
            "event_date": "event_date",
            "start_time": "start_time",
            "duration": "duration",
            "vehicle": "vehicle",
            "location": "address",
        },
    }
    
    col_map = columns_mapping.get(event_name, {})
    
    try:
        for idx, row in df.iterrows():
            try:
                # Extract event_date (REQUIRED)
                event_date = None
                if "Start Date" in df.columns and pd.notna(row.get("Start Date")):
                    event_date = str(row["Start Date"]).strip()
                
                if not event_date:
                    skipped += 1
                    continue
                
                # Extract start_time (TIME type for live DB)
                start_time = None
                if "Start Time" in df.columns and pd.notna(row.get("Start Time")):
                    start_time = str(row["Start Time"]).strip()
                
                if not start_time:
                    skipped += 1
                    continue
                
                # Extract vehicle (REQUIRED)
                vehicle = row.get("Vehicle")
                if pd.isna(vehicle) or not vehicle:
                    skipped += 1
                    continue
                
                # Extract location/address
                location = row.get("Address", "")
                if pd.isna(location):
                    location = ""
                
                # Extract duration
                duration = row.get("Duration", "")
                if pd.isna(duration):
                    duration = ""
                
                # Event-specific fields
                extra_fields = {}
                if event_name == "Trip":
                    extra_fields = {
                        "stop_time": row.get("Stop Time", ""),
                        "distance_gps": float(row["Distance (GPS)"]) if pd.notna(row.get("Distance (GPS)")) else None,
                        "max_speed": float(row["Max Speed"]) if pd.notna(row.get("Max Speed")) else None,
                        "avg_speed": float(row["Avg Speed"]) if pd.notna(row.get("Avg Speed")) else None,
                        "event_state": row.get("Event State", ""),
                    }
                elif event_name == "Speeding":
                    extra_fields = {
                        "max_speed": float(row["Speed"]) if pd.notna(row.get("Speed")) else None,
                    }
                
                # Build INSERT statement
                fields = ["app_id", "tag_id", "event_date", "start_time", "vehicle", "location", "duration"]
                values = [app_id, tag_id, event_date, start_time, vehicle, location, duration]
                
                for field_name, field_value in extra_fields.items():
                    if field_value is not None:
                        fields.append(field_name)
                        values.append(field_value)
                
                # Add created_at
                fields.append("created_at")
                values.append(datetime.utcnow())
                
                # Build SQL
                placeholders = ",".join([f":{field}" for field in fields])
                field_names = ",".join(fields)
                sql = f"INSERT INTO {table_name} ({field_names}) VALUES ({placeholders})"
                
                # Prepare parameters
                params = {field: value for field, value in zip(fields, values)}
                
                # Execute
                db.session.execute(text(sql), params)
                inserted += 1
                
            except IntegrityError as ie:
                # Duplicate
                db.session.rollback()
                skipped += 1
                if not first_error_printed:
                    print(f"\n[SKIP] Row {idx} - Duplicate: {str(ie)[:100]}", file=sys.stderr)
                    first_error_printed = True
            except Exception as e:
                # Other error
                db.session.rollback()
                failed += 1
                if not first_error_printed:
                    print(f"\n[ERROR] Row {idx}: {type(e).__name__}", file=sys.stderr)
                    print(f"  SQL: {sql[:200]}", file=sys.stderr)
                    print(f"  Params: {params}", file=sys.stderr)
                    print(f"  Error: {str(e)[:300]}", file=sys.stderr)
                    first_error_printed = True
        
        # Commit all inserts
        db.session.commit()
        
    except Exception as e:
        print(f"[ERROR] {event_name}: {type(e).__name__}: {str(e)}", file=sys.stderr)
        db.session.rollback()
    
    if inserted > 0:
        print(f"[OK] {event_name}: {inserted} inserted, {skipped} duplicates, {failed} errors")
    elif skipped > 0:
        print(f"[SKIP] {event_name}: {skipped} duplicates, {failed} errors")
    else:
        print(f"[FAILED] {event_name}: {failed} errors, {skipped} duplicates")
    
    return {
        "inserted": inserted,
        "skipped": skipped,
        "failed": failed,
        "event_type": event_name
    }
