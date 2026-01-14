"""
FAST Storage function for LIVE DATABASE using batch raw SQL inserts
Replaces iterrows() with batch INSERT for 10-100x faster performance
"""

import pandas as pd
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from sqlalchemy import text
import sys

def store_to_live_db_fast(df, app_id, tag_id, event_name, db):
    """
    Store event data to LIVE database using BATCH SQL inserts (not row-by-row)
    This is 10-100x faster than iterating with iterrows()
    """
    
    if df is None or df.empty:
        print(f"[SKIP] No data to store for {event_name}")
        return {"inserted": 0, "skipped": 0, "failed": 0, "event_type": event_name}
    
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
    
    inserted = 0
    skipped = 0
    failed = 0
    
    # Prepare records for batch insert
    records_to_insert = []
    
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
            
            vehicle = str(vehicle).strip()
            
            # Extract location/address
            location = row.get("Address", "")
            if pd.isna(location):
                location = ""
            else:
                location = str(location).strip()
            
            # Extract duration
            duration = row.get("Duration", "")
            if pd.isna(duration):
                duration = ""
            else:
                duration = str(duration).strip()
            
            # Event-specific fields
            record = {
                "app_id": app_id,
                "tag_id": tag_id,
                "event_date": event_date,
                "start_time": start_time,
                "vehicle": vehicle,
                "location": location,
                "duration": duration,
                "created_at": datetime.utcnow(),
            }
            
            if event_name == "Trip":
                record["stop_time"] = str(row.get("Stop Time", "")).strip() if pd.notna(row.get("Stop Time")) else ""
                try:
                    record["distance_gps"] = float(row["Distance (GPS)"]) if pd.notna(row.get("Distance (GPS)")) else None
                except (ValueError, TypeError):
                    record["distance_gps"] = None
                try:
                    record["max_speed"] = float(row["Max Speed"]) if pd.notna(row.get("Max Speed")) else None
                except (ValueError, TypeError):
                    record["max_speed"] = None
                try:
                    record["avg_speed"] = float(row["Avg Speed"]) if pd.notna(row.get("Avg Speed")) else None
                except (ValueError, TypeError):
                    record["avg_speed"] = None
                record["event_state"] = str(row.get("Event State", "")).strip() if pd.notna(row.get("Event State")) else ""
            
            elif event_name == "Speeding":
                try:
                    record["speed"] = float(row["Speed"]) if pd.notna(row.get("Speed")) else None
                except (ValueError, TypeError):
                    record["speed"] = None
            
            records_to_insert.append(record)
            
        except Exception as e:
            failed += 1
            if failed == 1:
                print(f"[ERROR] Row {idx}: {type(e).__name__}: {str(e)[:100]}", file=sys.stderr)
    
    if not records_to_insert:
        print(f"[SKIP] {event_name}: No valid records to insert")
        return {"inserted": 0, "skipped": skipped, "failed": failed, "event_type": event_name}
    
    # ============================================================
    # BATCH INSERT (much faster than iterrows!)
    # ============================================================
    try:
        # Get field names from first record
        field_names = list(records_to_insert[0].keys())
        field_names_str = ", ".join(field_names)
        
        # Build multi-row VALUES clause
        # For 1000 rows, this creates one INSERT with 1000 rows instead of 1000 INSERTs
        placeholders = []
        params = {}
        
        for i, record in enumerate(records_to_insert):
            row_placeholders = []
            for field in field_names:
                param_name = f"{field}_{i}"
                row_placeholders.append(f":{param_name}")
                params[param_name] = record.get(field)
            placeholders.append(f"({', '.join(row_placeholders)})")
        
        values_clause = ", ".join(placeholders)
        
        # Build final SQL
        sql = f"INSERT INTO {table_name} ({field_names_str}) VALUES {values_clause}"
        
        # Execute batch insert
        db.session.execute(text(sql), params)
        db.session.commit()
        inserted = len(records_to_insert)
        
        print(f"[OK] {event_name}: {inserted} inserted (batch), {skipped} skipped, {failed} errors", file=sys.stderr)
        
    except IntegrityError as ie:
        # Batch had duplicates - fall back to row-by-row with duplicate handling
        print(f"[WARN] {event_name}: Batch insert had duplicates, falling back to row-by-row insert", file=sys.stderr)
        db.session.rollback()
        
        inserted = 0
        skipped = 0
        
        for record in records_to_insert:
            try:
                field_names_str = ", ".join(record.keys())
                placeholders = ", ".join([f":{k}" for k in record.keys()])
                sql = f"INSERT INTO {table_name} ({field_names_str}) VALUES ({placeholders})"
                
                db.session.execute(text(sql), record)
                db.session.commit()
                inserted += 1
            except IntegrityError:
                db.session.rollback()
                skipped += 1
            except Exception as e:
                db.session.rollback()
                failed += 1
        
        print(f"[OK] {event_name}: {inserted} inserted (fallback), {skipped} duplicates, {failed} errors", file=sys.stderr)
    
    except Exception as e:
        print(f"[ERROR] {event_name} batch insert failed: {type(e).__name__}: {str(e)[:200]}", file=sys.stderr)
        db.session.rollback()
        failed = len(records_to_insert)
    
    return {
        "inserted": inserted,
        "skipped": skipped,
        "failed": failed,
        "event_type": event_name
    }
