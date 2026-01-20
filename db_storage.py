# """
# Database storage functions for all event types with incremental per-week logic.
# Handles deduplication via unique constraints and tracks insertion statistics.
# """

# import pandas as pd
# from datetime import datetime
# from sqlalchemy.exc import IntegrityError
# import logging
# import os
# import sys
# from sqlalchemy.dialects.postgresql import insert

# # Setup logging to file for debugging
# log_dir = os.path.dirname(os.path.abspath(__file__))
# logging.basicConfig(
#     filename=os.path.join(log_dir, 'db_storage.log'),
#     level=logging.DEBUG,
#     format='%(asctime)s - %(levelname)s - %(message)s'
# )

# from models import (
#     db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
# )

# # Detect if using live database (check for localhost = local, anything else = live)
# db_url = os.environ.get('DATABASE_URL', '')
# # USE_LIVE_DATABASE = 'localhost' not in db_url and '127.0.0.1' not in db_url and 'singapore-postgres.render.com' in db_url
# USE_LIVE_DATABASE = os.getenv("FORCE_LIVE_DB", "").lower() == "true"

# print(
#     f"[DEBUG] FORCE_LIVE_DB={os.getenv('FORCE_LIVE_DB')} | USE_LIVE_DATABASE={USE_LIVE_DATABASE}",
#     file=sys.stderr
# )


# print(f"[DEBUG] DATABASE_URL: {db_url[:50] if db_url else 'NOT SET'}...", file=sys.stderr)
# print(f"[DEBUG] USE_LIVE_DATABASE: {USE_LIVE_DATABASE}", file=sys.stderr)


# # Mapping event types to database models and key field configurations
# if USE_LIVE_DATABASE:
#     # ===== LIVE SERVER SCHEMA =====
#     # NOTE: Live database uses different column names and types!
#     # - Uses "location" instead of "address"
#     # - Uses "start_time" (TIME type) instead of "event_time" (TIMESTAMP)
#     EVENT_MODELS = {
#         "Trip": {
#             "model": FactTrip,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "secondary_time_col": "stop_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Distance (GPS)": "distance_gps",
#                 "Max Speed": "max_speed",
#                 "Avg Speed": "avg_speed",
#                 "Event State": "event_state",
#                 "Trip/Idle*": "event_state",
#             },
#         },
#         "Speeding": {
#             "model": FactSpeeding,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",  # <-- LIVE uses start_time, not event_time
#             "vehicle_col": "vehicle",
#             "address_col": "address",  # <-- Still called "address" in our model, but live DB uses "location"
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Speed": "speed",
#                 "Speed Limit": "speed_limit",
#                 "Over Limit": "over_limit",
#             },
#         },
#         "Idle": {
#             "model": FactIdle,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "AWH": {
#             "model": FactAWH,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "WH": {
#             "model": FactWH,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "HA": {
#             "model": FactHA,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Severity": "severity",
#             },
#         },
#         "HB": {
#             "model": FactHB,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Severity": "severity",
#             },
#         },
#         "WU": {
#             "model": FactWU,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Violation Type": "violation_type",
#             },
#         },
#     }
# else:
#     # ===== LOCAL TEST SCHEMA =====
#     EVENT_MODELS = {
#         "Trip": {
#             "model": FactTrip,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "secondary_time_col": "stop_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Distance (GPS)": "distance_gps",
#                 "Max Speed": "max_speed",
#                 "Avg Speed": "avg_speed",
#                 "Event State": "event_state",
#                 "Trip/Idle*": "event_state",
#             },
#         },
#         "Speeding": {
#             "model": FactSpeeding,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Speed": "speed",
#                 "Speed Limit": "speed_limit",
#                 "Over Limit": "over_limit",
#             },
#         },
#         "Idle": {
#             "model": FactIdle,
#             "date_col": "event_date",
#             "primary_time_col": "start_time",
#             "secondary_time_col": "stop_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "AWH": {
#             "model": FactAWH,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "WH": {
#             "model": FactWH,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Duration": "duration",
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#             },
#         },
#         "HA": {
#             "model": FactHA,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Severity": "severity",
#             },
#         },
#         "HB": {
#             "model": FactHB,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Severity": "severity",
#             },
#         },
#         "WU": {
#             "model": FactWU,
#             "date_col": "event_date",
#             "primary_time_col": "event_time",
#             "vehicle_col": "vehicle",
#             "column_mapping": {
#                 "Vehicle": "vehicle",
#                 "Address": "address",
#                 "Violation Type": "violation_type",
#             },
#         },
#     }


# def normalize_datetime_string(dt_string):
#     """Convert various datetime formats to datetime object"""
#     if pd.isna(dt_string) or dt_string is None or dt_string == "":
#         return None
    
#     if isinstance(dt_string, datetime):
#         return dt_string
    
#     dt_string = str(dt_string).strip()
    
#     try:
#         # Try common formats - ordered by specificity
#         formats_to_try = [
#             "%m/%d/%Y %H:%M:%S",   # 01/01/2025 05:01:00
#             "%m/%d/%Y %H:%M",      # 01/01/2025 05:01
#             "%Y-%m-%d %H:%M:%S",   # 2025-01-01 05:01:00
#             "%Y-%m-%d %H:%M",      # 2025-01-01 05:01
#             "%d/%m/%Y %H:%M:%S",   # 01/01/2025 05:01:00
#             "%d/%m/%Y %H:%M",      # 01/01/2025 05:01
#         ]
        
#         for fmt in formats_to_try:
#             try:
#                 result = datetime.strptime(dt_string, fmt)
#                 return result
#             except ValueError:
#                 continue
        
#         # Fallback: try pandas parsing (handles many formats)
#         parsed = pd.to_datetime(dt_string, errors='coerce')
#         if pd.notna(parsed):
#             return parsed.to_pydatetime()
        
#         return None
#     except:
#         return None


# def store_event_data_to_db(df, app_id, tag_id, event_name):
#     if df is None or df.empty:
#         return {"inserted": 0, "skipped": 0, "failed": 0}

#     config = EVENT_MODELS.get(event_name)
#     if not config:
#         return {"inserted": 0, "skipped": 0, "failed": 0}

#     model = config["model"]
#     records = df.to_dict(orient="records")

#     try:
#         stmt = insert(model).values(records)

#         # Deduplication keys (DB is source of truth)
#         conflict_cols = ["app_id", "tag_id", "event_date"]

#         if "start_time" in df.columns:
#             conflict_cols.append("start_time")
#         elif "event_time" in df.columns:
#             conflict_cols.append("event_time")

#         if "vehicle" in df.columns:
#             conflict_cols.append("vehicle")

#         stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

#         result = db.session.execute(stmt)
#         db.session.commit()

#         inserted = result.rowcount
#         skipped = len(records) - inserted

#         return {
#             "inserted": inserted,
#             "skipped": skipped,
#             "failed": 0,
#         }

#     except Exception as e:
#         db.session.rollback()
#         print(f"[DB ERROR] {event_name}: {e}", file=sys.stderr)
#         return {
#             "inserted": 0,
#             "skipped": 0,
#             "failed": len(records),
#         }



# def get_event_model(event_name):
#     """Get the database model for an event type"""
#     if event_name in EVENT_MODELS:
#         return EVENT_MODELS[event_name]["model"]
#     return None


# def get_stored_event_count(event_name, app_id=None, tag_id=None):
#     """Get count of records stored for an event type"""
#     model = get_event_model(event_name)
#     if not model:
#         return 0
    
#     query = db.session.query(model)
#     if app_id:
#         query = query.filter_by(app_id=app_id)
#     if tag_id:
#         query = query.filter_by(tag_id=tag_id)
    
#     return query.count()



# """
# Database storage router + ORM fallback
# """

# import os
# import sys
# import pandas as pd
# from datetime import datetime
# from sqlalchemy.dialects.postgresql import insert
# from sqlalchemy.exc import IntegrityError

# from models import (
#     db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
# )

# # --------------------------------------------------
# # 🔀 STORAGE ROUTER (STEP 2 – FINAL)
# # --------------------------------------------------

# FORCE_LIVE_DB = os.getenv("FORCE_LIVE_DB", "").lower() == "true"

# print(
#     f"[DB_STORAGE_ROUTER] FORCE_LIVE_DB={os.getenv('FORCE_LIVE_DB')} | use_live={FORCE_LIVE_DB}",
#     file=sys.stderr
# )

# # --------------------------------------------------
# # PUBLIC ENTRY POINT (USED BY data_pipeline.py)
# # --------------------------------------------------

# def store_event_data_to_db(df, app_id, tag_id, event_name):
#     """
#     Single entry point.
#     Routes automatically to FAST live DB or ORM local DB.
#     """

#     if FORCE_LIVE_DB:
#         from db_storage_live_fast import store_event_data_to_db as live_store
#         return live_store(df, app_id, tag_id, event_name, db)
#     else:
#         return store_event_data_to_db_orm(df, app_id, tag_id, event_name)

# # --------------------------------------------------
# # ORM STORAGE (LOCAL / FALLBACK)
# # --------------------------------------------------

# EVENT_MODELS = {
#     "Trip": FactTrip,
#     "Speeding": FactSpeeding,
#     "Idle": FactIdle,
#     "AWH": FactAWH,
#     "WH": FactWH,
#     "HA": FactHA,
#     "HB": FactHB,
#     "WU": FactWU,
# }

# def store_event_data_to_db_orm(df, app_id, tag_id, event_name):
#     if df is None or df.empty:
#         return {"inserted": 0, "skipped": 0, "failed": 0}

#     model = EVENT_MODELS.get(event_name)
#     if not model:
#         return {"inserted": 0, "skipped": 0, "failed": 0}

#     records = df.to_dict(orient="records")

#     try:
#         stmt = insert(model).values(records)

#         conflict_cols = ["app_id", "tag_id", "event_date"]

#         if "start_time" in df.columns:
#             conflict_cols.append("start_time")
#         if "vehicle" in df.columns:
#             conflict_cols.append("vehicle")

#         stmt = stmt.on_conflict_do_nothing(index_elements=conflict_cols)

#         result = db.session.execute(stmt)
#         db.session.commit()

#         inserted = result.rowcount
#         skipped = len(records) - inserted

#         return {
#             "inserted": inserted,
#             "skipped": skipped,
#             "failed": 0,
#         }

#     except Exception as e:
#         db.session.rollback()
#         print(f"[DB ORM ERROR] {event_name}: {e}", file=sys.stderr)
#         return {
#             "inserted": 0,
#             "skipped": 0,
#             "failed": len(records),
#         }


# db_storage.py
"""
Database storage router
All event ingestion uses FAST SQL storage
"""

from db_storage_live_fast import store_event_data_to_db

__all__ = ["store_event_data_to_db"]

