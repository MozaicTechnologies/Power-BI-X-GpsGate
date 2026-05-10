"""
Database storage router
All event ingestion uses FAST SQL storage
"""

from db_storage_live_fast import store_event_data_to_db

__all__ = ["store_event_data_to_db"]

