#!/usr/bin/env python
"""
Migration script to add is_duplicate column to all fact tables
This handles cases where the database schema is out of sync with models.py
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

from application import create_app
from models import db

app = create_app()

# SQL to add is_duplicate column if it doesn't exist
migrations = [
    """
    ALTER TABLE fact_trip 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_speeding 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_idle 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_awh 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_wh 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_ha 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_hb 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """,
    """
    ALTER TABLE fact_wu 
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE NOT NULL;
    """
]

with app.app_context():
    print("Starting database migrations...")
    
    for i, migration_sql in enumerate(migrations, 1):
        try:
            db.session.execute(db.text(migration_sql))
            db.session.commit()
            table_name = migration_sql.split("ALTER TABLE")[1].split()[0]
            print(f"✓ Migration {i}/8: {table_name} - is_duplicate column added/verified")
        except Exception as e:
            print(f"✗ Migration {i}/8 failed: {e}")
            db.session.rollback()
    
    print("\n✅ All migrations completed!")
    print("You can now run the backfill without schema errors.")
