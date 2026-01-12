#!/usr/bin/env python
"""Clear all fact tables and backfill with UPSERT logic"""
import os
from dotenv import load_dotenv

load_dotenv()

from application import create_app, db
from models import (
    FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
)

app = create_app()

print("=" * 80)
print("CLEARING ALL FACT TABLES")
print("=" * 80)

with app.app_context():
    tables = [
        ("fact_trip", FactTrip),
        ("fact_speeding", FactSpeeding),
        ("fact_idle", FactIdle),
        ("fact_awh", FactAWH),
        ("fact_wh", FactWH),
        ("fact_ha", FactHA),
        ("fact_hb", FactHB),
        ("fact_wu", FactWU),
    ]
    
    for table_name, model in tables:
        try:
            count = db.session.query(model).count()
            db.session.query(model).delete()
            db.session.commit()
            print(f"[OK] {table_name}: Deleted {count} records")
        except Exception as e:
            db.session.rollback()
            print(f"[ERROR] {table_name}: {str(e)}")

print("\n" + "=" * 80)
print("TABLES CLEARED - Ready for fresh backfill")
print("=" * 80)
print("\nNow run:")
print("  venv\\Scripts\\python.exe backfill_direct_python.py")
