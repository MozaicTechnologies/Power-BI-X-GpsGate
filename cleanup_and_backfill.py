#!/usr/bin/env python3
"""
Cleanup Script: Delete all fact table data except render and result
Then fetch fresh data for 2025 Week 1
"""
import os
import sys
sys.path.append('.')

from dotenv import load_dotenv
load_dotenv()

from models import db
from config import Config
from flask import Flask
from sqlalchemy import text

def main():
    app = Flask(__name__)
    app.config.from_object(Config)
    db.init_app(app)
    
    with app.app_context():
        print("=" * 80)
        print("CLEANUP: DELETE ALL FACT TABLE DATA")
        print("=" * 80)
        
        # List of fact tables to clean
        fact_tables = [
            'fact_trip',
            'fact_speeding', 
            'fact_idle',
            'fact_awh',
            'fact_wh',
            'fact_ha',
            'fact_hb',
            'fact_wu'
        ]
        
        print("\n1. CHECKING CURRENT DATA COUNTS")
        print("-" * 60)
        
        for table in fact_tables:
            try:
                result = db.session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                print(f"   {table:20} {count:>10,} records")
            except Exception as e:
                print(f"   {table:20} ERROR: {e}")
        
        print("\n2. DELETING ALL FACT TABLE DATA")
        print("-" * 60)
        
        total_deleted = 0
        for table in fact_tables:
            try:
                result = db.session.execute(text(f"DELETE FROM {table}"))
                db.session.commit()
                deleted = result.rowcount
                total_deleted += deleted
                print(f"   {table:20} DELETED {deleted:>10,} records")
            except Exception as e:
                print(f"   {table:20} ERROR: {e}")
                db.session.rollback()
        
        print(f"\n   TOTAL DELETED: {total_deleted:,} records")
        
        print("\n3. VERIFYING RENDER AND RESULT TABLES")
        print("-" * 60)
        
        for table in ['render', 'result']:
            try:
                result = db.session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.fetchone()[0]
                status = "OK" if count > 0 else "EMPTY"
                print(f"   {table:20} {count:>10,} records [{status}]")
            except Exception as e:
                print(f"   {table:20} ERROR: {e}")
        
        print("\n" + "=" * 80)
        print("CLEANUP COMPLETE - READY FOR FRESH DATA FETCH")
        print("=" * 80)
        print("\nNEXT STEP: Run backfill_direct_python.py with 2025-01-01 start date")
        print("This will use cached render/result data to fetch fresh event data")
        print("\n" + "=" * 80)

if __name__ == "__main__":
    main()
