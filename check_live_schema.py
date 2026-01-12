#!/usr/bin/env python
"""Check the schema of the live database"""
import os
from dotenv import load_dotenv

load_dotenv()

live_url = os.environ.get('DATABASE_LIVE_URL', '')
if not live_url:
    print("ERROR: DATABASE_LIVE_URL not found")
    exit(1)

os.environ['DATABASE_URL'] = live_url

from application import create_app, db
from sqlalchemy import inspect, text

app = create_app()

# Get list of tables
with app.app_context():
    inspector = inspect(db.engine)
    
    event_tables = [
        'fact_trip',
        'fact_speeding',
        'fact_idle',
        'fact_awh',
        'fact_wh',
        'fact_ha',
        'fact_hb',
        'fact_wu',
    ]
    
    for table_name in event_tables:
        print(f"\n{table_name.upper()}")
        print("-" * 60)
        
        if table_name not in inspector.get_table_names():
            print(f"  TABLE NOT FOUND")
            continue
        
        columns = inspector.get_columns(table_name)
        for col in columns:
            print(f"  {col['name']:20} {str(col['type']):20}")
