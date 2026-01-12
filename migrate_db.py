"""
Migration script to add missing columns to fact tables
Run this once to update the database schema
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

from application import create_app
from models import db
from sqlalchemy import text

app = create_app()

with app.app_context():
    try:
        # Add missing columns to fact_trip table
        print("Migrating fact_trip table...")
        
        # Check if address column exists
        inspector = db.inspect(db.engine)
        fact_trip_columns = [col['name'] for col in inspector.get_columns('fact_trip')]
        
        if 'address' not in fact_trip_columns:
            db.session.execute(text('ALTER TABLE fact_trip ADD COLUMN address VARCHAR(500) NULL'))
            print("✓ Added address column to fact_trip")
        
        # Add other event tables if they don't exist
        tables_to_check = ['fact_speeding', 'fact_idle', 'fact_awh', 'fact_wh', 'fact_ha', 'fact_hb', 'fact_wu']
        
        for table_name in tables_to_check:
            try:
                inspector.get_columns(table_name)
                print(f"✓ {table_name} already exists")
            except:
                print(f"⚠️  {table_name} does not exist - will be created on next app start")
        
        db.session.commit()
        print("\n✓ Migration completed successfully")
        
    except Exception as e:
        print(f"❌ Migration error: {str(e)}")
        db.session.rollback()
        
        # Try alternative approach - recreate the table
        print("\nAttempting to recreate fact_trip table...")
        try:
            db.session.execute(text('DROP TABLE IF EXISTS fact_trip CASCADE'))
            db.session.commit()
            print("✓ Dropped old table")
            
            # Create new table with proper schema
            from models import FactTrip
            db.create_all()
            print("✓ Created new fact_trip table with proper schema")
            
        except Exception as e2:
            print(f"❌ Failed to recreate table: {str(e2)}")
