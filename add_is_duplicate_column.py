#!/usr/bin/env python
"""Add is_duplicate column to all fact tables"""
from application import create_app, db
from sqlalchemy import text

app = create_app()

with app.app_context():
    tables = ['fact_trip', 'fact_speeding', 'fact_idle', 'fact_awh', 'fact_wh', 'fact_ha', 'fact_hb', 'fact_wu']
    
    for table in tables:
        try:
            # Check if column exists
            result = db.session.execute(
                text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND column_name='is_duplicate'")
            )
            if not result.fetchone():
                # Add column if it doesn't exist
                db.session.execute(text(f'ALTER TABLE {table} ADD COLUMN is_duplicate BOOLEAN DEFAULT FALSE NOT NULL'))
                db.session.commit()
                print(f'✓ Added is_duplicate to {table}')
            else:
                print(f'✓ is_duplicate already exists in {table}')
        except Exception as e:
            db.session.rollback()
            print(f'✗ Error with {table}: {str(e)[:100]}')

print("\nAll fact tables updated!")
