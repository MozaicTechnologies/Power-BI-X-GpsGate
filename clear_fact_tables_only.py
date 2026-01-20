#!/usr/bin/env python
"""Clear only fact tables (keep render and result tables)"""

import sys
from application import create_app, db
from models import (
    FactTrip, FactSpeeding, FactIdle, FactAWH,
    FactWH, FactHA, FactHB, FactWU
)

app = create_app()

with app.app_context():
    try:
        print("Clearing fact tables...")
        
        tables = [
            ('fact_trip', FactTrip),
            ('fact_speeding', FactSpeeding),
            ('fact_idle', FactIdle),
            ('fact_awh', FactAWH),
            ('fact_wh', FactWH),
            ('fact_ha', FactHA),
            ('fact_hb', FactHB),
            ('fact_wu', FactWU),
        ]
        
        for table_name, model_class in tables:
            count_before = db.session.query(model_class).count()
            db.session.query(model_class).delete()
            db.session.commit()
            print(f"  {table_name}: Deleted {count_before} records")
        
        print("\nFact tables cleared successfully!")
        print("Render and result tables preserved.")
        
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        db.session.rollback()
        sys.exit(1)
