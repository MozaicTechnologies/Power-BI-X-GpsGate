#!/usr/bin/env python
"""Clear dimension tables only."""

import sys
from sqlalchemy import text

from application import create_app, db


TABLES = [
    "dim_vehicle_custom_fields",
    "dim_drivers",
    "dim_vehicles",
    "dim_reports",
    "dim_event_rules",
    "dim_tags",
]


app = create_app()


with app.app_context():
    try:
        print("Clearing dimension tables...\n")
        for table_name in TABLES:
            db.session.execute(text(f"TRUNCATE TABLE {table_name}"))
            print(f"  cleared {table_name}")
        db.session.commit()
        print("\nDimension tables cleared successfully.")
    except Exception as exc:
        db.session.rollback()
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
