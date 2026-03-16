#!/usr/bin/env python
"""Get record counts from dimension tables."""

import sys
from sqlalchemy import text

from application import create_app, db


TABLES = [
    "dim_tags",
    "dim_event_rules",
    "dim_reports",
    "dim_vehicles",
    "dim_drivers",
    "dim_vehicle_custom_fields",
]


app = create_app()


with app.app_context():
    try:
        print("Fetching dimension table record counts...\n")
        for table_name in TABLES:
            count = db.session.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
            print(f"  {table_name}: {count} records")
        print("\nDimension table inspection completed successfully.")
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
