"""Get record counts from fact tables (raw SQL - accurate)"""

import sys
from sqlalchemy import text
from application import create_app, db

app = create_app()

TABLES = [
    "fact_trip",
    "fact_speeding",
    "fact_idle",
    "fact_awh",
    "fact_wh",
    "fact_ha",
    "fact_hb",
    "fact_wu",
]

with app.app_context():
    try:
        print("Fetching fact table record counts...\n")

        for t in TABLES:
            count = db.session.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
            print(f"  {t}: {count} records")

        print("\nFact table inspection completed successfully.")

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
