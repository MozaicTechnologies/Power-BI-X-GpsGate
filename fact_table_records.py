# #!/usr/bin/env python
# """Get record counts from fact tables"""

# import sys
# from application import create_app, db
# from models import (
#     FactTrip, FactSpeeding, FactIdle, FactAWH,
#     FactWH, FactHA, FactHB, FactWU
# )

# app = create_app()

# with app.app_context():
#     try:
#         print("Fetching fact table record counts...\n")

#         tables = [
#             ('fact_trip', FactTrip),
#             ('fact_speeding', FactSpeeding),
#             ('fact_idle', FactIdle),
#             ('fact_awh', FactAWH),
#             ('fact_wh', FactWH),
#             ('fact_ha', FactHA),
#             ('fact_hb', FactHB),
#             ('fact_wu', FactWU),
#         ]

#         for table_name, model_class in tables:
#             count = db.session.query(model_class).count()
#             print(f"  {table_name}: {count} records")

#         print("\nFact table inspection completed successfully.")

#     except Exception as e:
#         print(f"ERROR: {e}", file=sys.stderr)
#         sys.exit(1)


#!/usr/bin/env python
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
