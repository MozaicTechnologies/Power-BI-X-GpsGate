#!/usr/bin/env python
"""
Sync ALL FACT TABLES
FROM local database
TO Render (live) PostgreSQL database

Local  → SQLAlchemy (psycopg)
Render → psycopg3 (direct)

Safe • Sequential • Batch-based
"""

import os
from dotenv import load_dotenv
import psycopg
from sqlalchemy import create_engine, text

load_dotenv()

# ------------------------------------------------------------------
# RENDER / LIVE DATABASE
# ------------------------------------------------------------------
# RENDER_HOST = "dpg-d3fvdvdfvfdas0-a.singapore-postgres.render.com"
# RENDER_PORT = 5432
# RENDER_USER = "powedfvdfvdfpostgre_user"
# RENDER_PASSWORD = "EJzJUdfvdfvc8Eo1oBbt"
# RENDER_DBNAME = "poweefererererpostgre"


RENDER_HOST = "dpg-d39587nfte5s73ci7as0-a.singapore-postgres.render.com"
RENDER_PORT = 5432
RENDER_USER = "powerbixgpsgatexgdriverpostgre_user"
RENDER_PASSWORD = "EJzJUk7a8AGjr1BCxXslO1Pc8Eo1oBbt"
RENDER_DBNAME = "powerbixgpsgatexgdriverpostgre"

# ------------------------------------------------------------------
# LOCAL DATABASE
# ------------------------------------------------------------------
LOCAL_DB = os.getenv("DATABASE_URL")
if LOCAL_DB.startswith("postgresql://"):
    LOCAL_DB = LOCAL_DB.replace("postgresql://", "postgresql+psycopg://")

# ------------------------------------------------------------------
# FACT TABLES (ORDER MATTERS IF FK EXISTS)
# ------------------------------------------------------------------
FACT_TABLES = [
    # "fact_trip",
    # "fact_speeding",
    # "fact_idle",
    # "fact_awh",
    # "fact_wh",
    # "fact_ha",
    # "fact_hb",
    # "fact_wu",
    "dim_drivers",
    "dim_tags",
    "dim_vehicles",
    "dim_reports",
    "dim_event_rules",
    "dim_vehicle_custom_fields",
]

print("=" * 80)
print("SYNC FACT TABLES → LOCAL → RENDER (LIVE)")
print("=" * 80)
print(f"Local DB : {LOCAL_DB[:60]}...")
print(f"Render DB: {RENDER_HOST}\n")

try:
    # ------------------------------------------------------------------
    # 1️⃣ CONNECT TO RENDER DB (psycopg3)
    # ------------------------------------------------------------------
    print("[1/6] Connecting to Render DB...")
    render_conn = psycopg.connect(
        host=RENDER_HOST,
        port=RENDER_PORT,
        user=RENDER_USER,
        password=RENDER_PASSWORD,
        dbname=RENDER_DBNAME,
        sslmode="require",
        connect_timeout=30
    )
    render_cur = render_conn.cursor()
    print("      [OK] Connected to Render DB")

    # ------------------------------------------------------------------
    # 2️⃣ CONNECT TO LOCAL DB
    # ------------------------------------------------------------------
    print("[2/6] Connecting to Local DB...")
    local_engine = create_engine(LOCAL_DB, future=True)
    print("      [OK] Connected to Local DB")

    # ------------------------------------------------------------------
    # 3️⃣ PROCESS EACH FACT TABLE
    # ------------------------------------------------------------------
    for table in FACT_TABLES:
        print(f"\n[SYNC] Processing table: {table}")

        # --------------------------------------------------------------
        # Fetch column names
        # --------------------------------------------------------------
        with local_engine.connect() as conn:
            columns = conn.execute(text(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :t
                ORDER BY ordinal_position
            """), {"t": table}).scalars().all()

            rows = conn.execute(text(f"SELECT * FROM {table}")).fetchall()

        print(f"        Rows fetched: {len(rows)}")

        if not rows:
            print("        [SKIP] No data")
            continue

        # --------------------------------------------------------------
        # Clear live table
        # --------------------------------------------------------------
        print("        Clearing live table...")
        render_cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
        render_conn.commit()

        # --------------------------------------------------------------
        # Insert data (batch)
        # --------------------------------------------------------------
        col_sql = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        insert_sql = f"""
            INSERT INTO {table} ({col_sql})
            VALUES ({placeholders})
        """

        print("        Inserting data...")
        batch_size = 6000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            render_cur.executemany(insert_sql, batch)
            render_conn.commit()
            print(f"        Inserted {min(i + batch_size, len(rows))}/{len(rows)}")

        print(f"        [OK] Completed {table}")

    # ------------------------------------------------------------------
    # 4️⃣ FINAL VERIFICATION
    # ------------------------------------------------------------------
    print("\n[6/6] Verifying live counts...")
    for table in FACT_TABLES:
        render_cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = render_cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")

    print("\n" + "=" * 80)
    print("SUCCESS ✅ ALL FACT TABLES SYNCED TO RENDER DB")
    print("=" * 80)

    render_cur.close()
    render_conn.close()

except Exception as e:
    import traceback
    print("\n❌ SYNC FAILED")
    print(traceback.format_exc())
