#!/usr/bin/env python
"""
Fetch Render and Result table data using psycopg3 directly
Then insert into local database using SQLAlchemy
"""
import os
from dotenv import load_dotenv
import psycopg
from sqlalchemy import create_engine, text

load_dotenv()

# Database connection parameters
RENDER_HOST = "dpg-d39587nfte5s73ci7as0-a.singapore-postgres.render.com"
RENDER_USER = "powerbixgpsgatexgdriverpostgre_user"
RENDER_PASSWORD = "EJzJUk7a8AGjr1BCxXslO1Pc8Eo1oBbt"
RENDER_DBNAME = "powerbixgpsgatexgdriverpostgre"

# Local database
LOCAL_DB = os.getenv("DATABASE_URL")
if 'postgresql://' in LOCAL_DB:
    LOCAL_DB = LOCAL_DB.replace('postgresql://', 'postgresql+psycopg://')

print("=" * 80)
print("FETCH RENDER AND RESULT TABLE DATA FROM EXTERNAL DB")
print("=" * 80)
print(f"\nExternal DB: {RENDER_HOST}")
print(f"Local DB:    {LOCAL_DB[:60]}...\n")

try:
    # Connect to Render database using psycopg3 directly
    print("[1/5] Connecting to external Render database...")
    render_conn = psycopg.connect(
        host=RENDER_HOST,
        user=RENDER_USER,
        password=RENDER_PASSWORD,
        dbname=RENDER_DBNAME,
        sslmode="require",
        connect_timeout=30
    )
    print("      [OK] Connected to external DB")
    
    # Fetch Render table
    print("[2/5] Fetching Render table...")
    with render_conn.cursor() as cur:
        cur.execute("SELECT * FROM render")
        render_data = cur.fetchall()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='render' ORDER BY ordinal_position")
        render_columns = [row[0] for row in cur.fetchall()]
    print(f"      Found {len(render_data)} Render records")
    
    # Fetch Result table
    print("[3/5] Fetching Result table...")
    with render_conn.cursor() as cur:
        cur.execute("SELECT * FROM result")
        result_data = cur.fetchall()
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='result' ORDER BY ordinal_position")
        result_columns = [row[0] for row in cur.fetchall()]
    print(f"      Found {len(result_data)} Result records")
    
    render_conn.close()
    
    # Insert into local database
    print("[4/5] Connecting to local database...")
    local_engine = create_engine(LOCAL_DB)
    
    with local_engine.connect() as local_conn:
        # Clear tables
        local_conn.execute(text("DELETE FROM result"))
        local_conn.execute(text("DELETE FROM render"))
        local_conn.commit()
        print("      [OK] Cleared local tables")
    
    print("[5/5] Inserting data to local database...")
    
    # Insert Render records
    if render_data:
        render_col_str = ', '.join(render_columns)
        placeholders = ', '.join([f':{i}' for i in range(len(render_columns))])
        insert_render = f"INSERT INTO render ({render_col_str}) VALUES ({placeholders})"
        
        with local_engine.connect() as local_conn:
            for i, row in enumerate(render_data):
                row_dict = {str(j): row[j] for j in range(len(render_columns))}
                try:
                    local_conn.execute(text(insert_render), row_dict)
                    if (i + 1) % 500 == 0:
                        local_conn.commit()
                        print(f"      Processed {i + 1}/{len(render_data)} Render records...")
                except Exception as e:
                    pass
            
            local_conn.commit()
            print(f"      [OK] Inserted {len(render_data)} Render records")
    
    # Insert Result records
    if result_data:
        result_col_str = ', '.join(result_columns)
        placeholders = ', '.join([f':{i}' for i in range(len(result_columns))])
        insert_result = f"INSERT INTO result ({result_col_str}) VALUES ({placeholders})"
        
        with local_engine.connect() as local_conn:
            for i, row in enumerate(result_data):
                row_dict = {str(j): row[j] for j in range(len(result_columns))}
                try:
                    local_conn.execute(text(insert_result), row_dict)
                    if (i + 1) % 500 == 0:
                        local_conn.commit()
                        print(f"      Processed {i + 1}/{len(result_data)} Result records...")
                except Exception as e:
                    pass
            
            local_conn.commit()
            print(f"      [OK] Inserted {len(result_data)} Result records")
    
    print("\n" + "=" * 80)
    print("SUCCESS: DATA FETCH COMPLETE")
    print("=" * 80)
    
    # Verify
    with local_engine.connect() as conn:
        render_count = conn.execute(text("SELECT COUNT(*) FROM render")).scalar()
        result_count = conn.execute(text("SELECT COUNT(*) FROM result")).scalar()
        print(f"\nLocal DB now has:")
        print(f"  - Render table: {render_count:,} records")
        print(f"  - Result table: {result_count:,} records")
        print(f"\n[OK] Ready for backfill testing!")

except Exception as e:
    import traceback
    print(f"\n[ERROR]: {e}")
    print("\nFull traceback:")
    print(traceback.format_exc())
