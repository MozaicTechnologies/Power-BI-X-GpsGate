"""
Migrate Render and Result table data from DATABASE_LIVE_URL to local DATABASE_URL
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Get database URLs from .env
LOCAL_DB_URL = os.getenv("DATABASE_URL")
REMOTE_DB_URL = os.getenv("DATABASE_LIVE_URL")

if not LOCAL_DB_URL:
    print("ERROR: DATABASE_URL not set in .env")
    exit(1)

if not REMOTE_DB_URL:
    print("ERROR: DATABASE_LIVE_URL not set in .env")
    exit(1)

# Convert to psycopg dialect if needed
if REMOTE_DB_URL.startswith("postgresql://"):
    REMOTE_DB_URL = REMOTE_DB_URL.replace("postgresql://", "postgresql+psycopg://", 1)

from sqlalchemy import create_engine, text
import pandas as pd

print("=" * 80)
print("MIGRATION: Render & Result tables from Live Server to Local DB")
print("=" * 80)

try:
    print("\n[STEP 1] Connecting to databases...")
    
    print("  Connecting to LOCAL (Fleetdb)...")
    local_engine = create_engine(LOCAL_DB_URL, echo=False)
    local_conn = local_engine.connect()
    print("  OK Local database connected")
    
    print("  Connecting to REMOTE (Render.com)...")
    remote_engine = create_engine(REMOTE_DB_URL, echo=False, connect_args={"connect_timeout": 5})
    remote_conn = remote_engine.connect()
    print("  OK Remote database connected")
    
    # Step 2: Migrate Render table
    print("\n[STEP 2] Migrating Render table...")
    try:
        # Get data from remote using pandas
        render_df = pd.read_sql_table("render", remote_conn)
        print(f"  OK Retrieved {len(render_df)} render records from live database")
        
        if len(render_df) > 0:
            # Insert into local database
            render_df.to_sql("render", local_conn, if_exists="append", index=False)
            print(f"  OK Inserted {len(render_df)} render records to local database")
        
    except Exception as e:
        print(f"  WARNING Could not migrate render table: {str(e)[:100]}")
    
    # Step 3: Migrate Result table
    print("\n[STEP 3] Migrating Result table...")
    try:
        # Get data from remote using pandas
        result_df = pd.read_sql_table("result", remote_conn)
        print(f"  OK Retrieved {len(result_df)} result records from live database")
        
        if len(result_df) > 0:
            # Insert into local database
            result_df.to_sql("result", local_conn, if_exists="append", index=False)
            print(f"  OK Inserted {len(result_df)} result records to local database")
        
    except Exception as e:
        print(f"  WARNING Could not migrate result table: {str(e)[:100]}")
    
    # Step 4: Verify
    print("\n[STEP 4] Verification...")
    
    try:
        render_count = local_conn.execute(text("SELECT COUNT(*) FROM render")).scalar()
        print(f"  OK Local render records: {render_count}")
    except:
        print(f"  INFO Render table not available")
    
    try:
        result_count = local_conn.execute(text("SELECT COUNT(*) FROM result")).scalar()
        print(f"  OK Local result records: {result_count}")
    except:
        print(f"  INFO Result table not available")
    
    print("\n" + "=" * 80)
    print("MIGRATION COMPLETED")
    print("=" * 80)
    
except Exception as e:
    print(f"\nERROR FATAL: {str(e)}")
    print("\nNote: If you're getting a DNS error, the local machine cannot reach")
    print("the remote Render.com database. This is a network connectivity issue.")
    print("\nAlternative: You can manually export the data from Render and import locally.")
    import traceback
    traceback.print_exc()

finally:
    try:
        local_conn.close()
        remote_conn.close()
    except:
        pass
