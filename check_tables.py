from dotenv import load_dotenv
load_dotenv()
import os
import psycopg

db_url = os.environ.get('DATABASE_URL')
try:
    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT COUNT(*) FROM render')
            render_count = cur.fetchone()[0]
            print(f'Render table: {render_count} rows')
            
            cur.execute('SELECT COUNT(*) FROM result')
            result_count = cur.fetchone()[0]
            print(f'Result table: {result_count} rows')
except Exception as e:
    print(f'Error: {type(e).__name__}: {e}')
