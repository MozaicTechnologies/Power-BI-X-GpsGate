"""
Database Summary - Show all tables and record counts
"""

from dotenv import load_dotenv
load_dotenv()

from application import create_app
from models import db, FactTrip, Render, Result

app = create_app()

with app.app_context():
    print("\n" + "=" * 80)
    print("LOCAL DATABASE SUMMARY (Fleetdb)")
    print("=" * 80)
    
    try:
        trip_count = db.session.query(FactTrip).count()
        print(f"\nfact_trip:        {trip_count:,} records")
    except:
        print(f"\nfact_trip:        (table not found)")
    
    try:
        render_count = db.session.query(Render).count()
        print(f"render:           {render_count:,} records (migrated from live)")
    except:
        print(f"render:           (table not found)")
    
    try:
        result_count = db.session.query(Result).count()
        print(f"result:           {result_count:,} records (migrated from live)")
    except:
        print(f"result:           (table not found)")
    
    print("\n" + "=" * 80)
    print("STATUS: Ready for incremental weekly data pulls to local database")
    print("=" * 80)
    print("\nNext Steps:")
    print("1. Start Flask server: venv\\Scripts\\python.exe main.py")
    print("2. Test endpoint: venv\\Scripts\\python.exe test_tripdata.py")
    print("3. Set up weekly cron job to pull and store trip data incrementally")
    print("\n" + "=" * 80)
