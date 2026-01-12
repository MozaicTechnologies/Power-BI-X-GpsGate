"""
Clear fact_trip table and verify database
"""

from dotenv import load_dotenv
load_dotenv()

from application import create_app
from models import db, FactTrip

app = create_app()

with app.app_context():
    try:
        print("Clearing fact_trip table...")
        db.session.query(FactTrip).delete()
        db.session.commit()
        
        count = db.session.query(FactTrip).count()
        print(f"✓ fact_trip cleared. Remaining records: {count}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
