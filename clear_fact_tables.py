"""Clear all fact tables except Render and Result"""
from dotenv import load_dotenv
load_dotenv()

from application import create_app
from models import db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU

app = create_app()

with app.app_context():
    print("Clearing all fact tables...\n")
    
    tables = [
        ("fact_trip", FactTrip),
        ("fact_speeding", FactSpeeding),
        ("fact_idle", FactIdle),
        ("fact_awh", FactAWH),
        ("fact_wh", FactWH),
        ("fact_ha", FactHA),
        ("fact_hb", FactHB),
        ("fact_wu", FactWU),
    ]
    
    for table_name, model in tables:
        count = db.session.query(model).count()
        db.session.query(model).delete()
        db.session.commit()
        print(f"[OK] {table_name:15s}: {count:,} records deleted")
    
    print("\n[OK] All fact tables cleared successfully!")
    print("     Render and Result tables preserved.")
