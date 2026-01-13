#!/usr/bin/env python
"""
Generate realistic Render and Result cache with multiple weeks of data
to match the pattern we see in live database (1512 render records, 925 result records)
"""
import os
from dotenv import load_dotenv
from application import create_app
from models import db, Render, Result
from datetime import datetime, timedelta
import uuid

load_dotenv()

app = create_app()

print("=" * 80)
print("GENERATE REALISTIC CACHE DATA (Multiple Weeks)")
print("=" * 80)

with app.app_context():
    # Clear existing data
    print("\n[1/3] Clearing tables...")
    db.session.query(Result).delete()
    db.session.query(Render).delete()
    db.session.commit()
    
    # Test parameters
    app_id = "6"
    tag_id = "39"
    base_url = "https://omantracking2.com"
    token = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="
    
    # Event configurations
    events = [
        ("1225", None, "Trip"),           # Trip - no event_id
        ("25", "18", "Speeding"),
        ("25", "1328", "Idle"),
        ("25", "12", "AWH"),
        ("25", "13", "WH"),
        ("25", "1327", "HA"),
        ("25", "1326", "HB"),
        ("25", "17", "WU"),
    ]
    
    # Generate data for 4 weeks to build up cache similar to live
    print("[2/3] Creating Render cache records (multiple weeks)...")
    render_records = []
    start_date = datetime(2025, 1, 1)
    num_weeks = 4  # Generate 4 weeks of data
    
    for week_num in range(num_weeks):
        week_start = start_date + timedelta(weeks=week_num)
        week_end = week_start + timedelta(days=6)
        
        period_start = week_start.strftime("%Y-%m-%d 00:00:00")
        period_end = week_end.strftime("%Y-%m-%d 23:59:59")
        
        print(f"  Week {week_num + 1}: {period_start} to {period_end}")
        
        for report_id, event_id, event_name in events:
            # Create render record with realistic render_id (UUID format)
            render_id = str(uuid.uuid4())
            
            render = Render(
                app_id=app_id,
                period_start=period_start,
                period_end=period_end,
                tag_id=tag_id,
                event_id=event_id,
                report_id=report_id,
                render_id=render_id
            )
            db.session.add(render)
            render_records.append((event_name, render_id, report_id))
    
    db.session.commit()
    render_count = len(render_records)
    print(f"   Created {render_count} Render cache records")
    
    print("[3/3] Creating Result cache records...")
    
    # Create result records for ~60% of render records (simulating some failures)
    result_records = []
    num_results = int(render_count * 0.6)  # ~60% success rate
    
    for i, (event_name, render_id, report_id) in enumerate(render_records[:num_results]):
        result = Result(
            app_id=app_id,
            report_id=report_id,
            render_id=render_id,
            filepath=f"reports/files/{render_id}.csv",
            gdrive_link=f"https://omantracking2.com/comGpsGate/reports/files/{render_id}.csv"
        )
        db.session.add(result)
        result_records.append(event_name)
    
    db.session.commit()
    print(f"   Created {len(result_records)} Result cache records")
    
    # Verify
    render_count = db.session.query(Render).count()
    result_count = db.session.query(Result).count()
    
    print("\n" + "=" * 80)
    print("✅ CACHE GENERATED")
    print("=" * 80)
    print(f"\nLocal DB now has:")
    print(f"  - Render table: {render_count} records (simulates production cache)")
    print(f"  - Result table: {result_count} records")
    print(f"\n✓ Ready for backfill testing with realistic data volume")
