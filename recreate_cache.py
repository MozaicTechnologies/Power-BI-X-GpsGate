#!/usr/bin/env python
"""
Recreate Render and Result cache with test data to enable backfill
"""
import os
from dotenv import load_dotenv
from application import create_app
from models import db, Render, Result
from datetime import datetime

load_dotenv()

app = create_app()

print("=" * 80)
print("RECREATE RENDER AND RESULT CACHE FOR ONE WEEK TEST")
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
    
    # Week 1: Jan 1-7, 2025
    period_start = "2025-01-01 00:00:00"
    period_end = "2025-01-07 23:59:59"
    week_start_ts = 1735689600  # 2025-01-01 00:00:00 UTC
    week_end_ts = 1736294399    # 2025-01-07 23:59:59 UTC
    
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
    
    print("[2/3] Creating Render cache records...")
    render_records = []
    
    for report_id, event_id, event_name in events:
        # Create render record with fake render_id (UUID format)
        import uuid
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
        render_records.append((event_name, render_id))
    
    db.session.commit()
    print(f"   Created {len(render_records)} Render cache records")
    
    print("[3/3] Creating Result cache records...")
    
    # Create placeholder result records with gdrive_links
    # These would normally point to actual CSV files
    result_records = []
    
    for (report_id, event_id, event_name), render_id in zip(events, [r[1] for r in render_records]):
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
    print("✅ CACHE RECREATED")
    print("=" * 80)
    print(f"\nLocal DB now has:")
    print(f"  - Render table: {render_count} records")
    print(f"  - Result table: {result_count} records")
    print(f"\nReady for backfill for one week: {period_start} to {period_end}")
