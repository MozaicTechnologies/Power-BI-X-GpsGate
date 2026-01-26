#!/usr/bin/env python
"""
Scheduled Weekly Backfill Job
Runs every Sunday at 3 AM UTC
Backfills last 7 days of data for reconciliation
"""

import sys
import os
from datetime import datetime, timedelta
import traceback
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from application import create_app, db
from models import JobExecution
from logger_config import get_logger
from data_pipeline import process_event_data

logger = get_logger(__name__)

def run_weekly_backfill():
    """
    Weekly backfill workflow:
    1. Calculate last 7 days date range
    2. Process all 8 event types for the week
    3. Upsert/reconcile data (handles duplicates via primary keys)
    4. Record execution status
    """
    app = create_app()
    
    with app.app_context():
        # Create job execution record
        job = JobExecution(
            job_type='weekly_backfill',
            status='running',
            started_at=datetime.utcnow()
        )
        db.session.add(job)
        db.session.commit()
        
        try:
            # Calculate last 7 days
            today = datetime.utcnow().date()
            end_date = today - timedelta(days=1)  # Yesterday
            start_date = end_date - timedelta(days=6)  # 7 days ago
            
            start_str = start_date.strftime('%Y-%m-%d')
            end_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"🚀 Starting weekly backfill: {start_str} to {end_str}")
            
            # Event types to process
            event_types = [
                'Trip',
                'Speeding',
                'Idle',
                'AWH',  # After Work Hours
                'WH',   # Work Hours
                'HA',   # Harsh Acceleration
                'HB',   # Harsh Braking
                'WU'    # Weekend Usage
            ]
            
            results = {}
            total_inserted = 0
            total_skipped = 0
            total_failed = 0
            
            for event_type in event_types:
                logger.info(f"📊 Processing {event_type} for week...")
                
                try:
                    result = process_event_data(
                        event_type=event_type,
                        start_date=start_str,
                        end_date=end_str
                    )
                    
                    results[event_type] = {
                        'status': 'success',
                        'inserted': result.get('inserted', 0),
                        'skipped': result.get('skipped', 0),
                        'failed': result.get('failed', 0)
                    }
                    
                    total_inserted += result.get('inserted', 0)
                    total_skipped += result.get('skipped', 0)
                    total_failed += result.get('failed', 0)
                    
                    logger.info(
                        f"✅ {event_type}: "
                        f"inserted={result.get('inserted', 0)}, "
                        f"skipped={result.get('skipped', 0)} (duplicates), "
                        f"failed={result.get('failed', 0)}"
                    )
                    
                except Exception as e:
                    logger.error(f"❌ {event_type} failed: {str(e)}")
                    results[event_type] = {
                        'status': 'failed',
                        'error': str(e)
                    }
            
            # Update job execution record
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'start_date': start_str,
                'end_date': end_str,
                'total_skipped': total_skipped,
                'results': results
            }
            db.session.commit()
            
            logger.info(
                f"✅ Weekly backfill completed: "
                f"{total_inserted} inserted, {total_skipped} skipped (duplicates), "
                f"{total_failed} failed"
            )
            
            return {
                'success': True,
                'start_date': start_str,
                'end_date': end_str,
                'total_inserted': total_inserted,
                'total_skipped': total_skipped,
                'total_failed': total_failed,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"❌ Weekly backfill failed: {str(e)}")
            logger.error(traceback.format_exc())
            
            # Update job execution record
            job.status = 'failed'
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata = {'traceback': traceback.format_exc()}
            db.session.commit()
            
            return {
                'success': False,
                'error': str(e),
                'traceback': traceback.format_exc()
            }

if __name__ == '__main__':
    result = run_weekly_backfill()
    
    if result['success']:
        print(f"✅ Weekly backfill completed successfully")
        print(f"📊 Period: {result['start_date']} to {result['end_date']}")
        print(f"📊 Inserted: {result['total_inserted']}")
        print(f"📊 Skipped: {result['total_skipped']} (duplicates)")
        sys.exit(0)
    else:
        print(f"❌ Weekly backfill failed: {result['error']}")
        sys.exit(1)
