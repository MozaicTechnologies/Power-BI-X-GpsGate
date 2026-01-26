#!/usr/bin/env python
"""
Scheduled Daily Sync Job
Runs every day at 2 AM UTC
Syncs dimension tables and yesterday's fact data
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

def run_daily_sync():
    """
    Daily sync workflow:
    1. Calculate yesterday's date range
    2. Process all 8 event types for yesterday
    3. Record execution status
    """
    app = create_app()
    
    with app.app_context():
        # Create job execution record
        job = JobExecution(
            job_type='daily_sync',
            status='running',
            started_at=datetime.utcnow()
        )
        db.session.add(job)
        db.session.commit()
        
        try:
            # Calculate yesterday
            today = datetime.utcnow().date()
            yesterday = today - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            end_date = yesterday.strftime('%Y-%m-%d')
            
            logger.info(f"🚀 Starting daily sync for {start_date}")
            
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
            total_failed = 0
            
            for event_type in event_types:
                logger.info(f"📊 Processing {event_type}...")
                
                try:
                    result = process_event_with_dates(
                        app=app,
                        event_type=event_type,
                        start_date=start_date,
                        end_date=end_date
                    )
                    
                    results[event_type] = {
                        'status': 'success',
                        'inserted': result.get('inserted', 0),
                        'skipped': result.get('skipped', 0),
                        'failed': result.get('failed', 0)
                    }
                    
                    total_inserted += result.get('inserted', 0)
                    total_failed += result.get('failed', 0)
                    
                    logger.info(
                        f"✅ {event_type}: "
                        f"inserted={result.get('inserted', 0)}, "
                        f"skipped={result.get('skipped', 0)}, "
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
            job.job_metadata = results
            db.session.commit()
            
            logger.info(
                f"✅ Daily sync completed: "
                f"{total_inserted} inserted, {total_failed} failed"
            )
            
            return {
                'success': True,
                'date': start_date,
                'total_inserted': total_inserted,
                'total_failed': total_failed,
                'results': results
            }
            
        except Exception as e:
            logger.error(f"❌ Daily sync failed: {str(e)}")
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
    result = run_daily_sync()
    
    if result['success']:
        print(f"✅ Daily sync completed successfully")
        print(f"📊 Total inserted: {result['total_inserted']}")
        sys.exit(0)
    else:
        print(f"❌ Daily sync failed: {result['error']}")
        sys.exit(1)
