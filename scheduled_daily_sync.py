#!/usr/bin/env python
"""
Scheduled Daily Sync Job
Runs every day at 2 AM UTC
Syncs dimension tables and yesterday's fact data
"""

import sys
import os
import json
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
from config import Config

logger = get_logger(__name__)

# Event type to API configuration mapping (MATCHES working backfill script)
EVENT_CONFIG = {
    'Trip': {'report_id': '25', 'event_id': None, 'response_key': 'trip_events'},
    'Speeding': {'report_id': '25', 'event_id': '18', 'response_key': 'speed_events'},
    'Idle': {'report_id': '25', 'event_id': '1328', 'response_key': 'idle_events'},
    'AWH': {'report_id': '25', 'event_id': '12', 'response_key': 'awh_events'},
    'WH': {'report_id': '25', 'event_id': '13', 'response_key': 'wh_events'},
    'HA': {'report_id': '25', 'event_id': '1327', 'response_key': 'ha_events'},
    'HB': {'report_id': '25', 'event_id': '1326', 'response_key': 'hb_events'},
    'WU': {'report_id': '25', 'event_id': '17', 'response_key': 'wu_events'}
}


def process_event_with_dates(app, event_type, start_date, end_date):
    """
    Helper function to process event data for a specific date range.
    Creates Flask request context with proper payload.
    """
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")
    
    config = EVENT_CONFIG[event_type]
    
    # Prepare payload matching process_event_data signature
    payload = {
        "app_id": "6",
        "token": Config.TOKEN,
        "base_url": Config.BASE_URL,
        "report_id": config['report_id'],
        "tag_id": "39",  # Same as working backfill script
        "period_start": f"{start_date}T00:00:00Z",
        "period_end": f"{end_date}T23:59:59Z"
    }
    
    if config['event_id']:
        payload["event_id"] = config['event_id']
    
    # Call process_event_data within Flask request context
    with app.test_request_context(
        '/api/process',
        method='POST',
        data=json.dumps(payload),
        content_type='application/json'
    ):
        # process_event_data returns (jsonify(...), status_code) tuple
        response_tuple = process_event_data(
            event_name=event_type,
            response_key=config['response_key']
        )
        
        # Extract the response object and JSON data
        if isinstance(response_tuple, tuple):
            response_obj, status_code = response_tuple
            # Get JSON data from response object
            data = response_obj.get_json()
            # Return the accounting/totals data
            return data.get('accounting', {})
        else:
            # In case it's not a tuple (shouldn't happen, but safe fallback)
            return response_tuple


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
            
            # Step 1: Sync dimension tables
            logger.info("📋 Syncing dimension tables...")
            try:
                from sync_dimensions_from_api import main as sync_dimensions
                dimension_result = sync_dimensions()
                dimension_records = dimension_result if isinstance(dimension_result, int) else 0
                logger.info(f"✅ Dimension tables synced: {dimension_records} records processed")
            except Exception as e:
                logger.error(f"⚠️  Dimension sync failed: {str(e)}")
                dimension_records = 0
            
            # Step 2: Process fact data for yesterday
            logger.info(f"📊 Processing fact data for {start_date}...")
            
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
            job.records_processed = total_inserted + dimension_records
            job.errors = total_failed
            job.job_metadata = {
                'date': start_date,
                'dimension_records': dimension_records,
                'fact_results': results
            }
            db.session.commit()
            
            logger.info(
                f"✅ Daily sync completed: "
                f"dimensions={dimension_records}, facts={total_inserted} inserted, {total_failed} failed"
            )
            
            return {
                'success': True,
                'date': start_date,
                'dimension_records': dimension_records,
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
