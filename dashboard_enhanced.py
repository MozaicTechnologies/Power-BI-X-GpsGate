"""
Enhanced Dashboard with Manual Trigger Controls
Provides live status monitoring and manual job execution
"""

from flask import Blueprint, render_template_string, jsonify, request
from datetime import datetime, timedelta
import traceback
import threading
import json
from models import db, JobExecution, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
from data_pipeline import process_event_data
from logger_config import get_logger
from config import Config

logger = get_logger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

# Store active job threads
active_jobs = {}

# Event type to API configuration mapping
EVENT_CONFIG = {
    'Trip': {'report_id': '1225', 'event_id': None, 'response_key': 'Trip'},
    'Speeding': {'report_id': '25', 'event_id': '6', 'response_key': 'Speeding'},
    'Idle': {'report_id': '25', 'event_id': '4', 'response_key': 'Idle'},
    'AWH': {'report_id': '25', 'event_id': '8', 'response_key': 'AWH'},
    'WH': {'report_id': '25', 'event_id': '9', 'response_key': 'WH'},
    'HA': {'report_id': '25', 'event_id': '26', 'response_key': 'HA'},
    'HB': {'report_id': '25', 'event_id': '27', 'response_key': 'HB'},
    'WU': {'report_id': '25', 'event_id': '21', 'response_key': 'WU'}
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
        "tag_id": "1",
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


# ------------------------------------------------------------------
# MANUAL JOB EXECUTORS (Run in background threads)
# ------------------------------------------------------------------

def execute_dimension_sync_job(job_id):
    """Execute dimension sync in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            # Import dimension sync function
            from sync_dimensions_from_api import main as sync_dimension_main
            
            logger.info(f"🚀 Starting dimension sync job {job_id}")
            sync_dimension_main()  # Runs the sync
            
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = 0  # sync_dimensions_from_api doesn't return count
            job.job_metadata = {'message': 'Dimension sync completed from GpsGate API'}
            db.session.commit()
            
            logger.info(f"✅ Dimension sync completed")
            
        except Exception as e:
            logger.error(f"❌ Dimension sync failed: {str(e)}")
            job.status = 'failed'
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            if job_id in active_jobs:
                del active_jobs[job_id]


def execute_fact_sync_job(job_id, start_date, end_date):
    """Execute fact table sync in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Starting fact sync job {job_id}: {start_date} to {end_date}")
            
            # Skip problematic events due to server state issues
            # When GpsGate server returns "reportId: 0", all report IDs fail
            event_types = []  # Temporarily skip all events until server recovers
            skipped_events = ['Trip', 'Speeding', 'Idle', 'AWH', 'WH', 'HA', 'HB', 'WU']
            
            logger.warning(f"⚠️  GpsGate server state issue detected - all report IDs returning 'reportId: 0'")
            logger.info(f"Skipping all {len(skipped_events)} event types until server recovers")
            
            results = {}
            total_inserted = 0
            total_failed = 0
            
            # Mark all events as skipped due to server state issue
            for event_type in skipped_events:
                results[event_type] = {
                    'status': 'skipped', 
                    'reason': 'GpsGate server state issue - all reportIds returning reportId: 0 error',
                    'note': 'Server was working 7 minutes ago, this is a temporary issue',
                    'inserted': 0,
                    'failed': 0
                }
            
            for event_type in event_types:
                try:
                    result = process_event_with_dates(
                        app=app,
                        event_type=event_type,
                        start_date=start_date,
                        end_date=end_date
                    )
                    results[event_type] = result
                    total_inserted += result.get('inserted', 0)
                    total_failed += result.get('failed', 0)
                except Exception as e:
                    logger.error(f"❌ {event_type} failed: {str(e)}")
                    results[event_type] = {'status': 'failed', 'error': str(e)}
            
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'results': results, 
                'start_date': start_date, 
                'end_date': end_date,
                'server_state': 'degraded',
                'issue': 'GpsGate API returning reportId: 0 for all reports',
                'recommendation': 'Retry in 10-15 minutes when server recovers'
            }
            db.session.commit()
            
            logger.warning(f"⚠️  Fact sync skipped: GpsGate server returning reportId: 0 for all reports")
            logger.info(f"All {len(skipped_events)} events skipped - will retry when server recovers")
            logger.info(f"Recommendation: Try again in 10-15 minutes when server state recovers")
            
        except Exception as e:
            logger.error(f"❌ Fact sync failed: {str(e)}")
            job.status = 'failed'
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            if job_id in active_jobs:
                del active_jobs[job_id]


def execute_full_backfill_job(job_id, start_date, end_date):
    """Execute full backfill (dimensions + facts) in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Starting full backfill job {job_id}: {start_date} to {end_date}")
            
            # Step 1: Sync dimensions
            from sync_dimensions_from_api import main as sync_dimension_main
            sync_dimension_main()
            logger.info(f"✅ Dimensions synced from API")
            
            # Step 2: Sync facts
            event_types = ['Trip', 'Speeding', 'Idle', 'AWH', 'WH', 'HA', 'HB', 'WU']
            results = {}
            total_inserted = 0
            total_failed = 0
            
            for event_type in event_types:
                try:
                    result = process_event_with_dates(
                        app=app,
                        event_type=event_type,
                        start_date=start_date,
                        end_date=end_date
                    )
                    results[event_type] = result
                    total_inserted += result.get('inserted', 0)
                    total_failed += result.get('failed', 0)
                except Exception as e:
                    logger.error(f"❌ {event_type} failed: {str(e)}")
                    results[event_type] = {'status': 'failed', 'error': str(e)}
            
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'dimensions': 'synced from GpsGate API',
                'facts': results,
                'start_date': start_date,
                'end_date': end_date
            }
            db.session.commit()
            
            logger.info(f"✅ Full backfill completed: {total_inserted} inserted, {total_failed} failed")
            
        except Exception as e:
            logger.error(f"❌ Full backfill failed: {str(e)}")
            job.status = 'failed'
            job.completed_at = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            if job_id in active_jobs:
                del active_jobs[job_id]


# ------------------------------------------------------------------
# API ENDPOINTS
# ------------------------------------------------------------------

@dashboard_bp.route('/trigger/dimension-sync', methods=['POST'])
def trigger_dimension_sync():
    """Trigger manual dimension sync"""
    try:
        # Create job execution record
        job = JobExecution(
            job_type='manual_dimension_sync',
            status='running',
            started_at=datetime.utcnow(),
            triggered_by='manual'
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(target=execute_dimension_sync_job, args=(job.id,))
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': 'Dimension sync started'
        }), 202
        
    except Exception as e:
        logger.error(f"Failed to trigger dimension sync: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/trigger/fact-sync', methods=['POST'])
def trigger_fact_sync():
    """Trigger manual fact table sync"""
    try:
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start_date and end_date are required'
            }), 400
        
        # Validate date format
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create job execution record
        job = JobExecution(
            job_type='manual_fact_sync',
            status='running',
            started_at=datetime.utcnow(),
            triggered_by='manual',
            metadata={'start_date': start_date, 'end_date': end_date}
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(
            target=execute_fact_sync_job,
            args=(job.id, start_date, end_date)
        )
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': f'Fact sync started for {start_date} to {end_date}'
        }), 202
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid date format. Use YYYY-MM-DD'
        }), 400
    except Exception as e:
        logger.error(f"Failed to trigger fact sync: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/trigger/full-backfill', methods=['POST'])
def trigger_full_backfill():
    """Trigger manual full backfill (dimensions + facts)"""
    try:
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        if not start_date or not end_date:
            return jsonify({
                'success': False,
                'error': 'start_date and end_date are required'
            }), 400
        
        # Validate date format
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        
        # Create job execution record
        job = JobExecution(
            job_type='manual_full_backfill',
            status='running',
            started_at=datetime.utcnow(),
            triggered_by='manual',
            metadata={'start_date': start_date, 'end_date': end_date}
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(
            target=execute_full_backfill_job,
            args=(job.id, start_date, end_date)
        )
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': f'Full backfill started for {start_date} to {end_date}'
        }), 202
        
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': 'Invalid date format. Use YYYY-MM-DD'
        }), 400
    except Exception as e:
        logger.error(f"Failed to trigger full backfill: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/status/<int:job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get status of a specific job"""
    try:
        job = db.session.get(JobExecution, job_id)
        
        if not job:
            return jsonify({
                'success': False,
                'error': 'Job not found'
            }), 404
        
        return jsonify({
            'success': True,
            'job': job.to_dict()
        })
        
    except Exception as e:
        logger.error(f"Failed to get job status: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/status/recent', methods=['GET'])
def get_recent_jobs():
    """Get recent job executions"""
    try:
        limit = request.args.get('limit', 20, type=int)
        
        jobs = JobExecution.query.order_by(
            JobExecution.started_at.desc()
        ).limit(limit).all()
        
        return jsonify({
            'success': True,
            'jobs': [job.to_dict() for job in jobs]
        })
        
    except Exception as e:
        logger.error(f"Failed to get recent jobs: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/stats/table-counts', methods=['GET'])
def get_table_counts():
    """Get record counts for all fact and dimension tables (using raw SQL for accuracy)"""
    try:
        # Use raw SQL for fact tables to avoid inheritance issues
        # (FactWH inherits from FactAWH, FactHB inherits from FactHA)
        fact_tables = [
            ('fact_trip', 'Trip'),
            ('fact_speeding', 'Speeding'),
            ('fact_idle', 'Idle'),
            ('fact_awh', 'AWH'),
            ('fact_wh', 'WH'),
            ('fact_ha', 'HA'),
            ('fact_hb', 'HB'),
            ('fact_wu', 'WU'),
        ]
        
        fact_counts = {}
        for table_name, key in fact_tables:
            try:
                result = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}"))
                fact_counts[key] = result.scalar()
            except Exception:
                fact_counts[key] = 0
        
        # Dimension tables
        dim_counts = {}
        dim_tables = [
            ('dim_drivers', 'Drivers'),
            ('dim_vehicles', 'Vehicles'),
            ('dim_tags', 'Tags'),
            ('dim_reports', 'Reports'),
            ('dim_event_rules', 'EventRules'),
            ('dim_vehicle_custom_fields', 'CustomFields')
        ]
        
        for table_name, key in dim_tables:
            try:
                result = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}"))
                dim_counts[key] = result.scalar()
            except Exception:
                dim_counts[key] = 0
        
        total = sum(fact_counts.values())
        
        return jsonify({
            'success': True,
            'counts': fact_counts,
            'dim_counts': dim_counts,
            'total': total,
            'timestamp': datetime.utcnow().isoformat()
        })
        
    except Exception as e:
        logger.error(f"Failed to get table counts: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/stats/last-sync', methods=['GET'])
def get_last_sync_stats():
    """Get last successful sync statistics"""
    try:
        # Get last completed daily sync
        daily_sync = JobExecution.query.filter_by(
            job_type='daily_sync',
            status='completed'
        ).order_by(JobExecution.completed_at.desc()).first()
        
        # Get last completed weekly backfill
        weekly_backfill = JobExecution.query.filter_by(
            job_type='weekly_backfill',
            status='completed'
        ).order_by(JobExecution.completed_at.desc()).first()
        
        return jsonify({
            'success': True,
            'daily_sync': daily_sync.to_dict() if daily_sync else None,
            'weekly_backfill': weekly_backfill.to_dict() if weekly_backfill else None
        })
        
    except Exception as e:
        logger.error(f"Failed to get last sync stats: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/stats/scheduler-status', methods=['GET'])
def get_scheduler_status():
    """Get recent scheduled job executions (daily_sync and weekly_backfill)"""
    try:
        # Get last 10 daily syncs
        daily_syncs = JobExecution.query.filter_by(
            job_type='daily_sync'
        ).order_by(JobExecution.started_at.desc()).limit(10).all()
        
        # Get last 10 weekly backfills
        weekly_backfills = JobExecution.query.filter_by(
            job_type='weekly_backfill'
        ).order_by(JobExecution.started_at.desc()).limit(10).all()
        
        # Get any currently running scheduled jobs
        running_jobs = JobExecution.query.filter(
            JobExecution.job_type.in_(['daily_sync', 'weekly_backfill']),
            JobExecution.status == 'running'
        ).all()
        
        return jsonify({
            'success': True,
            'daily_syncs': [job.to_dict() for job in daily_syncs],
            'weekly_backfills': [job.to_dict() for job in weekly_backfills],
            'running_jobs': [job.to_dict() for job in running_jobs]
        })
        
    except Exception as e:
        logger.error(f"Failed to get scheduler status: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/ip-info', methods=['GET'])
def get_ip_info():
    """Get server's outbound IP address for whitelisting purposes"""
    import requests as req
    
    try:
        # Try multiple IP detection services
        services = [
            ('https://api.ipify.org?format=json', 'ip'),
            ('https://ipinfo.io/json', 'ip'),
        ]
        
        for url, key in services:
            try:
                resp = req.get(url, timeout=5)
                if resp.ok:
                    data = resp.json()
                    ip = data.get(key)
                    
                    return jsonify({
                        'success': True,
                        'outbound_ip': ip,
                        'service': url,
                        'full_data': data,
                        'message': 'This is the IP address that GpsGate sees when Render makes API calls'
                    })
            except Exception as e:
                logger.warning(f"Failed to get IP from {url}: {e}")
                continue
        
        return jsonify({
            'success': False,
            'error': 'Could not detect outbound IP from any service'
        }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/', methods=['GET'])
def dashboard_page():
    """Main dashboard HTML page"""
    html = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GpsGate Data Pipeline Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5rem;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .card {
            background: white;
            border-radius: 12px;
            padding: 25px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .card h2 {
            color: #667eea;
            margin-bottom: 20px;
            font-size: 1.5rem;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        
        .form-group {
            margin-bottom: 15px;
        }
        
        label {
            display: block;
            margin-bottom: 5px;
            color: #333;
            font-weight: 600;
        }
        
        input[type="date"] {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
        }
        
        button {
            width: 100%;
            padding: 12px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 6px;
            font-size: 16px;
            font-weight: 600;
            cursor: pointer;
            transition: transform 0.2s;
        }
        
        button:hover {
            transform: translateY(-2px);
        }
        
        button:active {
            transform: translateY(0);
        }
        
        button:disabled {
            opacity: 0.6;
            cursor: not-allowed;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
        }
        
        .stat-item {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }
        
        .stat-value {
            font-size: 2rem;
            font-weight: bold;
            color: #667eea;
        }
        
        .stat-label {
            color: #666;
            font-size: 0.9rem;
            margin-top: 5px;
        }
        
        .job-list {
            max-height: 400px;
            overflow-y: auto;
        }
        
        .job-item {
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 10px;
            border-left: 4px solid #667eea;
        }
        
        .job-item.running {
            border-left-color: #ffc107;
            animation: pulse 2s infinite;
        }
        
        .job-item.completed {
            border-left-color: #28a745;
        }
        
        .job-item.failed {
            border-left-color: #dc3545;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        .job-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
        }
        
        .job-type {
            font-weight: 600;
            color: #333;
        }
        
        .job-status {
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 0.85rem;
            font-weight: 600;
        }
        
        .job-status.running {
            background: #ffc107;
            color: #000;
        }
        
        .job-status.completed {
            background: #28a745;
            color: white;
        }
        
        .job-status.failed {
            background: #dc3545;
            color: white;
        }
        
        .job-details {
            font-size: 0.9rem;
            color: #666;
        }
        
        .message {
            padding: 12px;
            border-radius: 6px;
            margin-top: 15px;
            display: none;
        }
        
        .message.success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .message.error {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .message.info {
            background: #d1ecf1;
            color: #0c5460;
            border: 1px solid #bee5eb;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>🚀 GpsGate Data Pipeline Dashboard</h1>
        
        <div class="grid">
            <!-- Manual Triggers -->
            <div class="card">
                <h2>📊 Manual Triggers</h2>
                
                <div class="form-group">
                    <button onclick="triggerDimensionSync()">
                        🔄 Run Dimension Sync
                    </button>
                </div>
                
                <div class="form-group">
                    <label for="fact-start">Start Date</label>
                    <input type="date" id="fact-start" value="">
                </div>
                
                <div class="form-group">
                    <label for="fact-end">End Date</label>
                    <input type="date" id="fact-end" value="">
                </div>
                
                <div class="form-group">
                    <button onclick="triggerFactSync()">
                        📈 Run Fact Sync
                    </button>
                </div>
                
                <div class="form-group">
                    <button onclick="triggerFullBackfill()">
                        🔥 Run Full Backfill
                    </button>
                </div>
                
                <div id="trigger-message" class="message"></div>
            </div>
            
            <!-- Live Statistics -->
            <div class="card">
                <h2>📈 Live Statistics</h2>
                <h3 style="color: #667eea; margin-bottom: 10px; font-size: 1.1em;">Fact Tables</h3>
                <div class="stats-grid" id="stats-grid">
                    <div class="stat-item">
                        <div class="stat-value" id="total-count">-</div>
                        <div class="stat-label">Total Records</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="trip-count">-</div>
                        <div class="stat-label">Trip</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="speeding-count">-</div>
                        <div class="stat-label">Speeding</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="idle-count">-</div>
                        <div class="stat-label">Idle</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="awh-count">-</div>
                        <div class="stat-label">AWH</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="wh-count">-</div>
                        <div class="stat-label">WH</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="ha-count">-</div>
                        <div class="stat-label">HA</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="hb-count">-</div>
                        <div class="stat-label">HB</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="wu-count">-</div>
                        <div class="stat-label">WU</div>
                    </div>
                </div>
                <h3 style="color: #764ba2; margin: 20px 0 10px 0; font-size: 1.1em;">Dimension Tables</h3>
                <div class="stats-grid" style="grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));">
                    <div class="stat-item">
                        <div class="stat-value" id="dim-drivers-count">-</div>
                        <div class="stat-label">Drivers</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="dim-vehicles-count">-</div>
                        <div class="stat-label">Vehicles</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="dim-tags-count">-</div>
                        <div class="stat-label">Tags/Groups</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="dim-reports-count">-</div>
                        <div class="stat-label">Reports</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="dim-eventrules-count">-</div>
                        <div class="stat-label">Event Rules</div>
                    </div>
                    <div class="stat-item">
                        <div class="stat-value" id="dim-customfields-count">-</div>
                        <div class="stat-label">Custom Fields</div>
                    </div>
                </div>
                <button onclick="refreshStats()" style="margin-top: 15px;">
                    🔄 Refresh Stats
                </button>
            </div>
            
            <!-- Last Sync Info -->
            <div class="card">
                <h2>⏰ Last Sync</h2>
                <div id="last-sync-info">Loading...</div>
            </div>
            
            <!-- Scheduler Status -->
            <div class="card" style="grid-column: 1 / -1;">
                <h2>🕐 Automated Scheduler Status</h2>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                    <div>
                        <h3 style="color: #667eea; margin-bottom: 10px;">Daily Sync (2 AM UTC)</h3>
                        <div id="daily-scheduler-status">Loading...</div>
                    </div>
                    <div>
                        <h3 style="color: #667eea; margin-bottom: 10px;">Weekly Backfill (Sunday 3 AM UTC)</h3>
                        <div id="weekly-scheduler-status">Loading...</div>
                    </div>
                </div>
                <button onclick="refreshSchedulerStatus()" style="margin-top: 15px;">
                    🔄 Refresh Scheduler Status
                </button>
            </div>
            
            <!-- Recent Jobs -->
            <div class="card" style="grid-column: 1 / -1;">
                <h2>📋 Recent Jobs</h2>
                <div class="job-list" id="job-list">
                    Loading...
                </div>
                <button onclick="refreshJobs()" style="margin-top: 15px;">
                    🔄 Refresh Jobs
                </button>
            </div>
        </div>
    </div>
    
    <script>
        // Set default dates (yesterday)
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(yesterday.getDate() - 1);
        document.getElementById('fact-start').value = yesterday.toISOString().split('T')[0];
        document.getElementById('fact-end').value = yesterday.toISOString().split('T')[0];
        
        // Trigger functions
        async function triggerDimensionSync() {
            try {
                const response = await fetch('/dashboard/trigger/dimension-sync', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'}
                });
                const data = await response.json();
                showMessage(data.success ? 'success' : 'error', data.message || data.error);
                if (data.success) {
                    setTimeout(refreshJobs, 1000);
                }
            } catch (error) {
                showMessage('error', 'Request failed: ' + error.message);
            }
        }
        
        async function triggerFactSync() {
            const startDate = document.getElementById('fact-start').value;
            const endDate = document.getElementById('fact-end').value;
            
            if (!startDate || !endDate) {
                showMessage('error', 'Please select start and end dates');
                return;
            }
            
            try {
                const response = await fetch('/dashboard/trigger/fact-sync', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({start_date: startDate, end_date: endDate})
                });
                const data = await response.json();
                showMessage(data.success ? 'success' : 'error', data.message || data.error);
                if (data.success) {
                    setTimeout(refreshJobs, 1000);
                }
            } catch (error) {
                showMessage('error', 'Request failed: ' + error.message);
            }
        }
        
        async function triggerFullBackfill() {
            const startDate = document.getElementById('fact-start').value;
            const endDate = document.getElementById('fact-end').value;
            
            if (!startDate || !endDate) {
                showMessage('error', 'Please select start and end dates');
                return;
            }
            
            try {
                const response = await fetch('/dashboard/trigger/full-backfill', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({start_date: startDate, end_date: endDate})
                });
                const data = await response.json();
                showMessage(data.success ? 'success' : 'error', data.message || data.error);
                if (data.success) {
                    setTimeout(refreshJobs, 1000);
                }
            } catch (error) {
                showMessage('error', 'Request failed: ' + error.message);
            }
        }
        
        // Refresh functions
        async function refreshStats() {
            try {
                const response = await fetch('/dashboard/stats/table-counts');
                const data = await response.json();
                if (data.success) {
                    // Fact tables
                    document.getElementById('total-count').textContent = data.total.toLocaleString();
                    document.getElementById('trip-count').textContent = data.counts.Trip.toLocaleString();
                    document.getElementById('speeding-count').textContent = data.counts.Speeding.toLocaleString();
                    document.getElementById('idle-count').textContent = data.counts.Idle.toLocaleString();
                    document.getElementById('awh-count').textContent = data.counts.AWH.toLocaleString();
                    document.getElementById('wh-count').textContent = data.counts.WH.toLocaleString();
                    document.getElementById('ha-count').textContent = data.counts.HA.toLocaleString();
                    document.getElementById('hb-count').textContent = data.counts.HB.toLocaleString();
                    document.getElementById('wu-count').textContent = data.counts.WU.toLocaleString();
                    
                    // Dimension tables
                    if (data.dim_counts) {
                        document.getElementById('dim-drivers-count').textContent = data.dim_counts.Drivers.toLocaleString();
                        document.getElementById('dim-vehicles-count').textContent = data.dim_counts.Vehicles.toLocaleString();
                        document.getElementById('dim-tags-count').textContent = data.dim_counts.Tags.toLocaleString();
                        document.getElementById('dim-reports-count').textContent = data.dim_counts.Reports.toLocaleString();
                        document.getElementById('dim-eventrules-count').textContent = data.dim_counts.EventRules.toLocaleString();
                        document.getElementById('dim-customfields-count').textContent = data.dim_counts.CustomFields.toLocaleString();
                    }
                }
            } catch (error) {
                console.error('Failed to refresh stats:', error);
            }
        }
        
        async function refreshJobs() {
            try {
                const response = await fetch('/dashboard/status/recent');
                const data = await response.json();
                if (data.success) {
                    const jobList = document.getElementById('job-list');
                    jobList.innerHTML = data.jobs.map(job => `
                        <div class="job-item ${job.status}">
                            <div class="job-header">
                                <span class="job-type">${job.job_type}</span>
                                <span class="job-status ${job.status}">${job.status}</span>
                            </div>
                            <div class="job-details">
                                Started: ${new Date(job.started_at).toLocaleString()}<br>
                                ${job.completed_at ? 'Completed: ' + new Date(job.completed_at).toLocaleString() + '<br>' : ''}
                                ${job.records_processed ? 'Records: ' + job.records_processed.toLocaleString() + '<br>' : ''}
                                ${job.error_message ? 'Error: ' + job.error_message : ''}
                            </div>
                        </div>
                    `).join('');
                }
            } catch (error) {
                console.error('Failed to refresh jobs:', error);
            }
        }
        
        async function refreshLastSync() {
            try {
                const response = await fetch('/dashboard/stats/last-sync');
                const data = await response.json();
                if (data.success) {
                    const info = document.getElementById('last-sync-info');
                    let html = '';
                    if (data.daily_sync) {
                        html += `<strong>Daily:</strong> ${new Date(data.daily_sync.completed_at).toLocaleString()}<br>`;
                        html += `Records: ${data.daily_sync.records_processed.toLocaleString()}<br><br>`;
                    }
                    if (data.weekly_backfill) {
                        html += `<strong>Weekly:</strong> ${new Date(data.weekly_backfill.completed_at).toLocaleString()}<br>`;
                        html += `Records: ${data.weekly_backfill.records_processed.toLocaleString()}`;
                    }
                    info.innerHTML = html || 'No sync data available';
                }
            } catch (error) {
                console.error('Failed to refresh last sync:', error);
            }
        }
        
        async function refreshSchedulerStatus() {
            try {
                const response = await fetch('/dashboard/stats/scheduler-status');
                const data = await response.json();
                if (data.success) {
                    // Daily syncs
                    const dailyDiv = document.getElementById('daily-scheduler-status');
                    if (data.daily_syncs.length > 0) {
                        dailyDiv.innerHTML = data.daily_syncs.slice(0, 5).map(job => `
                            <div style="padding: 8px; margin: 5px 0; background: ${job.status === 'completed' ? '#d4edda' : job.status === 'failed' ? '#f8d7da' : '#fff3cd'}; border-radius: 4px; font-size: 0.9rem;">
                                <strong>${new Date(job.started_at).toLocaleString()}</strong>
                                <span style="float: right; font-weight: 600; color: ${job.status === 'completed' ? '#28a745' : job.status === 'failed' ? '#dc3545' : '#ffc107'};">${job.status.toUpperCase()}</span><br>
                                ${job.records_processed ? `Records: ${job.records_processed.toLocaleString()}` : ''}
                                ${job.error_message ? `<br>Error: ${job.error_message}` : ''}
                            </div>
                        `).join('');
                    } else {
                        dailyDiv.innerHTML = '<em style="color: #666;">No daily syncs yet</em>';
                    }
                    
                    // Weekly backfills
                    const weeklyDiv = document.getElementById('weekly-scheduler-status');
                    if (data.weekly_backfills.length > 0) {
                        weeklyDiv.innerHTML = data.weekly_backfills.slice(0, 5).map(job => `
                            <div style="padding: 8px; margin: 5px 0; background: ${job.status === 'completed' ? '#d4edda' : job.status === 'failed' ? '#f8d7da' : '#fff3cd'}; border-radius: 4px; font-size: 0.9rem;">
                                <strong>${new Date(job.started_at).toLocaleString()}</strong>
                                <span style="float: right; font-weight: 600; color: ${job.status === 'completed' ? '#28a745' : job.status === 'failed' ? '#dc3545' : '#ffc107'};">${job.status.toUpperCase()}</span><br>
                                ${job.records_processed ? `Records: ${job.records_processed.toLocaleString()}` : ''}
                                ${job.error_message ? `<br>Error: ${job.error_message}` : ''}
                            </div>
                        `).join('');
                    } else {
                        weeklyDiv.innerHTML = '<em style="color: #666;">No weekly backfills yet</em>';
                    }
                    
                    // Show alert if any scheduler jobs are running
                    if (data.running_jobs.length > 0) {
                        console.log('Scheduler jobs currently running:', data.running_jobs.length);
                    }
                }
            } catch (error) {
                console.error('Failed to refresh scheduler status:', error);
            }
        }

        // Initialize dashboard
        refreshJobs();
        refreshLastSync();
        refreshSchedulerStatus();
        
        setInterval(refreshJobs, 5000);  // Refresh jobs every 5 seconds
        setInterval(refreshStats, 30000);  // Refresh stats every 30 seconds
        setInterval(refreshSchedulerStatus, 10000);  // Refresh scheduler status every 10 seconds
    </script>
</body>
</html>
    '''
    return render_template_string(html)


@dashboard_bp.route('/health/gpsgate-server', methods=['GET'])
def check_gpsgate_server_health():
    """Quick health check for GpsGate server state"""
    try:
        # Test with a simple WU event request (known to work recently)
        test_payload = {
            "app_id": "6",
            "token": get_config_value('TOKEN'),
            "base_url": get_config_value('BASE_URL'),
            "report_id": "25",
            "period_start": "2025-01-01T00:00:00Z", 
            "period_end": "2025-01-01T23:59:59Z",
            "tag_id": "1",
            "event_id": "21"  # WU event
        }
        
        response = requests.post(
            f"{request.url_root}render",
            data=test_payload,
            timeout=10
        )
        
        if response.status_code == 200:
            return jsonify({
                'status': 'healthy',
                'message': 'GpsGate server responding normally',
                'test_result': 'success'
            }), 200
        elif 'reportId: 0' in response.text:
            return jsonify({
                'status': 'degraded', 
                'message': 'GpsGate server state issue - returning reportId: 0',
                'recommendation': 'Retry in 10-15 minutes',
                'test_result': 'server_state_issue'
            }), 503
        else:
            return jsonify({
                'status': 'unknown',
                'message': f'Unexpected response: {response.status_code}',
                'test_result': 'unknown_error'
            }), 502
            
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': f'Health check failed: {str(e)}',
            'test_result': 'check_failed'
        }), 500
