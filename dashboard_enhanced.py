"""
Enhanced Dashboard with Manual Trigger Controls
Provides live status monitoring and manual job execution
"""

from flask import Blueprint, render_template_string, jsonify, request
from datetime import datetime, timedelta
import traceback
import threading
import json
import requests
from models import (
    db,
    CustomerConfig,
    JobExecution,
    FactTrip,
    FactSpeeding,
    FactIdle,
    FactAWH,
    FactWH,
    FactHA,
    FactHB,
    FactWU,
)
from customer_runtime_config import EVENT_CONFIG, get_event_runtime_config, load_customers
from data_pipeline import process_event_data
from logger_config import get_logger
from config import Config

logger = get_logger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

# Store active job threads
active_jobs = {}


def mask_token(token: str | None) -> str:
    token = (token or "").strip()
    if len(token) <= 10:
        return token
    return f"{token[:6]}...{token[-4:]}"


def serialize_customer_config(customer: CustomerConfig) -> dict:
    return {
        "application_id": customer.application_id,
        "token": mask_token(customer.token),
        "tag_id": customer.tag_id,
        "trip_report_id": customer.trip_report_id,
        "event_report_id": customer.event_report_id,
    }


def get_dashboard_customer(application_id: str | None = None) -> CustomerConfig:
    if application_id:
        customer = db.session.get(CustomerConfig, str(application_id))
        if not customer:
            raise RuntimeError(f"No customer_config row found for application_id={application_id}")
        return customer

    customers = load_customers()
    if not customers:
        raise RuntimeError("No customer_config rows found for dashboard/manual trigger")

    customer = customers[0]
    logger.warning(
        f"Dashboard/manual trigger defaulted to application_id={customer.application_id} because no customer was specified"
    )
    return customer

def process_event_with_dates(app, event_type, start_date, end_date, customer=None):
    """
    Helper function to process event data for a specific date range.
    Creates Flask request context with proper payload.
    """
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")
    
    if customer is None:
        customer = get_dashboard_customer()

    runtime = get_event_runtime_config(customer, event_type, Config.BASE_URL)
    
    # Prepare payload matching process_event_data signature (MATCHES backfill_2025_week1.py)
    payload = {
        "app_id": runtime.app_id,
        "token": runtime.token,
        "base_url": runtime.base_url,
        "report_id": runtime.report_id,
        "tag_id": runtime.tag_id,
        "period_start": f"{start_date}T00:00:00Z",
        "period_end": f"{end_date}T23:59:59Z"
    }
    
    if runtime.event_id:
        payload["event_id"] = runtime.event_id
    
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
            response_key=runtime.response_key
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


def iter_week_ranges(start_date: str, end_date: str):
    """Yield inclusive 7-day windows between start_date and end_date."""
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    final = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current <= final:
        week_end = min(current + timedelta(days=6), final)
        yield current.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")
        current = week_end + timedelta(days=1)


# ------------------------------------------------------------------
# MANUAL JOB EXECUTORS (Run in background threads)
# ------------------------------------------------------------------

def execute_dimension_sync_job(job_id, application_id=None):
    """Execute dimension sync in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            # Import dimension sync function
            from sync_dimensions_from_api import main as sync_dimension_main
            
            logger.info(f"🚀 Starting dimension sync job {job_id}")
            total_records = sync_dimension_main(application_id)  # Now returns record count
            
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_records
            job.job_metadata = {
                'message': f'Dimension sync completed - {total_records:,} records processed',
                'total_records': total_records,
                'application_id': str(application_id) if application_id else None,
            }
            db.session.commit()
            
            logger.info(f"✅ Dimension sync completed: {total_records:,} records processed")
            
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


def execute_fact_sync_job(job_id, start_date, end_date, application_id=None):
    """Execute fact table sync in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Starting fact sync job {job_id}: {start_date} to {end_date}")
            
            # Process all event types (server state issue resolved)
            event_types = list(EVENT_CONFIG.keys())
            customer = get_dashboard_customer(application_id)

            week_ranges = list(iter_week_ranges(start_date, end_date))
            results = {}
            total_inserted = 0
            total_skipped = 0
            total_failed = 0

            for week_start, week_end in week_ranges:
                week_key = f"{week_start} -> {week_end}"
                results[week_key] = {}
                for event_type in event_types:
                    try:
                        logger.info(f"Processing {event_type} for {week_key}")
                        result = process_event_with_dates(
                            app=app,
                            event_type=event_type,
                            start_date=week_start,
                            end_date=week_end,
                            customer=customer
                        )
                        results[week_key][event_type] = result
                        total_inserted += result.get('inserted', 0)
                        total_skipped += result.get('skipped', 0)
                        total_failed += result.get('failed', 0)
                    except Exception as e:
                        logger.error(f"Fact sync failed for {event_type} {week_key}: {str(e)}")
                        results[week_key][event_type] = {'status': 'failed', 'error': str(e)}
                        total_failed += 1

            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'results': results,
                'start_date': start_date,
                'end_date': end_date,
                'total_events_processed': len(event_types),
                'application_id': str(customer.application_id),
                'weeks_processed': len(week_ranges),
                'total_skipped': total_skipped,
            }
            db.session.commit()

            logger.info(
                f"Fact sync completed: {total_inserted} inserted, "
                f"{total_skipped} skipped, {total_failed} failed"
            )
            return
            
            results = {}
            total_inserted = 0
            total_failed = 0
            
            for event_type in event_types:
                try:
                    logger.info(f"Processing {event_type} for {start_date} to {end_date}")
                    result = process_event_with_dates(
                        app=app,
                        event_type=event_type,
                        start_date=start_date,
                        end_date=end_date,
                        customer=customer
                    )
                    results[event_type] = result
                    total_inserted += result.get('inserted', 0)
                    total_failed += result.get('failed', 0)
                    logger.info(f"✅ {event_type}: {result.get('inserted', 0)} inserted, {result.get('failed', 0)} failed")
                except Exception as e:
                    logger.error(f"❌ {event_type} failed: {str(e)}")
                    results[event_type] = {'status': 'failed', 'error': str(e)}
                    total_failed += 1
            
            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'results': results, 
                'start_date': start_date, 
                'end_date': end_date,
                'total_events_processed': len(event_types),
                'application_id': str(customer.application_id)
            }
            db.session.commit()
            
            logger.info(f"✅ Fact sync completed: {total_inserted} inserted, {total_failed} failed")
            
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


def execute_full_backfill_job(job_id, start_date, end_date, application_id=None):
    """Execute full backfill (dimensions + facts) in background"""
    from application import create_app
    app = create_app()
    
    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Starting full backfill job {job_id}: {start_date} to {end_date}")
            
            # Step 1: Sync dimensions
            from sync_dimensions_from_api import main as sync_dimension_main
            sync_dimension_main(application_id)
            logger.info(f"✅ Dimensions synced from API")
            
            # Step 2: Sync facts
            event_types = ['Trip', 'Speeding', 'Idle', 'AWH', 'WH', 'HA', 'HB', 'WU']
            customer = get_dashboard_customer(application_id)

            week_ranges = list(iter_week_ranges(start_date, end_date))
            results = {}
            total_inserted = 0
            total_skipped = 0
            total_failed = 0

            for week_start, week_end in week_ranges:
                week_key = f"{week_start} -> {week_end}"
                results[week_key] = {}
                for event_type in event_types:
                    try:
                        result = process_event_with_dates(
                            app=app,
                            event_type=event_type,
                            start_date=week_start,
                            end_date=week_end,
                            customer=customer
                        )
                        results[week_key][event_type] = result
                        total_inserted += result.get('inserted', 0)
                        total_skipped += result.get('skipped', 0)
                        total_failed += result.get('failed', 0)
                    except Exception as e:
                        logger.error(f"Full backfill failed for {event_type} {week_key}: {str(e)}")
                        results[week_key][event_type] = {'status': 'failed', 'error': str(e)}
                        total_failed += 1

            job.status = 'completed'
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                'dimensions': 'synced from GpsGate API',
                'facts': results,
                'start_date': start_date,
                'end_date': end_date,
                'application_id': str(customer.application_id),
                'weeks_processed': len(week_ranges),
                'total_skipped': total_skipped,
            }
            db.session.commit()

            logger.info(
                f"Full backfill completed: {total_inserted} inserted, "
                f"{total_skipped} skipped, {total_failed} failed"
            )
            return

            results = {}
            total_inserted = 0
            total_failed = 0
            
            for event_type in event_types:
                try:
                    result = process_event_with_dates(
                        app=app,
                        event_type=event_type,
                        start_date=start_date,
                        end_date=end_date,
                        customer=customer
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
                'end_date': end_date,
                'application_id': str(customer.application_id)
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

@dashboard_bp.route('/customer-config', methods=['GET'])
def list_customer_config():
    """List current customer_config rows with masked tokens."""
    try:
        customers = (
            db.session.query(CustomerConfig)
            .order_by(CustomerConfig.application_id.asc())
            .all()
        )
        return jsonify({
            'success': True,
            'customers': [serialize_customer_config(customer) for customer in customers]
        })
    except Exception as e:
        logger.error(f"Failed to list customer_config: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/customer-config', methods=['POST'])
def save_customer_config():
    """Create or update customer_config with application_id and token."""
    try:
        data = request.get_json() or {}
        application_id = str(data.get('application_id', '')).strip()
        token = str(data.get('token', '')).strip()

        if not application_id or not token:
            return jsonify({
                'success': False,
                'error': 'application_id and token are required'
            }), 400

        customer = db.session.get(CustomerConfig, application_id)
        created = customer is None
        if customer is None:
            customer = CustomerConfig(application_id=application_id)
            db.session.add(customer)

        customer.token = token
        customer.tag_id = None
        customer.trip_report_id = None
        customer.event_report_id = None
        customer.awh_event_id = None
        customer.ha_event_id = None
        customer.hb_event_id = None
        customer.hc_event_id = None
        customer.wu_event_id = None
        customer.wh_event_id = None
        customer.speed_event_id = None
        customer.idle_event_id = None
        db.session.commit()

        return jsonify({
            'success': True,
            'message': (
                f"Customer config {'created' if created else 'updated'} for application_id={application_id}. "
                "Run Dimension Sync to populate report, tag, and event IDs."
            ),
            'customer': serialize_customer_config(customer)
        }), 201 if created else 200
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to save customer_config: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/trigger/dimension-sync', methods=['POST'])
def trigger_dimension_sync():
    """Trigger manual dimension sync"""
    try:
        data = request.get_json() or {}
        application_id = str(data.get('application_id', '')).strip() or None

        # Create job execution record
        job = JobExecution(
            job_type='manual_dimension_sync',
            status='running',
            started_at=datetime.utcnow(),
            triggered_by='manual',
            job_metadata={'application_id': application_id}
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(target=execute_dimension_sync_job, args=(job.id, application_id))
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': (
                f"Dimension sync started for application_id={application_id}"
                if application_id else
                'Dimension sync started'
            )
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
        application_id = str(data.get('application_id', '')).strip() or None
        
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
            job_metadata={'start_date': start_date, 'end_date': end_date, 'application_id': application_id}
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(
            target=execute_fact_sync_job,
            args=(job.id, start_date, end_date, application_id)
        )
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': (
                f'Fact sync started for application_id={application_id} from {start_date} to {end_date}'
                if application_id else
                f'Fact sync started for {start_date} to {end_date}'
            )
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
        application_id = str(data.get('application_id', '')).strip() or None
        
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
            job_metadata={'start_date': start_date, 'end_date': end_date, 'application_id': application_id}
        )
        db.session.add(job)
        db.session.commit()
        
        # Start background thread
        thread = threading.Thread(
            target=execute_full_backfill_job,
            args=(job.id, start_date, end_date, application_id)
        )
        thread.daemon = True
        thread.start()
        
        active_jobs[job.id] = thread
        
        return jsonify({
            'success': True,
            'job_id': job.id,
            'message': (
                f'Full backfill started for application_id={application_id} from {start_date} to {end_date}'
                if application_id else
                f'Full backfill started for {start_date} to {end_date}'
            )
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
        
        input[type="date"],
        input[type="text"],
        select,
        textarea {
            width: 100%;
            padding: 10px;
            border: 1px solid #ddd;
            border-radius: 6px;
            font-size: 14px;
            font-family: inherit;
        }

        textarea {
            min-height: 90px;
            resize: vertical;
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
                    <label for="customer-app-id">Customer App ID</label>
                    <input type="text" id="customer-app-id" placeholder="e.g. 93">
                </div>
                
                <div class="form-group">
                    <label for="customer-token">Customer Token</label>
                    <textarea id="customer-token" placeholder="Paste GPSGate token"></textarea>
                </div>
                
                <div class="form-group">
                    <button onclick="saveCustomerConfig()">
                        Save Customer Config
                    </button>
                </div>
                
                <div id="customer-config-list" class="job-list" style="max-height: 180px; margin-bottom: 15px;"></div>
                <div id="customer-config-message" class="message"></div>
                
                <div class="form-group">
                    <label for="manual-application-id">Run For Customer</label>
                    <select id="manual-application-id">
                        <option value="">Select customer</option>
                    </select>
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
        
        // Message display function
        function showMessage(type, text) {
            const msg = document.getElementById('trigger-message');
            msg.className = 'message ' + type;
            msg.textContent = text;
            msg.style.display = 'block';
            setTimeout(() => {
                msg.style.display = 'none';
            }, 5000);
        }

        function showCustomerConfigMessage(type, text) {
            const msg = document.getElementById('customer-config-message');
            msg.className = 'message ' + type;
            msg.textContent = text;
            msg.style.display = 'block';
            setTimeout(() => {
                msg.style.display = 'none';
            }, 5000);
        }

        function getSelectedApplicationId() {
            return document.getElementById('manual-application-id').value.trim();
        }

        async function refreshCustomerConfigs() {
            try {
                const response = await fetch('/dashboard/customer-config');
                const data = await response.json();
                const list = document.getElementById('customer-config-list');
                const select = document.getElementById('manual-application-id');
                const currentSelection = select.value;
                if (!data.success) {
                    list.innerHTML = '<em style="color: #666;">Failed to load customer config</em>';
                    return;
                }
                if (!data.customers.length) {
                    list.innerHTML = '<em style="color: #666;">No customer_config rows yet</em>';
                    select.innerHTML = '<option value="">Select customer</option>';
                    return;
                }
                select.innerHTML = '<option value="">Select customer</option>' + data.customers.map(customer => `
                    <option value="${customer.application_id}">App ${customer.application_id}</option>
                `).join('');
                if (currentSelection && data.customers.some(customer => customer.application_id === currentSelection)) {
                    select.value = currentSelection;
                } else {
                    select.value = data.customers[0].application_id;
                }
                list.innerHTML = data.customers.map(customer => `
                    <div class="job-item" style="padding: 10px; margin-bottom: 8px;">
                        <div class="job-header">
                            <span class="job-type">App ${customer.application_id}</span>
                        </div>
                        <div class="job-details">
                            Token: ${customer.token}<br>
                            Trip Report: ${customer.trip_report_id || '-'}<br>
                            Event Report: ${customer.event_report_id || '-'}<br>
                            Tag: ${customer.tag_id || '-'}
                        </div>
                    </div>
                `).join('');
            } catch (error) {
                console.error('Failed to refresh customer config:', error);
            }
        }

        async function saveCustomerConfig() {
            const applicationId = document.getElementById('customer-app-id').value.trim();
            const token = document.getElementById('customer-token').value.trim();

            if (!applicationId || !token) {
                showCustomerConfigMessage('error', 'Please enter both application ID and token');
                return;
            }

            try {
                const response = await fetch('/dashboard/customer-config', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({application_id: applicationId, token})
                });
                const data = await response.json();
                showCustomerConfigMessage(data.success ? 'success' : 'error', data.message || data.error);
                if (data.success) {
                    document.getElementById('customer-app-id').value = '';
                    document.getElementById('customer-token').value = '';
                    refreshCustomerConfigs();
                }
            } catch (error) {
                showCustomerConfigMessage('error', 'Request failed: ' + error.message);
            }
        }
        
        // Trigger functions
        async function triggerDimensionSync() {
            const applicationId = getSelectedApplicationId();
            if (!applicationId) {
                showMessage('error', 'Please select a customer');
                return;
            }
            try {
                const response = await fetch('/dashboard/trigger/dimension-sync', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({application_id: applicationId})
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
            const applicationId = getSelectedApplicationId();
            const startDate = document.getElementById('fact-start').value;
            const endDate = document.getElementById('fact-end').value;
            
            if (!applicationId) {
                showMessage('error', 'Please select a customer');
                return;
            }
            
            if (!startDate || !endDate) {
                showMessage('error', 'Please select start and end dates');
                return;
            }
            
            try {
                const response = await fetch('/dashboard/trigger/fact-sync', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        application_id: applicationId,
                        start_date: startDate,
                        end_date: endDate
                    })
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
            const applicationId = getSelectedApplicationId();
            const startDate = document.getElementById('fact-start').value;
            const endDate = document.getElementById('fact-end').value;
            
            if (!applicationId) {
                showMessage('error', 'Please select a customer');
                return;
            }
            
            if (!startDate || !endDate) {
                showMessage('error', 'Please select start and end dates');
                return;
            }
            
            try {
                const response = await fetch('/dashboard/trigger/full-backfill', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        application_id: applicationId,
                        start_date: startDate,
                        end_date: endDate
                    })
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

        function formatJobMetadata(job) {
            const metadata = job.metadata || {};
            const details = [];

            if (metadata.application_id) {
                details.push(`Customer: ${metadata.application_id}`);
            }
            if (metadata.start_date && metadata.end_date) {
                details.push(`Range: ${metadata.start_date} to ${metadata.end_date}`);
            }
            if (metadata.weeks_processed) {
                details.push(`Weeks: ${metadata.weeks_processed}`);
            }
            if (metadata.total_skipped) {
                details.push(`Skipped: ${Number(metadata.total_skipped).toLocaleString()}`);
            }
            if (metadata.total_records) {
                details.push(`Total Records: ${Number(metadata.total_records).toLocaleString()}`);
            }
            if (metadata.message) {
                details.push(metadata.message);
            }

            return details.length ? details.join('<br>') + '<br>' : '';
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
	                                ${formatJobMetadata(job)}
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
	                                ${formatJobMetadata(job)}
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
	                                ${formatJobMetadata(job)}
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
        refreshCustomerConfigs();
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
        customers = load_customers()
        if not customers:
            return jsonify({'status': 'degraded', 'message': 'No customer_config rows found'}), 503

        runtime = get_event_runtime_config(customers[0], "WU", Config.BASE_URL)
        test_payload = {
            "app_id": runtime.app_id,
            "token": runtime.token,
            "base_url": runtime.base_url,
            "report_id": runtime.report_id,
            "period_start": "2025-01-01T00:00:00Z", 
            "period_end": "2025-01-01T23:59:59Z",
            "tag_id": runtime.tag_id,
            "event_id": runtime.event_id
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
