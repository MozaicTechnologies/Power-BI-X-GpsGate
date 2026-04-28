"""
Enhanced Dashboard with Manual Trigger Controls
Provides live status monitoring and manual job execution
"""

from flask import Blueprint, render_template, jsonify, request
from datetime import datetime, timedelta
from urllib.parse import urljoin
import traceback
import threading
import json
import requests
import os
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
    DimTags,
    DimEventRules,
    DimReports,
    DimVehicles,
    DimDrivers,
    DimVehicleCustomFields,
)
from customer_runtime_config import EVENT_CONFIG, get_event_runtime_config, load_customers, normalize_token
from data_pipeline import process_event_data
from utils.logger import setup_logger
from config import Config

logger = setup_logger(__name__)

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
        "tag_name": customer.tag_name,
        "trip_report_name": customer.trip_report_name,
        "event_report_name": customer.event_report_name,
        "speed_event_rule_name": customer.speed_event_rule_name,
        "idle_event_rule_name": customer.idle_event_rule_name,
        "awh_event_rule_name": customer.awh_event_rule_name,
        "ha_event_rule_name": customer.ha_event_rule_name,
        "hb_event_rule_name": customer.hb_event_rule_name,
        "hc_event_rule_name": customer.hc_event_rule_name,
        "wu_event_rule_name": customer.wu_event_rule_name,
        "wh_event_rule_name": customer.wh_event_rule_name,
        "tag_id": customer.tag_id,
        "trip_report_id": customer.trip_report_id,
        "event_report_id": customer.event_report_id,
        "speed_event_id": customer.speed_event_id,
        "idle_event_id": customer.idle_event_id,
        "awh_event_id": customer.awh_event_id,
        "ha_event_id": customer.ha_event_id,
        "hb_event_id": customer.hb_event_id,
        "hc_event_id": customer.hc_event_id,
        "wu_event_id": customer.wu_event_id,
        "wh_event_id": customer.wh_event_id,
    }


NAME_TO_ID_FIELD_MAP = {
    "tag_name": "tag_id",
    "trip_report_name": "trip_report_id",
    "event_report_name": "event_report_id",
    "speed_event_rule_name": "speed_event_id",
    "idle_event_rule_name": "idle_event_id",
    "awh_event_rule_name": "awh_event_id",
    "ha_event_rule_name": "ha_event_id",
    "hb_event_rule_name": "hb_event_id",
    "hc_event_rule_name": "hc_event_id",
    "wu_event_rule_name": "wu_event_id",
    "wh_event_rule_name": "wh_event_id",
}

ID_FIELDS = tuple(NAME_TO_ID_FIELD_MAP.values())


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
        name_fields = {
            field_name: str(data.get(field_name, '')).strip() or None
            for field_name in NAME_TO_ID_FIELD_MAP
            if field_name in data
        }
        id_fields = {
            field_name: str(data.get(field_name, '')).strip() or None
            for field_name in ID_FIELDS
            if field_name in data
        }

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

        changed_name_fields = []
        for field_name, new_value in name_fields.items():
            old_value = getattr(customer, field_name)
            if old_value != new_value:
                changed_name_fields.append(field_name)
                setattr(customer, field_name, new_value)

        for id_field, new_id in id_fields.items():
            setattr(customer, id_field, new_id)

        for field_name in changed_name_fields:
            id_field = NAME_TO_ID_FIELD_MAP[field_name]
            if id_field not in id_fields:
                setattr(customer, id_field, None)

        db.session.commit()

        has_unfilled_ids = any(
            field_name in name_fields
            and name_fields[field_name]
            and not getattr(customer, NAME_TO_ID_FIELD_MAP[field_name])
            for field_name in NAME_TO_ID_FIELD_MAP
        )
        message = f"Customer config {'created' if created else 'updated'} for application_id={application_id}."
        if has_unfilled_ids:
            message += " Run Dimension Sync to populate missing report, tag, and event IDs."

        return jsonify({
            'success': True,
            'message': message,
            'customer': serialize_customer_config(customer)
        }), 201 if created else 200
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to save customer_config: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/customer-config/options', methods=['POST'])
def fetch_customer_config_options():
    """Fetch tags, reports, and event rules from GpsGate API for dropdown selection."""
    try:
        data = request.get_json() or {}
        application_id = str(data.get('application_id', '')).strip()
        token = str(data.get('token', '')).strip()

        if not application_id or not token:
            return jsonify({
                'success': False,
                'error': 'application_id and token are required'
            }), 400

        auth_token = normalize_token(token)
        base = Config.BASE_URL if Config.BASE_URL.endswith('/') else Config.BASE_URL + '/'
        headers = {'Authorization': auth_token}

        endpoints = {
            'tags': f"comGpsGate/api/v.1/applications/{application_id}/tags",
            'event_rules': f"comGpsGate/api/v.1/applications/{application_id}/eventrules",
            'reports': f"comGpsGate/api/v.1/applications/{application_id}/reports",
        }

        results = {}
        for key, path in endpoints.items():
            url = urljoin(base, path)
            resp = requests.get(url, headers=headers, timeout=30)
            if not resp.ok:
                return jsonify({
                    'success': False,
                    'error': f"GpsGate {key} request failed: {resp.status_code} {resp.text[:200]}"
                }), 502
            items = resp.json() or []
            results[key] = sorted(
                [
                    {'id': str(item.get('id')), 'name': (item.get('name') or '').strip()}
                    for item in items
                    if item.get('id') is not None
                ],
                key=lambda r: r['name'].lower(),
            )

        return jsonify({
            'success': True,
            **results,
        })
    except Exception as e:
        logger.error(f"Failed to fetch customer config options: {str(e)}")
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


@dashboard_bp.route('/cleanup', methods=['POST'])
def cleanup_data():
    """Clean up (delete) data for a specific customer - DANGEROUS OPERATION"""
    try:
        data = request.get_json() or {}
        table_type = data.get('table_type')
        application_id = str(data.get('application_id', '')).strip()
        api_key = data.get('api_key')

        logger.info(f"ADMIN CLEANUP REQUEST: table_type={table_type}, application_id={application_id}, api_key_provided={'Yes' if api_key else 'No'}")

        # Validate API key (should be set as environment variable)
        expected_key = os.getenv('ADMIN_CLEANUP_KEY', 'demo-admin-key-2026')  # Demo key for development
        if api_key != expected_key:
            logger.warning(f"ADMIN CLEANUP FAILED: Invalid API key for application_id={application_id}")
            return jsonify({
                'success': False,
                'error': 'Invalid admin API key'
            }), 403

        if not table_type or not application_id:
            logger.warning(f"ADMIN CLEANUP FAILED: Missing required parameters - table_type={table_type}, application_id={application_id}")
            return jsonify({
                'success': False,
                'error': 'table_type and application_id are required'
            }), 400

        # Validate and convert application_id to int for dimension tables
        try:
            application_id_int = int(application_id)
        except ValueError:
            return jsonify({
                'success': False,
                'error': 'application_id must be a valid integer'
            }), 400

        # Validate table type
        if table_type not in ['fact', 'dimension', 'both']:
            logger.warning(f"ADMIN CLEANUP FAILED: Invalid table_type={table_type}")
            return jsonify({
                'success': False,
                'error': 'Invalid table_type. Must be fact, dimension, or both'
            }), 400

        logger.info(f"ADMIN CLEANUP STARTED: table_type={table_type}, application_id={application_id}")
        total_deleted = 0
        operations = []
        errors = []

        # Perform deletions one by one to avoid transaction abortion
        if table_type in ['fact', 'both']:
            logger.debug(f"ADMIN CLEANUP: Processing fact tables for application_id={application_id}")

            # Delete from fact tables using SQLAlchemy models
            fact_models = [
                (FactTrip, 'Trip'),
                (FactSpeeding, 'Speeding'),
                (FactIdle, 'Idle'),
                (FactAWH, 'AWH'),
                (FactWH, 'WH'),
                (FactHA, 'HA'),
                (FactHB, 'HB'),
                (FactWU, 'WU'),
            ]

            for model_class, display_name in fact_models:
                try:
                    logger.debug(f"ADMIN CLEANUP: Processing {model_class.__tablename__}")

                    with db.session.begin():
                        # Count records before deletion
                        count_before = model_class.query.filter_by(app_id=application_id).count()
                        logger.info(f"ADMIN CLEANUP: Found {count_before} records in {display_name} for app_id={application_id}")

                        # Delete records
                        deleted = model_class.query.filter_by(app_id=application_id).delete()
                        total_deleted += deleted

                        logger.info(f"ADMIN CLEANUP: Deleted {deleted} records from {display_name} (app_id={application_id})")

                        if deleted > 0:
                            operations.append(f"Deleted {deleted} records from {display_name}")
                        else:
                            operations.append(f"No records found in {display_name}")
                except Exception as e:
                    error_msg = f"Failed to delete from {display_name}: {str(e)}"
                    logger.error(f"ADMIN CLEANUP ERROR in {model_class.__tablename__}: {str(e)}")
                    logger.error(f"ADMIN CLEANUP ERROR DETAILS: type={type(e).__name__}, args={e.args}")
                    errors.append(error_msg)
                except Exception as e:
                    error_msg = f"Failed to delete from {display_name}: {str(e)}"
                    logger.error(f"ADMIN CLEANUP ERROR: {error_msg}")
                    errors.append(error_msg)

        if table_type in ['dimension', 'both']:
            logger.debug(f"ADMIN CLEANUP: Processing dimension tables for application_id={application_id}")

            # Delete from dimension tables using SQLAlchemy models
            dim_models = [
                (DimDrivers, 'Drivers'),
                (DimVehicles, 'Vehicles'),
                (DimTags, 'Tags'),
                (DimReports, 'Reports'),
                (DimEventRules, 'EventRules'),
                (DimVehicleCustomFields, 'CustomFields')
            ]

            for model_class, display_name in dim_models:
                try:
                    logger.debug(f"ADMIN CLEANUP: Processing {model_class.__tablename__}")

                    with db.session.begin():
                        # Use raw SQL to avoid model column mismatches
                        from sqlalchemy import text

                        # Count records before deletion
                        count_result = db.session.execute(
                            text(f"SELECT COUNT(*) FROM {model_class.__tablename__} WHERE application_id = :app_id"),
                            {'app_id': application_id_int}
                        )
                        count_before = count_result.scalar()
                        logger.info(f"ADMIN CLEANUP: Found {count_before} records in {display_name} for application_id={application_id}")

                        # Delete records
                        delete_result = db.session.execute(
                            text(f"DELETE FROM {model_class.__tablename__} WHERE application_id = :app_id"),
                            {'app_id': application_id_int}
                        )
                        deleted = delete_result.rowcount
                        total_deleted += deleted

                        logger.info(f"ADMIN CLEANUP: Deleted {deleted} records from {display_name} (application_id={application_id})")

                        if deleted > 0:
                            operations.append(f"Deleted {deleted} records from {display_name}")
                        else:
                            operations.append(f"No records found in {display_name}")
                except Exception as e:
                    error_msg = f"Failed to delete from {display_name}: {str(e)}"
                    logger.error(f"ADMIN CLEANUP ERROR in {model_class.__tablename__}: {str(e)}")
                    logger.error(f"ADMIN CLEANUP ERROR DETAILS: type={type(e).__name__}, args={e.args}")
                    errors.append(error_msg)

        # Delete JobExecution records for this application
        logger.debug("ADMIN CLEANUP: Processing job execution records")

        # Special function for JobExecution with JSON field
        def cleanup_job_execution():
            try:
                with db.session.begin():
                    from sqlalchemy import text

                    # Count records before deletion
                    count_query = db.session.query(JobExecution).filter(
                        text("job_metadata->>'application_id' = :app_id")
                    ).params(app_id=application_id)

                    count_before = count_query.count()
                    logger.info(f"ADMIN CLEANUP: Found {count_before} job execution records for application_id={application_id}")

                    # Delete records
                    delete_query = db.session.query(JobExecution).filter(
                        text("job_metadata->>'application_id' = :app_id")
                    ).params(app_id=application_id)

                    deleted = delete_query.delete()
                    logger.info(f"ADMIN CLEANUP: Deleted {deleted} job execution records (application_id={application_id})")

                    return deleted, None
            except Exception as e:
                error_msg = f"Failed to delete job execution records: {str(e)}"
                logger.error(f"ADMIN CLEANUP ERROR in job_execution: {str(e)}")
                logger.error(f"ADMIN CLEANUP ERROR DETAILS: type={type(e).__name__}, args={e.args}")
                return 0, error_msg

        deleted, error = cleanup_job_execution()
        total_deleted += deleted

        if error:
            errors.append(error)
        elif deleted > 0:
            operations.append(f"Deleted {deleted} job execution records")
        else:
            operations.append("No job execution records found")

        logger.info(f"ADMIN CLEANUP COMPLETED: Total deleted={total_deleted}, operations={len(operations)}, errors={len(errors)} for application_id={application_id}, table_type={table_type}")

        # Delete the customer_config record completely
        logger.info(f"ADMIN CLEANUP: Deleting customer_config for application_id={application_id}")
        try:
            with db.session.begin():
                customer = db.session.get(CustomerConfig, application_id)
                if customer:
                    db.session.delete(customer)
                    operations.append("Deleted customer_config record")
                else:
                    operations.append("No customer_config found to delete")
        except Exception as e:
            errors.append(f"Failed to delete customer_config: {str(e)}")

        if errors:
            logger.error(f"ADMIN CLEANUP FAILED: {len(errors)} errors occurred")
            return jsonify({
                'success': False,
                'message': f'Cleanup failed with {len(errors)} errors. Check logs for details.',
                'operations': operations,
                'total_deleted': total_deleted,
                'errors': errors
            }), 500

        return jsonify({
            'success': True,
            'message': f'Successfully deleted {total_deleted} records for application_id={application_id}',
            'operations': operations,
            'total_deleted': total_deleted
        })

    except Exception as e:
        logger.error(f"ADMIN CLEANUP FAILED: Unexpected error - {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/', methods=['GET'])
def dashboard_page():
    """Main dashboard HTML page"""
    return render_template('dashboard.html')



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
