"""
Enhanced Dashboard with Manual Trigger Controls
Provides live status monitoring and manual job execution
"""

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required
from datetime import datetime, timedelta
from urllib.parse import urljoin
import traceback
import requests
import os
from app.models import (
    db,
    CustomerConfig,
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
from app.services.customer_config import get_event_runtime_config, load_customers, normalize_token
from app.utils.logger import setup_logger
from app.config import Config

logger = setup_logger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

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

# ------------------------------------------------------------------
# API ENDPOINTS
# ------------------------------------------------------------------

@dashboard_bp.route('/customer-config', methods=['GET'])
@login_required
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
@login_required
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


@dashboard_bp.route('/eligible-applications', methods=['GET'])
@login_required
def list_eligible_applications():
    """Fetch eligible applications from GpsGate using the admin token (env TOKEN_ADMIN)."""
    try:
        admin_token = (os.getenv('TOKEN_ADMIN') or '').strip()
        if not admin_token:
            return jsonify({
                'success': False,
                'error': 'TOKEN_ADMIN env var is not set'
            }), 500

        auth_token = normalize_token(admin_token)
        base = Config.BASE_URL if Config.BASE_URL.endswith('/') else Config.BASE_URL + '/'
        url = urljoin(base, 'comGpsGate/api/v.1/eligibleapplications')

        resp = requests.get(url, headers={'Authorization': auth_token}, timeout=30)
        if not resp.ok:
            return jsonify({
                'success': False,
                'error': f"GpsGate eligibleapplications failed: {resp.status_code} {resp.text[:200]}"
            }), 502

        items = resp.json() or []
        applications = sorted(
            [
                {'id': str(app.get('id')), 'name': (app.get('name') or '').strip()}
                for app in items
                if app.get('id') is not None
            ],
            key=lambda r: r['name'].lower(),
        )
        return jsonify({'success': True, 'applications': applications})
    except Exception as e:
        logger.error(f"Failed to list eligible applications: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/customer-config/options', methods=['POST'])
@login_required
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
@login_required
def trigger_dimension_sync():
    try:
        from app.tasks.sync_tasks import dimension_sync_task
        data = request.get_json() or {}
        application_id = str(data.get('application_id', '')).strip() or None
        task = dimension_sync_task.delay(application_id)
        return jsonify({'success': True, 'task_id': task.id}), 202
    except Exception as e:
        logger.error(f"trigger_dimension_sync failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/trigger/fact-sync', methods=['POST'])
@login_required
def trigger_fact_sync():
    try:
        from app.tasks.backfill_tasks import fact_sync_task
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        application_id = str(data.get('application_id', '')).strip() or None
        if not start_date or not end_date:
            return jsonify({'success': False, 'error': 'start_date and end_date are required'}), 400
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        task = fact_sync_task.delay(start_date, end_date, application_id)
        return jsonify({'success': True, 'task_id': task.id}), 202
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        logger.error(f"trigger_fact_sync failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/trigger/full-backfill', methods=['POST'])
@login_required
def trigger_full_backfill():
    try:
        from app.tasks.backfill_tasks import full_backfill_task
        data = request.get_json() or {}
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        application_id = str(data.get('application_id', '')).strip() or None
        if not start_date or not end_date:
            return jsonify({'success': False, 'error': 'start_date and end_date are required'}), 400
        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date, '%Y-%m-%d')
        task = full_backfill_task.delay(start_date, end_date, application_id)
        return jsonify({'success': True, 'task_id': task.id}), 202
    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        logger.error(f"trigger_full_backfill failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/task-status/<task_id>', methods=['GET'])
@login_required
def get_task_status(task_id):
    """Real-time Celery task status with progress percentage."""
    from celery.result import AsyncResult
    result = AsyncResult(task_id)

    info = result.info
    if isinstance(info, Exception):
        info = {'error': str(info), 'type': type(info).__name__}
    elif not isinstance(info, dict):
        info = {}

    percent = 100 if result.state in ('SUCCESS', 'FAILURE') else info.get('percent', 0)

    return jsonify({
        'task_id':  task_id,
        'state':    result.state,
        'percent':  percent,
        'status':   info.get('status', result.state),
        'info':     info,
        'result':   result.result if result.state == 'SUCCESS' else None,
    })


@dashboard_bp.route('/status/recent', methods=['GET'])
@login_required
def get_recent_jobs():
    """Live job status from Celery workers."""
    try:
        from app.celery_app import celery
        inspect = celery.control.inspect(timeout=3)
        active   = inspect.active()   or {}
        reserved = inspect.reserved() or {}

        jobs = []
        for worker, tasks in active.items():
            for t in tasks:
                jobs.append({
                    'id':         t['id'],
                    'job_type':   t['name'].replace('tasks.', ''),
                    'status':     'running',
                    'started_at': datetime.utcfromtimestamp(t['time_start']).isoformat() if t.get('time_start') else None,
                    'worker':     worker,
                })
        for worker, tasks in reserved.items():
            for t in tasks:
                jobs.append({
                    'id':         t['id'],
                    'job_type':   t['name'].replace('tasks.', ''),
                    'status':     'queued',
                    'started_at': None,
                    'worker':     worker,
                })

        return jsonify({'success': True, 'jobs': jobs})
    except Exception as e:
        logger.error(f"get_recent_jobs failed: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/stats/table-counts', methods=['GET'])
@login_required
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
@login_required
def get_last_sync_stats():
    """Get currently running sync tasks from Celery workers."""
    try:
        from app.celery_app import celery
        inspect = celery.control.inspect(timeout=3)
        active  = inspect.active() or {}

        from datetime import timezone
        running: dict[str, dict | None] = {'daily_sync': None, 'weekly_backfill': None}
        for _, tasks in active.items():
            for t in tasks:
                name = t['name'].replace('tasks.', '')
                if name in running:
                    started = datetime.fromtimestamp(t['time_start'], tz=timezone.utc).isoformat() if t.get('time_start') else None
                    running[name] = {'id': t['id'], 'job_type': name, 'status': 'running', 'started_at': started}

        return jsonify({'success': True, 'daily_sync': running['daily_sync'], 'weekly_backfill': running['weekly_backfill']})

    except Exception as e:
        logger.error(f"Failed to get last sync stats: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/stats/scheduler-status', methods=['GET'])
@login_required
def get_scheduler_status():
    """Get active and scheduled Celery tasks for daily_sync and weekly_backfill."""
    try:
        from app.celery_app import celery
        from datetime import timezone
        inspect   = celery.control.inspect(timeout=3)
        active    = inspect.active()    or {}
        scheduled = inspect.scheduled() or {}

        SCHEDULED_TASKS = {'tasks.daily_sync', 'tasks.weekly_backfill'}

        running_jobs = []
        for _, tasks in active.items():
            for t in tasks:
                if t['name'] in SCHEDULED_TASKS:
                    started = datetime.fromtimestamp(t['time_start'], tz=timezone.utc).isoformat() if t.get('time_start') else None
                    running_jobs.append({'id': t['id'], 'job_type': t['name'].replace('tasks.', ''), 'status': 'running', 'started_at': started})

        scheduled_jobs = []
        for _, tasks in scheduled.items():
            for t in tasks:
                req = t.get('request', {})
                if req.get('name') in SCHEDULED_TASKS:
                    scheduled_jobs.append({'id': req.get('id'), 'job_type': req.get('name', '').replace('tasks.', ''), 'status': 'scheduled', 'eta': t.get('eta')})

        return jsonify({'success': True, 'running_jobs': running_jobs, 'scheduled_jobs': scheduled_jobs, 'daily_syncs': [], 'weekly_backfills': []})

    except Exception as e:
        logger.error(f"Failed to get scheduler status: {str(e)}")
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/ip-info', methods=['GET'])
@login_required
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
@login_required
def cleanup_data():
    """Clean up (delete) data for a specific customer - DANGEROUS OPERATION"""
    try:
        data = request.get_json() or {}
        table_type = data.get('table_type')
        application_id = str(data.get('application_id', '')).strip()

        logger.info(f"ADMIN CLEANUP REQUEST: table_type={table_type}, application_id={application_id}")

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
@login_required
def dashboard_page():
    """Main dashboard HTML page"""
    return render_template('dashboard.html')



@dashboard_bp.route('/health/gpsgate-server', methods=['GET'])
@login_required
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
