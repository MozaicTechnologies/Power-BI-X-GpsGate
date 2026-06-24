"""
Enhanced Dashboard with Manual Trigger Controls
Provides live status monitoring and manual job execution
"""

from flask import Blueprint, render_template, jsonify, request
from flask_login import login_required
from datetime import datetime, timedelta
from sqlalchemy import func
from urllib.parse import urljoin
import traceback
import requests
import os
from app.models import (
    db,
    GpsGateApplication,
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
from app.services.customer_config import get_event_runtime_config, load_applications, normalize_token
from app.utils.logger import setup_logger
from app.config import Config

logger = setup_logger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

def mask_token(token: str | None) -> str:
    token = (token or "").strip()
    if len(token) <= 10:
        return token
    return f"{token[:6]}...{token[-4:]}"


def serialize_gpsgate_application(app: GpsGateApplication) -> dict:
    return {
        "id": app.id,
        "application_id": app.application_id,
        "token": mask_token(app.token),
        "full_token": (app.token or "").strip(),
        "tag_name": app.tag_name,
        "trip_report_name": app.trip_report_name,
        "event_report_name": app.event_report_name,
        "speed_event_rule_name": app.speed_event_rule_name,
        "idle_event_rule_name": app.idle_event_rule_name,
        "awh_event_rule_name": app.awh_event_rule_name,
        "ha_event_rule_name": app.ha_event_rule_name,
        "hb_event_rule_name": app.hb_event_rule_name,
        "hc_event_rule_name": app.hc_event_rule_name,
        "wu_event_rule_name": app.wu_event_rule_name,
        "wh_event_rule_name": app.wh_event_rule_name,
        "tag_id": app.tag_id,
        "trip_report_id": app.trip_report_id,
        "event_report_id": app.event_report_id,
        "speed_event_id": app.speed_event_id,
        "idle_event_id": app.idle_event_id,
        "awh_event_id": app.awh_event_id,
        "ha_event_id": app.ha_event_id,
        "hb_event_id": app.hb_event_id,
        "hc_event_id": app.hc_event_id,
        "wu_event_id": app.wu_event_id,
        "wh_event_id": app.wh_event_id,
    }


def get_dashboard_application(application_id: int | None = None) -> GpsGateApplication:
    if application_id:
        app = GpsGateApplication.query.filter_by(application_id=application_id).first()
        if not app:
            raise RuntimeError(f"No gpsgate_application row found for application_id={application_id}")
        return app

    applications = load_applications()
    if not applications:
        raise RuntimeError("No gpsgate_application rows found for dashboard/manual trigger")

    app = applications[0]
    logger.warning(
        f"Dashboard/manual trigger defaulted to application_id={app.application_id} because no application was specified"
    )
    return app

# ------------------------------------------------------------------
# API ENDPOINTS
# ------------------------------------------------------------------

@dashboard_bp.route('/customer-config', methods=['GET'])
@login_required
def list_customer_config():
    """List current gpsgate_application rows with masked tokens."""
    try:
        applications = (
            db.session.query(GpsGateApplication)
            .order_by(GpsGateApplication.application_id.asc())
            .all()
        )
        return jsonify({
            'success': True,
            'applications': [serialize_gpsgate_application(app) for app in applications]
        })
    except Exception as e:
        logger.error(f"Failed to list gpsgate_application: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@dashboard_bp.route('/customer-config', methods=['POST'])
@login_required
def save_customer_config():
    """Create or update gpsgate_application with application_id and token."""
    try:
        data = request.get_json() or {}
        application_id = int(data.get('application_id', '').strip() or 0)
        token = str(data.get('token', '')).strip()
        name_fields = {
            'tag_name': str(data.get('tag_name', '')).strip() or None,
            'trip_report_name': str(data.get('trip_report_name', '')).strip() or None,
            'event_report_name': str(data.get('event_report_name', '')).strip() or None,
            'speed_event_rule_name': str(data.get('speed_event_rule_name', '')).strip() or None,
            'idle_event_rule_name': str(data.get('idle_event_rule_name', '')).strip() or None,
            'awh_event_rule_name': str(data.get('awh_event_rule_name', '')).strip() or None,
            'ha_event_rule_name': str(data.get('ha_event_rule_name', '')).strip() or None,
            'hb_event_rule_name': str(data.get('hb_event_rule_name', '')).strip() or None,
            'hc_event_rule_name': str(data.get('hc_event_rule_name', '')).strip() or None,
            'wu_event_rule_name': str(data.get('wu_event_rule_name', '')).strip() or None,
            'wh_event_rule_name': str(data.get('wh_event_rule_name', '')).strip() or None,
        }
        id_fields = {
            'tag_id': str(data.get('tag_id', '')).strip() or None,
            'trip_report_id': str(data.get('trip_report_id', '')).strip() or None,
            'event_report_id': str(data.get('event_report_id', '')).strip() or None,
            'speed_event_id': str(data.get('speed_event_id', '')).strip() or None,
            'idle_event_id': str(data.get('idle_event_id', '')).strip() or None,
            'awh_event_id': str(data.get('awh_event_id', '')).strip() or None,
            'ha_event_id': str(data.get('ha_event_id', '')).strip() or None,
            'hb_event_id': str(data.get('hb_event_id', '')).strip() or None,
            'hc_event_id': str(data.get('hc_event_id', '')).strip() or None,
            'wu_event_id': str(data.get('wu_event_id', '')).strip() or None,
            'wh_event_id': str(data.get('wh_event_id', '')).strip() or None,
        }

        if not application_id or not token:
            return jsonify({
                'success': False,
                'error': 'application_id and token are required'
            }), 400

        app = GpsGateApplication.query.filter_by(application_id=application_id).first()
        created = app is None
        if app is None:
            app = GpsGateApplication(application_id=application_id)
            db.session.add(app)

        app.token = token

        for field_name, new_value in name_fields.items():
            setattr(app, field_name, new_value)

        for id_field, new_id in id_fields.items():
            setattr(app, id_field, new_id)

        db.session.commit()

        has_unfilled_ids = any(
            name_fields[field_name]
            and not getattr(app, NAME_TO_ID_FIELD_MAP.get(field_name, ''), None)
            for field_name in name_fields
        )
        message = f"GpsGate application {'created' if created else 'updated'} for application_id={application_id}."
        if has_unfilled_ids:
            message += " Run Dimension Sync to populate missing report, tag, and event IDs."

        return jsonify({
            'success': True,
            'message': message,
            'application': serialize_gpsgate_application(app)
        }), 201 if created else 200
    except Exception as e:
        db.session.rollback()
        logger.error(f"Failed to save gpsgate_application: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


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
        def safe_count(model) -> int:
            try:
                return db.session.query(func.count()).select_from(model).scalar() or 0
            except Exception:
                db.session.rollback()
                return 0

        fact_counts = {
            'Trip':     safe_count(FactTrip),
            'Speeding': safe_count(FactSpeeding),
            'Idle':     safe_count(FactIdle),
            'AWH':      safe_count(FactAWH),
            'WH':       safe_count(FactWH),
            'HA':       safe_count(FactHA),
            'HB':       safe_count(FactHB),
            'WU':       safe_count(FactWU),
        }

        dim_counts = {
            'Drivers':      safe_count(DimDrivers),
            'Vehicles':     safe_count(DimVehicles),
            'Tags':         safe_count(DimTags),
            'Reports':      safe_count(DimReports),
            'EventRules':   safe_count(DimEventRules),
            'CustomFields': safe_count(DimVehicleCustomFields),
        }
        
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

        gpsgate_app = GpsGateApplication.query.filter_by(application_id=application_id_int).first()
        if not gpsgate_app:
            return jsonify({
                'success': False,
                'error': f'No gpsgate_application found for application_id={application_id}'
            }), 404
        gpsgate_pk_id = gpsgate_app.id

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
                        count_before = model_class.query.filter_by(gpsgate_application_id=gpsgate_pk_id).count()
                        logger.info(f"ADMIN CLEANUP: Found {count_before} records in {display_name} for application_id={application_id}")

                        # Delete records
                        deleted = model_class.query.filter_by(gpsgate_application_id=gpsgate_pk_id).delete()
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
                        count_before = db.session.query(func.count()).select_from(model_class).filter(
                            model_class.application_id == application_id_int
                        ).scalar() or 0
                        logger.info(f"ADMIN CLEANUP: Found {count_before} records in {display_name} for application_id={application_id}")

                        deleted = db.session.query(model_class).filter(
                            model_class.application_id == application_id_int
                        ).delete(synchronize_session=False)
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

        # Delete the gpsgate_application record completely
        logger.info(f"ADMIN CLEANUP: Deleting gpsgate_application for application_id={application_id}")
        try:
            with db.session.begin():
                app_to_delete = GpsGateApplication.query.filter_by(application_id=application_id_int).first()
                if app_to_delete:
                    db.session.delete(app_to_delete)
                    operations.append("Deleted gpsgate_application record")
                else:
                    operations.append("No gpsgate_application found to delete")
        except Exception as e:
            errors.append(f"Failed to delete gpsgate_application: {str(e)}")

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


_BROWSE_TABLE_MAP = {
    'gpsgate_application':       GpsGateApplication,
    'fact_trip':                 FactTrip,
    'fact_speeding':             FactSpeeding,
    'fact_idle':                 FactIdle,
    'fact_awh':                  FactAWH,
    'fact_wh':                   FactWH,
    'fact_ha':                   FactHA,
    'fact_hb':                   FactHB,
    'fact_wu':                   FactWU,
    'dim_tags':                  DimTags,
    'dim_event_rules':           DimEventRules,
    'dim_reports':               DimReports,
    'dim_vehicles':              DimVehicles,
    'dim_drivers':               DimDrivers,
    'dim_vehicle_custom_fields': DimVehicleCustomFields,
}


@dashboard_bp.route('/browse', methods=['GET'])
@login_required
def list_browse_tables():
    """Return all browseable tables with their row counts."""
    tables = []
    for name, model in _BROWSE_TABLE_MAP.items():
        try:
            count = db.session.query(func.count()).select_from(model).scalar() or 0
        except Exception:
            db.session.rollback()
            count = -1
        tables.append({'name': name, 'count': count})
    return jsonify({'success': True, 'tables': tables})


@dashboard_bp.route('/browse/<table_name>', methods=['GET'])
@login_required
def browse_table(table_name):
    """Return paginated rows from any registered table."""
    model = _BROWSE_TABLE_MAP.get(table_name)
    if not model:
        return jsonify({'success': False, 'error': f'Unknown table: {table_name}'}), 404

    page     = max(1, request.args.get('page', 1, type=int))
    per_page = min(100, max(10, request.args.get('per_page', 50, type=int)))

    try:
        total  = db.session.query(func.count()).select_from(model).scalar() or 0
        db_rows = db.session.query(model).offset((page - 1) * per_page).limit(per_page).all()
        columns = [c.name for c in model.__table__.columns]
        rows = []
        for row in db_rows:
            r = {}
            for col in columns:
                val = getattr(row, col, None)
                r[col] = str(val) if val is not None else None
            rows.append(r)
        return jsonify({
            'success':  True,
            'table':    table_name,
            'total':    total,
            'page':     page,
            'per_page': per_page,
            'pages':    max(1, (total + per_page - 1) // per_page),
            'columns':  columns,
            'rows':     rows,
        })
    except Exception:
        db.session.rollback()
        logger.exception("browse_table | table=%s", table_name)
        return jsonify({'success': False, 'error': 'Query failed'}), 500


@dashboard_bp.route('/health/gpsgate-server', methods=['GET'])
@login_required
def check_gpsgate_server_health():
    """Quick health check for GpsGate server state"""
    try:
        applications = load_applications()
        if not applications:
            return jsonify({'status': 'degraded', 'message': 'No gpsgate_application rows found'}), 503

        runtime = get_event_runtime_config(applications[0], "WU", Config.BASE_URL)
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
