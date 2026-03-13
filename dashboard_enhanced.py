"""
dashboard_enhanced.py  (MULTI-TENANT VERSION)
==============================================
Enhanced Dashboard with Manual Trigger Controls.
Provides live status monitoring and manual job execution
for ALL active customers.

Changes from original:
- Removed hardcoded app_id="6", token=Config.TOKEN, tag_id="39"
- process_event_with_dates now accepts a CustomerConfig object
- All job executors loop over active customers
- Added /customer-configs CRUD API endpoints
- Added Customer Management card to the dashboard HTML
"""

from flask import Blueprint, render_template_string, jsonify, request
from datetime import datetime, timedelta
import traceback
import threading
import json
from models import db, JobExecution, CustomerConfig
from models import FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
from data_pipeline import process_event_data
from logger_config import get_logger

logger = get_logger(__name__)

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/dashboard')

# Store active job threads
active_jobs = {}

# Event type to API configuration mapping
EVENT_CONFIG = {
    'Trip':     {'report_id': '25', 'event_id': None,   'response_key': 'trip_events'},
    'Speeding': {'report_id': '25', 'event_id': '18',   'response_key': 'speed_events'},
    'Idle':     {'report_id': '25', 'event_id': '1328', 'response_key': 'idle_events'},
    'AWH':      {'report_id': '25', 'event_id': '12',   'response_key': 'awh_events'},
    'WH':       {'report_id': '25', 'event_id': '13',   'response_key': 'wh_events'},
    'HA':       {'report_id': '25', 'event_id': '1327', 'response_key': 'ha_events'},
    'HB':       {'report_id': '25', 'event_id': '1326', 'response_key': 'hb_events'},
    'WU':       {'report_id': '25', 'event_id': '17',   'response_key': 'wu_events'},
}


# ------------------------------------------------------------------
# CORE HELPER — replaces the old hardcoded version
# ------------------------------------------------------------------

def process_event_with_dates(app, event_type: str, customer: CustomerConfig,
                              start_date: str, end_date: str) -> dict:
    """
    Process one event type for one customer over a date range.
    Credentials come entirely from the CustomerConfig object —
    no hardcoded app_id / token / tag_id anywhere.
    """
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")

    config = EVENT_CONFIG[event_type]

    payload = {
        "app_id":       customer.app_id,      # ← from DB
        "token":        customer.token,        # ← from DB
        "base_url":     customer.base_url,     # ← from DB
        "report_id":    config['report_id'],
        "tag_id":       customer.tag_id,       # ← from DB
        "period_start": f"{start_date}T00:00:00Z",
        "period_end":   f"{end_date}T23:59:59Z",
    }

    if config['event_id']:
        payload["event_id"] = config['event_id']

    with app.test_request_context(
        '/api/process',
        method='POST',
        data=json.dumps(payload),
        content_type='application/json'
    ):
        response_tuple = process_event_data(
            event_name=event_type,
            response_key=config['response_key']
        )

        if isinstance(response_tuple, tuple):
            response_obj, _ = response_tuple
            data = response_obj.get_json()
            return data.get('accounting', {})

        return {}


def get_active_customers(app) -> list:
    """Return all active CustomerConfig rows inside an app context."""
    with app.app_context():
        return CustomerConfig.query.filter_by(is_active=True).order_by(CustomerConfig.id).all()


# ------------------------------------------------------------------
# BACKGROUND JOB EXECUTORS
# ------------------------------------------------------------------

def execute_dimension_sync_job(job_id):
    """Sync dimension tables for all customers."""
    from application import create_app
    app = create_app()

    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            from sync_dimensions_from_api import main as sync_dimension_main

            logger.info(f"🚀 Starting dimension sync job {job_id}")
            total_records = sync_dimension_main()   # already loops all customers

            job.status            = 'completed'
            job.completed_at      = datetime.utcnow()
            job.records_processed = total_records
            job.job_metadata      = {
                'message':       f'Dimension sync completed — {total_records:,} records',
                'total_records': total_records,
            }
            db.session.commit()
            logger.info(f"✅ Dimension sync done: {total_records:,} records")

        except Exception as e:
            logger.error(f"❌ Dimension sync failed: {e}")
            job.status        = 'failed'
            job.completed_at  = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata  = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            active_jobs.pop(job_id, None)


def execute_fact_sync_job(job_id, start_date, end_date):
    """Sync fact tables for ALL active customers."""
    from application import create_app
    app = create_app()

    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Fact sync job {job_id}: {start_date} → {end_date}")

            customers = CustomerConfig.query.filter_by(is_active=True).all()
            if not customers:
                raise RuntimeError("No active customers found in customer_configs")

            all_results    = {}
            total_inserted = 0
            total_failed   = 0

            for customer in customers:
                logger.info(f"Processing customer: {customer.name} (app_id={customer.app_id})")
                customer_inserted = 0
                customer_failed   = 0

                for event_type in EVENT_CONFIG:
                    try:
                        result = process_event_with_dates(
                            app=app,
                            event_type=event_type,
                            customer=customer,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        customer_inserted += result.get('inserted', 0)
                        customer_failed   += result.get('failed', 0)
                        logger.info(
                            f"  [{customer.name}] {event_type}: "
                            f"{result.get('inserted', 0)} inserted"
                        )
                    except Exception as e:
                        logger.error(f"  [{customer.name}] {event_type} failed: {e}")
                        customer_failed += 1

                all_results[customer.app_id] = {
                    'name':     customer.name,
                    'inserted': customer_inserted,
                    'failed':   customer_failed,
                }
                total_inserted += customer_inserted
                total_failed   += customer_failed

            job.status            = 'completed'
            job.completed_at      = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors            = total_failed
            job.job_metadata      = {
                'start_date':  start_date,
                'end_date':    end_date,
                'customers':   len(customers),
                'per_customer': all_results,
            }
            db.session.commit()
            logger.info(f"✅ Fact sync done: {total_inserted} inserted, {total_failed} failed")

        except Exception as e:
            logger.error(f"❌ Fact sync failed: {e}")
            job.status        = 'failed'
            job.completed_at  = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata  = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            active_jobs.pop(job_id, None)


def execute_full_backfill_job(job_id, start_date, end_date):
    """Dimensions + facts for ALL active customers."""
    from application import create_app
    app = create_app()

    with app.app_context():
        job = db.session.get(JobExecution, job_id)
        try:
            logger.info(f"🚀 Full backfill job {job_id}: {start_date} → {end_date}")

            # Step 1: dimensions
            from sync_dimensions_from_api import main as sync_dimension_main
            sync_dimension_main()
            logger.info("✅ Dimensions synced")

            # Step 2: facts
            customers = CustomerConfig.query.filter_by(is_active=True).all()
            if not customers:
                raise RuntimeError("No active customers found in customer_configs")

            all_results    = {}
            total_inserted = 0
            total_failed   = 0

            for customer in customers:
                logger.info(f"Processing customer: {customer.name}")
                customer_inserted = 0
                customer_failed   = 0

                for event_type in EVENT_CONFIG:
                    try:
                        result = process_event_with_dates(
                            app=app,
                            event_type=event_type,
                            customer=customer,
                            start_date=start_date,
                            end_date=end_date,
                        )
                        customer_inserted += result.get('inserted', 0)
                        customer_failed   += result.get('failed', 0)
                    except Exception as e:
                        logger.error(f"  [{customer.name}] {event_type} failed: {e}")
                        customer_failed += 1

                all_results[customer.app_id] = {
                    'name':     customer.name,
                    'inserted': customer_inserted,
                    'failed':   customer_failed,
                }
                total_inserted += customer_inserted
                total_failed   += customer_failed

            job.status            = 'completed'
            job.completed_at      = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors            = total_failed
            job.job_metadata      = {
                'dimensions':  'synced',
                'start_date':  start_date,
                'end_date':    end_date,
                'customers':   len(customers),
                'per_customer': all_results,
            }
            db.session.commit()
            logger.info(f"✅ Full backfill done: {total_inserted} inserted")

        except Exception as e:
            logger.error(f"❌ Full backfill failed: {e}")
            job.status        = 'failed'
            job.completed_at  = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata  = {'traceback': traceback.format_exc()}
            db.session.commit()
        finally:
            active_jobs.pop(job_id, None)


# ------------------------------------------------------------------
# CUSTOMER CONFIG CRUD ENDPOINTS
# ------------------------------------------------------------------

@dashboard_bp.route('/customers', methods=['GET'])
def list_customers():
    """List all customers (token masked)."""
    customers = CustomerConfig.query.order_by(CustomerConfig.id).all()
    return jsonify({
        'success': True,
        'customers': [c.to_dict() for c in customers],
        'total': len(customers),
    })


@dashboard_bp.route('/customers', methods=['POST'])
def add_customer():
    """Add a new customer."""
    data = request.get_json() or {}

    required = ['app_id', 'token', 'base_url']
    missing  = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({'success': False, 'error': f"Missing fields: {missing}"}), 400

    if CustomerConfig.query.filter_by(app_id=str(data['app_id'])).first():
        return jsonify({'success': False, 'error': f"app_id {data['app_id']} already exists"}), 409

    customer = CustomerConfig(
        app_id    = str(data['app_id']),
        name      = data.get('name', f"Customer {data['app_id']}"),
        token     = data['token'],
        base_url  = data['base_url'].rstrip('/'),
        tag_id    = data.get('tag_id'),
        report_id = data.get('report_id', '25'),
        is_active = data.get('is_active', True),
    )
    db.session.add(customer)
    db.session.commit()

    logger.info(f"✅ New customer added: {customer.name} (app_id={customer.app_id})")
    return jsonify({'success': True, 'customer': customer.to_dict()}), 201


@dashboard_bp.route('/customers/<app_id>', methods=['PUT'])
def update_customer(app_id):
    """Update an existing customer."""
    customer = CustomerConfig.query.filter_by(app_id=str(app_id)).first()
    if not customer:
        return jsonify({'success': False, 'error': f"Customer {app_id} not found"}), 404

    data = request.get_json() or {}

    if 'name'      in data: customer.name      = data['name']
    if 'token'     in data: customer.token     = data['token']
    if 'base_url'  in data: customer.base_url  = data['base_url'].rstrip('/')
    if 'tag_id'    in data: customer.tag_id    = data['tag_id']
    if 'report_id' in data: customer.report_id = data['report_id']
    if 'is_active' in data: customer.is_active = bool(data['is_active'])

    customer.updated_at = datetime.utcnow()
    db.session.commit()

    logger.info(f"✅ Customer updated: {customer.name} (app_id={customer.app_id})")
    return jsonify({'success': True, 'customer': customer.to_dict()})


@dashboard_bp.route('/customers/<app_id>', methods=['DELETE'])
def delete_customer(app_id):
    """Deactivate a customer (soft delete — sets is_active=False)."""
    customer = CustomerConfig.query.filter_by(app_id=str(app_id)).first()
    if not customer:
        return jsonify({'success': False, 'error': f"Customer {app_id} not found"}), 404

    customer.is_active  = False
    customer.updated_at = datetime.utcnow()
    db.session.commit()

    logger.info(f"⚠️  Customer deactivated: {customer.name} (app_id={customer.app_id})")
    return jsonify({'success': True, 'message': f"Customer {app_id} deactivated"})


# ------------------------------------------------------------------
# EXISTING TRIGGER ENDPOINTS (unchanged signature, updated internals)
# ------------------------------------------------------------------

@dashboard_bp.route('/trigger/dimension-sync', methods=['POST'])
def trigger_dimension_sync():
    try:
        job = JobExecution(
            job_type='manual_dimension_sync', status='running',
            started_at=datetime.utcnow(), triggered_by='manual'
        )
        db.session.add(job)
        db.session.commit()

        thread = threading.Thread(target=execute_dimension_sync_job, args=(job.id,))
        thread.daemon = True
        thread.start()
        active_jobs[job.id] = thread

        return jsonify({'success': True, 'job_id': job.id, 'message': 'Dimension sync started'}), 202

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/trigger/fact-sync', methods=['POST'])
def trigger_fact_sync():
    try:
        data       = request.get_json() or {}
        start_date = data.get('start_date')
        end_date   = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'success': False, 'error': 'start_date and end_date are required'}), 400

        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date,   '%Y-%m-%d')

        job = JobExecution(
            job_type='manual_fact_sync', status='running',
            started_at=datetime.utcnow(), triggered_by='manual',
            job_metadata={'start_date': start_date, 'end_date': end_date}
        )
        db.session.add(job)
        db.session.commit()

        thread = threading.Thread(
            target=execute_fact_sync_job, args=(job.id, start_date, end_date)
        )
        thread.daemon = True
        thread.start()
        active_jobs[job.id] = thread

        return jsonify({
            'success': True, 'job_id': job.id,
            'message': f'Fact sync started for {start_date} to {end_date}'
        }), 202

    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/trigger/full-backfill', methods=['POST'])
def trigger_full_backfill():
    try:
        data       = request.get_json() or {}
        start_date = data.get('start_date')
        end_date   = data.get('end_date')

        if not start_date or not end_date:
            return jsonify({'success': False, 'error': 'start_date and end_date are required'}), 400

        datetime.strptime(start_date, '%Y-%m-%d')
        datetime.strptime(end_date,   '%Y-%m-%d')

        job = JobExecution(
            job_type='manual_full_backfill', status='running',
            started_at=datetime.utcnow(), triggered_by='manual',
            job_metadata={'start_date': start_date, 'end_date': end_date}
        )
        db.session.add(job)
        db.session.commit()

        thread = threading.Thread(
            target=execute_full_backfill_job, args=(job.id, start_date, end_date)
        )
        thread.daemon = True
        thread.start()
        active_jobs[job.id] = thread

        return jsonify({
            'success': True, 'job_id': job.id,
            'message': f'Full backfill started for {start_date} to {end_date}'
        }), 202

    except ValueError:
        return jsonify({'success': False, 'error': 'Invalid date format. Use YYYY-MM-DD'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ------------------------------------------------------------------
# STATUS ENDPOINTS (unchanged)
# ------------------------------------------------------------------

@dashboard_bp.route('/status/<int:job_id>', methods=['GET'])
def get_job_status(job_id):
    try:
        job = db.session.get(JobExecution, job_id)
        if not job:
            return jsonify({'success': False, 'error': 'Job not found'}), 404
        return jsonify({'success': True, 'job': job.to_dict()})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/status/recent', methods=['GET'])
def get_recent_jobs():
    try:
        limit = request.args.get('limit', 20, type=int)
        jobs  = JobExecution.query.order_by(
            JobExecution.started_at.desc()
        ).limit(limit).all()
        return jsonify({'success': True, 'jobs': [j.to_dict() for j in jobs]})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/stats/table-counts', methods=['GET'])
def get_table_counts():
    try:
        fact_tables = [
            ('fact_trip',      'Trip'),
            ('fact_speeding',  'Speeding'),
            ('fact_idle',      'Idle'),
            ('fact_awh',       'AWH'),
            ('fact_wh',        'WH'),
            ('fact_ha',        'HA'),
            ('fact_hb',        'HB'),
            ('fact_wu',        'WU'),
        ]
        dim_tables = [
            ('dim_drivers',               'Drivers'),
            ('dim_vehicles',              'Vehicles'),
            ('dim_tags',                  'Tags'),
            ('dim_reports',               'Reports'),
            ('dim_event_rules',           'EventRules'),
            ('dim_vehicle_custom_fields', 'CustomFields'),
        ]

        fact_counts = {}
        for table_name, key in fact_tables:
            try:
                result = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}"))
                fact_counts[key] = result.scalar()
            except Exception:
                fact_counts[key] = 0

        dim_counts = {}
        for table_name, key in dim_tables:
            try:
                result = db.session.execute(db.text(f"SELECT COUNT(*) FROM {table_name}"))
                dim_counts[key] = result.scalar()
            except Exception:
                dim_counts[key] = 0

        # Per-customer breakdown on fact tables
        customers = CustomerConfig.query.filter_by(is_active=True).all()
        per_customer = {}
        for customer in customers:
            per_customer[customer.app_id] = {'name': customer.name}
            for table_name, key in fact_tables:
                try:
                    result = db.session.execute(
                        db.text(f"SELECT COUNT(*) FROM {table_name} WHERE app_id = :aid"),
                        {"aid": customer.app_id}
                    )
                    per_customer[customer.app_id][key] = result.scalar()
                except Exception:
                    per_customer[customer.app_id][key] = 0

        return jsonify({
            'success':      True,
            'counts':       fact_counts,
            'dim_counts':   dim_counts,
            'per_customer': per_customer,
            'total':        sum(fact_counts.values()),
            'timestamp':    datetime.utcnow().isoformat(),
        })

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/stats/last-sync', methods=['GET'])
def get_last_sync_stats():
    try:
        daily  = JobExecution.query.filter_by(job_type='daily_sync',     status='completed') \
                                   .order_by(JobExecution.completed_at.desc()).first()
        weekly = JobExecution.query.filter_by(job_type='weekly_backfill', status='completed') \
                                   .order_by(JobExecution.completed_at.desc()).first()
        return jsonify({
            'success':        True,
            'daily_sync':     daily.to_dict()  if daily  else None,
            'weekly_backfill': weekly.to_dict() if weekly else None,
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


@dashboard_bp.route('/stats/scheduler-status', methods=['GET'])
def get_scheduler_status():
    try:
        daily_syncs      = JobExecution.query.filter_by(job_type='daily_sync') \
                                             .order_by(JobExecution.started_at.desc()).limit(10).all()
        weekly_backfills = JobExecution.query.filter_by(job_type='weekly_backfill') \
                                             .order_by(JobExecution.started_at.desc()).limit(10).all()
        running_jobs     = JobExecution.query.filter(
            JobExecution.job_type.in_(['daily_sync', 'weekly_backfill']),
            JobExecution.status == 'running'
        ).all()

        return jsonify({
            'success':          True,
            'daily_syncs':      [j.to_dict() for j in daily_syncs],
            'weekly_backfills': [j.to_dict() for j in weekly_backfills],
            'running_jobs':     [j.to_dict() for j in running_jobs],
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 500


# ------------------------------------------------------------------
# DASHBOARD HTML
# ------------------------------------------------------------------

@dashboard_bp.route('/', methods=['GET'])
def dashboard_page():
    html = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>GpsGate Data Pipeline Dashboard</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh; padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        h1 { color:white; text-align:center; margin-bottom:30px;
             font-size:2.5rem; text-shadow:2px 2px 4px rgba(0,0,0,.2); }
        .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(350px,1fr));
                gap:20px; margin-bottom:20px; }
        .card { background:white; border-radius:12px; padding:25px;
                box-shadow:0 4px 6px rgba(0,0,0,.1); }
        .card h2 { color:#667eea; margin-bottom:20px; font-size:1.5rem;
                   border-bottom:2px solid #667eea; padding-bottom:10px; }
        .card h3 { color:#667eea; margin:15px 0 8px; font-size:1.1em; }
        .form-group { margin-bottom:15px; }
        label { display:block; margin-bottom:5px; color:#333; font-weight:600; }
        input[type="date"], input[type="text"], input[type="password"] {
            width:100%; padding:10px; border:1px solid #ddd;
            border-radius:6px; font-size:14px; }
        button {
            width:100%; padding:12px;
            background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);
            color:white; border:none; border-radius:6px;
            font-size:16px; font-weight:600; cursor:pointer; transition:transform .2s; }
        button:hover { transform:translateY(-2px); }
        button.btn-sm { padding:6px 12px; font-size:13px; width:auto; }
        button.btn-danger {
            background:linear-gradient(135deg,#e53e3e,#c53030); }
        button.btn-success {
            background:linear-gradient(135deg,#38a169,#276749); }
        .stats-grid { display:grid; grid-template-columns:repeat(2,1fr); gap:15px; }
        .stat-item { background:#f8f9fa; padding:15px; border-radius:8px; text-align:center; }
        .stat-value { font-size:2rem; font-weight:bold; color:#667eea; }
        .stat-label { color:#666; font-size:.9rem; margin-top:5px; }
        .job-list { max-height:400px; overflow-y:auto; }
        .job-item { background:#f8f9fa; padding:15px; border-radius:8px;
                    margin-bottom:10px; border-left:4px solid #667eea; }
        .job-item.running  { border-left-color:#ffc107; }
        .job-item.completed { border-left-color:#28a745; }
        .job-item.failed    { border-left-color:#dc3545; }
        .job-header { display:flex; justify-content:space-between; margin-bottom:5px; }
        .job-type { font-weight:600; color:#333; }
        .job-status { padding:2px 8px; border-radius:4px; font-size:.85rem; font-weight:600; }
        .job-status.running   { background:#ffc107; color:#000; }
        .job-status.completed { background:#28a745; color:white; }
        .job-status.failed    { background:#dc3545; color:white; }
        .job-details { font-size:.9rem; color:#666; }
        .message { padding:12px; border-radius:6px; margin-top:15px; display:none; }
        .message.success { background:#d4edda; color:#155724; border:1px solid #c3e6cb; }
        .message.error   { background:#f8d7da; color:#721c24; border:1px solid #f5c6cb; }
        /* Customer table */
        .customer-table { width:100%; border-collapse:collapse; font-size:.9rem; }
        .customer-table th { background:#667eea; color:white; padding:8px 12px; text-align:left; }
        .customer-table td { padding:8px 12px; border-bottom:1px solid #eee; }
        .customer-table tr:hover td { background:#f8f9fa; }
        .badge { display:inline-block; padding:2px 8px; border-radius:10px;
                 font-size:.8rem; font-weight:600; }
        .badge-active   { background:#d4edda; color:#155724; }
        .badge-inactive { background:#f8d7da; color:#721c24; }
        /* Per-customer breakdown */
        .breakdown-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr));
                          gap:10px; margin-top:10px; }
        .breakdown-card { background:#f0f4ff; border-radius:8px; padding:12px; }
        .breakdown-name { font-weight:600; color:#667eea; margin-bottom:6px; }
        .breakdown-row  { display:flex; justify-content:space-between;
                          font-size:.85rem; color:#555; }
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
                <button onclick="triggerDimensionSync()">🔄 Run Dimension Sync (all customers)</button>
            </div>
            <div class="form-group">
                <label for="fact-start">Start Date</label>
                <input type="date" id="fact-start">
            </div>
            <div class="form-group">
                <label for="fact-end">End Date</label>
                <input type="date" id="fact-end">
            </div>
            <div class="form-group">
                <button onclick="triggerFactSync()">📈 Run Fact Sync (all customers)</button>
            </div>
            <div class="form-group">
                <button onclick="triggerFullBackfill()">🔥 Run Full Backfill (all customers)</button>
            </div>
            <div id="trigger-message" class="message"></div>
        </div>

        <!-- Live Statistics -->
        <div class="card">
            <h2>📈 Live Statistics</h2>
            <h3>Fact Tables (grand total)</h3>
            <div class="stats-grid">
                <div class="stat-item"><div class="stat-value" id="total-count">-</div><div class="stat-label">Total Records</div></div>
                <div class="stat-item"><div class="stat-value" id="trip-count">-</div><div class="stat-label">Trip</div></div>
                <div class="stat-item"><div class="stat-value" id="speeding-count">-</div><div class="stat-label">Speeding</div></div>
                <div class="stat-item"><div class="stat-value" id="idle-count">-</div><div class="stat-label">Idle</div></div>
                <div class="stat-item"><div class="stat-value" id="awh-count">-</div><div class="stat-label">AWH</div></div>
                <div class="stat-item"><div class="stat-value" id="wh-count">-</div><div class="stat-label">WH</div></div>
                <div class="stat-item"><div class="stat-value" id="ha-count">-</div><div class="stat-label">HA</div></div>
                <div class="stat-item"><div class="stat-value" id="hb-count">-</div><div class="stat-label">HB</div></div>
                <div class="stat-item"><div class="stat-value" id="wu-count">-</div><div class="stat-label">WU</div></div>
            </div>
            <h3>Per-Customer Breakdown</h3>
            <div class="breakdown-grid" id="per-customer-breakdown">Loading...</div>
            <button onclick="refreshStats()" style="margin-top:15px;">🔄 Refresh Stats</button>
        </div>

        <!-- Customer Management -->
        <div class="card" style="grid-column: 1 / -1;">
            <h2>🏢 Customer Management</h2>
            <table class="customer-table" id="customer-table">
                <thead>
                    <tr>
                        <th>App ID</th><th>Name</th><th>Base URL</th>
                        <th>Tag ID</th><th>Report ID</th><th>Status</th><th>Actions</th>
                    </tr>
                </thead>
                <tbody id="customer-tbody">
                    <tr><td colspan="7" style="text-align:center;padding:20px;">Loading...</td></tr>
                </tbody>
            </table>
            <div style="margin-top:20px; border-top:1px solid #eee; padding-top:20px;">
                <h3 style="margin-bottom:12px;">➕ Add New Customer</h3>
                <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:10px;">
                    <div><label>App ID *</label><input type="text" id="new-app-id" placeholder="e.g. 12"></div>
                    <div><label>Name</label><input type="text" id="new-name" placeholder="e.g. Client B"></div>
                    <div><label>Base URL *</label><input type="text" id="new-base-url" placeholder="https://tracking.example.com"></div>
                    <div><label>Token *</label><input type="password" id="new-token" placeholder="v2:..."></div>
                    <div><label>Tag ID</label><input type="text" id="new-tag-id" placeholder="e.g. 39"></div>
                    <div><label>Report ID</label><input type="text" id="new-report-id" placeholder="25"></div>
                </div>
                <button onclick="addCustomer()" style="margin-top:12px;">➕ Add Customer</button>
                <div id="customer-message" class="message"></div>
            </div>
        </div>

        <!-- Last Sync -->
        <div class="card">
            <h2>⏰ Last Sync</h2>
            <div id="last-sync-info">Loading...</div>
        </div>

        <!-- Scheduler Status -->
        <div class="card" style="grid-column: 1 / -1;">
            <h2>🕐 Automated Scheduler Status</h2>
            <div style="display:grid; grid-template-columns:1fr 1fr; gap:20px;">
                <div>
                    <h3>Daily Sync (2 AM UTC)</h3>
                    <div id="daily-scheduler-status">Loading...</div>
                </div>
                <div>
                    <h3>Weekly Backfill (Sunday 3 AM UTC)</h3>
                    <div id="weekly-scheduler-status">Loading...</div>
                </div>
            </div>
            <button onclick="refreshSchedulerStatus()" style="margin-top:15px;">🔄 Refresh</button>
        </div>

        <!-- Recent Jobs -->
        <div class="card" style="grid-column: 1 / -1;">
            <h2>📋 Recent Jobs</h2>
            <div class="job-list" id="job-list">Loading...</div>
            <button onclick="refreshJobs()" style="margin-top:15px;">🔄 Refresh Jobs</button>
        </div>

    </div>
</div>

<script>
    // Set default dates
    const today = new Date();
    const yesterday = new Date(today); yesterday.setDate(yesterday.getDate()-1);
    document.getElementById('fact-start').value = yesterday.toISOString().split('T')[0];
    document.getElementById('fact-end').value   = yesterday.toISOString().split('T')[0];

    function showMessage(id, type, text) {
        const el = document.getElementById(id);
        el.className = 'message ' + type;
        el.textContent = text;
        el.style.display = 'block';
        setTimeout(() => el.style.display = 'none', 6000);
    }

    // ---------- TRIGGER FUNCTIONS ----------
    async function triggerDimensionSync() {
        const r = await fetch('/dashboard/trigger/dimension-sync', {method:'POST', headers:{'Content-Type':'application/json'}});
        const d = await r.json();
        showMessage('trigger-message', d.success ? 'success' : 'error', d.message || d.error);
        if (d.success) setTimeout(refreshJobs, 1000);
    }
    async function triggerFactSync() {
        const s = document.getElementById('fact-start').value;
        const e = document.getElementById('fact-end').value;
        if (!s || !e) { showMessage('trigger-message','error','Select dates first'); return; }
        const r = await fetch('/dashboard/trigger/fact-sync', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({start_date:s,end_date:e})});
        const d = await r.json();
        showMessage('trigger-message', d.success ? 'success' : 'error', d.message || d.error);
        if (d.success) setTimeout(refreshJobs, 1000);
    }
    async function triggerFullBackfill() {
        const s = document.getElementById('fact-start').value;
        const e = document.getElementById('fact-end').value;
        if (!s || !e) { showMessage('trigger-message','error','Select dates first'); return; }
        const r = await fetch('/dashboard/trigger/full-backfill', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({start_date:s,end_date:e})});
        const d = await r.json();
        showMessage('trigger-message', d.success ? 'success' : 'error', d.message || d.error);
        if (d.success) setTimeout(refreshJobs, 1000);
    }

    // ---------- CUSTOMER MANAGEMENT ----------
    async function loadCustomers() {
        const r = await fetch('/dashboard/customers');
        const d = await r.json();
        const tbody = document.getElementById('customer-tbody');
        if (!d.success || !d.customers.length) {
            tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:20px;">No customers found</td></tr>';
            return;
        }
        tbody.innerHTML = d.customers.map(c => `
            <tr>
                <td><strong>${c.app_id}</strong></td>
                <td>${c.name || '—'}</td>
                <td style="font-size:.8rem;">${c.base_url}</td>
                <td>${c.tag_id || '—'}</td>
                <td>${c.report_id || '25'}</td>
                <td><span class="badge ${c.is_active ? 'badge-active' : 'badge-inactive'}">${c.is_active ? 'Active' : 'Inactive'}</span></td>
                <td>
                    <button class="btn-sm btn-danger" onclick="deactivateCustomer('${c.app_id}')">Deactivate</button>
                </td>
            </tr>
        `).join('');
    }

    async function addCustomer() {
        const payload = {
            app_id:    document.getElementById('new-app-id').value.trim(),
            name:      document.getElementById('new-name').value.trim(),
            base_url:  document.getElementById('new-base-url').value.trim(),
            token:     document.getElementById('new-token').value.trim(),
            tag_id:    document.getElementById('new-tag-id').value.trim(),
            report_id: document.getElementById('new-report-id').value.trim() || '25',
        };
        if (!payload.app_id || !payload.token || !payload.base_url) {
            showMessage('customer-message', 'error', 'App ID, Token, and Base URL are required');
            return;
        }
        const r = await fetch('/dashboard/customers', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload)});
        const d = await r.json();
        showMessage('customer-message', d.success ? 'success' : 'error', d.success ? `Customer ${payload.app_id} added!` : d.error);
        if (d.success) {
            loadCustomers();
            ['new-app-id','new-name','new-base-url','new-token','new-tag-id','new-report-id'].forEach(id => document.getElementById(id).value = '');
        }
    }

    async function deactivateCustomer(appId) {
        if (!confirm(`Deactivate customer ${appId}? They will be excluded from all future syncs.`)) return;
        const r = await fetch(`/dashboard/customers/${appId}`, {method:'DELETE'});
        const d = await r.json();
        showMessage('customer-message', d.success ? 'success' : 'error', d.message || d.error);
        if (d.success) loadCustomers();
    }

    // ---------- STATS ----------
    async function refreshStats() {
        const r = await fetch('/dashboard/stats/table-counts');
        const d = await r.json();
        if (!d.success) return;

        document.getElementById('total-count').textContent    = d.total.toLocaleString();
        document.getElementById('trip-count').textContent     = d.counts.Trip.toLocaleString();
        document.getElementById('speeding-count').textContent = d.counts.Speeding.toLocaleString();
        document.getElementById('idle-count').textContent     = d.counts.Idle.toLocaleString();
        document.getElementById('awh-count').textContent      = d.counts.AWH.toLocaleString();
        document.getElementById('wh-count').textContent       = d.counts.WH.toLocaleString();
        document.getElementById('ha-count').textContent       = d.counts.HA.toLocaleString();
        document.getElementById('hb-count').textContent       = d.counts.HB.toLocaleString();
        document.getElementById('wu-count').textContent       = d.counts.WU.toLocaleString();

        // Per-customer breakdown
        if (d.per_customer) {
            const breakdown = document.getElementById('per-customer-breakdown');
            breakdown.innerHTML = Object.entries(d.per_customer).map(([appId, info]) => `
                <div class="breakdown-card">
                    <div class="breakdown-name">${info.name} (${appId})</div>
                    ${['Trip','Speeding','Idle','AWH','WH','HA','HB','WU'].map(k => `
                        <div class="breakdown-row"><span>${k}</span><span>${(info[k]||0).toLocaleString()}</span></div>
                    `).join('')}
                </div>
            `).join('');
        }
    }

    async function refreshJobs() {
        const r = await fetch('/dashboard/status/recent');
        const d = await r.json();
        if (!d.success) return;
        const jobList = document.getElementById('job-list');
        jobList.innerHTML = d.jobs.map(job => `
            <div class="job-item ${job.status}">
                <div class="job-header">
                    <span class="job-type">${job.job_type}</span>
                    <span class="job-status ${job.status}">${job.status}</span>
                </div>
                <div class="job-details">
                    Started: ${new Date(job.started_at).toLocaleString()}<br>
                    ${job.completed_at ? 'Completed: ' + new Date(job.completed_at).toLocaleString() + '<br>' : ''}
                    ${job.records_processed ? 'Records: ' + job.records_processed.toLocaleString() + '<br>' : ''}
                    ${job.error_message ? '<span style="color:#dc3545;">Error: ' + job.error_message + '</span>' : ''}
                </div>
            </div>
        `).join('') || '<p style="color:#666;">No jobs yet</p>';
    }

    async function refreshLastSync() {
        const r = await fetch('/dashboard/stats/last-sync');
        const d = await r.json();
        const el = document.getElementById('last-sync-info');
        if (!d.success) { el.textContent = 'Error loading'; return; }
        let html = '';
        if (d.daily_sync)
            html += `<strong>Daily:</strong> ${new Date(d.daily_sync.completed_at).toLocaleString()}<br>Records: ${(d.daily_sync.records_processed||0).toLocaleString()}<br><br>`;
        if (d.weekly_backfill)
            html += `<strong>Weekly:</strong> ${new Date(d.weekly_backfill.completed_at).toLocaleString()}<br>Records: ${(d.weekly_backfill.records_processed||0).toLocaleString()}`;
        el.innerHTML = html || 'No sync data yet';
    }

    async function refreshSchedulerStatus() {
        const r = await fetch('/dashboard/stats/scheduler-status');
        const d = await r.json();
        if (!d.success) return;

        const fmt = jobs => jobs.slice(0,5).map(j => `
            <div style="padding:8px;margin:5px 0;background:${j.status==='completed'?'#d4edda':j.status==='failed'?'#f8d7da':'#fff3cd'};border-radius:4px;font-size:.9rem;">
                <strong>${new Date(j.started_at).toLocaleString()}</strong>
                <span style="float:right;font-weight:600;color:${j.status==='completed'?'#28a745':j.status==='failed'?'#dc3545':'#ffc107'};">${j.status.toUpperCase()}</span><br>
                ${j.records_processed ? 'Records: ' + j.records_processed.toLocaleString() : ''}
                ${j.error_message ? '<br><span style="color:#dc3545;">'+j.error_message+'</span>' : ''}
            </div>`).join('') || '<em style="color:#666;">None yet</em>';

        document.getElementById('daily-scheduler-status').innerHTML  = fmt(d.daily_syncs);
        document.getElementById('weekly-scheduler-status').innerHTML = fmt(d.weekly_backfills);
    }

    // ---------- INIT ----------
    loadCustomers();
    refreshStats();
    refreshJobs();
    refreshLastSync();
    refreshSchedulerStatus();

    setInterval(refreshJobs,             5000);
    setInterval(refreshStats,           30000);
    setInterval(refreshSchedulerStatus, 10000);
</script>
</body>
</html>'''
    return render_template_string(html)