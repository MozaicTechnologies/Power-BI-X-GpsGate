#!/usr/bin/env python
"""
daily_sync.py  (MULTI-TENANT VERSION)
======================================
Scheduled Daily Sync Job — runs every day at 2 AM UTC.
Syncs dimension tables + yesterday's fact data
for ALL active customers in customer_configs.

Changes from original:
- Removed hardcoded app_id="6", token=Config.TOKEN, tag_id="39"
- Reads customer list from customer_configs table
- Loops all customers; each customer's records tagged with their app_id
"""

import sys
import os
import json
from datetime import datetime, timedelta
import traceback

print(f"[DAILY_SYNC] Starting at {datetime.utcnow()}")
print(f"[DAILY_SYNC] Python: {sys.executable}")
print(f"[DAILY_SYNC] CWD: {os.getcwd()}")

try:
    from dotenv import load_dotenv
    load_dotenv()
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    from application import create_app, db
    from models import JobExecution, CustomerConfig
    from logger_config import get_logger
    from data_pipeline import process_event_data
    print("[DAILY_SYNC] All imports successful")

except Exception as e:
    print(f"[DAILY_SYNC] FATAL import error: {e}")
    print(traceback.format_exc())
    sys.exit(1)

logger = get_logger(__name__)

# ------------------------------------------------------------------
# EVENT CONFIG (report/event IDs — same for all customers)
# ------------------------------------------------------------------
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

EVENT_TYPES = list(EVENT_CONFIG.keys())


# ------------------------------------------------------------------
# HELPER — process one event type for one customer + date range
# ------------------------------------------------------------------
def process_event_for_customer(app, event_type: str, customer: CustomerConfig,
                                start_date: str, end_date: str) -> dict:
    """
    Builds payload from customer record (no hardcoded values),
    calls process_event_data inside a Flask request context,
    returns accounting dict {raw, inserted, skipped, failed}.
    """
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")

    config = EVENT_CONFIG[event_type]

    payload = {
        "app_id":       customer.app_id,
        "token":        customer.token,
        "base_url":     customer.base_url,
        "report_id":    config['report_id'],
        "tag_id":       customer.tag_id,
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


# ------------------------------------------------------------------
# HELPER — sync facts for one customer
# ------------------------------------------------------------------
def sync_facts_for_customer(app, customer: CustomerConfig,
                             start_date: str, end_date: str) -> dict:
    """
    Runs all 8 event types for a single customer.
    Returns per-event results + totals.
    """
    results     = {}
    total_raw   = total_inserted = total_skipped = total_failed = 0

    for event_type in EVENT_TYPES:
        logger.info(f"  [{customer.name}] Processing {event_type}...")
        try:
            result = process_event_for_customer(
                app, event_type, customer, start_date, end_date
            )
            results[event_type] = {
                'status':   'success',
                'raw':      result.get('raw', 0),
                'inserted': result.get('inserted', 0),
                'skipped':  result.get('skipped', 0),
                'failed':   result.get('failed', 0),
            }
            total_raw      += result.get('raw', 0)
            total_inserted += result.get('inserted', 0)
            total_skipped  += result.get('skipped', 0)
            total_failed   += result.get('failed', 0)

            logger.info(
                f"  [{customer.name}] {event_type} ✅ "
                f"raw={result.get('raw',0)} inserted={result.get('inserted',0)} "
                f"skipped={result.get('skipped',0)} failed={result.get('failed',0)}"
            )

        except Exception as e:
            logger.error(f"  [{customer.name}] {event_type} ❌ {e}")
            results[event_type] = {'status': 'failed', 'error': str(e)}

    return {
        'results':         results,
        'total_raw':       total_raw,
        'total_inserted':  total_inserted,
        'total_skipped':   total_skipped,
        'total_failed':    total_failed,
    }


# ------------------------------------------------------------------
# MAIN DAILY SYNC
# ------------------------------------------------------------------
def run_daily_sync():
    app = create_app()

    with app.app_context():

        # ── Create job execution record ──────────────────────────
        job = JobExecution(
            job_type='daily_sync',
            status='running',
            started_at=datetime.utcnow(),
            triggered_by='scheduler',
        )
        db.session.add(job)
        db.session.commit()

        try:
            # ── Date range: yesterday ────────────────────────────
            today      = datetime.utcnow().date()
            yesterday  = today - timedelta(days=1)
            start_date = yesterday.strftime('%Y-%m-%d')
            end_date   = yesterday.strftime('%Y-%m-%d')

            logger.info(f"🚀 Daily sync for {start_date}")

            # ── Load all active customers ────────────────────────
            customers = CustomerConfig.query.filter_by(is_active=True).all()

            if not customers:
                logger.warning("⚠️  No active customers in customer_configs!")
                job.status = 'completed'
                job.completed_at = datetime.utcnow()
                job.job_metadata = {'warning': 'No active customers found'}
                db.session.commit()
                return {'success': True, 'warning': 'No active customers'}

            logger.info(f"Found {len(customers)} active customer(s): "
                        f"{[c.name for c in customers]}")

            # ── Step 1: Sync dimension tables ────────────────────
            logger.info("📋 Syncing dimension tables...")
            dimension_records = 0
            try:
                from sync_dimensions_from_api import main as sync_dimensions
                dimension_records = sync_dimensions()
                logger.info(f"✅ Dimensions synced: {dimension_records:,} records")
            except Exception as e:
                logger.error(f"⚠️  Dimension sync failed: {e}")

            # ── Step 2: Sync facts for every customer ────────────
            logger.info("📊 Syncing fact tables for all customers...")

            all_customer_results = {}
            grand_inserted       = 0
            grand_raw            = 0
            grand_skipped        = 0
            grand_failed         = 0

            for customer in customers:
                logger.info(f"\n{'─'*50}")
                logger.info(f"Customer: {customer.name} (app_id={customer.app_id})")
                logger.info(f"{'─'*50}")

                customer_result = sync_facts_for_customer(
                    app, customer, start_date, end_date
                )

                all_customer_results[customer.app_id] = {
                    'name':     customer.name,
                    **customer_result,
                }

                grand_raw      += customer_result['total_raw']
                grand_inserted += customer_result['total_inserted']
                grand_skipped  += customer_result['total_skipped']
                grand_failed   += customer_result['total_failed']

                logger.info(
                    f"✅ {customer.name} complete — "
                    f"inserted={customer_result['total_inserted']} "
                    f"skipped={customer_result['total_skipped']} "
                    f"failed={customer_result['total_failed']}"
                )

            # ── Update job record ────────────────────────────────
            job.status           = 'completed'
            job.completed_at     = datetime.utcnow()
            job.records_processed = grand_inserted + dimension_records
            job.errors           = grand_failed
            job.job_metadata     = {
                'date':               start_date,
                'customers_processed': len(customers),
                'dimension_records':  dimension_records,
                'grand_raw':          grand_raw,
                'grand_skipped':      grand_skipped,
                'per_customer':       all_customer_results,
            }
            db.session.commit()

            logger.info(
                f"\n✅ Daily sync complete — "
                f"customers={len(customers)} "
                f"raw={grand_raw} inserted={grand_inserted} "
                f"skipped={grand_skipped} failed={grand_failed}"
            )

            return {
                'success':             True,
                'date':                start_date,
                'customers_processed': len(customers),
                'dimension_records':   dimension_records,
                'grand_raw':           grand_raw,
                'grand_inserted':      grand_inserted,
                'grand_skipped':       grand_skipped,
                'grand_failed':        grand_failed,
                'per_customer':        all_customer_results,
            }

        except Exception as e:
            logger.error(f"❌ Daily sync failed: {e}")
            logger.error(traceback.format_exc())

            job.status        = 'failed'
            job.completed_at  = datetime.utcnow()
            job.error_message = str(e)
            job.job_metadata  = {'traceback': traceback.format_exc()}
            db.session.commit()

            return {'success': False, 'error': str(e)}


# ------------------------------------------------------------------
# ENTRY POINT
# ------------------------------------------------------------------
if __name__ == '__main__':
    result = run_daily_sync()

    if result.get('success'):
        print(f"\n✅ Daily sync completed")
        print(f"   Customers : {result.get('customers_processed', 0)}")
        print(f"   Inserted  : {result.get('grand_inserted', 0)}")
        print(f"   Skipped   : {result.get('grand_skipped', 0)}")
        sys.exit(0)
    else:
        print(f"\n❌ Daily sync failed: {result.get('error')}")
        sys.exit(1)