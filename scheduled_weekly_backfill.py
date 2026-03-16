#!/usr/bin/env python
"""Scheduled weekly backfill job using customer_config runtime values."""

import os
import sys
import json
import traceback
from datetime import datetime, timedelta

from dotenv import load_dotenv


print(f"[WEEKLY_BACKFILL] Starting scheduled weekly backfill at {datetime.utcnow()}")
print(f"[WEEKLY_BACKFILL] Python executable: {sys.executable}")
print(f"[WEEKLY_BACKFILL] Working directory: {os.getcwd()}")
print(f"[WEEKLY_BACKFILL] Script path: {os.path.abspath(__file__)}")

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from application import create_app, db
from config import Config
from customer_runtime_config import EVENT_CONFIG, get_event_runtime_config, load_customers
from data_pipeline import process_event_data
from logger_config import get_logger
from models import JobExecution


logger = get_logger(__name__)


def process_event_with_dates(app, customer, event_type, start_date, end_date):
    runtime = get_event_runtime_config(customer, event_type, Config.BASE_URL)

    payload = {
        "app_id": runtime.app_id,
        "token": runtime.token,
        "base_url": runtime.base_url,
        "report_id": runtime.report_id,
        "tag_id": runtime.tag_id,
        "period_start": f"{start_date}T00:00:00Z",
        "period_end": f"{end_date}T23:59:59Z",
    }
    if runtime.event_id:
        payload["event_id"] = runtime.event_id

    with app.test_request_context(
        "/api/process",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        response_tuple = process_event_data(
            event_name=event_type,
            response_key=runtime.response_key,
        )
        if isinstance(response_tuple, tuple):
            response_obj, _status_code = response_tuple
            return response_obj.get_json().get("accounting", {})
        return response_tuple


def run_weekly_backfill():
    app = create_app()

    with app.app_context():
        job = JobExecution(
            job_type="weekly_backfill",
            status="running",
            started_at=datetime.utcnow(),
        )
        db.session.add(job)
        db.session.commit()

        try:
            today = datetime.utcnow().date()
            end_date = today - timedelta(days=1)
            start_date = end_date - timedelta(days=6)
            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            logger.info(f"Starting weekly backfill: {start_str} to {end_str}")

            customers = load_customers()
            if not customers:
                raise RuntimeError("No customer_config rows found for weekly backfill")

            event_types = list(EVENT_CONFIG.keys())
            results = {}
            total_raw = 0
            total_inserted = 0
            total_skipped = 0
            total_failed = 0

            for customer in customers:
                app_results = {}
                for event_type in event_types:
                    logger.info(f"Processing app={customer.application_id} event={event_type}")
                    try:
                        result = process_event_with_dates(
                            app=app,
                            customer=customer,
                            event_type=event_type,
                            start_date=start_str,
                            end_date=end_str,
                        )
                        raw = result.get("raw", 0)
                        inserted = result.get("inserted", 0)
                        skipped = result.get("skipped", 0)
                        failed = result.get("failed", 0)

                        app_results[event_type] = {
                            "status": "success",
                            "raw": raw,
                            "inserted": inserted,
                            "skipped": skipped,
                            "failed": failed,
                        }

                        total_raw += raw
                        total_inserted += inserted
                        total_skipped += skipped
                        total_failed += failed
                    except Exception as exc:
                        logger.error(f"app={customer.application_id} {event_type} failed: {exc}")
                        app_results[event_type] = {"status": "failed", "error": str(exc)}
                        total_failed += 1

                results[str(customer.application_id)] = app_results

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted
            job.errors = total_failed
            job.job_metadata = {
                "start_date": start_str,
                "end_date": end_str,
                "total_raw": total_raw,
                "total_skipped": total_skipped,
                "results": results,
                "total_customers_processed": len(customers),
            }
            db.session.commit()

            logger.info(
                "Weekly backfill completed: "
                f"raw={total_raw}, inserted={total_inserted}, skipped={total_skipped}, failed={total_failed}"
            )
            return {
                "success": True,
                "start_date": start_str,
                "end_date": end_str,
                "total_raw": total_raw,
                "total_inserted": total_inserted,
                "total_skipped": total_skipped,
                "total_failed": total_failed,
                "results": results,
            }
        except Exception as exc:
            logger.error(f"Weekly backfill failed: {exc}")
            logger.error(traceback.format_exc())
            job.status = "failed"
            job.completed_at = datetime.utcnow()
            job.error_message = str(exc)
            job.job_metadata = {"traceback": traceback.format_exc()}
            db.session.commit()
            return {
                "success": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }


if __name__ == "__main__":
    result = run_weekly_backfill()
    if result["success"]:
        print("Weekly backfill completed successfully")
        sys.exit(0)
    print(f"Weekly backfill failed: {result['error']}")
    sys.exit(1)
