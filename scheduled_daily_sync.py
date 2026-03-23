#!/usr/bin/env python
"""Scheduled daily sync job using customer_config runtime values."""

import os
import sys
import json
import traceback
from datetime import datetime, timedelta

from dotenv import load_dotenv


print(f"[DAILY_SYNC] Starting scheduled daily sync at {datetime.utcnow()}")
print(f"[DAILY_SYNC] Python executable: {sys.executable}")
print(f"[DAILY_SYNC] Working directory: {os.getcwd()}")
print(f"[DAILY_SYNC] Script path: {os.path.abspath(__file__)}")

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


def run_daily_sync():
    app = create_app()

    with app.app_context():
        job = JobExecution(
            job_type="daily_sync",
            status="running",
            started_at=datetime.utcnow(),
        )
        db.session.add(job)
        db.session.commit()

        try:
            today = datetime.utcnow().date()
            yesterday = today - timedelta(days=1)
            start_date = yesterday.strftime("%Y-%m-%d")
            end_date = yesterday.strftime("%Y-%m-%d")

            logger.info(f"Starting daily sync for {start_date}")

            logger.info("Syncing dimension tables")
            try:
                from sync_dimensions_from_api import main as sync_dimensions

                dimension_result = sync_dimensions()
                dimension_records = dimension_result if isinstance(dimension_result, int) else 0
                logger.info(f"Dimension tables synced: {dimension_records} records processed")
            except Exception as exc:
                logger.error(f"Dimension sync failed: {exc}")
                dimension_records = 0

            customers = load_customers()
            if not customers:
                raise RuntimeError("No customer_config rows found for daily sync")

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
                            start_date=start_date,
                            end_date=end_date,
                        )
                        app_results[event_type] = {
                            "status": "success",
                            "raw": result.get("raw", 0),
                            "inserted": result.get("inserted", 0),
                            "skipped": result.get("skipped", 0),
                            "failed": result.get("failed", 0),
                        }
                        total_raw += result.get("raw", 0)
                        total_inserted += result.get("inserted", 0)
                        total_skipped += result.get("skipped", 0)
                        total_failed += result.get("failed", 0)
                    except Exception as exc:
                        logger.error(f"app={customer.application_id} {event_type} failed: {exc}")
                        app_results[event_type] = {"status": "failed", "error": str(exc)}
                        total_failed += 1

                results[str(customer.application_id)] = app_results

            total_records = total_raw + dimension_records

            job.status = "completed"
            job.completed_at = datetime.utcnow()
            job.records_processed = total_inserted + dimension_records
            job.errors = total_failed
            job.job_metadata = {
                "date": start_date,
                "dimension_records": dimension_records,
                "total_raw": total_raw,
                "total_skipped": total_skipped,
                "total_records": total_records,
                "message": (
                    f"Daily sync completed - dimensions {dimension_records:,}, "
                    f"raw fact rows {total_raw:,}, inserted {total_inserted:,}"
                ),
                "fact_results": results,
                "total_customers_processed": len(customers),
            }
            db.session.commit()

            logger.info(
                "Daily sync completed: "
                f"dimensions={dimension_records}, raw={total_raw}, inserted={total_inserted}, "
                f"skipped={total_skipped}, failed={total_failed}"
            )
            return {
                "success": True,
                "date": start_date,
                "dimension_records": dimension_records,
                "total_raw": total_raw,
                "total_inserted": total_inserted,
                "total_skipped": total_skipped,
                "total_failed": total_failed,
                "results": results,
            }
        except Exception as exc:
            logger.error(f"Daily sync failed: {exc}")
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
    result = run_daily_sync()
    if result["success"]:
        print("Daily sync completed successfully")
        sys.exit(0)
    print(f"Daily sync failed: {result['error']}")
    sys.exit(1)
