from datetime import datetime, timedelta

from app.celery_app import celery
from app.services.customer_config import EVENT_CONFIG, load_customers
from app.services.event_processor import run_event_for_dates


def _progress(self, done, total, status, **extra):
    percent = int(done / total * 100) if total else 0
    self.update_state(
        state="PROGRESS",
        meta={"current": done, "total": total, "percent": percent, "status": status, **extra},
    )


# ---------------------------------------------------------------------------
# Dimension sync
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.dimension_sync", track_started=True)
def dimension_sync_task(self, application_id=None):
    self.update_state(state="PROGRESS", meta={"percent": 0, "status": "Starting dimension sync…"})

    from app.services.sync_dimensions import main as sync_main

    def on_progress(status, current, total, percent=None):
        if percent is None:
            percent = int(current / total * 100) if total else 0
        self.update_state(
            state="PROGRESS",
            meta={"percent": percent, "status": status, "current": current, "total": total},
        )

    total = sync_main(application_id, on_progress=on_progress) or 0

    return {"status": "completed", "records": total}


# ---------------------------------------------------------------------------
# Daily sync  (triggered by Celery Beat every day at 02:00 UTC)
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.daily_sync", track_started=True)
def daily_sync_task(self):
    today = datetime.utcnow().date()
    start_date = end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    _progress(self, 0, 1, f"Syncing dimensions for {start_date}")

    try:
        from app.services.sync_dimensions import main as sync_dimensions
        dim_records = sync_dimensions() or 0
    except Exception:
        dim_records = 0

    customers = load_customers()
    event_types = list(EVENT_CONFIG.keys())
    total_steps = len(customers) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    for customer in customers:
        app_results = {}
        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} / app={customer.application_id}",
                event_type=et,
                customer=str(customer.application_id),
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, start_date, end_date, customer)
                app_results[et] = {"status": "success", **result}
                total_inserted += result.get("inserted", 0)
                total_skipped  += result.get("skipped", 0)
                total_failed   += result.get("failed", 0)
            except Exception as exc:
                app_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
            done += 1
        results[str(customer.application_id)] = app_results

    return {
        "status": "completed",
        "date": start_date,
        "dimension_records": dim_records,
        "total_inserted": total_inserted,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "results": results,
    }


# ---------------------------------------------------------------------------
# Weekly backfill  (triggered by Celery Beat every Monday at 03:00 UTC)
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.weekly_backfill", track_started=True)
def weekly_backfill_task(self):
    today = datetime.utcnow().date()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    customers = load_customers()
    event_types = list(EVENT_CONFIG.keys())
    total_steps = len(customers) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    for customer in customers:
        app_results = {}
        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} / app={customer.application_id}",
                event_type=et,
                customer=str(customer.application_id),
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, start_str, end_str, customer)
                app_results[et] = {"status": "success", **result}
                total_inserted += result.get("inserted", 0)
                total_skipped  += result.get("skipped", 0)
                total_failed   += result.get("failed", 0)
            except Exception as exc:
                app_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
            done += 1
        results[str(customer.application_id)] = app_results

    return {
        "status": "completed",
        "start_date": start_str,
        "end_date": end_str,
        "total_inserted": total_inserted,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "results": results,
    }
