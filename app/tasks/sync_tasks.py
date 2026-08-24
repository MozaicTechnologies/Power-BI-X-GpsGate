import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.celery_app import celery
from app.services.customer_config import EVENT_CONFIG, load_applications
from app.services.event_processor import run_event_for_dates
from app.utils.logger import setup_logger

logger = setup_logger("TASKS")
MUSCAT_TZ = ZoneInfo("Asia/Muscat")


def _muscat_today():
    """Return the operational date used by scheduled GpsGate syncs."""
    return datetime.now(MUSCAT_TZ).date()


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
    logger.info("[dim_sync] STARTED | task_id=%s | app=%s", self.request.id, application_id or "all")
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

    logger.info("[dim_sync] DONE | task_id=%s | app=%s | records=%d",
                self.request.id, application_id or "all", total)
    return {"status": "completed", "records": total}


# ---------------------------------------------------------------------------
# Daily sync  (triggered by Celery Beat every day at 02:00 Muscat)
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.daily_sync", track_started=True)
def daily_sync_task(self):
    t0 = time.time()
    today = _muscat_today()
    start_date = end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info("[daily_sync] STARTED | task_id=%s | date=%s", self.request.id, start_date)
    _progress(self, 0, 1, f"Syncing dimensions for {start_date}")

    try:
        from app.services.sync_dimensions import main as sync_dimensions
        dim_records = sync_dimensions() or 0
        logger.info("[daily_sync] DIM_SYNC DONE | date=%s | records=%d", start_date, dim_records)
    except Exception:
        logger.exception("[daily_sync] DIM_SYNC FAILED | date=%s", start_date)
        dim_records = 0

    customers = load_applications()
    event_types = list(EVENT_CONFIG.keys())
    total_steps = len(customers) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    logger.info("[daily_sync] FACT_SYNC START | date=%s | apps=%d | events=%d | total_steps=%d",
                start_date, len(customers), len(event_types), total_steps)

    for app in customers:
        app_results = {}
        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} / app={app.application_id}",
                event_type=et,
                customer=str(app.application_id),
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, start_date, end_date, app)
                ins  = result.get("inserted", 0)
                skip = result.get("skipped",  0)
                fail = result.get("failed",   0)
                app_results[et] = {"status": "success", **result}
                total_inserted += ins
                total_skipped  += skip
                total_failed   += fail
                logger.info("[daily_sync] EVENT OK | app=%s | event=%s | date=%s | inserted=%d skipped=%d failed=%d",
                            app.application_id, et, start_date, ins, skip, fail)
            except Exception as exc:
                app_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
                logger.exception("[daily_sync] EVENT FAIL | app=%s | event=%s | date=%s",
                                 app.application_id, et, start_date)
            done += 1
            _progress(
                self, done, total_steps,
                f"Done {et} / app={app.application_id}",
                event_type=et,
                customer=str(app.application_id),
                inserted=total_inserted,
            )
        results[str(app.application_id)] = app_results

    elapsed = time.time() - t0
    logger.info(
        "[daily_sync] DONE | task_id=%s | date=%s | dim_records=%d | "
        "inserted=%d skipped=%d failed=%d | elapsed=%.1fs",
        self.request.id, start_date, dim_records,
        total_inserted, total_skipped, total_failed, elapsed,
    )

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
# Weekly backfill  (triggered by Celery Beat every Monday at 03:00 Muscat)
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.weekly_backfill", track_started=True)
def weekly_backfill_task(self):
    t0 = time.time()
    today = _muscat_today()
    end_date = today - timedelta(days=1)
    start_date = end_date - timedelta(days=6)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    customers = load_applications()
    event_types = list(EVENT_CONFIG.keys())
    total_steps = len(customers) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    logger.info(
        "[weekly_backfill] STARTED | task_id=%s | range=%s→%s | apps=%d | events=%d | total_steps=%d",
        self.request.id, start_str, end_str, len(customers), len(event_types), total_steps,
    )

    for app in customers:
        app_results = {}
        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} / app={app.application_id}",
                event_type=et,
                customer=str(app.application_id),
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, start_str, end_str, app)
                ins  = result.get("inserted", 0)
                skip = result.get("skipped",  0)
                fail = result.get("failed",   0)
                app_results[et] = {"status": "success", **result}
                total_inserted += ins
                total_skipped  += skip
                total_failed   += fail
                logger.info(
                    "[weekly_backfill] EVENT OK | app=%s | event=%s | range=%s→%s | inserted=%d skipped=%d failed=%d",
                    app.application_id, et, start_str, end_str, ins, skip, fail,
                )
            except Exception as exc:
                app_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
                logger.exception("[weekly_backfill] EVENT FAIL | app=%s | event=%s | range=%s→%s",
                                 app.application_id, et, start_str, end_str)
            done += 1
        results[str(app.application_id)] = app_results

    elapsed = time.time() - t0
    logger.info(
        "[weekly_backfill] DONE | task_id=%s | range=%s→%s | "
        "inserted=%d skipped=%d failed=%d | elapsed=%.1fs",
        self.request.id, start_str, end_str,
        total_inserted, total_skipped, total_failed, elapsed,
    )

    return {
        "status": "completed",
        "start_date": start_str,
        "end_date": end_str,
        "total_inserted": total_inserted,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "results": results,
    }
