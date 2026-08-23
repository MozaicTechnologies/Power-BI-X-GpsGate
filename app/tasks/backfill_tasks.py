import time

from app.celery_app import celery
from app.services.customer_config import EVENT_CONFIG, load_applications
from app.services.event_processor import iter_week_ranges, run_event_for_dates
from app.utils.logger import setup_logger, task_log_context

logger = setup_logger("TASKS")


def _get_application(application_id):
    if application_id:
        from app.models import GpsGateApplication, db
        app = GpsGateApplication.query.filter_by(application_id=int(application_id)).first()
        if not app:
            raise RuntimeError(f"No gpsgate_application for application_id={application_id}")
        return app
    applications = load_applications()
    if not applications:
        raise RuntimeError("No rows in gpsgate_application")
    return applications[0]


def _progress(self, done, total, status, **extra):
    percent = int(done / total * 100) if total else 0
    self.update_state(
        state="PROGRESS",
        meta={"current": done, "total": total, "percent": percent, "status": status, **extra},
    )


# ---------------------------------------------------------------------------
# Fact sync  (facts only, specific date range)
# ---------------------------------------------------------------------------

@celery.task(bind=True, name="tasks.fact_sync", track_started=True)
def fact_sync_task(self, start_date: str, end_date: str, application_id=None):
    t0 = time.time()
    app    = _get_application(application_id)
    event_types = list(EVENT_CONFIG.keys())
    week_ranges = list(iter_week_ranges(start_date, end_date))
    total_steps = len(week_ranges) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    logger.info(
        "[fact_sync] STARTED | task_id=%s | app=%s | %s→%s | weeks=%d | events=%d | total_steps=%d",
        self.request.id, application_id, start_date, end_date,
        len(week_ranges), len(event_types), total_steps,
    )

    for week_start, week_end in week_ranges:
        week_key     = f"{week_start} → {week_end}"
        week_results = {}

        logger.info("[fact_sync] WEEK | app=%s | %s", application_id, week_key)

        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} for {week_key}",
                event_type=et,
                week=week_key,
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, week_start, week_end, app)
                week_results[et] = result
                ins  = result.get("inserted", 0)
                skip = result.get("skipped",  0)
                fail = result.get("failed",   0)
                total_inserted += ins
                total_skipped  += skip
                total_failed   += fail
                logger.info(
                    "[fact_sync] EVENT OK | app=%s | week=%s | event=%s | inserted=%d skipped=%d failed=%d",
                    application_id, week_key, et, ins, skip, fail,
                )
            except Exception as exc:
                week_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
                logger.exception(
                    "[fact_sync] EVENT FAIL | app=%s | week=%s | event=%s",
                    application_id, week_key, et,
                )
            done += 1
            _progress(
                self, done, total_steps,
                f"Done {et} for {week_key}",
                event_type=et,
                week=week_key,
                inserted=total_inserted,
            )

        results[week_key] = week_results

    elapsed = time.time() - t0
    logger.info(
        "[fact_sync] DONE | task_id=%s | app=%s | %s→%s | "
        "inserted=%d skipped=%d failed=%d | elapsed=%.1fs",
        self.request.id, application_id, start_date, end_date,
        total_inserted, total_skipped, total_failed, elapsed,
    )

    return {
        "status":         "completed",
        "start_date":     start_date,
        "end_date":       end_date,
        "total_inserted": total_inserted,
        "total_skipped":  total_skipped,
        "total_failed":   total_failed,
        "results":        results,
    }


# ---------------------------------------------------------------------------
# Full backfill  (dimensions + facts, specific date range)
# ---------------------------------------------------------------------------

def _run_full_backfill(self, start_date: str, end_date: str, application_id=None):
    t0 = time.time()
    app = _get_application(application_id)
    event_types = list(EVENT_CONFIG.keys())
    week_ranges = list(iter_week_ranges(start_date, end_date))
    total_steps = 1 + len(week_ranges) * len(event_types)  # +1 for dim sync
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    logger.info(
        "[full_backfill] STARTED | task_id=%s | app=%s | %s→%s | weeks=%d | events=%d | total_steps=%d",
        self.request.id, application_id, start_date, end_date,
        len(week_ranges), len(event_types), total_steps,
    )

    # --- Step 1: Dimension sync ---
    _progress(self, done, total_steps, "Syncing dimension tables…", phase="dimensions")
    try:
        from app.services.sync_dimensions import main as sync_main
        dim_records = sync_main(application_id) or 0
        logger.info("[full_backfill] DIM_SYNC DONE | app=%s | records=%d", application_id, dim_records)
    except Exception:
        logger.exception("[full_backfill] DIM_SYNC FAILED | app=%s", application_id)
        dim_records = 0
    done += 1

    # --- Step 2: Fact tables per week ---
    for week_start, week_end in week_ranges:
        week_key = f"{week_start} → {week_end}"
        week_results = {}

        logger.info("[full_backfill] WEEK | app=%s | %s", application_id, week_key)

        for et in event_types:
            _progress(
                self, done, total_steps,
                f"Processing {et} for {week_key}",
                phase="facts",
                event_type=et,
                week=week_key,
                inserted=total_inserted,
            )
            try:
                result = run_event_for_dates(et, week_start, week_end, app)
                ins  = result.get("inserted", 0)
                skip = result.get("skipped",  0)
                fail = result.get("failed",   0)
                week_results[et] = result
                total_inserted += ins
                total_skipped  += skip
                total_failed   += fail
                logger.info(
                    "[full_backfill] EVENT OK | app=%s | week=%s | event=%s | inserted=%d skipped=%d failed=%d",
                    application_id, week_key, et, ins, skip, fail,
                )
            except Exception as exc:
                week_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
                logger.exception(
                    "[full_backfill] EVENT FAIL | app=%s | week=%s | event=%s",
                    application_id, week_key, et,
                )
            done += 1
            _progress(
                self, done, total_steps,
                f"Done {et} for {week_key}",
                phase="facts",
                event_type=et,
                week=week_key,
                inserted=total_inserted,
            )
        results[week_key] = week_results

    elapsed = time.time() - t0
    logger.info(
        "[full_backfill] DONE | task_id=%s | app=%s | %s→%s | dim_records=%d | "
        "inserted=%d skipped=%d failed=%d | elapsed=%.1fs",
        self.request.id, application_id, start_date, end_date, dim_records,
        total_inserted, total_skipped, total_failed, elapsed,
    )

    return {
        "status": "completed",
        "start_date": start_date,
        "end_date": end_date,
        "dimension_records": dim_records,
        "total_inserted": total_inserted,
        "total_skipped": total_skipped,
        "total_failed": total_failed,
        "results": results,
    }


@celery.task(bind=True, name="tasks.full_backfill", track_started=True)
def full_backfill_task(self, start_date: str, end_date: str, application_id=None):
    task_id = self.request.id or "unknown"
    with task_log_context("full_backfill", task_id) as log_file:
        logger.info(
            "[full_backfill] DEDICATED LOG | task_id=%s | file=%s",
            task_id, log_file,
        )
        try:
            result = _run_full_backfill(self, start_date, end_date, application_id)
            result["log_file"] = log_file
            return result
        except Exception:
            logger.exception(
                "[full_backfill] UNHANDLED FAILURE | task_id=%s | app=%s | %s→%s",
                task_id, application_id, start_date, end_date,
            )
            raise
