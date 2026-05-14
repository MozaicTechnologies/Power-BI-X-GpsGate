import logging

from app.celery_app import celery
from app.services.customer_config import EVENT_CONFIG, load_customers
from app.services.event_processor import iter_week_ranges, run_event_for_dates

logger = logging.getLogger(__name__)


def _get_customer(application_id):
    if application_id:
        from app.models import CustomerConfig, db
        customer = db.session.get(CustomerConfig, str(application_id))
        if not customer:
            raise RuntimeError(f"No customer_config for application_id={application_id}")
        return customer
    customers = load_customers()
    if not customers:
        raise RuntimeError("No rows in customer_config")
    return customers[0]


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
    customer    = _get_customer(application_id)
    event_types = list(EVENT_CONFIG.keys())
    week_ranges = list(iter_week_ranges(start_date, end_date))
    total_steps = len(week_ranges) * len(event_types)
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    logger.info(
        "[fact_sync] START | app=%s | %s → %s | weeks=%d | event_types=%d | total_steps=%d",
        application_id, start_date, end_date, len(week_ranges), len(event_types), total_steps,
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
                result = run_event_for_dates(et, week_start, week_end, customer)
                week_results[et] = result
                ins  = result.get("inserted", 0)
                skip = result.get("skipped",  0)
                fail = result.get("failed",   0)
                total_inserted += ins
                total_skipped  += skip
                total_failed   += fail
                logger.info(
                    "[fact_sync] OK  | app=%s | week=%s | event=%s | inserted=%d skipped=%d failed=%d",
                    application_id, week_key, et, ins, skip, fail,
                )
            except Exception as exc:
                week_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
                logger.error(
                    "[fact_sync] ERR | app=%s | week=%s | event=%s | %s",
                    application_id, week_key, et, exc,
                )
            done += 1

        results[week_key] = week_results

    logger.info(
        "[fact_sync] DONE | app=%s | %s → %s | inserted=%d skipped=%d failed=%d",
        application_id, start_date, end_date, total_inserted, total_skipped, total_failed,
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

@celery.task(bind=True, name="tasks.full_backfill", track_started=True)
def full_backfill_task(self, start_date: str, end_date: str, application_id=None):
    customer = _get_customer(application_id)
    event_types = list(EVENT_CONFIG.keys())
    week_ranges = list(iter_week_ranges(start_date, end_date))
    total_steps = 1 + len(week_ranges) * len(event_types)  # +1 for dim sync
    done = 0
    total_inserted = total_skipped = total_failed = 0
    results = {}

    # --- Step 1: Dimension sync ---
    _progress(self, done, total_steps, "Syncing dimension tables…", phase="dimensions")
    try:
        from app.services.sync_dimensions import main as sync_main
        dim_records = sync_main(application_id) or 0
    except Exception as exc:
        dim_records = 0
    done += 1

    # --- Step 2: Fact tables per week ---
    for week_start, week_end in week_ranges:
        week_key = f"{week_start} → {week_end}"
        week_results = {}
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
                result = run_event_for_dates(et, week_start, week_end, customer)
                week_results[et] = result
                total_inserted += result.get("inserted", 0)
                total_skipped  += result.get("skipped", 0)
                total_failed   += result.get("failed", 0)
            except Exception as exc:
                week_results[et] = {"status": "failed", "error": str(exc)}
                total_failed += 1
            done += 1
        results[week_key] = week_results

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
