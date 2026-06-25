from celery import Celery, Task
from datetime import datetime, timezone

celery = Celery("power_bi_gpsgate")

# Maps task short-name → positional index of application_id in args (0-based, excluding self)
_APPID_ARG_INDEX = {
    "dimension_sync": 0,
    "fact_sync":      2,
    "full_backfill":  2,
}


def _extract_meta_from_args(task_short, req_args, req_kwargs):
    """Pull application_id / date range out of a task's args+kwargs."""
    raw_app_id = req_kwargs.get("application_id")
    if raw_app_id is None:
        idx = _APPID_ARG_INDEX.get(task_short)
        if idx is not None and len(req_args) > idx:
            raw_app_id = req_args[idx]
    try:
        application_id = int(raw_app_id) if raw_app_id is not None else None
    except (ValueError, TypeError):
        application_id = None

    start_date = req_kwargs.get("start_date") or (req_args[0] if task_short in ("fact_sync", "full_backfill") and req_args else None)
    end_date   = req_kwargs.get("end_date")   or (req_args[1] if task_short in ("fact_sync", "full_backfill") and len(req_args) > 1 else None)

    return application_id, start_date, end_date


def configure_celery(app):
    """Bind Celery to Flask app so every task runs inside app context."""
    celery.conf.update(app.config.get("CELERY", {}))
    celery.conf.resultrepr_maxsize = 100000

    class FlaskTask(Task):
        def __call__(self, *args, **kwargs):
            from app.models import db, JobLog

            job_type   = self.name.replace("tasks.", "")
            task_id    = self.request.id
            req_args   = list(self.request.args or args)
            req_kwargs = dict(self.request.kwargs or kwargs)

            application_id, start_date, end_date = _extract_meta_from_args(job_type, req_args, req_kwargs)

            with app.app_context():
                # ── record job start ──────────────────────────────────────
                log = JobLog(
                    task_id=task_id,
                    job_type=job_type,
                    status="running",
                    application_id=application_id,
                    started_at=datetime.now(timezone.utc),
                    job_metadata={
                        "application_id": application_id,
                        "start_date": start_date,
                        "end_date": end_date,
                    },
                )
                db.session.add(log)
                try:
                    db.session.commit()
                except Exception:
                    db.session.rollback()

                # ── run the actual task ───────────────────────────────────
                try:
                    result = self.run(*args, **kwargs)
                except Exception as exc:
                    try:
                        entry = JobLog.query.filter_by(task_id=task_id).first()
                        if entry:
                            entry.status        = "failed"
                            entry.completed_at  = datetime.now(timezone.utc)
                            entry.error_message = str(exc)[:2000]
                            db.session.commit()
                    except Exception:
                        db.session.rollback()
                    raise

                # ── record job completion ─────────────────────────────────
                try:
                    entry = JobLog.query.filter_by(task_id=task_id).first()
                    if entry:
                        entry.status       = "completed"
                        entry.completed_at = datetime.now(timezone.utc)
                        if isinstance(result, dict):
                            entry.records_processed = (
                                result.get("total_inserted") or result.get("records") or 0
                            )
                            entry.job_metadata = {
                                "application_id":   application_id,
                                "start_date":       result.get("start_date")  or start_date,
                                "end_date":         result.get("end_date")    or end_date,
                                "date":             result.get("date"),
                                "total_inserted":   result.get("total_inserted"),
                                "total_skipped":    result.get("total_skipped"),
                                "total_failed":     result.get("total_failed"),
                                "dimension_records": result.get("dimension_records"),
                            }
                        db.session.commit()
                except Exception:
                    db.session.rollback()

                return result

    celery.Task = FlaskTask
    app.extensions["celery"] = celery
    return celery
