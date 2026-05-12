"""
Shared helper used by both dashboard routes and Celery tasks.
Runs process_event_data for a single event type + date range.
Must be called within a Flask app context.
"""

import json
from datetime import datetime, timedelta
from flask import current_app

from app.config import Config
from app.services.customer_config import get_event_runtime_config


def iter_week_ranges(start_date: str, end_date: str):
    """Yield inclusive 7-day windows between start_date and end_date."""
    current = datetime.strptime(start_date, "%Y-%m-%d").date()
    final = datetime.strptime(end_date, "%Y-%m-%d").date()
    while current <= final:
        week_end = min(current + timedelta(days=6), final)
        yield current.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")
        current = week_end + timedelta(days=1)


def run_event_for_dates(event_type: str, start_date: str, end_date: str, customer) -> dict:
    """
    Call process_event_data for one event type and date range.
    Returns the 'accounting' dict: {raw, inserted, skipped, failed}.
    """
    from app.routes.pipeline import process_event_data

    app = current_app._get_current_object()
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
            response_obj, _ = response_tuple
            return response_obj.get_json().get("accounting", {})
        return {}
