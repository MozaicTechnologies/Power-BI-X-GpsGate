"""Helpers for loading runtime event configuration from gpsgate_application."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


EVENT_CONFIG = {
    "Trip": {"report_field": "trip_report_id", "event_field": None, "response_key": "trip_events"},
    "Speeding": {"report_field": "event_report_id", "event_field": "speed_event_id", "response_key": "speed_events"},
    "Idle": {"report_field": "event_report_id", "event_field": "idle_event_id", "response_key": "idle_events"},
    "AWH": {"report_field": "event_report_id", "event_field": "awh_event_id", "response_key": "awh_events"},
    "WH": {"report_field": "event_report_id", "event_field": "wh_event_id", "response_key": "wh_events"},
    "HA": {"report_field": "event_report_id", "event_field": "ha_event_id", "response_key": "ha_events"},
    "HB": {"report_field": "event_report_id", "event_field": "hb_event_id", "response_key": "hb_events"},
    "WU": {"report_field": "event_report_id", "event_field": "wu_event_id", "response_key": "wu_events"},
}


@dataclass
class EventRuntimeConfig:
    app_id: int
    token: str
    base_url: str
    report_id: str
    tag_id: str
    event_id: Optional[str]
    response_key: str


def normalize_token(token: str | None) -> str:
    token = (token or "").strip()
    if not token:
        raise RuntimeError("Missing token in gpsgate_application")
    if token.lower().startswith("bearer "):
        return token
    if token.startswith("v1:") or token.startswith("v2:"):
        return token
    return f"v2:{token}"


def load_applications(app_id_filter: int | None = None):
    from app.models import GpsGateApplication

    query = GpsGateApplication.query
    if app_id_filter:
        query = query.filter_by(application_id=app_id_filter)
    return query.order_by(GpsGateApplication.application_id).all()


# Keep old function name as alias for backwards compatibility
load_customers = load_applications


def get_event_runtime_config(app, event_type: str, base_url: str) -> EventRuntimeConfig:
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")

    event_meta = EVENT_CONFIG[event_type]
    report_id = getattr(app, event_meta["report_field"], None)
    event_field = event_meta["event_field"]
    event_id = getattr(app, event_field, None) if event_field else None

    missing = []
    if not app.application_id:
        missing.append("application_id")
    if not app.token:
        missing.append("token")
    if not app.tag_id:
        missing.append("tag_id")
    if not report_id:
        missing.append(event_meta["report_field"])
    if event_field and not event_id:
        missing.append(event_field)

    if missing:
        raise RuntimeError(
            f"gpsgate_application missing required fields for application_id={app.application_id}: {', '.join(missing)}"
        )

    return EventRuntimeConfig(
        app_id=str(app.application_id),
        token=normalize_token(app.token),
        base_url=base_url,
        report_id=str(report_id),
        tag_id=str(app.tag_id),
        event_id=str(event_id) if event_id else None,
        response_key=event_meta["response_key"],
    )
