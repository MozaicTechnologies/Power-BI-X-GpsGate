"""Helpers for loading runtime event configuration from customer_config."""

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
    app_id: str
    token: str
    base_url: str
    report_id: str
    tag_id: str
    event_id: Optional[str]
    response_key: str


def normalize_token(token: str | None) -> str:
    token = (token or "").strip()
    if not token:
        raise RuntimeError("Missing token in customer_config")
    if token.lower().startswith("bearer "):
        return token
    if token.startswith("v1:") or token.startswith("v2:"):
        return token
    return f"v2:{token}"


def load_customers(app_id_filter: str | None = None):
    from models import CustomerConfig

    query = CustomerConfig.query
    if app_id_filter:
        query = query.filter_by(application_id=str(app_id_filter))
    return query.order_by(CustomerConfig.application_id).all()


def get_event_runtime_config(customer, event_type: str, base_url: str) -> EventRuntimeConfig:
    if event_type not in EVENT_CONFIG:
        raise ValueError(f"Unknown event type: {event_type}")

    event_meta = EVENT_CONFIG[event_type]
    report_id = getattr(customer, event_meta["report_field"], None)
    event_field = event_meta["event_field"]
    event_id = getattr(customer, event_field, None) if event_field else None

    missing = []
    if not customer.application_id:
        missing.append("application_id")
    if not customer.token:
        missing.append("token")
    if not customer.tag_id:
        missing.append("tag_id")
    if not report_id:
        missing.append(event_meta["report_field"])
    if event_field and not event_id:
        missing.append(event_field)

    if missing:
        raise RuntimeError(
            f"customer_config missing required fields for application_id={customer.application_id}: {', '.join(missing)}"
        )

    return EventRuntimeConfig(
        app_id=str(customer.application_id),
        token=normalize_token(customer.token),
        base_url=base_url,
        report_id=str(report_id),
        tag_id=str(customer.tag_id),
        event_id=str(event_id) if event_id else None,
        response_key=event_meta["response_key"],
    )
