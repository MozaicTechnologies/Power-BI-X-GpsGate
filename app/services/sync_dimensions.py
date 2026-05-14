#!/usr/bin/env python
"""Sync dimension tables from GpsGate API and refresh customer_config IDs."""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import requests
from dotenv import load_dotenv
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert  # type: ignore[import]

load_dotenv()

BASE_URL = os.getenv("BASE_URL", "https://omantracking2.com")

NAME_LOOKUP_FIELDS = (
    "tag_name",
    "trip_report_name",
    "event_report_name",
    "speed_event_rule_name",
    "idle_event_rule_name",
    "awh_event_rule_name",
    "ha_event_rule_name",
    "hb_event_rule_name",
    "hc_event_rule_name",
    "wu_event_rule_name",
    "wh_event_rule_name",
)

EVENT_RULE_NAME_TO_ID_FIELD = {
    "speed_event_rule_name": "speed_event_id",
    "idle_event_rule_name":  "idle_event_id",
    "awh_event_rule_name":   "awh_event_id",
    "ha_event_rule_name":    "ha_event_id",
    "hb_event_rule_name":    "hb_event_id",
    "hc_event_rule_name":    "hc_event_id",
    "wu_event_rule_name":    "wu_event_id",
    "wh_event_rule_name":    "wh_event_id",
}


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


def normalize_lookup_name(value: str | None) -> str:
    value = (value or "").strip().lower()
    return re.sub(r"^[^a-z0-9]+|[^a-z0-9]+$", "", value)


def normalize_token(token: str | None) -> str:
    token = (token or "").strip()
    if not token:
        raise RuntimeError("Missing token")
    if token.lower().startswith("bearer "):
        return token
    if token.startswith("v1:") or token.startswith("v2:"):
        return token
    return f"v2:{token}"


def _norm_expr(col):
    """SQLAlchemy equivalent of regexp_replace(lower(trim(col)), ...)"""
    return func.regexp_replace(func.lower(func.trim(col)), r"^[^a-z0-9]+|[^a-z0-9]+$", "", "g")


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_customer_configs(session, only_application_id: str | None = None) -> list[dict]:
    from app.models import CustomerConfig

    q = session.query(CustomerConfig).order_by(CustomerConfig.application_id)
    if only_application_id:
        q = q.filter(CustomerConfig.application_id == str(only_application_id))

    rows = q.all()
    if not rows:
        if only_application_id:
            raise RuntimeError(f"No customer_config row found for application_id={only_application_id}")
        raise RuntimeError("No customer_config rows found")

    configs = []
    for row in rows:
        config = {
            "application_id": int(row.application_id),
            "token": normalize_token(row.token),
        }
        for field in NAME_LOOKUP_FIELDS:
            val = getattr(row, field, None)
            config[field] = (val or "").strip() or None
        configs.append(config)
    return configs


# ---------------------------------------------------------------------------
# ID lookups
# ---------------------------------------------------------------------------

def lookup_tag_id(session, application_id: int, tag_name: str | None) -> str | None:
    if not tag_name:
        return None
    from app.models import DimTags

    row = session.query(DimTags.id).filter(
        DimTags.application_id == application_id,
        _norm_expr(DimTags.name) == normalize_lookup_name(tag_name),
    ).order_by(DimTags.id).first()
    return str(row[0]) if row else None


def lookup_named_ids(session, *, model, application_id: int, names_to_columns: dict[str, str]) -> dict[str, str]:
    normalized = {normalize_lookup_name(name): col for name, col in names_to_columns.items() if normalize_lookup_name(name)}
    if not normalized:
        return {}

    norm_col = _norm_expr(model.name).label("norm")
    rows = session.query(model.id, norm_col).filter(
        model.application_id == application_id,
        _norm_expr(model.name).in_(list(normalized.keys())),
    ).order_by("norm", model.id).all()

    values: dict[str, str] = {}
    for record_id, norm in rows:
        values.setdefault(normalized[norm], str(record_id))
    return values


def lookup_report_ids(session, application_id: int, report_names_to_columns: dict[str, str]) -> dict[str, str]:
    from app.models import DimReports
    return lookup_named_ids(session, model=DimReports, application_id=application_id, names_to_columns=report_names_to_columns)


# ---------------------------------------------------------------------------
# customer_config update
# ---------------------------------------------------------------------------

def update_customer_config_from_dims(session, customer_config: dict) -> None:
    from app.models import CustomerConfig, DimEventRules

    application_id = customer_config["application_id"]

    report_ids = lookup_report_ids(session, application_id, {
        customer_config["trip_report_name"]:  "trip_report_id",
        customer_config["event_report_name"]: "event_report_id",
    })
    event_rule_ids = lookup_named_ids(
        session,
        model=DimEventRules,
        application_id=application_id,
        names_to_columns={customer_config[f]: id_f for f, id_f in EVENT_RULE_NAME_TO_ID_FIELD.items()},
    )

    updates: dict = {}
    missing: list[str] = []

    if customer_config.get("tag_name"):
        tag_id = lookup_tag_id(session, application_id, customer_config["tag_name"])
        updates["tag_id"] = tag_id
        if tag_id is None:
            missing.append("tag_id")

    if customer_config.get("trip_report_name"):
        v = report_ids.get("trip_report_id")
        updates["trip_report_id"] = v
        if v is None:
            missing.append("trip_report_id")

    if customer_config.get("event_report_name"):
        v = report_ids.get("event_report_id")
        updates["event_report_id"] = v
        if v is None:
            missing.append("event_report_id")

    for name_field, id_field in EVENT_RULE_NAME_TO_ID_FIELD.items():
        if not customer_config.get(name_field):
            continue
        v = event_rule_ids.get(id_field)
        updates[id_field] = v
        if v is None:
            missing.append(id_field)

    if not updates:
        log(f"customer_config for app {application_id} has no mapping names configured", "WARN")
        return

    session.query(CustomerConfig).filter(
        CustomerConfig.application_id == str(application_id)
    ).update(updates)

    if missing:
        log(f"customer_config updated for app {application_id} — missing: {', '.join(sorted(missing))}", "WARN")
    else:
        log(f"customer_config updated for app {application_id}")


# ---------------------------------------------------------------------------
# API helper
# ---------------------------------------------------------------------------

def call_api(*, method="GET", base_url, path, token, params=None, json_payload=None, data_payload=None, timeout=30, retries=3):
    base = base_url if base_url.endswith("/") else base_url + "/"
    url = urljoin(base, path.lstrip("/"))
    headers = {"Authorization": token}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method=method, url=url, params=params, json=json_payload, data=data_payload, headers=headers, timeout=timeout)
            if resp.ok:
                return resp.json()
            raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
        except Exception:
            if attempt == retries:
                raise
            log(f"Retry {attempt}/{retries} -> {path}", "WARN")
            time.sleep(2)


# ---------------------------------------------------------------------------
# Sync functions
# ---------------------------------------------------------------------------

def sync_tags(session, application_id: int, auth_token: str) -> int:
    from app.models import DimTags

    log(f"Syncing dim_tags for app {application_id}")
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/tags", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimTags).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    log(f"dim_tags ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_event_rules(session, application_id: int, auth_token: str) -> int:
    from app.models import DimEventRules

    log(f"Syncing dim_event_rules for app {application_id}")
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/eventrules", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimEventRules).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    log(f"dim_event_rules ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_reports(session, application_id: int, auth_token: str) -> int:
    from app.models import DimReports

    log(f"Syncing dim_reports for app {application_id}")
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/reports", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimReports).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    log(f"dim_reports ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_vehicles_and_drivers(session, application_id: int, auth_token: str) -> int:
    from app.models import DimVehicles, DimDrivers

    log(f"Syncing dim_vehicles + dim_drivers for app {application_id}")
    users = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/users", token=auth_token, timeout=60)

    vehicle_rows = []
    driver_rows = []

    for idx, user in enumerate(users, start=1):
        if idx % 50 == 0:
            log(f"Users processed for app {application_id}: {idx}/{len(users)}")

        track_point = user.get("trackPoint") or {}
        position    = track_point.get("position") or {}
        devices     = user.get("devices") or []
        device_name = devices[0].get("name") if devices else None
        imei        = devices[0].get("imei") if devices else None

        if imei:
            vehicle_rows.append({
                "id": int(user["id"]), "application_id": application_id,
                "name": user.get("name"), "username": user.get("username"),
                "imei": imei,
                "latitude": position.get("latitude"), "longitude": position.get("longitude"),
                "last_utc": track_point.get("utc"), "valid": track_point.get("valid"),
                "device_name": device_name,
            })

        if user.get("driverID"):
            driver_rows.append({
                "id": int(user["id"]), "application_id": application_id,
                "name": user.get("name"), "username": user.get("username"),
                "driver_id": user.get("driverID"), "device_name": device_name, "imei": imei,
                "latitude": position.get("latitude"), "longitude": position.get("longitude"),
                "utc": track_point.get("utc"), "validity": track_point.get("valid"),
            })

    if vehicle_rows:
        stmt = pg_insert(DimVehicles).values(vehicle_rows)
        session.execute(stmt.on_conflict_do_update(
            index_elements=["id", "application_id"],
            set_={c: getattr(stmt.excluded, c) for c in ("name", "username", "imei", "latitude", "longitude", "last_utc", "valid", "device_name")},
        ))

    if driver_rows:
        stmt = pg_insert(DimDrivers).values(driver_rows)
        session.execute(stmt.on_conflict_do_update(
            index_elements=["id", "application_id"],
            set_={c: getattr(stmt.excluded, c) for c in ("name", "username", "driver_id", "device_name", "imei", "latitude", "longitude", "utc", "validity")},
        ))

    log(f"dim_vehicles ok {len(vehicle_rows)} for app {application_id}")
    log(f"dim_drivers ok {len(driver_rows)} for app {application_id}")
    return len(vehicle_rows) + len(driver_rows)


def sync_vehicle_custom_fields(session, application_id: int, auth_token: str, on_progress=None) -> int:
    from app.models import DimVehicleCustomFields

    log(f"Syncing dim_vehicle_custom_fields for app {application_id}")
    users = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/users", token=auth_token, timeout=60)

    rows: list[dict] = []
    skipped = 0
    total_processed = 0
    BATCH = 500

    def _flush():
        if not rows:
            return
        stmt = pg_insert(DimVehicleCustomFields).values(rows)
        session.execute(stmt.on_conflict_do_update(
            constraint="uq_dim_vehicle_custom_fields",
            set_={"field_value": stmt.excluded.field_value},
        ))

    for idx, user in enumerate(users, start=1):
        user_id = user.get("id")
        if not user_id:
            continue
        if idx % 25 == 0:
            log(f"Custom fields progress for app {application_id}: {idx}/{len(users)}")
            if on_progress:
                on_progress(f"App {application_id} — Custom Fields {idx}/{len(users)}", idx, len(users))
        try:
            fields = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/users/{user_id}/customfields", token=auth_token, timeout=30)
        except Exception:
            skipped += 1
            continue
        for field in fields:
            rows.append({"application_id": application_id, "vehicle_id": int(user_id), "field_name": field.get("name"), "field_value": str(field.get("value"))})
            total_processed += 1
        if len(rows) >= BATCH:
            _flush()
            rows.clear()

    _flush()
    log(f"dim_vehicle_custom_fields ok {total_processed} for app {application_id} | skipped {skipped}")
    return total_processed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_STEPS = ["Tags", "Event Rules", "Reports", "Vehicles & Drivers", "Custom Fields"]
_N_STEPS = len(_STEPS)


def _run_sync(session, only_application_id: str | None = None, on_progress=None) -> int:
    log("Starting dimension sync")
    start   = time.time()
    total   = 0

    customers   = load_customer_configs(session, only_application_id)
    n_customers = len(customers)
    total_steps = n_customers * _N_STEPS
    done        = 0

    def _report(step_label: str, sub_status: str = ""):
        if not on_progress:
            return
        percent = int(done / total_steps * 100) if total_steps else 0
        status  = f"App {app_id} — {step_label}"
        if sub_status:
            status += f" ({sub_status})"
        on_progress(status, done, total_steps, percent)

    for cust_idx, customer in enumerate(customers):
        app_id     = customer["application_id"]
        auth_token = customer["token"]
        log(f"Starting customer dimension sync for app {app_id}")

        _report("Tags");                         total += sync_tags(session, app_id, auth_token);                                              session.commit(); done += 1
        _report("Event Rules");                  total += sync_event_rules(session, app_id, auth_token);                                       session.commit(); done += 1
        _report("Reports");                      total += sync_reports(session, app_id, auth_token);                                           session.commit(); done += 1
        _report("Vehicles & Drivers");           total += sync_vehicles_and_drivers(session, app_id, auth_token);                             session.commit(); done += 1
        _report("Custom Fields");                total += sync_vehicle_custom_fields(session, app_id, auth_token, on_progress=lambda s, c, t: _report("Custom Fields", s.split("—")[-1].strip() if "—" in s else s)); session.commit(); done += 1

        update_customer_config_from_dims(session, customer)
        session.commit()

    if on_progress:
        on_progress("Completed", total_steps, total_steps, 100)

    log(f"Completed in {round(time.time() - start, 2)}s — Total records: {total:,}")
    return total


def main(only_application_id: str | int | None = None, on_progress=None) -> int:
    from app.models import db

    selected = str(only_application_id) if only_application_id is not None else None

    try:
        from flask import has_app_context
        if has_app_context():
            return _run_sync(db.session, selected, on_progress)
        raise RuntimeError("no app context")
    except RuntimeError:
        if selected is None:
            import argparse as _ap
            selected = _ap.ArgumentParser().parse_known_args()[0].__dict__.get("application_id")
        from app import create_app
        app = create_app()
        with app.app_context():
            return _run_sync(db.session, selected, on_progress)


if __name__ == "__main__":
    import argparse as _ap
    _parser = _ap.ArgumentParser(description="Sync dimensions using customer_config")
    _parser.add_argument("--application-id", dest="application_id")
    _args = _parser.parse_args()
    main(_args.application_id)
