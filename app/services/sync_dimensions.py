#!/usr/bin/env python
"""Sync dimension tables from GpsGate API and refresh gpsgate_application IDs."""

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

from app.utils.logger import setup_logger

load_dotenv()

logger = setup_logger("SYNC_DIM")

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

def load_gpsgate_applications(session, only_application_id: int | None = None) -> list[dict]:
    from app.models import GpsGateApplication

    q = session.query(GpsGateApplication).order_by(GpsGateApplication.application_id)
    if only_application_id:
        q = q.filter(GpsGateApplication.application_id == int(only_application_id))

    rows = q.all()
    if not rows:
        if only_application_id:
            raise RuntimeError(f"No gpsgate_application row found for application_id={only_application_id}")
        raise RuntimeError("No gpsgate_application rows found")

    configs = []
    for row in rows:
        config = {
            "application_id": row.application_id,
            "token": normalize_token(row.token),
        }
        for field in NAME_LOOKUP_FIELDS:
            val = getattr(row, field, None)
            config[field] = (val or "").strip() or None
        configs.append(config)
    return configs


# Keep old function name as alias for backwards compatibility
load_customer_configs = load_gpsgate_applications


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
# gpsgate_application update
# ---------------------------------------------------------------------------

def update_gpsgate_application_from_dims(session, gpsgate_application: dict) -> None:
    from app.models import GpsGateApplication, DimEventRules

    application_id = gpsgate_application["application_id"]

    report_ids = lookup_report_ids(session, application_id, {
        gpsgate_application["trip_report_name"]:  "trip_report_id",
        gpsgate_application["event_report_name"]: "event_report_id",
    })
    event_rule_ids = lookup_named_ids(
        session,
        model=DimEventRules,
        application_id=application_id,
        names_to_columns={gpsgate_application[f]: id_f for f, id_f in EVENT_RULE_NAME_TO_ID_FIELD.items()},
    )

    updates: dict = {}
    missing: list[str] = []

    if gpsgate_application.get("tag_name"):
        tag_id = lookup_tag_id(session, application_id, gpsgate_application["tag_name"])
        updates["tag_id"] = tag_id
        if tag_id is None:
            missing.append("tag_id")

    if gpsgate_application.get("trip_report_name"):
        v = report_ids.get("trip_report_id")
        updates["trip_report_id"] = v
        if v is None:
            missing.append("trip_report_id")

    if gpsgate_application.get("event_report_name"):
        v = report_ids.get("event_report_id")
        updates["event_report_id"] = v
        if v is None:
            missing.append("event_report_id")

    for name_field, id_field in EVENT_RULE_NAME_TO_ID_FIELD.items():
        if not gpsgate_application.get(name_field):
            continue
        v = event_rule_ids.get(id_field)
        updates[id_field] = v
        if v is None:
            missing.append(id_field)

    if not updates:
        logger.warning("update_app_from_dims | no mapping names configured | app=%s", application_id)
        return

    session.query(GpsGateApplication).filter(
        GpsGateApplication.application_id == application_id
    ).update(updates)

    if missing:
        logger.warning("update_app_from_dims | DONE | app=%s | missing_ids=%s",
                       application_id, ", ".join(sorted(missing)))
    else:
        logger.info("update_app_from_dims | DONE | app=%s | all IDs resolved", application_id)


# Keep old function name as alias for backwards compatibility
update_customer_config_from_dims = update_gpsgate_application_from_dims


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
            logger.warning("call_api | RETRY %d/%d path=%s", attempt, retries, path)
            time.sleep(2)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fetch_all_users(application_id: int, auth_token: str, page_size: int = 1000) -> list:
    all_users: list = []
    from_index = 0
    batch = 1
    while True:
        data = call_api(
            base_url=BASE_URL,
            path=f"comGpsGate/api/v.1/applications/{application_id}/users?take={page_size}&FromIndex={from_index}",
            token=auth_token,
            timeout=60,
        )
        if not data:
            break
        all_users.extend(data)
        logger.debug("_fetch_all_users | app=%s batch=%d fetched=%d total=%d", application_id, batch, len(data), len(all_users))
        if len(data) < page_size:
            break
        last_id = data[-1].get("id")
        if not last_id or last_id == from_index:
            break
        from_index = last_id
        batch += 1
    logger.info("_fetch_all_users | DONE | app=%s total=%d", application_id, len(all_users))
    return all_users


# ---------------------------------------------------------------------------
# Sync functions
# ---------------------------------------------------------------------------

def sync_tags(session, application_id: int, auth_token: str) -> int:
    from app.models import DimTags

    logger.info("sync_tags | START | app=%s", application_id)
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/tags", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimTags).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    logger.info("sync_tags | DONE | app=%s rows=%d", application_id, len(rows))
    return len(rows)


def sync_event_rules(session, application_id: int, auth_token: str) -> int:
    from app.models import DimEventRules

    logger.info("sync_event_rules | START | app=%s", application_id)
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/eventrules", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimEventRules).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    logger.info("sync_event_rules | DONE | app=%s rows=%d", application_id, len(rows))
    return len(rows)


def sync_reports(session, application_id: int, auth_token: str) -> int:
    from app.models import DimReports

    logger.info("sync_reports | START | app=%s", application_id)
    data = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/reports", token=auth_token) or []
    rows = [{"id": int(r["id"]), "application_id": application_id, "name": r["name"]} for r in data]
    if rows:
        stmt = pg_insert(DimReports).values(rows)
        session.execute(stmt.on_conflict_do_update(index_elements=["id", "application_id"], set_={"name": stmt.excluded.name}))
    logger.info("sync_reports | DONE | app=%s rows=%d", application_id, len(rows))
    return len(rows)


def sync_vehicles_and_drivers(session, application_id: int, auth_token: str) -> int:
    from app.models import DimVehicles, DimDrivers

    logger.info("sync_vehicles_and_drivers | START | app=%s", application_id)

    try:
        roles = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/roles", token=auth_token) or []
        unit_role = next((r for r in roles if r.get("name") == "_Unit"), None)
        unit_user_ids = set(unit_role.get("usersIds") or []) if unit_role else set()
        logger.info("sync_vehicles_and_drivers | _Unit count=%d | app=%s", len(unit_user_ids), application_id)
    except Exception:
        logger.warning("sync_vehicles_and_drivers | roles fetch failed, vehicle_rows will be empty | app=%s", application_id)
        unit_user_ids = set()

    users = _fetch_all_users(application_id, auth_token)

    vehicle_rows = []
    driver_rows = []

    for idx, user in enumerate(users, start=1):
        if idx % 50 == 0:
            logger.debug("sync_vehicles_and_drivers | PROGRESS | app=%s users=%d/%d",
                         application_id, idx, len(users))

        track_point = user.get("trackPoint") or {}
        position    = track_point.get("position") or {}
        devices     = user.get("devices") or []
        device_name = devices[0].get("name") if devices else None
        imei        = next((d.get("imei") for d in devices if d.get("imei")), None)

        if int(user["id"]) in unit_user_ids:
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

    logger.info("sync_vehicles_and_drivers | DONE | app=%s vehicles=%d drivers=%d",
                application_id, len(vehicle_rows), len(driver_rows))
    return len(vehicle_rows) + len(driver_rows)


def sync_vehicle_custom_fields(session, application_id: int, auth_token: str, on_progress=None) -> int:
    from app.models import DimVehicleCustomFields, DimVehicles

    logger.info("sync_vehicle_custom_fields | START | app=%s", application_id)

    vehicle_ids = [
        row[0] for row in
        session.query(DimVehicles.id).filter(DimVehicles.application_id == application_id).all()
    ]
    logger.info("sync_vehicle_custom_fields | vehicles from DB=%d | app=%s", len(vehicle_ids), application_id)

    rows: list[dict] = []
    skipped = 0
    total_processed = 0
    BATCH = 500
    total = len(vehicle_ids)

    def _flush():
        if not rows:
            return
        stmt = pg_insert(DimVehicleCustomFields).values(rows)
        session.execute(stmt.on_conflict_do_update(
            constraint="uq_dim_vehicle_custom_fields",
            set_={"field_value": stmt.excluded.field_value},
        ))

    for idx, vehicle_id in enumerate(vehicle_ids, start=1):
        if idx % 25 == 0:
            logger.debug("sync_vehicle_custom_fields | PROGRESS | app=%s vehicles=%d/%d",
                         application_id, idx, total)
            if on_progress:
                on_progress(f"App {application_id} — Custom Fields {idx}/{total}", idx, total)
        try:
            fields = call_api(base_url=BASE_URL, path=f"comGpsGate/api/v.1/applications/{application_id}/users/{vehicle_id}/customfields", token=auth_token, timeout=30)
        except Exception:
            skipped += 1
            logger.warning("sync_vehicle_custom_fields | VEHICLE_SKIP | app=%s vehicle_id=%s", application_id, vehicle_id)
            continue
        for field in (fields or []):
            rows.append({"application_id": application_id, "vehicle_id": int(vehicle_id), "field_name": field.get("name"), "field_value": str(field.get("value"))})
            total_processed += 1
        if len(rows) >= BATCH:
            _flush()
            rows.clear()

    _flush()
    logger.info("sync_vehicle_custom_fields | DONE | app=%s rows=%d skipped=%d",
                application_id, total_processed, skipped)
    return total_processed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

_STEPS = ["Tags", "Event Rules", "Reports", "Vehicles & Drivers", "Custom Fields"]
_N_STEPS = len(_STEPS)


def _run_sync(session, only_application_id: str | None = None, on_progress=None) -> int:
    t0 = time.time()
    logger.info("_run_sync | START | app=%s", only_application_id or "all")
    total = 0

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
        logger.info("_run_sync | CUSTOMER_START | app=%s (%d/%d)", app_id, cust_idx + 1, n_customers)

        _report("Tags");                         total += sync_tags(session, app_id, auth_token);                                              session.commit(); done += 1
        _report("Event Rules");                  total += sync_event_rules(session, app_id, auth_token);                                       session.commit(); done += 1
        _report("Reports");                      total += sync_reports(session, app_id, auth_token);                                           session.commit(); done += 1
        _report("Vehicles & Drivers");           total += sync_vehicles_and_drivers(session, app_id, auth_token);                             session.commit(); done += 1
        _report("Custom Fields");                total += sync_vehicle_custom_fields(session, app_id, auth_token, on_progress=lambda s, c, t: _report("Custom Fields", s.split("—")[-1].strip() if "—" in s else s)); session.commit(); done += 1

        update_customer_config_from_dims(session, customer)
        session.commit()

    if on_progress:
        on_progress("Completed", total_steps, total_steps, 100)

    elapsed = round(time.time() - t0, 2)
    logger.info("_run_sync | DONE | app=%s total_records=%d elapsed=%.2fs",
                only_application_id or "all", total, elapsed)
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
