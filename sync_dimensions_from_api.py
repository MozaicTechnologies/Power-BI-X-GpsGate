#!/usr/bin/env python
"""Sync dimension tables from GpsGate API and refresh customer_config IDs."""

from __future__ import annotations

import argparse
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

import psycopg
import requests
from dotenv import load_dotenv


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = os.getenv("BASE_URL", "https://omantracking2.com")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

if DATABASE_URL.startswith("postgresql+psycopg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


TAG_NAME = "Show on Map"
NORMALIZED_NAME_SQL = "regexp_replace(lower(trim(name)), '^[^a-z0-9]+|[^a-z0-9]+$', '', 'g')"

REPORT_MAP = {
    "trip_report_id": ["Trip and Idle (Tag)-BI Format"],
    "event_report_id": ["Event Rule detailed (Tag)", "Event Rule detailed (User)"],
}

EVENT_RULE_MAP = {
    "Over Speeding +150km/h": "speed_event_id",
    "30 min idle": "idle_event_id",
    "After Working Hours Usage": "awh_event_id",
    "Harsh Acceleration": "ha_event_id",
    "Harsh Braking": "hb_event_id",
    "Harsh Cornering": "hc_event_id",
    "Weekend Usage": "wu_event_id",
    "Working Hours Usage": "wh_event_id",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync dimensions using customer_config")
    parser.add_argument(
        "--application-id",
        help="Optional application_id to sync from customer_config",
    )
    return parser.parse_args()


def load_customer_configs(cur, only_application_id: str | None = None) -> list[dict]:
    if only_application_id:
        cur.execute(
            """
            select application_id::text, token
            from customer_config
            where application_id = %s
            order by application_id
            """,
            (str(only_application_id),),
        )
    else:
        cur.execute(
            """
            select application_id::text, token
            from customer_config
            order by application_id
            """
        )

    rows = cur.fetchall()
    configs = [
        {
            "application_id": int(application_id),
            "token": normalize_token(token),
        }
        for application_id, token in rows
    ]

    if configs:
        return configs

    if only_application_id:
        raise RuntimeError(f"No customer_config row found for application_id={only_application_id}")

    raise RuntimeError("No customer_config rows found")


def lookup_tag_id(cur, application_id: int) -> str | None:
    cur.execute(
        f"""
        select id::text
        from dim_tags
        where application_id = %s
          and {NORMALIZED_NAME_SQL} = %s
        order by id
        limit 1
        """,
        (application_id, normalize_lookup_name(TAG_NAME)),
    )
    row = cur.fetchone()
    return row[0] if row else None


def lookup_named_ids(
    cur,
    *,
    table_name: str,
    application_id: int,
    names_to_columns: dict[str, str],
) -> dict[str, str]:
    normalized_names = {
        normalize_lookup_name(name): column_name
        for name, column_name in names_to_columns.items()
    }
    cur.execute(
        f"""
        select id::text, {NORMALIZED_NAME_SQL} as normalized_name
        from {table_name}
        where application_id = %s
          and {NORMALIZED_NAME_SQL} = any(%s)
        order by normalized_name, id
        """,
        (application_id, list(normalized_names.keys())),
    )

    values: dict[str, str] = {}
    for record_id, normalized_name in cur.fetchall():
        column_name = normalized_names[normalized_name]
        values.setdefault(column_name, record_id)
    return values


def lookup_report_ids(cur, application_id: int) -> dict[str, str]:
    normalized_candidates = {
        column_name: [normalize_lookup_name(name) for name in candidate_names]
        for column_name, candidate_names in REPORT_MAP.items()
    }
    all_candidates = [
        candidate
        for candidate_names in normalized_candidates.values()
        for candidate in candidate_names
    ]

    cur.execute(
        f"""
        select id::text, {NORMALIZED_NAME_SQL} as normalized_name
        from dim_reports
        where application_id = %s
          and {NORMALIZED_NAME_SQL} = any(%s)
        order by normalized_name, id
        """,
        (application_id, all_candidates),
    )

    available_by_name: dict[str, str] = {}
    for record_id, normalized_name in cur.fetchall():
        available_by_name.setdefault(normalized_name, record_id)

    result: dict[str, str] = {}
    for column_name, candidate_names in normalized_candidates.items():
        for candidate_name in candidate_names:
            if candidate_name in available_by_name:
                result[column_name] = available_by_name[candidate_name]
                break
    return result


def update_customer_config_from_dims(cur, application_id: int) -> None:
    customer_config_application_id = str(application_id)
    report_ids = lookup_report_ids(cur, application_id)
    event_rule_ids = lookup_named_ids(
        cur,
        table_name="dim_event_rules",
        application_id=application_id,
        names_to_columns=EVENT_RULE_MAP,
    )

    payload = {
        "application_id": customer_config_application_id,
        "tag_id": lookup_tag_id(cur, application_id),
        "trip_report_id": report_ids.get("trip_report_id"),
        "event_report_id": report_ids.get("event_report_id"),
        "speed_event_id": event_rule_ids.get("speed_event_id"),
        "idle_event_id": event_rule_ids.get("idle_event_id"),
        "awh_event_id": event_rule_ids.get("awh_event_id"),
        "ha_event_id": event_rule_ids.get("ha_event_id"),
        "hb_event_id": event_rule_ids.get("hb_event_id"),
        "hc_event_id": event_rule_ids.get("hc_event_id"),
        "wu_event_id": event_rule_ids.get("wu_event_id"),
        "wh_event_id": event_rule_ids.get("wh_event_id"),
    }

    cur.execute(
        """
        update customer_config
        set tag_id = %(tag_id)s,
            trip_report_id = %(trip_report_id)s,
            event_report_id = %(event_report_id)s,
            speed_event_id = %(speed_event_id)s,
            idle_event_id = %(idle_event_id)s,
            awh_event_id = %(awh_event_id)s,
            ha_event_id = %(ha_event_id)s,
            hb_event_id = %(hb_event_id)s,
            hc_event_id = %(hc_event_id)s,
            wu_event_id = %(wu_event_id)s,
            wh_event_id = %(wh_event_id)s
        where application_id = %(application_id)s
        """,
        payload,
    )

    missing = [
        column_name
        for column_name, value in payload.items()
        if column_name != "application_id" and value is None
    ]
    if missing:
        log(
            f"customer_config updated for app {application_id} with missing mappings: {', '.join(sorted(missing))}",
            "WARN",
        )
    else:
        log(f"customer_config updated for app {application_id}")


def call_api(
    *,
    method: str = "GET",
    base_url: str,
    path: str,
    token: str,
    params: dict | None = None,
    json_payload: dict | None = None,
    data_payload: dict | None = None,
    timeout: int = 30,
    retries: int = 3,
):
    base = base_url if base_url.endswith("/") else base_url + "/"
    url = urljoin(base, path.lstrip("/"))
    headers = {"Authorization": token}

    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(
                method=method,
                url=url,
                params=params,
                json=json_payload,
                data=data_payload,
                headers=headers,
                timeout=timeout,
            )
            if resp.ok:
                return resp.json()
            raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
        except Exception:
            if attempt == retries:
                raise
            log(f"Retry {attempt}/{retries} -> {path}", "WARN")
            time.sleep(2)


def sync_tags(cur, application_id: int, auth_token: str) -> int:
    log(f"Syncing dim_tags for app {application_id}")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{application_id}/tags",
        token=auth_token,
    )

    rows = [(int(r["id"]), application_id, r["name"]) for r in data]
    cur.executemany(
        """
        insert into dim_tags (id, application_id, name)
        values (%s,%s,%s)
        on conflict (application_id, id) do update
        set name = excluded.name
        """,
        rows,
    )
    log(f"dim_tags ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_event_rules(cur, application_id: int, auth_token: str) -> int:
    log(f"Syncing dim_event_rules for app {application_id}")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{application_id}/eventrules",
        token=auth_token,
    )

    rows = [(int(r["id"]), application_id, r["name"]) for r in data]
    cur.executemany(
        """
        insert into dim_event_rules (id, application_id, name)
        values (%s,%s,%s)
        on conflict (application_id, id) do update
        set name = excluded.name
        """,
        rows,
    )
    log(f"dim_event_rules ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_reports(cur, application_id: int, auth_token: str) -> int:
    log(f"Syncing dim_reports for app {application_id}")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{application_id}/reports",
        token=auth_token,
    )

    rows = [(int(r["id"]), application_id, r["name"]) for r in data]
    cur.executemany(
        """
        insert into dim_reports (id, application_id, name)
        values (%s,%s,%s)
        on conflict (application_id, id) do update
        set name = excluded.name
        """,
        rows,
    )
    log(f"dim_reports ok {len(rows)} for app {application_id}")
    return len(rows)


def sync_vehicles_and_drivers(cur, application_id: int, auth_token: str) -> int:
    log(f"Syncing dim_vehicles + dim_drivers for app {application_id}")
    users = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{application_id}/users",
        token=auth_token,
        timeout=60,
    )

    vehicle_rows = []
    driver_rows = []

    for idx, user in enumerate(users, start=1):
        if idx % 50 == 0:
            log(f"Users processed for app {application_id}: {idx}/{len(users)}")

        track_point = user.get("trackPoint") or {}
        position = track_point.get("position") or {}
        devices = user.get("devices") or []

        device_name = None
        imei = None
        if devices:
            device_name = devices[0].get("name")
            imei = devices[0].get("imei")

        if imei:
            vehicle_rows.append(
                (
                    int(user["id"]),
                    application_id,
                    user.get("name"),
                    user.get("username"),
                    imei,
                    position.get("latitude"),
                    position.get("longitude"),
                    track_point.get("utc"),
                    track_point.get("valid"),
                    device_name,
                )
            )

        if user.get("driverID"):
            driver_rows.append(
                (
                    int(user["id"]),
                    application_id,
                    user.get("name"),
                    user.get("username"),
                    user.get("driverID"),
                    device_name,
                    imei,
                    position.get("latitude"),
                    position.get("longitude"),
                    track_point.get("utc"),
                    track_point.get("valid"),
                )
            )

    if vehicle_rows:
        cur.executemany(
            """
            insert into dim_vehicles (
                id, application_id, name, username, imei,
                latitude, longitude, last_utc, valid, device_name
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (application_id, id) do update set
                imei = excluded.imei,
                name = excluded.name,
                username = excluded.username,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                last_utc = excluded.last_utc,
                valid = excluded.valid,
                device_name = excluded.device_name
            """,
            vehicle_rows,
        )

    if driver_rows:
        cur.executemany(
            """
            insert into dim_drivers (
                id, application_id, name, username, driver_id,
                device_name, imei, latitude, longitude, utc, validity
            )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            on conflict (application_id, id) do update set
                name = excluded.name,
                username = excluded.username,
                driver_id = excluded.driver_id,
                device_name = excluded.device_name,
                imei = excluded.imei,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                utc = excluded.utc,
                validity = excluded.validity
            """,
            driver_rows,
        )

    log(f"dim_vehicles ok {len(vehicle_rows)} for app {application_id}")
    log(f"dim_drivers ok {len(driver_rows)} for app {application_id}")
    return len(vehicle_rows) + len(driver_rows)


def sync_vehicle_custom_fields(cur, application_id: int, auth_token: str) -> int:
    log(f"Syncing dim_vehicle_custom_fields for app {application_id}")
    users = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{application_id}/users",
        token=auth_token,
        timeout=60,
    )

    rows = []
    skipped = 0
    total_processed = 0

    for idx, user in enumerate(users, start=1):
        user_id = user.get("id")
        if not user_id:
            continue

        if idx % 25 == 0:
            log(f"Custom fields progress for app {application_id}: {idx}/{len(users)}")

        try:
            fields = call_api(
                base_url=BASE_URL,
                path=f"comGpsGate/api/v.1/applications/{application_id}/users/{user_id}/customfields",
                token=auth_token,
                timeout=30,
            )
        except Exception:
            skipped += 1
            continue

        for field in fields:
            rows.append(
                (
                    application_id,
                    int(user_id),
                    field.get("name"),
                    str(field.get("value")),
                )
            )
            total_processed += 1

        if len(rows) >= 500:
            cur.executemany(
                """
                insert into dim_vehicle_custom_fields
                    (application_id, vehicle_id, field_name, field_value)
                values (%s,%s,%s,%s)
                on conflict (application_id, vehicle_id, field_name)
                do update set field_value = excluded.field_value
                """,
                rows,
            )
            rows.clear()

    if rows:
        cur.executemany(
            """
            insert into dim_vehicle_custom_fields
                (application_id, vehicle_id, field_name, field_value)
            values (%s,%s,%s,%s)
            on conflict (application_id, vehicle_id, field_name)
            do update set field_value = excluded.field_value
            """,
            rows,
        )

    log(f"dim_vehicle_custom_fields ok {total_processed} for app {application_id} | skipped {skipped}")
    return total_processed


def main(only_application_id: str | int | None = None) -> int:
    log("Starting dimension sync")
    start = time.time()
    total_records = 0
    args = None if only_application_id is not None else parse_args()
    selected_application_id = (
        str(only_application_id)
        if only_application_id is not None
        else args.application_id
    )

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            customer_configs = load_customer_configs(cur, selected_application_id)

            for customer in customer_configs:
                application_id = customer["application_id"]
                auth_token = customer["token"]

                log(f"Starting customer dimension sync for app {application_id}")

                total_records += sync_tags(cur, application_id, auth_token)
                conn.commit()

                total_records += sync_event_rules(cur, application_id, auth_token)
                conn.commit()

                total_records += sync_reports(cur, application_id, auth_token)
                conn.commit()

                total_records += sync_vehicles_and_drivers(cur, application_id, auth_token)
                conn.commit()

                total_records += sync_vehicle_custom_fields(cur, application_id, auth_token)
                conn.commit()

                update_customer_config_from_dims(cur, application_id)
                conn.commit()

    log(f"Completed in {round(time.time() - start, 2)}s - Total records: {total_records:,}")
    return total_records


if __name__ == "__main__":
    main()
