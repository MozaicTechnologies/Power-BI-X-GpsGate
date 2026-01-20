# #!/usr/bin/env python
# """
# Sync dimension tables from GpsGate API
# (Replaces Power BI M Queries)

# Targets:
# - dim_tags
# - dim_event_rules
# - dim_reports
# - dim_vehicles
# - dim_drivers
# - dim_vehicle_custom_fields
# """

# import os
# import requests
# import psycopg
# from urllib.parse import urljoin
# from dotenv import load_dotenv

# # ------------------------------------------------------------------
# # ENV / CONFIG
# # ------------------------------------------------------------------
# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")
# if not DATABASE_URL:
#     raise RuntimeError("❌ DATABASE_URL not set in environment")

# BASE_URL = "https://omantracking2.com"
# APP_ID = 6
# AUTH_TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="

# # ------------------------------------------------------------------
# # API HELPER (LOCAL VERSION OF call_api)
# # ------------------------------------------------------------------
# def call_api(
#     *,
#     method="GET",
#     base_url: str,
#     path: str,
#     token: str,
#     params: dict | None = None,
#     json_payload: dict | None = None,
#     data_payload: dict | None = None,
#     timeout: int = 30,
# ):
#     if not base_url or not path:
#         raise ValueError("base_url and path are required")

#     headers = {"Authorization": token}
#     url = urljoin(base_url if base_url.endswith("/") else base_url + "/", path.lstrip("/"))

#     resp = requests.request(
#         method=method,
#         url=url,
#         params=params,
#         json=json_payload,
#         data=data_payload,
#         headers=headers,
#         timeout=timeout,
#     )

#     resp.raise_for_status()

#     if "application/json" in resp.headers.get("Content-Type", "").lower():
#         return resp.json()
#     return resp.text


# def api_get(path: str):
#     """
#     Wrapper matching Power BI M-query behavior:
#     returns Response[data]
#     """
#     response = call_api(
#         method="GET",
#         base_url=BASE_URL,
#         path=path,
#         token=AUTH_TOKEN,
#     )

#     if isinstance(response, dict) and "data" in response:
#         return response["data"]

#     # some endpoints return list directly
#     return response


# # ------------------------------------------------------------------
# # SYNC FUNCTIONS
# # ------------------------------------------------------------------
# def sync_tags(cur):
#     rows = api_get(f"comGpsGate/api/v.1/applications/{APP_ID}/tags")
#     for r in rows:
#         cur.execute(
#             """
#             INSERT INTO dim_tags (id, application_id, name)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (id) DO UPDATE
#             SET name = EXCLUDED.name
#             """,
#             (int(r["id"]), APP_ID, r["name"]),
#         )


# def sync_event_rules(cur):
#     rows = api_get(f"comGpsGate/api/v.1/applications/{APP_ID}/eventrules")
#     for r in rows:
#         cur.execute(
#             """
#             INSERT INTO dim_event_rules (id, application_id, name)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (id) DO UPDATE
#             SET name = EXCLUDED.name
#             """,
#             (int(r["id"]), APP_ID, r["name"]),
#         )


# def sync_reports(cur):
#     rows = api_get(f"comGpsGate/api/v.1/applications/{APP_ID}/reports")
#     for r in rows:
#         cur.execute(
#             """
#             INSERT INTO dim_reports (id, application_id, name)
#             VALUES (%s, %s, %s)
#             ON CONFLICT (id) DO UPDATE
#             SET name = EXCLUDED.name
#             """,
#             (int(r["id"]), APP_ID, r["name"]),
#         )


# def sync_vehicles(cur):
#     users = api_get(f"comGpsGate/api/v.1/applications/{APP_ID}/users")

#     for u in users:
#         tp = u.get("trackPoint") or {}
#         pos = tp.get("position") or {}

#         for d in u.get("devices", []):
#             cur.execute(
#                 """
#                 INSERT INTO dim_vehicles (
#                     id, application_id, name, username, imei,
#                     latitude, longitude, last_utc, valid, device_name
#                 )
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 ON CONFLICT (id) DO UPDATE SET
#                     imei = EXCLUDED.imei,
#                     latitude = EXCLUDED.latitude,
#                     longitude = EXCLUDED.longitude,
#                     last_utc = EXCLUDED.last_utc,
#                     valid = EXCLUDED.valid,
#                     device_name = EXCLUDED.device_name
#                 """,
#                 (
#                     int(u["id"]),
#                     APP_ID,
#                     u.get("name"),
#                     u.get("username"),
#                     d.get("imei"),
#                     pos.get("latitude"),
#                     pos.get("longitude"),
#                     tp.get("utc"),
#                     tp.get("valid"),
#                     d.get("name"),
#                 ),
#             )


# def sync_drivers(cur):
#     users = api_get(f"comGpsGate/api/v.1/applications/{APP_ID}/users")

#     for u in users:
#         if u.get("driverID"):
#             cur.execute(
#                 """
#                 INSERT INTO dim_drivers (id, application_id, name, username, driver_id)
#                 VALUES (%s,%s,%s,%s,%s)
#                 ON CONFLICT (id) DO UPDATE SET
#                     name = EXCLUDED.name,
#                     driver_id = EXCLUDED.driver_id
#                 """,
#                 (
#                     int(u["id"]),
#                     APP_ID,
#                     u.get("name"),
#                     u.get("username"),
#                     u.get("driverID"),
#                 ),
#             )


# def sync_vehicle_custom_fields(cur):
#     vehicles = call_api(
#         method="GET",
#         base_url=BASE_URL,
#         path=f"comGpsGate/api/v.1/applications/{APP_ID}/users",
#         token=AUTH_TOKEN
#     )

#     for v in vehicles:
#         vid = v.get("id")
#         if not vid:
#             continue

#         try:
#             fields = call_api(
#                 method="GET",
#                 base_url=BASE_URL,
#                 path=f"comGpsGate/api/v.1/applications/{APP_ID}/users/{vid}/customfields",
#                 token=AUTH_TOKEN,
#                 timeout=60
#             )
#         except Exception as e:
#             print(f"⚠️ Skipping custom fields for vehicle {vid}: {e}")
#             continue

#         if not isinstance(fields, list):
#             continue

#         for f in fields:
#             cur.execute(
#                 """
#                 INSERT INTO dim_vehicle_custom_fields
#                     (vehicle_id, field_name, field_value)
#                 VALUES (%s, %s, %s)
#                 ON CONFLICT (vehicle_id, field_name)
#                 DO UPDATE SET field_value = EXCLUDED.field_value
#                 """,
#                 (
#                     int(vid),
#                     f.get("name"),
#                     str(f.get("value")) if f.get("value") is not None else None
#                 )
#             )



# # ------------------------------------------------------------------
# # MAIN
# # ------------------------------------------------------------------
# def main():
#     print("🚀 Starting dimension sync…")

#     db_url = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")

#     with psycopg.connect(db_url) as conn:
#         with conn.cursor() as cur:
#             sync_tags(cur)
#             sync_event_rules(cur)
#             sync_reports(cur)
#             sync_vehicles(cur)
#             sync_drivers(cur)
#             sync_vehicle_custom_fields(cur)

#         conn.commit()

#     print("✅ Dimension sync completed successfully")


# if __name__ == "__main__":
#     main()


#!/usr/bin/env python
"""
Sync dimension tables from GpsGate API → PostgreSQL

Replaces Power BI M Queries:
- Tags
- Event Rules
- Reports
- Vehicles
- Drivers (ENRICHED)
- Vehicle Custom Fields (fnUserCustomFields_Direct)

Features:
- Fast batch inserts
- Progress logs
- Retry-safe API calls
- Commits after each sync
- Uses DATABASE_URL from env
"""

import os
import time
import requests
import psycopg
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv

# ------------------------------------------------------------------
# ENV
# ------------------------------------------------------------------
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
BASE_URL = "https://omantracking2.com"
APP_ID = 6
AUTH_TOKEN = "v2:MDAwMDAyODkwMDozOWM1MzQ1YWU2N2I4YWQ3MThhZA=="

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")

# psycopg3 requires standard postgres URL
if DATABASE_URL.startswith("postgresql+psycopg://"):
    DATABASE_URL = DATABASE_URL.replace(
        "postgresql+psycopg://", "postgresql://"
    )

# ------------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------------
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)

# ------------------------------------------------------------------
# API CALL (DIRECT)
# ------------------------------------------------------------------
def call_api(
    method="GET",
    base_url=None,
    path=None,
    params=None,
    json=None,
    data=None,
    token=None,
    timeout=30,
    retries=3,
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
                json=json,
                data=data,
                headers=headers,
                timeout=timeout,
            )
            if resp.ok:
                return resp.json()
            raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
        except Exception:
            if attempt == retries:
                raise
            log(f"Retry {attempt}/{retries} → {path}", "WARN")
            time.sleep(2)

# ------------------------------------------------------------------
# SYNC FUNCTIONS
# ------------------------------------------------------------------
def sync_tags(cur):
    log("Syncing dim_tags")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{APP_ID}/tags",
        token=AUTH_TOKEN,
    )

    rows = [(int(r["id"]), APP_ID, r["name"]) for r in data]

    cur.executemany(
        """
        INSERT INTO dim_tags (id, application_id, name)
        VALUES (%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
        """,
        rows,
    )
    log(f"dim_tags ✓ {len(rows)}")


def sync_event_rules(cur):
    log("Syncing dim_event_rules")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{APP_ID}/eventrules",
        token=AUTH_TOKEN,
    )

    rows = [(int(r["id"]), APP_ID, r["name"]) for r in data]

    cur.executemany(
        """
        INSERT INTO dim_event_rules (id, application_id, name)
        VALUES (%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
        """,
        rows,
    )
    log(f"dim_event_rules ✓ {len(rows)}")


def sync_reports(cur):
    log("Syncing dim_reports")
    data = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{APP_ID}/reports",
        token=AUTH_TOKEN,
    )

    rows = [(int(r["id"]), APP_ID, r["name"]) for r in data]

    cur.executemany(
        """
        INSERT INTO dim_reports (id, application_id, name)
        VALUES (%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name
        """,
        rows,
    )
    log(f"dim_reports ✓ {len(rows)}")


def sync_vehicles_and_drivers(cur):
    log("Syncing dim_vehicles + dim_drivers (ENRICHED)")

    users = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{APP_ID}/users",
        token=AUTH_TOKEN,
        timeout=60,
    )

    vehicle_rows = []
    driver_rows = []

    for idx, u in enumerate(users, start=1):
        if idx % 50 == 0:
            log(f"Users processed: {idx}/{len(users)}")

        tp = u.get("trackPoint") or {}
        pos = tp.get("position") or {}
        devices = u.get("devices") or []

        device_name = None
        imei = None

        if devices:
            device_name = devices[0].get("name")
            imei = devices[0].get("imei")

        # Vehicles
        if imei:
            vehicle_rows.append(
                (
                    int(u["id"]),
                    APP_ID,
                    u.get("name"),
                    u.get("username"),
                    imei,
                    pos.get("latitude"),
                    pos.get("longitude"),
                    tp.get("utc"),
                    tp.get("valid"),
                    device_name,
                )
            )

        # Drivers (flattened like Power BI)
        if u.get("driverID"):
            driver_rows.append(
                (
                    int(u["id"]),
                    APP_ID,
                    u.get("name"),
                    u.get("username"),
                    u.get("driverID"),
                    device_name,
                    imei,
                    pos.get("latitude"),
                    pos.get("longitude"),
                    tp.get("utc"),
                    tp.get("valid"),
                )
            )

    if vehicle_rows:
        cur.executemany(
            """
            INSERT INTO dim_vehicles (
                id, application_id, name, username, imei,
                latitude, longitude, last_utc, valid, device_name
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                imei = EXCLUDED.imei,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                last_utc = EXCLUDED.last_utc,
                valid = EXCLUDED.valid,
                device_name = EXCLUDED.device_name
            """,
            vehicle_rows,
        )

    if driver_rows:
        cur.executemany(
            """
            INSERT INTO dim_drivers (
                id, application_id, name, username, driver_id,
                device_name, imei,
                latitude, longitude, utc, validity
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                username = EXCLUDED.username,
                driver_id = EXCLUDED.driver_id,
                device_name = EXCLUDED.device_name,
                imei = EXCLUDED.imei,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                utc = EXCLUDED.utc,
                validity = EXCLUDED.validity
            """,
            driver_rows,
        )

    log(f"dim_vehicles ✓ {len(vehicle_rows)}")
    log(f"dim_drivers ✓ {len(driver_rows)}")


def sync_vehicle_custom_fields(cur):
    log("Syncing dim_vehicle_custom_fields")

    users = call_api(
        base_url=BASE_URL,
        path=f"comGpsGate/api/v.1/applications/{APP_ID}/users",
        token=AUTH_TOKEN,
        timeout=60,
    )

    rows = []
    skipped = 0

    for idx, u in enumerate(users, start=1):
        vid = u.get("id")
        if not vid:
            continue

        if idx % 25 == 0:
            log(f"Custom fields progress: {idx}/{len(users)}")

        try:
            fields = call_api(
                base_url=BASE_URL,
                path=f"comGpsGate/api/v.1/applications/{APP_ID}/users/{vid}/customfields",
                token=AUTH_TOKEN,
                timeout=30,
            )
        except Exception:
            skipped += 1
            continue

        for f in fields:
            rows.append((int(vid), f.get("name"), str(f.get("value"))))

        if len(rows) >= 500:
            cur.executemany(
                """
                INSERT INTO dim_vehicle_custom_fields
                    (vehicle_id, field_name, field_value)
                VALUES (%s,%s,%s)
                ON CONFLICT (vehicle_id, field_name)
                DO UPDATE SET field_value = EXCLUDED.field_value
                """,
                rows,
            )
            rows.clear()

    if rows:
        cur.executemany(
            """
            INSERT INTO dim_vehicle_custom_fields
                (vehicle_id, field_name, field_value)
            VALUES (%s,%s,%s)
            ON CONFLICT (vehicle_id, field_name)
            DO UPDATE SET field_value = EXCLUDED.field_value
            """,
            rows,
        )

    log(f"dim_vehicle_custom_fields ✓ | skipped {skipped}")

# ------------------------------------------------------------------
# MAIN
# ------------------------------------------------------------------
def main():
    log("🚀 Starting dimension sync")
    start = time.time()

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            sync_tags(cur)
            conn.commit()

            sync_event_rules(cur)
            conn.commit()

            sync_reports(cur)
            conn.commit()

            sync_vehicles_and_drivers(cur)
            conn.commit()

            sync_vehicle_custom_fields(cur)
            conn.commit()

    log(f"✅ Completed in {round(time.time() - start, 2)}s")

if __name__ == "__main__":
    main()
