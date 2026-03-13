# """
# sync_dimensions_from_api.py  (MULTI-TENANT VERSION)
# ====================================================
# Syncs dimension tables from GpsGate API → PostgreSQL
# for ALL active customers in customer_configs.

# Changes from original:
# - Reads customer list from customer_configs table (no hardcoded APP_ID / TOKEN)
# - Passes app_id into every dimension table row
# - dim_vehicles / dim_drivers unique key is now (id, app_id) to support
#   same user-id appearing in two different applications
# """

# import os
# import time
# import requests
# import psycopg
# from datetime import datetime
# from urllib.parse import urljoin
# from dotenv import load_dotenv

# # ------------------------------------------------------------------
# # ENV
# # ------------------------------------------------------------------
# load_dotenv()

# DATABASE_URL = os.getenv("DATABASE_URL")

# if not DATABASE_URL:
#     raise RuntimeError("DATABASE_URL missing")

# # psycopg3 requires standard postgres URL
# if DATABASE_URL.startswith("postgresql+psycopg://"):
#     DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


# # ------------------------------------------------------------------
# # LOGGING
# # ------------------------------------------------------------------
# def log(msg, level="INFO"):
#     ts = datetime.now().strftime("%H:%M:%S")
#     print(f"[{ts}] [{level}] {msg}", flush=True)


# # ------------------------------------------------------------------
# # LOAD CUSTOMERS FROM DB
# # ------------------------------------------------------------------
# def load_customers(conn) -> list[dict]:
#     """
#     Returns a list of dicts, one per active customer.
#     Example: [{"app_id": "6", "token": "...", "base_url": "...", "tag_id": "39", "report_id": "25"}]
#     """
#     cur = conn.cursor()
#     cur.execute("""
#         SELECT app_id, name, token, base_url, tag_id, report_id
#         FROM customer_configs
#         WHERE is_active = TRUE
#         ORDER BY id
#     """)
#     rows = cur.fetchall()
#     customers = []
#     for row in rows:
#         customers.append({
#             "app_id":    row[0],
#             "name":      row[1],
#             "token":     row[2],
#             "base_url":  row[3],
#             "tag_id":    row[4],
#             "report_id": row[5],
#         })
#     log(f"Loaded {len(customers)} active customer(s): {[c['name'] for c in customers]}")
#     return customers


# # ------------------------------------------------------------------
# # API CALL (DIRECT — unchanged from original)
# # ------------------------------------------------------------------
# def call_api(method="GET", base_url=None, path=None, params=None,
#              json=None, data=None, token=None, timeout=30, retries=3):
#     base = base_url if base_url.endswith("/") else base_url + "/"
#     url = urljoin(base, path.lstrip("/"))
#     headers = {"Authorization": token}

#     for attempt in range(1, retries + 1):
#         try:
#             resp = requests.request(
#                 method=method, url=url, params=params,
#                 json=json, data=data, headers=headers, timeout=timeout,
#             )
#             if resp.ok:
#                 return resp.json()
#             raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
#         except Exception:
#             if attempt == retries:
#                 raise
#             log(f"Retry {attempt}/{retries} → {path}", "WARN")
#             time.sleep(2)


# # ------------------------------------------------------------------
# # PER-CUSTOMER SYNC FUNCTIONS
# # ------------------------------------------------------------------

# def sync_tags(cur, customer: dict) -> int:
#     app_id    = customer["app_id"]
#     base_url  = customer["base_url"]
#     token     = customer["token"]

#     log(f"[{customer['name']}] Syncing dim_tags")
#     data = call_api(
#         base_url=base_url,
#         path=f"comGpsGate/api/v.1/applications/{app_id}/tags",
#         token=token,
#     )

#     rows = [(int(r["id"]), int(app_id), r["name"], app_id) for r in data]

#     cur.executemany(
#         """
#         INSERT INTO dim_tags (id, application_id, name, app_id)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (id) DO UPDATE
#             SET name   = EXCLUDED.name,
#                 app_id = EXCLUDED.app_id
#         """,
#         rows,
#     )
#     log(f"[{customer['name']}] dim_tags ✓ {len(rows)}")
#     return len(rows)


# def sync_event_rules(cur, customer: dict) -> int:
#     app_id   = customer["app_id"]
#     base_url = customer["base_url"]
#     token    = customer["token"]

#     log(f"[{customer['name']}] Syncing dim_event_rules")
#     data = call_api(
#         base_url=base_url,
#         path=f"comGpsGate/api/v.1/applications/{app_id}/eventrules",
#         token=token,
#     )

#     rows = [(int(r["id"]), int(app_id), r["name"], app_id) for r in data]

#     cur.executemany(
#         """
#         INSERT INTO dim_event_rules (id, application_id, name, app_id)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (id) DO UPDATE
#             SET name   = EXCLUDED.name,
#                 app_id = EXCLUDED.app_id
#         """,
#         rows,
#     )
#     log(f"[{customer['name']}] dim_event_rules ✓ {len(rows)}")
#     return len(rows)


# def sync_reports(cur, customer: dict) -> int:
#     app_id   = customer["app_id"]
#     base_url = customer["base_url"]
#     token    = customer["token"]

#     log(f"[{customer['name']}] Syncing dim_reports")
#     data = call_api(
#         base_url=base_url,
#         path=f"comGpsGate/api/v.1/applications/{app_id}/reports",
#         token=token,
#     )

#     rows = [(int(r["id"]), int(app_id), r["name"], app_id) for r in data]

#     cur.executemany(
#         """
#         INSERT INTO dim_reports (id, application_id, name, app_id)
#         VALUES (%s, %s, %s, %s)
#         ON CONFLICT (id) DO UPDATE
#             SET name   = EXCLUDED.name,
#                 app_id = EXCLUDED.app_id
#         """,
#         rows,
#     )
#     log(f"[{customer['name']}] dim_reports ✓ {len(rows)}")
#     return len(rows)


# def sync_vehicles_and_drivers(cur, customer: dict) -> int:
#     app_id   = customer["app_id"]
#     base_url = customer["base_url"]
#     token    = customer["token"]

#     log(f"[{customer['name']}] Syncing dim_vehicles + dim_drivers")

#     users = call_api(
#         base_url=base_url,
#         path=f"comGpsGate/api/v.1/applications/{app_id}/users",
#         token=token,
#         timeout=60,
#     )

#     vehicle_rows = []
#     driver_rows  = []

#     for idx, u in enumerate(users, start=1):
#         if idx % 50 == 0:
#             log(f"[{customer['name']}] Users processed: {idx}/{len(users)}")

#         tp      = u.get("trackPoint") or {}
#         pos     = tp.get("position") or {}
#         devices = u.get("devices") or []

#         device_name = devices[0].get("name") if devices else None
#         imei        = devices[0].get("imei") if devices else None

#         if imei:
#             vehicle_rows.append((
#                 int(u["id"]), int(app_id), u.get("name"), u.get("username"),
#                 imei, pos.get("latitude"), pos.get("longitude"),
#                 tp.get("utc"), tp.get("valid"), device_name,
#                 app_id,   # ← new column
#             ))

#         if u.get("driverID"):
#             driver_rows.append((
#                 int(u["id"]), int(app_id), u.get("name"), u.get("username"),
#                 u.get("driverID"), device_name, imei,
#                 pos.get("latitude"), pos.get("longitude"),
#                 tp.get("utc"), tp.get("valid"),
#                 app_id,   # ← new column
#             ))

#     if vehicle_rows:
#         cur.executemany(
#             """
#             INSERT INTO dim_vehicles (
#                 id, application_id, name, username, imei,
#                 latitude, longitude, last_utc, valid, device_name,
#                 app_id
#             )
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#             ON CONFLICT (id) DO UPDATE SET
#                 imei        = EXCLUDED.imei,
#                 latitude    = EXCLUDED.latitude,
#                 longitude   = EXCLUDED.longitude,
#                 last_utc    = EXCLUDED.last_utc,
#                 valid       = EXCLUDED.valid,
#                 device_name = EXCLUDED.device_name,
#                 app_id      = EXCLUDED.app_id
#             """,
#             vehicle_rows,
#         )

#     if driver_rows:
#         cur.executemany(
#             """
#             INSERT INTO dim_drivers (
#                 id, application_id, name, username, driver_id,
#                 device_name, imei, latitude, longitude, utc, validity,
#                 app_id
#             )
#             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#             ON CONFLICT (id) DO UPDATE SET
#                 name        = EXCLUDED.name,
#                 username    = EXCLUDED.username,
#                 driver_id   = EXCLUDED.driver_id,
#                 device_name = EXCLUDED.device_name,
#                 imei        = EXCLUDED.imei,
#                 latitude    = EXCLUDED.latitude,
#                 longitude   = EXCLUDED.longitude,
#                 utc         = EXCLUDED.utc,
#                 validity    = EXCLUDED.validity,
#                 app_id      = EXCLUDED.app_id
#             """,
#             driver_rows,
#         )

#     log(f"[{customer['name']}] dim_vehicles ✓ {len(vehicle_rows)}")
#     log(f"[{customer['name']}] dim_drivers  ✓ {len(driver_rows)}")
#     return len(vehicle_rows) + len(driver_rows)


# def sync_vehicle_custom_fields(cur, customer: dict) -> int:
#     app_id   = customer["app_id"]
#     base_url = customer["base_url"]
#     token    = customer["token"]

#     log(f"[{customer['name']}] Syncing dim_vehicle_custom_fields")

#     users = call_api(
#         base_url=base_url,
#         path=f"comGpsGate/api/v.1/applications/{app_id}/users",
#         token=token,
#         timeout=60,
#     )

#     rows    = []
#     skipped = 0
#     total   = 0

#     for idx, u in enumerate(users, start=1):
#         vid = u.get("id")
#         if not vid:
#             continue

#         if idx % 25 == 0:
#             log(f"[{customer['name']}] Custom fields: {idx}/{len(users)}")

#         try:
#             fields = call_api(
#                 base_url=base_url,
#                 path=f"comGpsGate/api/v.1/applications/{app_id}/users/{vid}/customfields",
#                 token=token,
#                 timeout=30,
#             )
#         except Exception:
#             skipped += 1
#             continue

#         for f in fields:
#             rows.append((int(vid), f.get("name"), str(f.get("value")), app_id))
#             total += 1

#         if len(rows) >= 500:
#             cur.executemany(
#                 """
#                 INSERT INTO dim_vehicle_custom_fields
#                     (vehicle_id, field_name, field_value, app_id)
#                 VALUES (%s, %s, %s, %s)
#                 ON CONFLICT (vehicle_id, field_name)
#                 DO UPDATE SET
#                     field_value = EXCLUDED.field_value,
#                     app_id      = EXCLUDED.app_id
#                 """,
#                 rows,
#             )
#             rows.clear()

#     if rows:
#         cur.executemany(
#             """
#             INSERT INTO dim_vehicle_custom_fields
#                 (vehicle_id, field_name, field_value, app_id)
#             VALUES (%s, %s, %s, %s)
#             ON CONFLICT (vehicle_id, field_name)
#             DO UPDATE SET
#                 field_value = EXCLUDED.field_value,
#                 app_id      = EXCLUDED.app_id
#             """,
#             rows,
#         )

#     log(f"[{customer['name']}] dim_vehicle_custom_fields ✓ | skipped {skipped}")
#     return total


# # ------------------------------------------------------------------
# # MAIN — loops all customers
# # ------------------------------------------------------------------
# def main():
#     log("🚀 Starting multi-tenant dimension sync")
#     start       = time.time()
#     grand_total = 0

#     with psycopg.connect(DATABASE_URL) as conn:
#         customers = load_customers(conn)

#         if not customers:
#             log("⚠️  No active customers found in customer_configs!", "WARN")
#             return 0

#         for customer in customers:
#             log(f"\n{'='*60}")
#             log(f"Processing customer: {customer['name']} (app_id={customer['app_id']})")
#             log(f"{'='*60}")

#             customer_total = 0

#             with conn.cursor() as cur:
#                 customer_total += sync_tags(cur, customer)
#                 conn.commit()

#                 customer_total += sync_event_rules(cur, customer)
#                 conn.commit()

#                 customer_total += sync_reports(cur, customer)
#                 conn.commit()

#                 customer_total += sync_vehicles_and_drivers(cur, customer)
#                 conn.commit()

#                 customer_total += sync_vehicle_custom_fields(cur, customer)
#                 conn.commit()

#             log(f"✅ {customer['name']}: {customer_total:,} records synced")
#             grand_total += customer_total

#     elapsed = round(time.time() - start, 2)
#     log(f"\n✅ All customers synced in {elapsed}s — grand total: {grand_total:,} records")
#     return grand_total


# if __name__ == "__main__":
#     main()


#-------------------------------------------------------------------------
"""
sync_dimensions_from_api.py  (MULTI-TENANT VERSION - FIXED)
============================================================
Fixes:
1. Removed old report_id column reference (now uses trip_report_id / event_report_id)
2. Syncs report_format_id from GpsGate API into dim_reports
3. Adds report_format_id column to dim_reports if missing
4. Prints verification of format IDs for key reports at end
"""

import os
import time
import requests
import psycopg
from datetime import datetime
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL missing")
if DATABASE_URL.startswith("postgresql+psycopg://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql+psycopg://", "postgresql://")


def log(msg, level="INFO"):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [{level}] {msg}", flush=True)


def load_customers(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT app_id, name, token, base_url, tag_id,
               trip_report_id, event_report_id
        FROM customer_configs
        WHERE is_active = TRUE
        ORDER BY id
    """)
    rows = cur.fetchall()
    customers = []
    for row in rows:
        customers.append({
            "app_id":          str(row[0]),
            "name":            row[1],
            "token":           row[2],
            "base_url":        row[3],
            "tag_id":          str(row[4]) if row[4] else None,
            "trip_report_id":  str(row[5]) if row[5] else None,
            "event_report_id": str(row[6]) if row[6] else None,
        })
    log(f"Loaded {len(customers)} active customer(s)")
    for c in customers:
        log(f"  {c['name']}: app_id={c['app_id']} tag={c['tag_id']} "
            f"trip_report={c['trip_report_id']} event_report={c['event_report_id']}")
    return customers


def call_api(method="GET", base_url=None, path=None, params=None,
             json=None, data=None, token=None, timeout=30, retries=3):
    base = base_url if base_url.endswith("/") else base_url + "/"
    url = urljoin(base, path.lstrip("/"))
    headers = {"Authorization": token}
    for attempt in range(1, retries + 1):
        try:
            resp = requests.request(method=method, url=url, params=params,
                                    json=json, data=data, headers=headers, timeout=timeout)
            if resp.ok:
                return resp.json()
            raise RuntimeError(f"{resp.status_code} {resp.text[:200]}")
        except Exception:
            if attempt == retries:
                raise
            log(f"Retry {attempt}/{retries} -> {path}", "WARN")
            time.sleep(2)


def ensure_report_format_id_column(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name='dim_reports' AND column_name='report_format_id'
    """)
    if not cur.fetchone():
        log("Adding report_format_id column to dim_reports...")
        cur.execute("ALTER TABLE dim_reports ADD COLUMN report_format_id INTEGER")
        conn.commit()
        log("report_format_id column added.")
    else:
        log("report_format_id column already exists.")


def sync_tags(cur, customer):
    app_id = customer["app_id"]
    log(f"[{customer['name']}] Syncing dim_tags")
    data = call_api(base_url=customer["base_url"],
                    path=f"comGpsGate/api/v.1/applications/{app_id}/tags",
                    token=customer["token"])
    rows = [(int(r["id"]), int(app_id), r["name"], app_id) for r in data]
    cur.executemany("""
        INSERT INTO dim_tags (id, application_id, name, app_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, app_id=EXCLUDED.app_id
    """, rows)
    log(f"[{customer['name']}] dim_tags ok {len(rows)}")
    return len(rows)


def sync_event_rules(cur, customer):
    app_id = customer["app_id"]
    log(f"[{customer['name']}] Syncing dim_event_rules")
    data = call_api(base_url=customer["base_url"],
                    path=f"comGpsGate/api/v.1/applications/{app_id}/eventrules",
                    token=customer["token"])
    rows = [(int(r["id"]), int(app_id), r["name"], app_id) for r in data]
    cur.executemany("""
        INSERT INTO dim_event_rules (id, application_id, name, app_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET name=EXCLUDED.name, app_id=EXCLUDED.app_id
    """, rows)
    log(f"[{customer['name']}] dim_event_rules ok {len(rows)}")
    return len(rows)


def sync_reports(cur, customer):
    """Syncs dim_reports INCLUDING report_format_id fetched per-report from API."""
    app_id = customer["app_id"]
    log(f"[{customer['name']}] Syncing dim_reports with report_format_id")
    data = call_api(base_url=customer["base_url"],
                    path=f"comGpsGate/api/v.1/applications/{app_id}/reports",
                    token=customer["token"])
    rows = []
    for r in data:
        report_id = int(r["id"])
        name = r["name"]
        report_format_id = r.get("reportFormatId")
        if report_format_id is None:
            try:
                detail = call_api(
                    base_url=customer["base_url"],
                    path=f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}",
                    token=customer["token"], timeout=15)
                report_format_id = detail.get("reportFormatId")
            except Exception as e:
                log(f"  Could not fetch detail for report {report_id}: {e}", "WARN")
        rows.append((report_id, int(app_id), name, app_id, report_format_id))
        log(f"  report id={report_id} format_id={report_format_id} name={name}")

    cur.executemany("""
        INSERT INTO dim_reports (id, application_id, name, app_id, report_format_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name=EXCLUDED.name,
            app_id=EXCLUDED.app_id,
            report_format_id=EXCLUDED.report_format_id
    """, rows)
    log(f"[{customer['name']}] dim_reports ok {len(rows)}")
    return len(rows)


def sync_vehicles_and_drivers(cur, customer):
    app_id = customer["app_id"]
    log(f"[{customer['name']}] Syncing dim_vehicles + dim_drivers")
    users = call_api(base_url=customer["base_url"],
                     path=f"comGpsGate/api/v.1/applications/{app_id}/users",
                     token=customer["token"], timeout=60)
    vehicle_rows = []
    driver_rows = []
    for idx, u in enumerate(users, start=1):
        if idx % 50 == 0:
            log(f"[{customer['name']}] Users processed: {idx}/{len(users)}")
        tp = u.get("trackPoint") or {}
        pos = tp.get("position") or {}
        devices = u.get("devices") or []
        device_name = devices[0].get("name") if devices else None
        imei = devices[0].get("imei") if devices else None
        if imei:
            vehicle_rows.append((int(u["id"]), int(app_id), u.get("name"), u.get("username"),
                                  imei, pos.get("latitude"), pos.get("longitude"),
                                  tp.get("utc"), tp.get("valid"), device_name, app_id))
        if u.get("driverID"):
            driver_rows.append((int(u["id"]), int(app_id), u.get("name"), u.get("username"),
                                 u.get("driverID"), device_name, imei,
                                 pos.get("latitude"), pos.get("longitude"),
                                 tp.get("utc"), tp.get("valid"), app_id))
    if vehicle_rows:
        cur.executemany("""
            INSERT INTO dim_vehicles (id, application_id, name, username, imei,
                latitude, longitude, last_utc, valid, device_name, app_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                imei=EXCLUDED.imei, latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                last_utc=EXCLUDED.last_utc, valid=EXCLUDED.valid,
                device_name=EXCLUDED.device_name, app_id=EXCLUDED.app_id
        """, vehicle_rows)
    if driver_rows:
        cur.executemany("""
            INSERT INTO dim_drivers (id, application_id, name, username, driver_id,
                device_name, imei, latitude, longitude, utc, validity, app_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
                name=EXCLUDED.name, username=EXCLUDED.username, driver_id=EXCLUDED.driver_id,
                device_name=EXCLUDED.device_name, imei=EXCLUDED.imei,
                latitude=EXCLUDED.latitude, longitude=EXCLUDED.longitude,
                utc=EXCLUDED.utc, validity=EXCLUDED.validity, app_id=EXCLUDED.app_id
        """, driver_rows)
    log(f"[{customer['name']}] dim_vehicles ok {len(vehicle_rows)}")
    log(f"[{customer['name']}] dim_drivers  ok {len(driver_rows)}")
    return len(vehicle_rows) + len(driver_rows)


def sync_vehicle_custom_fields(cur, customer):
    app_id = customer["app_id"]
    log(f"[{customer['name']}] Syncing dim_vehicle_custom_fields")
    users = call_api(base_url=customer["base_url"],
                     path=f"comGpsGate/api/v.1/applications/{app_id}/users",
                     token=customer["token"], timeout=60)
    rows = []
    skipped = 0
    total = 0
    for idx, u in enumerate(users, start=1):
        vid = u.get("id")
        if not vid:
            continue
        if idx % 25 == 0:
            log(f"[{customer['name']}] Custom fields: {idx}/{len(users)}")
        try:
            fields = call_api(base_url=customer["base_url"],
                              path=f"comGpsGate/api/v.1/applications/{app_id}/users/{vid}/customfields",
                              token=customer["token"], timeout=30)
        except Exception:
            skipped += 1
            continue
        for f in fields:
            rows.append((int(vid), f.get("name"), str(f.get("value")), app_id))
            total += 1
        if len(rows) >= 500:
            cur.executemany("""
                INSERT INTO dim_vehicle_custom_fields (vehicle_id, field_name, field_value, app_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (vehicle_id, field_name) DO UPDATE SET
                    field_value=EXCLUDED.field_value, app_id=EXCLUDED.app_id
            """, rows)
            rows.clear()
    if rows:
        cur.executemany("""
            INSERT INTO dim_vehicle_custom_fields (vehicle_id, field_name, field_value, app_id)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (vehicle_id, field_name) DO UPDATE SET
                field_value=EXCLUDED.field_value, app_id=EXCLUDED.app_id
        """, rows)
    log(f"[{customer['name']}] dim_vehicle_custom_fields ok | skipped {skipped}")
    return total


def verify_report_format_ids(conn, customers):
    cur = conn.cursor()
    print()
    log("=== REPORT FORMAT ID VERIFICATION ===")
    log("These are the format IDs render.py should use per customer:")
    for c in customers:
        app_id = c["app_id"]
        for label, rid in [("trip_report", c["trip_report_id"]),
                           ("event_report", c["event_report_id"])]:
            if not rid:
                continue
            cur.execute(
                "SELECT id, name, report_format_id FROM dim_reports WHERE id=%s AND app_id=%s",
                (int(rid), app_id)
            )
            row = cur.fetchone()
            if row:
                log(f"  [{c['name']}] {label}: id={row[0]} format_id={row[2]} name={row[1]}")
            else:
                log(f"  [{c['name']}] {label}: id={rid} NOT FOUND in dim_reports", "WARN")


def main():
    log("Starting multi-tenant dimension sync")
    start = time.time()
    grand_total = 0

    with psycopg.connect(DATABASE_URL) as conn:
        ensure_report_format_id_column(conn)
        customers = load_customers(conn)

        if not customers:
            log("No active customers found!", "WARN")
            return 0

        for customer in customers:
            log(f"\n{'='*60}")
            log(f"Customer: {customer['name']} (app_id={customer['app_id']})")
            log(f"{'='*60}")
            customer_total = 0
            with conn.cursor() as cur:
                customer_total += sync_tags(cur, customer)
                conn.commit()
                customer_total += sync_event_rules(cur, customer)
                conn.commit()
                customer_total += sync_reports(cur, customer)
                conn.commit()
                customer_total += sync_vehicles_and_drivers(cur, customer)
                conn.commit()
                customer_total += sync_vehicle_custom_fields(cur, customer)
                conn.commit()
            log(f"Done {customer['name']}: {customer_total:,} records")
            grand_total += customer_total

        verify_report_format_ids(conn, customers)

    elapsed = round(time.time() - start, 2)
    log(f"\nAll done in {elapsed}s — grand total: {grand_total:,} records")
    return grand_total


if __name__ == "__main__":
    main()