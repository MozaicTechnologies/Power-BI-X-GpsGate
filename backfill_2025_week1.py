"""
backfill_2025_week1.py  (MULTI-TENANT VERSION)
===============================================
All IDs (report_id, tag_id, event_ids) are read from
customer_configs — nothing is hardcoded.
"""

import os
import sys
import json
import argparse
import traceback
from datetime import datetime

os.environ["BACKFILL_MODE"]        = "false"
os.environ["FETCH_CURRENT_WEEK"]   = "false"

print("[BACKFILL] Starting multi-tenant backfill", flush=True)
print(f"[BACKFILL] Python: {sys.version}", flush=True)

from dotenv import load_dotenv
load_dotenv()

from application import create_app
from data_pipeline import process_event_data

app = create_app()

parser = argparse.ArgumentParser(description="Multi-Tenant Dynamic Backfill Script")
parser.add_argument("--week_start", required=True, help="Start date (YYYY-MM-DD)")
parser.add_argument("--week_end",   required=True, help="End date (YYYY-MM-DD, inclusive)")
parser.add_argument("--app_id",     required=False, default=None,
                    help="(Optional) Run for a single customer app_id only.")
args = parser.parse_args()

week_start = datetime.strptime(args.week_start, "%Y-%m-%d")
week_end   = datetime.strptime(args.week_end,   "%Y-%m-%d")

print(f"[BACKFILL] Date range : {week_start.date()} -> {week_end.date()}", flush=True)
if args.app_id:
    print(f"[BACKFILL] Filtering  : app_id={args.app_id}", flush=True)
else:
    print("[BACKFILL] Running for : ALL active customers", flush=True)

# ------------------------------------------------------------------
# ENDPOINTS — names and keys only, NO hardcoded IDs
# event_id_col maps to the column name in customer_configs
# ------------------------------------------------------------------
ENDPOINTS = [
    {"name": "Trip",     "key": "trip_events",  "event_id_col": None},
    {"name": "Speeding", "key": "speed_events", "event_id_col": "event_id_speed"},
    {"name": "Idle",     "key": "idle_events",  "event_id_col": "event_id_idle"},
    {"name": "AWH",      "key": "awh_events",   "event_id_col": "event_id_awh"},
    {"name": "WH",       "key": "wh_events",    "event_id_col": "event_id_wh"},
    {"name": "HA",       "key": "ha_events",    "event_id_col": "event_id_ha"},
    {"name": "HB",       "key": "hb_events",    "event_id_col": "event_id_hb"},
    {"name": "WU",       "key": "wu_events",    "event_id_col": "event_id_wu"},
]


def load_customers(app_id_filter=None):
    from models import CustomerConfig
    query = CustomerConfig.query.filter_by(is_active=True)
    if app_id_filter:
        query = query.filter_by(app_id=str(app_id_filter))
    customers = query.order_by(CustomerConfig.id).all()
    if not customers:
        msg = f"app_id={app_id_filter}" if app_id_filter else "any active customer"
        print(f"[BACKFILL] No active customer found for {msg}")
    return customers


def run_event_for_customer(customer, ep, week_start, week_end):
    """
    Builds payload entirely from customer object.
    - Trip events  → uses customer.trip_report_id
    - Other events → uses customer.event_report_id
    - Event IDs    → read from customer.event_id_* columns
    """
    report_id = customer.trip_report_id if ep["name"] == "Trip" else customer.event_report_id
    event_id  = getattr(customer, ep["event_id_col"], None) if ep["event_id_col"] else None

    payload = {
        "app_id":       customer.app_id,
        "token":        customer.token,
        "base_url":     customer.base_url,
        "report_id":    report_id,
        "tag_id":       customer.tag_id,
        "period_start": f"{week_start.strftime('%Y-%m-%d')}T00:00:00Z",
        "period_end":   f"{week_end.strftime('%Y-%m-%d')}T23:59:59Z",
    }
    if event_id:
        payload["event_id"] = event_id

    print("\n[REQUEST PAYLOAD]")
    print(json.dumps({**payload, "token": "***"}, indent=2))

    with app.test_request_context(
        "/internal-backfill",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        try:
            response, status = process_event_data(ep["name"], ep["key"])
            result = response.get_json() if hasattr(response, "get_json") else response
            return result.get("accounting", {})
        except Exception as e:
            print(f"[ERROR] process_event_data raised: {e}")
            print(traceback.format_exc())
            return {}


# ------------------------------------------------------------------
# EXECUTION
# ------------------------------------------------------------------
print("=" * 80)
print("MULTI-TENANT BACKFILL EXECUTION STARTED")
print("=" * 80)

with app.app_context():
    customers = load_customers(app_id_filter=args.app_id)

    if not customers:
        print("[BACKFILL] Nothing to do. Exiting.")
        sys.exit(0)

    print(f"\n[BACKFILL] Customers to process: {len(customers)}")
    for c in customers:
        print(f"  - {c.name} (app_id={c.app_id})")
        print(f"    tag_id={c.tag_id}  trip_report={c.trip_report_id}  event_report={c.event_report_id}")
        print(f"    speed={c.event_id_speed}  idle={c.event_id_idle}  awh={c.event_id_awh}")
        print(f"    ha={c.event_id_ha}  hb={c.event_id_hb}  hc={c.event_id_hc}")
        print(f"    wu={c.event_id_wu}  wh={c.event_id_wh}")

    grand_totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}

    for customer in customers:
        print(f"\n{'='*80}")
        print(f"CUSTOMER: {customer.name}  (app_id={customer.app_id})")
        print(f"{'='*80}")

        # Validate required IDs exist before running
        missing = [f for f, v in [
            ("trip_report_id",  customer.trip_report_id),
            ("event_report_id", customer.event_report_id),
            ("tag_id",          customer.tag_id),
        ] if not v]

        if missing:
            print(f"[BACKFILL] Skipping {customer.name} — missing config: {missing}")
            print(f"[BACKFILL] Run: python setup_customer_ids.py")
            continue

        customer_totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}

        for idx, ep in enumerate(ENDPOINTS, start=1):
            print(f"\n[{idx}/{len(ENDPOINTS)}] {customer.name} -> {ep['name']}")
            print("-" * 60)

            accounting = run_event_for_customer(customer, ep, week_start, week_end)

            raw      = accounting.get("raw", 0)
            inserted = accounting.get("inserted", 0)
            skipped  = accounting.get("skipped", 0)
            failed   = accounting.get("failed", 0)

            print("[RESULT]")
            print(f"  Raw fetched : {raw}")
            print(f"  Inserted    : {inserted}")
            print(f"  Skipped     : {skipped}")
            print(f"  Failed      : {failed}")

            for k in customer_totals:
                customer_totals[k] += accounting.get(k, 0)

        print(f"\n{'─'*60}")
        print(f"CUSTOMER SUMMARY: {customer.name}")
        for k, v in customer_totals.items():
            print(f"  {k.capitalize():<10}: {v}")
        print(f"{'─'*60}")

        for k in grand_totals:
            grand_totals[k] += customer_totals[k]

print("\n" + "=" * 80)
print("GRAND TOTAL (ALL CUSTOMERS)")
for k, v in grand_totals.items():
    print(f"  {k.capitalize():<10}: {v}")
print("=" * 80)
print("BACKFILL COMPLETE")
print("=" * 80)