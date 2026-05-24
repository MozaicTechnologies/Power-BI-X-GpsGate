#!/usr/bin/env python
"""Run backfill for a supplied date range using gpsgate_application values."""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback
from datetime import datetime

from dotenv import load_dotenv


os.environ["BACKFILL_MODE"] = "false"
os.environ["FETCH_CURRENT_WEEK"] = "false"

print("[BACKFILL] Starting gpsgate-application backfill", flush=True)
print(f"[BACKFILL] Python: {sys.version}", flush=True)

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.routes.pipeline import process_event_data


app = create_app()
BASE_URL = os.getenv("BASE_URL", "https://omantracking2.com")


ENDPOINTS = [
    {"name": "Trip", "key": "trip_events", "event_id_col": None},
    {"name": "Speeding", "key": "speed_events", "event_id_col": "speed_event_id"},
    {"name": "Idle", "key": "idle_events", "event_id_col": "idle_event_id"},
    {"name": "AWH", "key": "awh_events", "event_id_col": "awh_event_id"},
    {"name": "WH", "key": "wh_events", "event_id_col": "wh_event_id"},
    {"name": "HA", "key": "ha_events", "event_id_col": "ha_event_id"},
    {"name": "HB", "key": "hb_events", "event_id_col": "hb_event_id"},
    {"name": "WU", "key": "wu_events", "event_id_col": "wu_event_id"},
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill using customer_config")
    parser.add_argument("--week_start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--week_end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--app_id",
        default=None,
        help="Optional application_id filter",
    )
    return parser.parse_args()


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
    applications = query.order_by(GpsGateApplication.application_id).all()

    if not applications:
        label = f"application_id={app_id_filter}" if app_id_filter else "any application"
        print(f"[BACKFILL] No gpsgate_application row found for {label}", flush=True)
    return applications


# Keep old function name as alias for backwards compatibility
load_customers = load_applications


def build_payload(app, endpoint: dict, week_start: datetime, week_end: datetime) -> dict:
    report_id = app.trip_report_id if endpoint["name"] == "Trip" else app.event_report_id
    event_id = getattr(app, endpoint["event_id_col"], None) if endpoint["event_id_col"] else None

    payload = {
        "app_id": app.application_id,
        "token": normalize_token(app.token),
        "base_url": BASE_URL,
        "report_id": report_id,
        "tag_id": app.tag_id,
        "period_start": f"{week_start.strftime('%Y-%m-%d')}T00:00:00Z",
        "period_end": f"{week_end.strftime('%Y-%m-%d')}T23:59:59Z",
    }
    if event_id:
        payload["event_id"] = event_id
    return payload


def validate_application_shared(app) -> list[str]:
    missing = []
    required_values = [
        ("application_id", app.application_id),
        ("token", app.token),
        ("tag_id", app.tag_id),
    ]

    for field_name, value in required_values:
        if not value:
            missing.append(field_name)

    return missing


def validate_endpoint(customer, endpoint: dict) -> list[str]:
    missing = []

    if endpoint["name"] == "Trip":
        if not customer.trip_report_id:
            missing.append("trip_report_id")
        return missing

    if not customer.event_report_id:
        missing.append("event_report_id")

    field_name = endpoint["event_id_col"]
    if field_name and not getattr(customer, field_name):
        missing.append(field_name)

    return missing


def run_event(endpoint: dict, payload: dict) -> dict:
    print("[REQUEST PAYLOAD]")
    print(json.dumps({**payload, "token": "***"}, indent=2))

    with app.test_request_context(
        "/internal-backfill",
        method="POST",
        data=json.dumps(payload),
        content_type="application/json",
    ):
        try:
            response, _status = process_event_data(endpoint["name"], endpoint["key"])
            result = response.get_json() if hasattr(response, "get_json") else response
            return result.get("accounting", {})
        except Exception:
            print("[ERROR] Backfill failed")
            print(traceback.format_exc())
            return {}


def main() -> int:
    args = parse_args()
    week_start = datetime.strptime(args.week_start, "%Y-%m-%d")
    week_end = datetime.strptime(args.week_end, "%Y-%m-%d")

    print(f"[BACKFILL] Date range: {week_start.date()} -> {week_end.date()}", flush=True)
    if args.app_id:
        print(f"[BACKFILL] Filtering  : application_id={args.app_id}", flush=True)
    else:
        print("[BACKFILL] Running for : ALL applications", flush=True)

    print("=" * 80)
    print("BACKFILL EXECUTION STARTED")
    print("=" * 80)

    grand_totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}

    app_id_int = int(args.app_id) if args.app_id else None
    with app.app_context():
        applications = load_applications(app_id_int)
        if not applications:
            print("[BACKFILL] Nothing to do. Exiting.", flush=True)
            return 0

        for app in applications:
            print(f"\n{'=' * 80}")
            print(f"APPLICATION: application_id={app.application_id}")
            print(f"{'=' * 80}")

            missing = validate_application_shared(app)
            if missing:
                print(
                    f"[BACKFILL] Skipping application_id={app.application_id} - missing config: {sorted(set(missing))}",
                    flush=True,
                )
                print("[BACKFILL] Run dimension sync first", flush=True)
                continue

            app_totals = {"raw": 0, "inserted": 0, "skipped": 0, "failed": 0}

            for idx, endpoint in enumerate(ENDPOINTS, start=1):
                print(f"\n[{idx}/{len(ENDPOINTS)}] application_id={app.application_id} -> {endpoint['name']}")
                print("-" * 80)

                endpoint_missing = validate_endpoint(app, endpoint)
                if endpoint_missing:
                    print(
                        f"[BACKFILL] Skipping event {endpoint['name']} for application_id={app.application_id} "
                        f"- missing config: {sorted(set(endpoint_missing))}",
                        flush=True,
                    )
                    continue

                payload = build_payload(app, endpoint, week_start, week_end)
                accounting = run_event(endpoint, payload)

                raw = accounting.get("raw", 0)
                inserted = accounting.get("inserted", 0)
                skipped = accounting.get("skipped", 0)
                failed = accounting.get("failed", 0)

                print("[RESULT]")
                print(f"  Raw fetched: {raw}")
                print(f"  Inserted: {inserted}")
                print(f"  Skipped: {skipped}")
                print(f"  Failed: {failed}")

                app_totals["raw"] += raw
                app_totals["inserted"] += inserted
                app_totals["skipped"] += skipped
                app_totals["failed"] += failed

            print("\n" + "-" * 80)
            print(f"APPLICATION SUMMARY: application_id={app.application_id}")
            for key, value in app_totals.items():
                print(f"  {key.capitalize():<10}: {value}")
                grand_totals[key] += value
            print("-" * 80)

    print("\n" + "=" * 80)
    print("GRAND TOTAL (ALL CUSTOMERS)")
    for key, value in grand_totals.items():
        print(f"  {key.capitalize():<10}: {value}")
    print("=" * 80)
    print("BACKFILL COMPLETE")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
