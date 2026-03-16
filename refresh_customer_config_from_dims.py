#!/usr/bin/env python
"""Refresh customer_config from existing dimension tables only."""

from __future__ import annotations

import argparse
import os

import psycopg
from dotenv import load_dotenv

from sync_dimensions_from_api import log, update_customer_config_from_dims


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh customer_config from dim tables")
    parser.add_argument(
        "--application-id",
        help="Optional application_id to refresh",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_dotenv(".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            if args.application_id:
                application_ids = [int(str(args.application_id).strip())]
            else:
                cur.execute(
                    """
                    select application_id::text
                    from customer_config
                    order by application_id
                    """
                )
                application_ids = [int(row[0]) for row in cur.fetchall()]

            if not application_ids:
                raise RuntimeError("No customer_config rows found")

            for application_id in application_ids:
                log(f"Refreshing customer_config from dims for app {application_id}")
                update_customer_config_from_dims(cur, application_id)
                conn.commit()

    log("customer_config refresh completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
