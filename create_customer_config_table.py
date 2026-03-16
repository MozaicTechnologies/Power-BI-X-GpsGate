#!/usr/bin/env python
"""Create or migrate the customer_config table to the current schema."""

from __future__ import annotations

import os
from datetime import datetime

import psycopg
from dotenv import load_dotenv


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


EXPECTED_COLUMNS = {
    "application_id": "varchar(50)",
    "token": "text",
    "tag_id": "varchar(50)",
    "trip_report_id": "varchar(50)",
    "event_report_id": "varchar(50)",
    "awh_event_id": "varchar(50)",
    "ha_event_id": "varchar(50)",
    "hb_event_id": "varchar(50)",
    "hc_event_id": "varchar(50)",
    "wu_event_id": "varchar(50)",
    "wh_event_id": "varchar(50)",
    "speed_event_id": "varchar(50)",
    "idle_event_id": "varchar(50)",
}


def column_exists(cur, table_name: str, column_name: str) -> bool:
    cur.execute(
        """
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
          and column_name = %s
        """,
        (table_name, column_name),
    )
    return cur.fetchone() is not None


def ensure_customer_config_schema(cur) -> None:
    cur.execute(
        """
        create table if not exists customer_config (
            application_id varchar(50) primary key,
            token text not null,
            tag_id varchar(50),
            trip_report_id varchar(50),
            event_report_id varchar(50),
            awh_event_id varchar(50),
            ha_event_id varchar(50),
            hb_event_id varchar(50),
            hc_event_id varchar(50),
            wu_event_id varchar(50),
            wh_event_id varchar(50),
            speed_event_id varchar(50),
            idle_event_id varchar(50)
        )
        """
    )

    for column_name, column_type in EXPECTED_COLUMNS.items():
        if column_exists(cur, "customer_config", column_name):
            continue
        log(f"Adding customer_config.{column_name}")
        cur.execute(
            f"alter table customer_config add column {column_name} {column_type}"
        )

    for nullable_column in (
        "tag_id",
        "trip_report_id",
        "event_report_id",
        "awh_event_id",
        "ha_event_id",
        "hb_event_id",
        "hc_event_id",
        "wu_event_id",
        "wh_event_id",
        "speed_event_id",
        "idle_event_id",
    ):
        cur.execute(
            f"alter table customer_config alter column {nullable_column} drop not null"
        )

    cur.execute("alter table customer_config alter column token set not null")


def main() -> int:
    load_dotenv(".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            ensure_customer_config_schema(cur)
        conn.commit()

    log("customer_config table schema is ready")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
