#!/usr/bin/env python
"""
Update dimension tables for multi-application support.

This script:
1. Adds application_id to dim_vehicle_custom_fields if missing
2. Backfills dim_vehicle_custom_fields.application_id from dim_vehicles and dim_drivers
3. Optionally assigns a default application_id to unresolved orphan rows
4. Replaces single-column primary keys with application-scoped composite keys

Run this before using application-scoped upserts in sync_dimensions_from_api.py.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import psycopg
from dotenv import load_dotenv


TABLES = (
    "dim_tags",
    "dim_event_rules",
    "dim_reports",
    "dim_vehicles",
    "dim_drivers",
)


def log(message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def constraint_exists(cur, table_name: str, constraint_name: str) -> bool:
    cur.execute(
        """
        select 1
        from information_schema.table_constraints
        where table_schema = 'public'
          and table_name = %s
          and constraint_name = %s
        """,
        (table_name, constraint_name),
    )
    return cur.fetchone() is not None


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


def export_unresolved_rows(cur, output_path: Path) -> int:
    cur.execute(
        """
        select vehicle_id, field_name, field_value
        from dim_vehicle_custom_fields
        where application_id is null
        order by vehicle_id, field_name
        """
    )
    rows = [
        {"vehicle_id": row[0], "field_name": row[1], "field_value": row[2]}
        for row in cur.fetchall()
    ]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    return len(rows)


def ensure_application_id_on_custom_fields(cur, default_application_id: int | None) -> None:
    if not column_exists(cur, "dim_vehicle_custom_fields", "application_id"):
        log("Adding application_id to dim_vehicle_custom_fields")
        cur.execute(
            """
            alter table dim_vehicle_custom_fields
            add column application_id integer
            """
        )

    log("Backfilling dim_vehicle_custom_fields.application_id from dim_vehicles")
    cur.execute(
        """
        update dim_vehicle_custom_fields cf
        set application_id = v.application_id
        from dim_vehicles v
        where cf.vehicle_id = v.id
          and cf.application_id is null
        """
    )

    log("Backfilling dim_vehicle_custom_fields.application_id from dim_drivers")
    cur.execute(
        """
        update dim_vehicle_custom_fields cf
        set application_id = d.application_id
        from dim_drivers d
        where cf.vehicle_id = d.id
          and cf.application_id is null
        """
    )

    if default_application_id is not None:
        log(
            "Assigning default application_id "
            f"{default_application_id} to unresolved dim_vehicle_custom_fields rows"
        )
        cur.execute(
            """
            update dim_vehicle_custom_fields
            set application_id = %s
            where application_id is null
            """,
            (default_application_id,),
        )

    cur.execute(
        """
        select count(*)
        from dim_vehicle_custom_fields
        where application_id is null
        """
    )
    missing = cur.fetchone()[0]
    if missing:
        output_path = Path("output") / "dim_vehicle_custom_fields_unresolved.json"
        exported = export_unresolved_rows(cur, output_path)
        raise RuntimeError(
            "dim_vehicle_custom_fields still has "
            f"{missing} rows with null application_id. "
            f"Exported {exported} unresolved rows to {output_path}. "
            "Rerun with --default-application-id <id> if you want to force-fill them."
        )

    cur.execute(
        """
        alter table dim_vehicle_custom_fields
        alter column application_id set not null
        """
    )


def replace_primary_key(cur, table_name: str, old_pk: str, new_columns_sql: str) -> None:
    if constraint_exists(cur, table_name, old_pk):
        log(f"Dropping {old_pk} on {table_name}")
        cur.execute(f"alter table {table_name} drop constraint {old_pk}")

    new_pk = f"{table_name}_pkey"
    if not constraint_exists(cur, table_name, new_pk):
        log(f"Adding composite primary key on {table_name} ({new_columns_sql})")
        cur.execute(
            f"alter table {table_name} add constraint {new_pk} primary key ({new_columns_sql})"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate dimension tables for multi-app support")
    parser.add_argument(
        "--default-application-id",
        type=int,
        help="Force-fill unresolved dim_vehicle_custom_fields rows with this application_id",
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
            ensure_application_id_on_custom_fields(cur, args.default_application_id)

            for table_name in TABLES:
                replace_primary_key(cur, table_name, f"{table_name}_pkey", "application_id, id")

            replace_primary_key(
                cur,
                "dim_vehicle_custom_fields",
                "dim_vehicle_custom_fields_pkey",
                "application_id, vehicle_id, field_name",
            )

        conn.commit()

    log("Dimension table migration completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
