#!/usr/bin/env python
"""Run DB migrations needed before starting on Render."""

from __future__ import annotations

import os

import psycopg
from dotenv import load_dotenv

from create_customer_config_table import ensure_customer_config_schema, log
from migrate_dimension_tables_for_multi_app import (
    TABLES,
    ensure_application_id_on_custom_fields,
    replace_primary_key,
)


def main() -> int:
    load_dotenv(".env")
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set")

    db_url = db_url.replace("postgresql+psycopg://", "postgresql://")

    with psycopg.connect(db_url) as conn:
        with conn.cursor() as cur:
            log("Ensuring customer_config schema")
            ensure_customer_config_schema(cur)

            log("Ensuring multi-app dimension schema")
            ensure_application_id_on_custom_fields(
                cur,
                default_application_id=None,
                delete_unresolved=True,
            )
            for table_name in TABLES:
                replace_primary_key(cur, table_name, f"{table_name}_pkey", "application_id, id")
            replace_primary_key(
                cur,
                "dim_vehicle_custom_fields",
                "dim_vehicle_custom_fields_pkey",
                "application_id, vehicle_id, field_name",
            )

        conn.commit()

    log("Render pre-deploy migrations completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
