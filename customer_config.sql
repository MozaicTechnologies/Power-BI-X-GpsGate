"""
setup_customer_ids.py
=====================
Automatically looks up all required IDs from dim_tags, dim_reports, and dim_event_rules
by keyword matching, then updates customer_configs with the correct values.

Run this ONCE after syncing dimensions:
    python setup_customer_ids.py

Run it again any time report/event IDs change on the GpsGate side.
"""

import sys
import logging
from application import create_app, db
from sqlalchemy import text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger(__name__)

# =============================================================================
# KEYWORD CONFIG — edit these if GpsGate renames a report/rule
# =============================================================================

TAG_KEYWORD = "show on map"  # case-insensitive match in dim_tags.name

TRIP_REPORT_KEYWORD  = "trip and idle (tag)-bi format"       # in dim_reports.name
EVENT_REPORT_KEYWORD = "event rule"                           # fallback broad match

# Ordered list: first keyword that matches wins
EVENT_REPORT_KEYWORDS = [
    "bi - event rule",                # app 93 specific
    "event rule detailed (tag)",      # app 6 specific
    "event rule detailed",            # fallback
]

EVENT_KEYWORDS = {
    "event_id_speed": ["over speeding +150", "over speeding +150km", "over speeding +140"],
    "event_id_idle":  ["30 min idle", "30min idle", "idle 30"],
    "event_id_awh":   ["after working hours usage", "after working hours"],
    "event_id_ha":    ["harsh acceleration"],
    "event_id_hb":    ["harsh braking"],
    "event_id_hc":    ["harsh cornering"],
    "event_id_wu":    ["weekend usage"],
    "event_id_wh":    ["EXCLUDE:after|working hours usage"],  # EXCLUDE:word means skip rows containing that word
}

# =============================================================================


def find_tag_id(conn, app_id: str) -> str | None:
    """Find tag_id by keyword in dim_tags for a given app_id."""
    rows = conn.execute(
        text("SELECT id, name FROM dim_tags WHERE app_id = :app_id ORDER BY id"),
        {"app_id": app_id}
    ).fetchall()

    for row in rows:
        if TAG_KEYWORD.lower() in row.name.lower():
            log.info(f"  [tag]          app_id={app_id} → id={row.id}  name='{row.name}'")
            return str(row.id)

    log.warning(f"  [tag]          app_id={app_id} → NO MATCH for '{TAG_KEYWORD}'")
    log.warning(f"  Available tags: {[(r.id, r.name) for r in rows]}")
    return None


def find_trip_report_id(conn, app_id: str) -> str | None:
    """Find trip report ID by keyword in dim_reports."""
    rows = conn.execute(
        text("SELECT id, name FROM dim_reports WHERE app_id = :app_id ORDER BY id"),
        {"app_id": app_id}
    ).fetchall()

    for row in rows:
        if TRIP_REPORT_KEYWORD.lower() in row.name.lower():
            log.info(f"  [trip_report]  app_id={app_id} → id={row.id}  name='{row.name}'")
            return str(row.id)

    log.warning(f"  [trip_report]  app_id={app_id} → NO MATCH for '{TRIP_REPORT_KEYWORD}'")
    log.warning(f"  Available reports: {[(r.id, r.name) for r in rows]}")
    return None


def find_event_report_id(conn, app_id: str) -> str | None:
    """Find event report ID — tries keywords in priority order."""
    rows = conn.execute(
        text("SELECT id, name FROM dim_reports WHERE app_id = :app_id ORDER BY id"),
        {"app_id": app_id}
    ).fetchall()

    for keyword in EVENT_REPORT_KEYWORDS:
        for row in rows:
            if keyword.lower() in row.name.lower():
                log.info(f"  [event_report] app_id={app_id} → id={row.id}  name='{row.name}'  (matched: '{keyword}')")
                return str(row.id)

    log.warning(f"  [event_report] app_id={app_id} → NO MATCH for any keyword")
    log.warning(f"  Available reports: {[(r.id, r.name) for r in rows]}")
    return None


def find_event_ids(conn, app_id: str) -> dict:
    """Find all 8 event rule IDs by keyword matching in dim_event_rules.
    
    Keyword format:
      - "some keyword"            → simple contains match
      - "EXCLUDE:word|keyword"    → match 'keyword' but only if 'word' is NOT in the name
    """
    rows = conn.execute(
        text("SELECT id, name FROM dim_event_rules WHERE app_id = :app_id ORDER BY id"),
        {"app_id": app_id}
    ).fetchall()

    results = {}
    for col, keywords in EVENT_KEYWORDS.items():
        matched = False
        for keyword in keywords:
            # Parse EXCLUDE: prefix  e.g. "EXCLUDE:after|working hours usage"
            exclude_word = None
            search_term  = keyword
            if keyword.startswith("EXCLUDE:"):
                parts        = keyword[len("EXCLUDE:"):].split("|", 1)
                exclude_word = parts[0].lower()
                search_term  = parts[1].lower() if len(parts) > 1 else ""
            else:
                search_term = keyword.lower()

            for row in rows:
                name_lower = row.name.lower()
                # Skip if exclude word is present in name
                if exclude_word and exclude_word in name_lower:
                    continue
                if search_term in name_lower:
                    log.info(f"  [{col:<20}] app_id={app_id} → id={row.id}  name='{row.name}'  (matched: '{keyword}')")
                    results[col] = str(row.id)
                    matched = True
                    break
            if matched:
                break
        if not matched:
            log.warning(f"  [{col:<20}] app_id={app_id} → NO MATCH for {keywords}")
            results[col] = None

    return results


def update_customer(conn, app_id: str, updates: dict):
    """Apply all found IDs to customer_configs."""
    # Build SET clause only for non-None values
    set_parts = []
    params = {"app_id": app_id}
    for col, val in updates.items():
        if val is not None:
            set_parts.append(f"{col} = :{col}")
            params[col] = val

    if not set_parts:
        log.error(f"  No values to update for app_id={app_id}")
        return

    sql = f"UPDATE customer_configs SET {', '.join(set_parts)}, updated_at = NOW() WHERE app_id = :app_id"
    result = conn.execute(text(sql), params)
    log.info(f"  → Updated {result.rowcount} row(s) in customer_configs for app_id={app_id}")


def verify(conn):
    """Print final state of customer_configs."""
    rows = conn.execute(text("""
        SELECT
            app_id, name,
            tag_id,
            trip_report_id,
            event_report_id,
            event_id_speed,
            event_id_idle,
            event_id_awh,
            event_id_ha,
            event_id_hb,
            event_id_hc,
            event_id_wu,
            event_id_wh
        FROM customer_configs
        ORDER BY app_id
    """)).fetchall()

    print("\n" + "=" * 70)
    print("CUSTOMER CONFIG VERIFICATION")
    print("=" * 70)
    for row in rows:
        print(f"\n  Customer : {row.name} (app_id={row.app_id})")
        print(f"  tag_id          : {row.tag_id}")
        print(f"  trip_report_id  : {row.trip_report_id}")
        print(f"  event_report_id : {row.event_report_id}")
        print(f"  Speed           : {row.event_id_speed}")
        print(f"  Idle            : {row.event_id_idle}")
        print(f"  AWH             : {row.event_id_awh}")
        print(f"  HA              : {row.event_id_ha}")
        print(f"  HB              : {row.event_id_hb}")
        print(f"  HC              : {row.event_id_hc}")
        print(f"  WU              : {row.event_id_wu}")
        print(f"  WH              : {row.event_id_wh}")
    print("=" * 70 + "\n")


def main():
    app = create_app()
    with app.app_context():
        # First make sure all required columns exist
        with db.engine.connect() as conn:
            log.info("Ensuring customer_configs has all required columns...")
            conn.execute(text("""
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS trip_report_id  VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_report_id VARCHAR(50);
                ALTER TABLE customer_configs DROP COLUMN IF EXISTS report_id;
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_speed  VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_idle   VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_awh    VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_ha     VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_hb     VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_hc     VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_wu     VARCHAR(50);
                ALTER TABLE customer_configs ADD COLUMN IF NOT EXISTS event_id_wh     VARCHAR(50);
            """))
            conn.commit()
            log.info("Columns ready.")

            # Get all active customers
            customers = conn.execute(
                text("SELECT app_id, name FROM customer_configs WHERE is_active = TRUE ORDER BY app_id")
            ).fetchall()

            if not customers:
                log.error("No active customers found in customer_configs. Run insert_customers.sql first.")
                sys.exit(1)

            log.info(f"Found {len(customers)} active customer(s): {[c.name for c in customers]}")

            all_ok = True
            for customer in customers:
                app_id = customer.app_id
                print(f"\n{'─' * 60}")
                log.info(f"Processing: {customer.name} (app_id={app_id})")
                print(f"{'─' * 60}")

                tag_id          = find_tag_id(conn, app_id)
                trip_report_id  = find_trip_report_id(conn, app_id)
                event_report_id = find_event_report_id(conn, app_id)
                event_ids       = find_event_ids(conn, app_id)

                updates = {
                    "tag_id":          tag_id,
                    "trip_report_id":  trip_report_id,
                    "event_report_id": event_report_id,
                    **event_ids
                }

                # Check for any missing values
                missing = [k for k, v in updates.items() if v is None]
                if missing:
                    log.warning(f"  Missing IDs for {customer.name}: {missing}")
                    all_ok = False

                update_customer(conn, app_id, updates)

            conn.commit()

            # Print final verification table
            verify(conn)

            if all_ok:
                log.info("✅ All customers configured successfully — no missing IDs.")
            else:
                log.warning("⚠️  Some IDs could not be matched. Check warnings above and update keywords in this script.")


if __name__ == "__main__":
    main()