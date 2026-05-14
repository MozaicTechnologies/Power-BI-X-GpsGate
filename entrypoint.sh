#!/bin/bash
set -e

echo "[ENTRYPOINT] Setting up database..."

if [ ! -d "migrations" ]; then
    echo "[ENTRYPOINT] No migrations folder found. Initializing..."
    flask db init
    flask db migrate -m "initial schema"
fi

flask db upgrade

echo "[ENTRYPOINT] Starting application..."
exec "$@"

#OR USE THESE COMMANDS
# docker compose exec web flask db migrate -m "add duration_s to fact_trip"
# docker compose exec web flask db upgrade
