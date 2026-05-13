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
