#!/bin/bash
set -e

echo "[ENTRYPOINT] Running database migrations..."
flask db upgrade

echo "[ENTRYPOINT] Starting gunicorn..."
exec "$@"
