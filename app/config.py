
import os
import logging
from celery.schedules import crontab

_log = logging.getLogger("CONFIG")


class Config:

    SECRET_KEY = os.getenv("SECRET_KEY")

    raw_url = os.getenv("DATABASE_URL")

    if not raw_url:
        _log.warning("DATABASE_URL is not set — falling back to SQLite for local development")
        raw_url = "sqlite:////tmp/render.db"

    # Normalize postgres:// and postgresql:// to SQLAlchemy+psycopg driver format
    if raw_url.startswith("postgres://"):
        raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw_url.startswith("postgresql://") and not raw_url.startswith("postgresql+psycopg://"):
        raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)

    SQLALCHEMY_DATABASE_URI = raw_url
    _log.info("Database driver configured: %s", raw_url.split("://")[0] if "://" in raw_url else "unknown")

    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_POOL_SIZE = 10       # base connections kept open
    SQLALCHEMY_MAX_OVERFLOW = 10    # extra connections allowed under load
    SQLALCHEMY_POOL_TIMEOUT = 30    # seconds to wait for a connection
    SQLALCHEMY_POOL_RECYCLE = 300   # recycle idle connections every 5 min

    # ------------------------------------------------------------------
    # Celery
    # ------------------------------------------------------------------
    CELERY = {
        "broker_url":            os.getenv("CELERY_BROKER_URL",    "redis://localhost:6379/0"),
        "result_backend":        os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/0"),
        "task_serializer":       "json",
        "result_serializer":     "json",
        "accept_content":        ["json"],
        "result_expires":        86400,   # keep results 24 h
        "task_track_started":    True,
        "task_send_sent_event":  True,
        "worker_send_task_events": True,
        "beat_schedule": {
            "daily-sync": {
                "task":     "tasks.daily_sync",
                "schedule": crontab(hour=2, minute=0),
            },
            "weekly-backfill": {
                "task":     "tasks.weekly_backfill",
                "schedule": crontab(hour=3, minute=0, day_of_week=1),
            },
        },
    }

    # GpsGate API settings
    TOKEN = os.getenv("TOKEN")
    BASE_URL = os.getenv("BASE_URL", "https://omantracking2.com")

    if TOKEN:
        _log.info("GpsGate TOKEN is configured (length=%d)", len(TOKEN))
    else:
        _log.warning("GpsGate TOKEN env var is not set")
