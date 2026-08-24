"""
Celery worker entry point.

Start worker:
    celery -A celery_worker worker --loglevel=info --concurrency=2

Start beat scheduler:
    celery -A celery_worker beat --loglevel=info

Start Flower:
    celery -A celery_worker flower --port=5555
"""
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
    force=True,
)

from dotenv import load_dotenv
load_dotenv()

from app import create_app
from app.celery_app import celery

flask_app = create_app()

# Register all tasks
import app.tasks  # noqa: F401

logging.getLogger("CELERY_BOOT").info(
    "Celery ready | timezone=%s enable_utc=%s schedules=%s registered_sync_tasks=%s",
    celery.conf.timezone,
    celery.conf.enable_utc,
    sorted(celery.conf.beat_schedule.keys()),
    sorted(name for name in celery.tasks if name in {"tasks.daily_sync", "tasks.weekly_backfill"}),
)
