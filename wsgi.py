from app import create_app

# Create app with all blueprints already registered
# (backfill_scheduler blueprint is registered in application.py)
app = create_app()

from app.models import init_db

init_db(app)