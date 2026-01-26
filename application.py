from dotenv import load_dotenv
load_dotenv()  # Load .env file first

from flask import Flask
from models import db
from config import Config
from flask_migrate import Migrate

def create_app():
    app = Flask(__name__)
    app.json.ensure_ascii = False
    app.json.sort_keys = False
    app.json.allow_nan = False
    app.config.from_object(Config)
    db.init_app(app)

    ## with app.app_context():
    ##    db.create_all()
    import models

    Migrate(app, db, render_as_batch=True)
    
    # Register render and result blueprints
    from render import render_bp
    from result import result_bp
    app.register_blueprint(render_bp)
    app.register_blueprint(result_bp)
    
    # Register backfill scheduler API
    from backfill_scheduler import backfill_api
    app.register_blueprint(backfill_api)
    
    # Register manual backfill/fetch API
    from api import api_bp
    app.register_blueprint(api_bp)
    
    # Register enhanced dashboard with manual triggers
    from dashboard_enhanced import dashboard_bp
    app.register_blueprint(dashboard_bp)
    
    return app
