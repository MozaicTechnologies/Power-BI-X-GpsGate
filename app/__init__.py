from dotenv import load_dotenv
load_dotenv()

from flask import Flask
from .models import db
from .config import Config
from flask_migrate import Migrate
from .routes.auth import login_manager, limiter


def create_app():
    app = Flask(__name__)
    app.json.ensure_ascii = False
    app.json.sort_keys = False
    app.json.allow_nan = False
    app.config.from_object(Config)

    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'
    app.config['SESSION_COOKIE_SECURE'] = not app.debug

    db.init_app(app)
    login_manager.init_app(app)
    limiter.init_app(app)

    import app.models

    Migrate(app, db, render_as_batch=True)

    from .routes.auth import auth_bp
    app.register_blueprint(auth_bp)

    from .routes.render import render_bp
    from .routes.result import result_bp
    app.register_blueprint(render_bp)
    app.register_blueprint(result_bp)

    from .routes.backfill import backfill_api
    app.register_blueprint(backfill_api)

    from .routes.api import api_bp
    app.register_blueprint(api_bp)

    from .routes.dashboard import dashboard_bp
    app.register_blueprint(dashboard_bp)

    from .routes.pipeline import pipeline_bp
    app.register_blueprint(pipeline_bp)

    return app
