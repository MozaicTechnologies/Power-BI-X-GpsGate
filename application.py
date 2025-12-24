# from flask import Flask
# from models import db
# from config import Config
# from flask_migrate import Migrate

# def create_app():
#     app = Flask(__name__)
#     app.json.ensure_ascii = False
#     app.json.sort_keys = False
#     app.json.allow_nan = False
#     app.config.from_object(Config)
#     db.init_app(app)

#     ## with app.app_context():
#     ##    db.create_all()
#     import models

#     Migrate(app, db, render_as_batch=True)
    
#     return app

#-----------------------------------------------
from flask import Flask
from flask.json.provider import DefaultJSONProvider # Add this
from models import db
from config import Config
from flask_migrate import Migrate
import numpy as np
import pandas as pd

# This replaces the old PandasJSONEncoder class
class CustomJSONProvider(DefaultJSONProvider):
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, (pd.Timestamp, pd.DatetimeIndex)):
            return obj.isoformat()
        return super().default(obj)

def create_app():
    app = Flask(__name__)
    
    # Set the modern JSON Provider
    app.json_provider_class = CustomJSONProvider
    app.json = CustomJSONProvider(app)

    app.json.ensure_ascii = False
    app.json.sort_keys = False
    app.json.allow_nan = False
    
    app.config.from_object(Config)
    db.init_app(app)
    
    from models import Render, Result, FactIdle, FactSpeeding, FactAWH, FactHA, FactHB, FactWH, FactWU, FactTrip
    with app.app_context():
        import models
    Migrate(app, db, render_as_batch=True)
    
    return app