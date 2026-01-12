from application import create_app
from render import render_bp
from fetch import fetch_bp
from result import result_bp
from fix import fix_bp
from gpsgate_api import api_bp
from data_pipeline import pipeline_bp, PandasJSONEncoder
from trip_data_pipeline import trip_bp

app = create_app()
app.json_encoder = PandasJSONEncoder
app.register_blueprint(render_bp)
app.register_blueprint(fetch_bp)
app.register_blueprint(result_bp)
app.register_blueprint(fix_bp)
app.register_blueprint(api_bp)
app.register_blueprint(pipeline_bp)
app.register_blueprint(trip_bp)  # ✓ Trip-specific endpoint with database storage