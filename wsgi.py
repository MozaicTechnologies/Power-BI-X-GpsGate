# from application import create_app
# from render import render_bp
# from fetch import fetch_bp
# from result import result_bp
# from fix import fix_bp
# from gpsgate_api import api_bp
# from data_pipeline import pipeline_bp, PandasJSONEncoder
# # from data_pipeline import pipeline_bp, PandasJSONEncoder

# app = create_app()
# app.json_encoder = PandasJSONEncoder
# app.register_blueprint(render_bp)
# app.register_blueprint(fetch_bp)
# app.register_blueprint(result_bp)
# app.register_blueprint(fix_bp)
# app.register_blueprint(api_bp)
# app.register_blueprint(pipeline_bp)

#-------------------------------------------

from application import create_app
from render import render_bp
from fetch import fetch_bp
from result import result_bp
from fix import fix_bp
from gpsgate_api import api_bp
from data_pipeline import pipeline_bp

app = create_app()

# Register Blueprints
app.register_blueprint(render_bp)
app.register_blueprint(fetch_bp)
app.register_blueprint(result_bp)
app.register_blueprint(fix_bp)
app.register_blueprint(api_bp)
app.register_blueprint(pipeline_bp)