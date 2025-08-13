from application import create_app
from render import render_bp
from fetch import fetch_bp

app = create_app()
app.register_blueprint(render_bp)
app.register_blueprint(fetch_bp)
