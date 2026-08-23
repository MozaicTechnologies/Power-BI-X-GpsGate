
from flask import Blueprint, request, jsonify
from app.services.gpsgate_reports import create_report_render

render_bp = Blueprint("render", __name__)


def _payload_dict():
    """Accept JSON or x-www-form-urlencoded and normalize to a dict."""
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    if request.form:
        return request.form.to_dict(flat=True)
    return {}


@render_bp.route("/health")
def health():
    return "ok"


@render_bp.route("/render", methods=["POST"])
def render_report():
    """Handle render requests using the report model's parameter schema."""
    payload = _payload_dict()
    data, status = create_report_render(payload)
    return jsonify(data), status
