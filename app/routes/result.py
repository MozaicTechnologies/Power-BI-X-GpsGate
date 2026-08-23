from flask import Blueprint, request, jsonify
from app.services.gpsgate_reports import wait_for_report_result

result_bp = Blueprint("result", __name__)


@result_bp.route("/health")
def health():
    return "ok"


@result_bp.route("/result", methods=["POST"])
def fetch_result():
    """Poll for render result and return download link."""
    if request.is_json:
        payload = request.get_json()
    else:
        payload = request.form.to_dict()

    data, status = wait_for_report_result(payload)
    return jsonify(data), status
