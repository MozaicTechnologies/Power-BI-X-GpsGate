from flask import Blueprint, request, jsonify
from models import db, Render

fetch_bp = Blueprint('fetch_bp', __name__)

@fetch_bp.route('/fetch', methods=['POST'])
def handle_fetch():
    data = request.form or request.get_json()

    app_id = data.get('app_id')
    tag_id = data.get('tag_id')
    report_id = data.get('report_id')

    # ── 3. Validation
    if not all([app_id, tag_id, report_id]):
        return jsonify({"error": "Missing required parameters"}), 400

    try:
        results = Render.query.filter_by(
            app_id=app_id,
            tag_id=tag_id,
            report_id=report_id
        ).all()

        rows = [
            {
                "app_id": row.app_id,
                "tag_id": row.tag_id,
                "report_id": row.report_id,
                "render_id": row.render_id
            }
            for row in results
        ]

        return jsonify(rows), 200

    except Exception as e:
        return jsonify({"error": "Server error", "details": str(e)}), 500
