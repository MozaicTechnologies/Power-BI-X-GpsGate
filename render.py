from flask import Blueprint, request, jsonify
import requests
from models import db, Render

render_bp = Blueprint('render_bp', __name__)

@render_bp.route('/render', methods=['POST'])
def handle_render():
    data = request.form or request.get_json()

    app_id = data.get('app_id')
    period_start = data.get('period_start')
    period_end = data.get('period_end')
    tag_id = data.get('tag_id')
    report_id = data.get('report_id')
    token = data.get('token')
    base_url = data.get('base_url')
    event_id = data.get('event_id')

    if not all([app_id, period_start, period_end, tag_id, report_id, token, base_url]):
        return jsonify({"error": "Missing required parameters."}), 400

    # Check for existing record
    existing = Render.query.filter_by(
        app_id=app_id,
        period_start=period_start,
        period_end=period_end,
        tag_id=tag_id,
        report_id=report_id,
        event_id=event_id
    ).first()

    if existing:
        return jsonify({
            "render_id": existing.render_id,
            "report_id": existing.report_id
        }), 200

    # Prepare payload for GpsGate API
    parameters = [
        {
            "parameterName": "Period",
            "periodStart": period_start,
            "periodEnd": period_end,
            "value": "Custom",
            "visible": False
        },
        {
            "parameterName": "Tag" if event_id else "TagID",
            "arrayValues": [tag_id]
        }
    ]

    if event_id:
        parameters.append({
            "parameterName": "EventRule",
            "arrayValues": [event_id]
        })

    payload = {
        "parameters": parameters,
        "reportFormatId": 2,
        "reportId": report_id
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": token
    }

    url = f"{base_url}comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"
    print(url)
    print(payload)
    print(headers)

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code != 200:
            return jsonify({"error": "GpsGate API error", "details": response.text}), 502

        render_data = response.json()
        render_id = render_data.get('id')

        if not render_id:
            return jsonify({"error": "Render ID not returned."}), 500

        # Insert new record
        new_record = Render(
            app_id=app_id,
            period_start=period_start,
            period_end=period_end,
            tag_id=tag_id,
            event_id=event_id,
            report_id=report_id,
            render_id=render_id
        )
        db.session.add(new_record)
        db.session.commit()

        return jsonify({
            "app_id": app_id,
            "period_start": period_start,
            "period_end": period_end,
            "tag_id": tag_id,
            "event_id": event_id,
            "report_id": report_id,
            "render_id": render_id
        }), 200

    except Exception as e:
        return jsonify({"error": "Server error", "details": str(e)}), 500
