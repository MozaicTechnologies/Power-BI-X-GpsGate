from flask import Blueprint, request, jsonify
import requests
from models import db, Render

render_bp = Blueprint('render_bp', __name__)

@render_bp.route('/render', methods=['POST'])
def handle_render():
    try:
        data = request.form or request.get_json()
        print(f"Received data: {data}")

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
        print(f"Querying with: app_id={app_id}, tag_id={tag_id}, report_id={report_id}")
        existing = Render.query.filter_by(
            app_id=str(app_id),
            period_start=period_start,
            period_end=period_end,
            tag_id=str(tag_id) if tag_id else None,
            report_id=str(report_id),
            event_id=str(event_id) if event_id else None
        ).first()

        if existing:
            print(f"Found existing record: {existing.render_id}")
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
            "reportId": int(report_id),
            "sendEmail": False
        }

        headers = {
            "Content-Type": "application/json",
            "Authorization": token
        }

        from urllib.parse import urljoin
        path = f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"
        url = urljoin(base_url if base_url.endswith('/') else base_url + '/', path)
        print(f"URL: {url}")
        print(f"Payload: {payload}")
        print(f"Headers: {headers}")

        response = requests.post(url, json=payload, headers=headers, timeout=30)
        print(f"GpsGate API Status: {response.status_code}")
        print(f"GpsGate API Response: {response.text}")
        print(f"GpsGate API Headers: {response.headers}")
        
        if response.status_code != 200:
            # Try to parse error details
            try:
                error_json = response.json()
                error_details = str(error_json)
            except:
                error_details = response.text
            print(f"Error details: {error_details}")
            return jsonify({"error": "GpsGate API error", "status": response.status_code, "details": error_details}), 502

        render_data = response.json()
        render_id = str(render_data.get('id'))

        if not render_id or render_id == 'None':
            return jsonify({"error": "Render ID not returned.", "response": render_data}), 500

        # Insert new record - convert all IDs to strings to match VARCHAR columns
        new_record = Render(
            app_id=str(app_id),
            period_start=period_start,
            period_end=period_end,
            tag_id=str(tag_id) if tag_id else None,
            event_id=str(event_id) if event_id else None,
            report_id=str(report_id),
            render_id=render_id
        )
        db.session.add(new_record)
        db.session.commit()
        print(f"Created new record with render_id: {render_id}")

        return jsonify({
            "app_id": str(app_id),
            "period_start": period_start,
            "period_end": period_end,
            "tag_id": str(tag_id) if tag_id else None,
            "event_id": str(event_id) if event_id else None,
            "report_id": str(report_id),
            "render_id": render_id
        }), 200

    except requests.exceptions.RequestException as req_err:
        import traceback
        print(f"Request error in /render: {str(req_err)}")
        print(traceback.format_exc())
        return jsonify({"error": "Request failed", "details": str(req_err)}), 502
    except Exception as e:
        import traceback
        print(f"Error in /render: {str(e)}")
        print(traceback.format_exc())
        return jsonify({"error": "Server error", "details": str(e)}), 500