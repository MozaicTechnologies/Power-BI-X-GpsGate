from flask import Blueprint, request, jsonify
import requests
from models import db, Result
from gdrive import upload_bytes_as_csv
import requests, re
from urllib.parse import urlparse, urljoin
from datetime import datetime, timezone


result_bp = Blueprint('result_bp', __name__)

def _ok_null(app_id, report_id, render_id, filepath=None, reason="pending"):
    # Always 200 for Power BI; null link signals "not ready yet"
    return jsonify({
        "app_id": app_id,
        "report_id": report_id,
        "render_id": render_id,
        "filepath": filepath,      # temp/None
        "gdrive_link": None,       # IMPORTANT: null for PBI
        "reason": reason           # optional hint for debugging
    }), 200

def make_csv_filename(app_id: str, report_id: str, render_id: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")  # UTC, sortable
    name = f"{app_id}_{report_id}_r{render_id}_{ts}.csv"
    return re.sub(r"[^A-Za-z0-9._-]+", "-", name)  # sanitize just in case

@result_bp.route('/result', methods=['POST'])
def handle_result():
    data = request.form or request.get_json()

    app_id = data.get('app_id')
    report_id = data.get('report_id')
    token = data.get('token')
    base_url = data.get('base_url')
    render_id = data.get('render_id')

    if not all([app_id, render_id, report_id, token, base_url]):
        return _ok_null(app_id, report_id, render_id, reason="missing_params")

    # Check for existing record
    existing = Result.query.filter_by(
        app_id=app_id,
        render_id=render_id,
        report_id=report_id
    ).first()

    if existing:
        return jsonify({
            "render_id": existing.render_id,
            "report_id": existing.report_id,
            "filepath": existing.filepath,
            "gdrive_link": existing.gdrive_link
        }), 200

    # Prepare payload for GpsGate API

    headers = {
        "Content-Type": "application/json",
        "Authorization": token
    }

    from urllib.parse import urljoin
    # Ensure render_id is integer
    render_id = int(render_id) if isinstance(render_id, str) else render_id
    path = f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}"
    url = urljoin(base_url if base_url.endswith('/') else base_url + '/', path)
    print(url)
    print(headers)

    try:
        response = requests.get(url, headers=headers, timeout=60)
        if response.status_code != 200:
            return _ok_null(app_id, report_id, render_id, reason=f"upstream_error:{response.status_code}")

        result_data = response.json()
        filepath = result_data.get('outputFile')

        if not filepath:
            return _ok_null(app_id, report_id, render_id, reason="not_ready")

    # Download the CSV from GpsGate (Authorization usually required)

        file_url = filepath if urlparse(filepath).scheme else urljoin(base_url, filepath.lstrip("/"))
        file_resp = requests.get(file_url, headers=headers, timeout=120, stream=True)
        if file_resp.status_code != 200:
            return _ok_null(app_id, report_id, render_id, reason="Failed to download CSV from GpsGate")
        content = file_resp.content

    # Try to get a friendly filename from headers, fallback to a pattern
        filename = make_csv_filename(app_id, report_id, render_id)

    # Upload to Google Drive (public link)
        try:
            file_id, direct_link = upload_bytes_as_csv(content, filename)
        except Exception as e:
            return _ok_null(app_id, report_id, render_id, reason="Drive upload failed")


        # Insert new record
        new_record = Result(
            app_id=app_id,
            filepath=filepath,
            report_id=report_id,
            render_id=render_id,
            gdrive_file_id=file_id,
            gdrive_link=direct_link,
            uploaded_at=datetime.utcnow()
        )
        db.session.add(new_record)
        db.session.commit()

        return jsonify({
            "app_id": app_id,
            "filepath": filepath,
            "report_id": report_id,
            "render_id": render_id,
            "gdrive_link": direct_link
        }), 200

    except Exception as e:
        return _ok_null(app_id, report_id, render_id, reason="Server error")
