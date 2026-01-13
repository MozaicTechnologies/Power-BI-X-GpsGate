from flask import Blueprint, request, jsonify
import requests
from urllib.parse import urljoin

api_bp = Blueprint('api_bp', __name__)

@api_bp.route('/api', methods=['POST'])
def call_api():
    """
    Generic GpsGate proxy (no DB writes).
    Body can be JSON or form-encoded with:
    {
      "method": "GET" | "POST" | "PUT" | "PATCH" | "DELETE",   # default "GET"
      "base_url": "https://your-gpsgate.example/",             # required
      "path": "comGpsGate/api/v.1/applications/123/tags",      # required (relative)
      "params": {...},                                         # optional (query string)
      "headers": {"Authorization": "Bearer ..."},              # optional; token is auto-merged
      "token": "Bearer ...",                                   # optional; merged into headers if provided
      "json": {...},                                           # optional (JSON body)
      "data": "raw-or-form-body",                              # optional (raw/form body)
      "timeout": 30                                            # optional (seconds)
    }
    """
    data = request.form or request.get_json() or {}

    method = (data.get('method') or 'GET').upper()
    base_url = data.get('base_url')
    path = data.get('path')
    params = data.get('params') or None
    json_payload = data.get('json')
    data_payload = data.get('data')
    timeout = float(data.get('timeout') or 30)

    headers = dict(data.get('headers') or {})
    token = data.get('token')
    if token and 'Authorization' not in headers:
        headers['Authorization'] = token

    if not path:
        return jsonify({"ok": False, "error": "Missing required parameter: path"}), 400
    if not base_url:
        return jsonify({"ok": False, "error": "Missing required parameter: base_url"}), 400

    # Safe join (force single slash)
    base = base_url if base_url.endswith('/') else base_url + '/'
    url = urljoin(base, path.lstrip('/'))

    try:
        resp = requests.request(
            method=method,
            url=url,
            params=params,
            json=json_payload,
            data=data_payload,
            headers=headers,
            timeout=timeout,
        )
    except requests.RequestException as e:
        return jsonify({"ok": False, "error": "Network error", "details": str(e)}), 502

    # Parse body (JSON if possible)
    body = None
    ctype = (resp.headers.get('Content-Type') or '').lower()
    try:
        body = resp.json() if 'application/json' in ctype else resp.text
    except Exception:
        body = resp.text

    if not resp.ok:
        # bubble up GpsGate error body and status
        return jsonify({"ok": False, "status": resp.status_code, "data": body}), 502

    return jsonify({"ok": True, "status": resp.status_code, "data": body}), 200


def render_endpoint(payload):
    """
    Direct GpsGate API call to render a report.
    
    Args:
        payload (dict): {
            "app_id": "6",
            "period_start": "2025-01-01 00:00:00",
            "period_end": "2025-01-07 23:59:59",
            "tag_id": "39",
            "report_id": "1225",
            "token": "v2:...",
            "base_url": "https://omantracking2.com",
            "event_id": "18"  # optional
        }
    
    Returns:
        dict: {
            "status_code": 200,
            "data": {"id": "...", "render_id": "..."}
        }
    """
    try:
        app_id = payload.get("app_id")
        token = payload.get("token")
        base_url = payload.get("base_url")
        report_id = payload.get("report_id")
        tag_id = payload.get("tag_id")
        event_id = payload.get("event_id")
        period_start = payload.get("period_start")
        period_end = payload.get("period_end")
        
        # Build GpsGate API URL
        url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"
        
        # Build request body
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
                "arrayValues": [str(tag_id)]
            }
        ]
        
        # Add EventRule parameter if event_id provided
        if event_id:
            parameters.append({
                "parameterName": "EventRule",
                "arrayValues": [str(event_id)]
            })
        
        body = {
            "parameters": parameters,
            "reportFormatId": 2,
            "reportId": int(report_id),
            "sendEmail": False
        }
        
        # Headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": token
        }
        
        # Make request
        resp = requests.post(url, json=body, headers=headers, timeout=(10, 30))
        
        if resp.status_code != 200:
            return {
                "status_code": resp.status_code,
                "data": resp.json() if resp.headers.get('Content-Type', '').lower().find('json') >= 0 else {"error": resp.text}
            }
        
        response_data = resp.json()
        
        # GpsGate returns {"id": "..."}, map it to "render_id"
        return {
            "status_code": 200,
            "data": {
                "render_id": response_data.get("id") or response_data.get("render_id")
            }
        }
    
    except Exception as e:
        import sys
        print(f"[ERROR] render_endpoint failed: {str(e)}", file=sys.stderr)
        return {
            "status_code": 500,
            "data": {"error": str(e)}
        }


def result_endpoint(payload):
    """
    Direct GpsGate API call to get report result (download link).
    
    Args:
        payload (dict): {
            "app_id": "6",
            "render_id": "...",
            "token": "v2:...",
            "base_url": "https://omantracking2.com",
            "report_id": "1225"
        }
    
    Returns:
        dict: {
            "status_code": 200,
            "data": {"gdrive_link": "..."}
        }
    """
    try:
        app_id = payload.get("app_id")
        token = payload.get("token")
        base_url = payload.get("base_url")
        report_id = payload.get("report_id")
        render_id = payload.get("render_id")
        
        # Build GpsGate API URL
        url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}/result"
        
        # Headers
        headers = {
            "Authorization": token
        }
        
        # Make request (can take longer for CSV)
        resp = requests.get(url, headers=headers, timeout=(10, 120))
        
        if resp.status_code != 200:
            return {
                "status_code": resp.status_code,
                "data": resp.json() if resp.headers.get('Content-Type', '').lower().find('json') >= 0 else {"error": resp.text}
            }
        
        response_data = resp.json()
        
        # Return GDrive link or direct link
        return {
            "status_code": 200,
            "data": {
                "gdrive_link": response_data.get("gdrive_link") or response_data.get("link") or response_data
            }
        }
    
    except Exception as e:
        import sys
        print(f"[ERROR] result_endpoint failed: {str(e)}", file=sys.stderr)
        return {
            "status_code": 500,
            "data": {"error": str(e)}
        }


def download_csv_from_gdrive(gdrive_link, auth_token=None):
    """
    Download CSV content from GDrive or GpsGate.
    
    Args:
        gdrive_link (str): URL to CSV file
        auth_token (str): Optional GpsGate token for authorization
    
    Returns:
        str: CSV content, or None if failed
    """
    try:
        headers = {}
        if auth_token and "omantracking2.com" in gdrive_link:
            headers["Authorization"] = auth_token
        
        resp = requests.get(gdrive_link, headers=headers, timeout=(10, 120))
        
        if resp.status_code == 200:
            return resp.text
        else:
            import sys
            print(f"[ERROR] CSV download failed: {resp.status_code}", file=sys.stderr)
            return None
    
    except Exception as e:
        import sys
        print(f"[ERROR] CSV download exception: {str(e)}", file=sys.stderr)
        return None
