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
#comment
