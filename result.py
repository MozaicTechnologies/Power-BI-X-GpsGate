import time
import requests
from flask import Blueprint, request, jsonify, Response

result_bp = Blueprint("result", __name__)

DEFAULT_TIMEOUT = 60

@result_bp.route("/health")
def health():
    return "ok"

@result_bp.route("/result", methods=["POST"])
def fetch_result():
    """Poll for render result and return download link or file content"""
    # Accept both JSON and form data
    if request.is_json:
        payload = request.get_json()
    else:
        payload = request.form.to_dict()

    print(f"[RESULT] Received payload keys: {sorted(list(payload.keys()))}")

    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    token = payload.get("token")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    
    # rendering_id can come as render_id or rendering_id
    rendering_id = payload.get("rendering_id") or payload.get("render_id")

    if not all([base_url, token, app_id, rendering_id]):
        return jsonify({
            "ok": False,
            "error": "Missing required fields: base_url, token, app_id, rendering_id",
            "received": sorted(list(payload.keys()))
        }), 400

    headers = {
        "Accept": "application/json",
        "Authorization": token
    }

    # Poll the rendering status
    if report_id:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}"
    else:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}"

    print(f"[RESULT] Polling: {status_url}")

    max_wait_s = 300
    waited = 0
    sleep_s = 2

    while waited < max_wait_s:
        try:
            resp = requests.get(status_url, headers=headers, timeout=DEFAULT_TIMEOUT)
            
            if resp.status_code != 200:
                # Retry on transient errors
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(sleep_s)
                    waited += sleep_s
                    continue
                
                # Fail on auth/not found errors
                return jsonify({
                    "ok": False,
                    "error": "Failed to check render status",
                    "status": resp.status_code,
                    "response": resp.text[:500] if resp.text else None
                }), 502

            data = resp.json() if resp.text else {}
            
            # Check if ready
            if data.get("isReady") is True:
                output_file = data.get("outputFile")
                
                if not output_file:
                    return jsonify({
                        "ok": False,
                        "error": "No output file generated",
                        "response": data
                    }), 500

                # Build full URL
                if output_file.startswith("/"):
                    gdrive_link = f"{base_url}{output_file}"
                else:
                    gdrive_link = output_file

                print(f"[RESULT] Success! Link: {gdrive_link}")

                return jsonify({
                    "ok": True,
                    "gdrive_link": gdrive_link,
                    "link": gdrive_link,
                    "rendering_id": str(rendering_id)
                }), 200

            # Not ready yet, wait
            if waited > 0 and waited % 10 == 0:
                print(f"[RESULT] Still waiting... {waited}s elapsed")

            time.sleep(sleep_s)
            waited += sleep_s
            sleep_s = min(10, sleep_s * 1.5)

        except Exception as e:
            print(f"[RESULT] Exception: {e}")
            time.sleep(sleep_s)
            waited += sleep_s

    print(f"[RESULT] Timeout after {waited}s")
    return jsonify({
        "ok": False,
        "error": "Render timeout",
        "waited_seconds": waited
    }), 504
