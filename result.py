from flask import Blueprint, request, jsonify
import requests
from models import db, Result

result_bp = Blueprint('result_bp', __name__)

@result_bp.route('/result', methods=['POST'])
def handle_result():
    """
    Get the result/download link for a completed render.
    First checks the Result cache table, then polls GpsGate API if needed.
    """
    try:
        data = request.form or request.get_json()
        print(f"Result request data: {data}")

        app_id = data.get('app_id')
        render_id = data.get('render_id')
        report_id = data.get('report_id')
        token = data.get('token')
        base_url = data.get('base_url')

        if not all([app_id, render_id, report_id, token, base_url]):
            return jsonify({"error": "Missing required parameters"}), 400

        # FIRST: Check if we have a cached result
        print(f"[DEBUG] Checking Result cache for render_id: {render_id}")
        cached_result = Result.query.filter_by(render_id=str(render_id)).first()
        
        if cached_result and cached_result.gdrive_link:
            print(f"[OK] Found cached result with gdrive_link: {cached_result.gdrive_link[:60]}...")
            return jsonify({
                "gdrive_link": cached_result.gdrive_link,
                "link": cached_result.gdrive_link
            }), 200

        # If no cache, try to poll GpsGate API
        print(f"[DEBUG] No cached result, polling GpsGate API...")
        
        # Poll the rendering status to get the outputFile
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}"
        headers = {
            "Authorization": token
        }

        # Poll for up to 2 minutes
        max_attempts = 120
        for attempt in range(max_attempts):
            status_resp = requests.get(status_url, headers=headers, timeout=(10, 30))
            
            if status_resp.status_code != 200:
                print(f"[ERROR] Status check failed: {status_resp.status_code}")
                return jsonify({"error": "Failed to check render status"}), 502
            
            status_data = status_resp.json()
            
            if status_data.get("isReady"):
                # Report is ready, extract output file
                output_file = status_data.get("outputFile")
                
                if not output_file:
                    print(f"[ERROR] No outputFile in ready response")
                    return jsonify({"error": "No output file generated"}), 500
                
                # Build full download URL if it's a relative path
                if output_file.startswith("/"):
                    gdrive_link = f"{base_url}{output_file}"
                else:
                    gdrive_link = output_file
                
                print(f"[OK] Output file ready: {gdrive_link}")
                return jsonify({
                    "gdrive_link": gdrive_link,
                    "link": gdrive_link
                }), 200
            
            # Not ready yet, wait and retry
            if attempt > 0 and attempt % 10 == 0:
                print(f"[DEBUG] Waiting for render... {attempt}s elapsed")
            
            import time
            time.sleep(1)
        
        # Timeout
        print(f"[ERROR] Render not ready after {max_attempts}s")
        return jsonify({"error": "Render timeout"}), 504

    except Exception as e:
        import traceback
        print(f"[ERROR] Error in /result: {str(e)}")
        print(traceback.format_exc())
        return jsonify({"error": "Server error", "details": str(e)}), 500
