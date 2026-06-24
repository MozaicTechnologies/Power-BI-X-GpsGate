import time
import requests
from flask import Blueprint, request, jsonify
from app.utils.logger import setup_logger

result_bp = Blueprint("result", __name__)
logger = setup_logger("RESULT")

DEFAULT_TIMEOUT = 60


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

    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    token = payload.get("token")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    rendering_id = payload.get("rendering_id") or payload.get("render_id")

    if not all([base_url, token, app_id, rendering_id]):
        missing = [k for k, v in {
            "base_url": base_url, "token": bool(token),
            "app_id": app_id, "rendering_id": rendering_id,
        }.items() if not v]
        logger.warning("fetch_result | MISSING_FIELDS | app_id=%s missing=%s", app_id, missing)
        return jsonify({
            "ok": False,
            "error": "Missing required fields: base_url, token, app_id, rendering_id",
            "received": sorted(list(payload.keys()))
        }), 400

    headers = {
        "Accept": "application/json",
        "Authorization": token
    }

    if report_id:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}"
    else:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}"

    logger.info("fetch_result | START | app_id=%s rendering_id=%s report_id=%s",
                app_id, rendering_id, report_id)
    logger.debug("fetch_result | polling_url=%s", status_url)

    max_wait_s = 300
    waited = 0
    sleep_s = 2

    while waited < max_wait_s:
        try:
            resp = requests.get(status_url, headers=headers, timeout=DEFAULT_TIMEOUT)

            if resp.status_code != 200:
                if resp.status_code in (429, 500, 502, 503, 504):
                    logger.debug("fetch_result | TRANSIENT_ERROR | app_id=%s rendering_id=%s status=%d waited=%ds",
                                 app_id, rendering_id, resp.status_code, waited)
                    time.sleep(sleep_s)
                    waited += sleep_s
                    continue

                logger.error("fetch_result | HTTP_ERROR | app_id=%s rendering_id=%s status=%d",
                             app_id, rendering_id, resp.status_code)
                return jsonify({
                    "ok": False,
                    "error": "Failed to check render status",
                    "status": resp.status_code,
                    "response": resp.text[:500] if resp.text else None
                }), 502

            data = resp.json() if resp.text else {}

            if data.get("isReady") is True:
                output_file = data.get("outputFile")

                if not output_file:
                    logger.error("fetch_result | NO_OUTPUT_FILE | app_id=%s rendering_id=%s",
                                 app_id, rendering_id)
                    return jsonify({
                        "ok": False,
                        "error": "No output file generated",
                        "response": data
                    }), 500

                gdrive_link = f"{base_url}{output_file}" if output_file.startswith("/") else output_file

                logger.info("fetch_result | READY | app_id=%s rendering_id=%s waited=%ds",
                            app_id, rendering_id, waited)

                return jsonify({
                    "ok": True,
                    "gdrive_link": gdrive_link,
                    "link": gdrive_link,
                    "rendering_id": str(rendering_id)
                }), 200

            if waited > 0 and waited % 30 == 0:
                logger.debug("fetch_result | WAITING | app_id=%s rendering_id=%s waited=%ds",
                             app_id, rendering_id, waited)

            time.sleep(sleep_s)
            waited += sleep_s
            sleep_s = min(10, sleep_s * 1.5)

        except Exception:
            logger.exception("fetch_result | EXCEPTION | app_id=%s rendering_id=%s waited=%ds",
                             app_id, rendering_id, waited)
            time.sleep(sleep_s)
            waited += sleep_s

    logger.warning("fetch_result | TIMEOUT | app_id=%s rendering_id=%s waited=%ds",
                   app_id, rendering_id, waited)
    return jsonify({
        "ok": False,
        "error": "Render timeout",
        "waited_seconds": waited
    }), 504
