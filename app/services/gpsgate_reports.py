"""GpsGate report rendering and result polling without internal HTTP calls."""

import time
from urllib.parse import urljoin

import requests

from app.utils.logger import setup_logger

render_logger = setup_logger("RENDER")
result_logger = setup_logger("RESULT")

RENDER_TIMEOUT = 45
RESULT_REQUEST_TIMEOUT = 60


def _headers(token: str, *, json_body: bool = False) -> dict:
    headers = {"Accept": "application/json", "Authorization": token}
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers


def _build_urls(base_url: str, app_id: str, report_id: str) -> tuple[str, str]:
    base = base_url.rstrip("/") + "/"
    report_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}")
    renderings_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings")
    return report_url, renderings_url


def _merge_parameters(report_parameters, period_start, period_end, tag_id=None, event_id=None):
    merged = []
    for parameter in report_parameters if isinstance(report_parameters, list) else []:
        if not isinstance(parameter, dict):
            continue
        item = dict(parameter)
        name = (item.get("parameterName") or "").strip().lower()
        if name == "period":
            item.update(value="Custom", periodStart=period_start, periodEnd=period_end)
            item["visible"] = item.get("visible", False)
        if tag_id and any(word in name for word in ("group", "tag", "view")) and "arrayValues" in item:
            item["arrayValues"] = [str(tag_id)]
        if event_id and "event" in name and any(word in name for word in ("rule", "id")) and "arrayValues" in item:
            item["arrayValues"] = [str(event_id)]
        merged.append(item)
    return merged


def create_report_render(payload: dict) -> tuple[dict, int]:
    """Create a GpsGate report rendering and return a JSON-ready result."""
    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    token = payload.get("token")
    period_start = payload.get("period_start")
    period_end = payload.get("period_end")
    tag_id = payload.get("tag_id")
    event_id = payload.get("event_id")

    required = {
        "base_url": base_url, "app_id": app_id, "report_id": report_id,
        "token": bool(token), "period_start": period_start, "period_end": period_end,
    }
    if not all(required.values()):
        return {
            "ok": False,
            "error": "Missing required fields: base_url, app_id, report_id, token, period_start, period_end",
            "received": sorted(payload.keys()),
        }, 400

    report_url, renderings_url = _build_urls(str(base_url), str(app_id), str(report_id))
    try:
        response = requests.get(report_url, headers=_headers(str(token)), timeout=(10, 30))
    except Exception as exc:
        render_logger.exception("render_report | FETCH_MODEL_EXCEPTION | app_id=%s report_id=%s", app_id, report_id)
        return {"ok": False, "error": "Failed to read report model", "details": str(exc), "url": report_url}, 502

    if response.status_code != 200:
        return {
            "ok": False, "error": "Failed to read report model", "status": response.status_code,
            "url": report_url, "response": (response.text or "")[:800],
        }, 502

    model = response.json() if response.content else {}
    body = {
        "reportId": str(report_id),
        "parameters": _merge_parameters(model.get("parameters", []), period_start, period_end, tag_id, event_id),
        "reportFormatId": int(payload.get("reportFormatId") or 2),
        "sendEmail": False,
    }
    last_status = None
    last_body = None
    for attempt in range(1, 4):
        try:
            response = requests.post(
                renderings_url, headers=_headers(str(token), json_body=True),
                json=body, timeout=RENDER_TIMEOUT,
            )
            last_status, last_body = response.status_code, response.text
            if response.ok:
                data = response.json() if response.text else {}
                render_id = data.get("id") or data.get("renderingId") or data.get("rendering_id")
                return {"render_id": render_id, "rendering_id": render_id, "ok": True}, 200
            if response.status_code not in (429, 500, 502, 503, 504):
                break
        except Exception as exc:
            last_body = str(exc)
            render_logger.warning("render_report | ATTEMPT_EXCEPTION | attempt=%d app_id=%s: %s", attempt, app_id, exc)
        time.sleep(1.5 * attempt)

    return {
        "ok": False, "error": "Render failed", "status": last_status,
        "response": last_body[:500] if last_body else None, "request_body": body,
    }, 502


def wait_for_report_result(payload: dict, *, max_wait_s: int = 300, cancel_check=None) -> tuple[dict, int]:
    """Poll GpsGate directly until a rendering is ready."""
    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    token = payload.get("token")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    rendering_id = payload.get("rendering_id") or payload.get("render_id")
    required = {"base_url": base_url, "token": bool(token), "app_id": app_id, "rendering_id": rendering_id}
    if not all(required.values()):
        return {
            "ok": False, "error": "Missing required fields: base_url, token, app_id, rendering_id",
            "received": sorted(payload.keys()),
        }, 400

    if report_id:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}"
    else:
        status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}"

    headers = {"Accept": "application/json", "Authorization": token}
    waited, sleep_s = 0, 2
    while waited < max_wait_s:
        if cancel_check:
            cancel_check()
        try:
            response = requests.get(status_url, headers=headers, timeout=RESULT_REQUEST_TIMEOUT)
            if response.status_code != 200:
                if response.status_code not in (429, 500, 502, 503, 504):
                    return {
                        "ok": False, "error": "Failed to check render status", "status": response.status_code,
                        "response": response.text[:500] if response.text else None,
                    }, 502
            else:
                data = response.json() if response.text else {}
                if data.get("isReady") is True:
                    output_file = data.get("outputFile")
                    if not output_file:
                        return {"ok": False, "error": "No output file generated", "response": data}, 500
                    link = f"{base_url}{output_file}" if output_file.startswith("/") else output_file
                    return {"ok": True, "gdrive_link": link, "link": link, "rendering_id": str(rendering_id)}, 200
        except Exception:
            result_logger.exception("fetch_result | EXCEPTION | app_id=%s rendering_id=%s waited=%ds", app_id, rendering_id, waited)

        if cancel_check:
            cancel_check()
        time.sleep(sleep_s)
        waited += sleep_s
        sleep_s = min(10, sleep_s * 1.5)

    return {"ok": False, "error": "Render timeout", "waited_seconds": waited}, 504
