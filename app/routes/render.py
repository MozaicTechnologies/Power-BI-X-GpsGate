
import time
from urllib.parse import urljoin

import requests
from flask import Blueprint, request, jsonify
from app.utils.logger import setup_logger

render_bp = Blueprint("render", __name__)
logger = setup_logger("RENDER")

DEFAULT_TIMEOUT = 45


def _payload_dict():
    """Accept JSON or x-www-form-urlencoded and normalize to a dict."""
    data = request.get_json(silent=True)
    if isinstance(data, dict):
        return data
    if request.form:
        return request.form.to_dict(flat=True)
    return {}


def _headers(token: str, *, json_body: bool = False) -> dict:
    headers = {
        "Accept": "application/json",
        "Authorization": token,
    }
    if json_body:
        headers["Content-Type"] = "application/json"
    return headers


def _build_urls(base_url: str, app_id: str, report_id: str) -> tuple[str, str]:
    base = base_url.rstrip("/") + "/"
    report_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}")
    renderings_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings")
    return report_url, renderings_url


def _coerce_id(value):
    return str(value)


def _merge_parameters(report_parameters, period_start, period_end, tag_id=None, event_id=None):
    """Merge caller inputs into the report model's parameter payload."""
    if not isinstance(report_parameters, list):
        report_parameters = []

    merged = []
    for parameter in report_parameters:
        if not isinstance(parameter, dict):
            continue

        merged_parameter = dict(parameter)
        parameter_name = (merged_parameter.get("parameterName") or "").strip()
        parameter_name_lower = parameter_name.lower()

        if parameter_name_lower == "period":
            merged_parameter["value"] = "Custom"
            merged_parameter["periodStart"] = period_start
            merged_parameter["periodEnd"] = period_end
            merged_parameter["visible"] = merged_parameter.get("visible", False)

        if tag_id:
            looks_like_tag_parameter = (
                "group" in parameter_name_lower
                or "tag" in parameter_name_lower
                or "view" in parameter_name_lower
            )
            if looks_like_tag_parameter and (
                "arrayValues" in merged_parameter or merged_parameter.get("arrayValues") is not None
            ):
                merged_parameter["arrayValues"] = [_coerce_id(tag_id)]

        if event_id:
            looks_like_event_parameter = (
                "event" in parameter_name_lower
                and ("rule" in parameter_name_lower or "id" in parameter_name_lower)
            )
            if looks_like_event_parameter and (
                "arrayValues" in merged_parameter or merged_parameter.get("arrayValues") is not None
            ):
                merged_parameter["arrayValues"] = [_coerce_id(event_id)]

        merged.append(merged_parameter)

    return merged


@render_bp.route("/health")
def health():
    return "ok"


@render_bp.route("/render", methods=["POST"])
def render_report():
    """Handle render requests using the report model's parameter schema."""
    payload = _payload_dict()

    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    token = payload.get("token")
    period_start = payload.get("period_start")
    period_end = payload.get("period_end")
    tag_id = payload.get("tag_id")
    event_id = payload.get("event_id")

    logger.debug("render_report | app_id=%s report_id=%s period=%s→%s tag_id=%s event_id=%s",
                 app_id, report_id, period_start, period_end, tag_id, event_id)

    if not all([base_url, app_id, report_id, token, period_start, period_end]):
        missing = [k for k, v in {
            "base_url": base_url, "app_id": app_id, "report_id": report_id,
            "token": bool(token), "period_start": period_start, "period_end": period_end,
        }.items() if not v]
        logger.warning("render_report | MISSING_FIELDS | app_id=%s missing=%s", app_id, missing)
        return jsonify({
            "ok": False,
            "error": "Missing required fields: base_url, app_id, report_id, token, period_start, period_end",
            "received": sorted(list(payload.keys()))
        }), 400

    report_url, renderings_url = _build_urls(str(base_url), str(app_id), str(report_id))
    headers = _headers(str(token))

    logger.info("render_report | FETCH_MODEL | app_id=%s report_id=%s", app_id, report_id)
    try:
        report_resp = requests.get(report_url, headers=headers, timeout=(10, 30))
    except Exception as exc:
        logger.exception("render_report | FETCH_MODEL_EXCEPTION | app_id=%s report_id=%s", app_id, report_id)
        return jsonify({
            "ok": False,
            "error": "Failed to read report model",
            "details": str(exc),
            "url": report_url,
        }), 502

    if report_resp.status_code != 200:
        logger.error("render_report | FETCH_MODEL_FAILED | app_id=%s report_id=%s status=%d",
                     app_id, report_id, report_resp.status_code)
        return jsonify({
            "ok": False,
            "error": "Failed to read report model",
            "status": report_resp.status_code,
            "url": report_url,
            "response": (report_resp.text or "")[:800],
        }), 502

    report_model = report_resp.json() if report_resp.content else {}
    report_parameters = report_model.get("parameters", [])
    merged_parameters = _merge_parameters(
        report_parameters=report_parameters,
        period_start=period_start,
        period_end=period_end,
        tag_id=tag_id,
        event_id=event_id,
    )

    body = {
        "reportId": _coerce_id(report_id),
        "parameters": merged_parameters,
        "reportFormatId": int(payload.get("reportFormatId") or 2),
        "sendEmail": False,
    }

    logger.info("render_report | POST_RENDERING | app_id=%s report_id=%s period=%s→%s params=%d",
                app_id, report_id, period_start, period_end, len(merged_parameters))

    last_status = None
    last_body = None

    for attempt in range(1, 4):
        try:
            resp = requests.post(
                renderings_url,
                headers=_headers(str(token), json_body=True),
                json=body,
                timeout=DEFAULT_TIMEOUT,
            )
            last_status = resp.status_code
            last_body = resp.text

            logger.debug("render_report | attempt=%d status=%d app_id=%s report_id=%s",
                         attempt, resp.status_code, app_id, report_id)

            if not resp.ok:
                logger.warning("render_report | ATTEMPT_FAILED | attempt=%d status=%d app_id=%s report_id=%s body=%.300s",
                               attempt, resp.status_code, app_id, report_id, resp.text)

            if resp.ok:
                data = resp.json() if resp.text else {}
                rendering_id = (
                    data.get("id")
                    or data.get("renderingId")
                    or data.get("rendering_id")
                )
                logger.info("render_report | SUCCESS | app_id=%s report_id=%s rendering_id=%s attempt=%d",
                            app_id, report_id, rendering_id, attempt)
                return jsonify({
                    "render_id": rendering_id,
                    "rendering_id": rendering_id,
                    "ok": True
                }), 200

            # Retry on transient errors
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * attempt)
                continue

            # Non-retryable error
            break

        except Exception as e:
            last_body = str(e)
            logger.warning("render_report | ATTEMPT_EXCEPTION | attempt=%d app_id=%s: %s",
                           attempt, app_id, e)
            time.sleep(1.5 * attempt)

    logger.error("render_report | FAILED_ALL_RETRIES | app_id=%s report_id=%s last_status=%s",
                 app_id, report_id, last_status)
    return jsonify({
        "ok": False,
        "error": "Render failed",
        "status": last_status,
        "response": last_body[:500] if last_body else None,
        "request_body": body,
    }), 502
