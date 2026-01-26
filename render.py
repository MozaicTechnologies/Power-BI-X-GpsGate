# from flask import Blueprint, request, jsonify
# import time
# import requests
# from urllib.parse import urljoin
# from datetime import datetime, timezone

# from models import db, Render

# render_bp = Blueprint("render_bp", __name__)


# def _payload_dict():
#     """Accept JSON or x-www-form-urlencoded and normalize to dict."""
#     data = request.get_json(silent=True)
#     if isinstance(data, dict):
#         return data
#     if request.form:
#         return request.form.to_dict(flat=True)
#     return {}


# def _headers(token: str) -> dict:
#     # GpsGate examples use Authorization header with the token value.
#     # If your server expects "Bearer <token>", change it here once.
#     return {
#         "Accept": "application/json",
#         "Authorization": token,
#     }


# def _build_urls(base_url: str, app_id: str, report_id: str):
#     base = base_url.rstrip("/") + "/"
#     report_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}")
#     renderings_url = urljoin(base, f"comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings")
#     return report_url, renderings_url


# def _merge_parameters(report_parameters, period_start, period_end, tag_id=None, event_id=None):
#     """
#     GpsGate parameter model fields (important ones):
#       - parameterName
#       - value (for periodic/single value params)
#       - periodStart / periodEnd (only for parameterName == "Period")
#       - arrayValues (for list params like TagIDs, EventRuleIDs, etc.)
#     :contentReference[oaicite:4]{index=4}
#     """
#     if not isinstance(report_parameters, list):
#         report_parameters = []

#     merged = []
#     for p in report_parameters:
#         if not isinstance(p, dict):
#             continue
#         p2 = dict(p)
#         pname = (p2.get("parameterName") or "").strip()

#         # 1) Period parameter
#         if pname.lower() == "period":
#             # Ensure "Custom" period and set periodStart/periodEnd
#             p2["value"] = "Custom"
#             p2["periodStart"] = period_start
#             p2["periodEnd"] = period_end
#             p2["visible"] = p2.get("visible", False)

#         # 2) Tag/Group parameter (template-specific naming)
#         # We try safe heuristics: if it already uses arrayValues AND looks like group/tag/view, set it.
#         if tag_id:
#             low = pname.lower()
#             if ("group" in low or "tag" in low or "view" in low) and ("arrayValues" in p2 or p2.get("arrayValues") is not None):
#                 p2["arrayValues"] = [int(tag_id)]

#         # 3) Event Rule parameter (template-specific naming)
#         if event_id:
#             low = pname.lower()
#             if ("event" in low and ("rule" in low or "id" in low)) and ("arrayValues" in p2 or p2.get("arrayValues") is not None):
#                 p2["arrayValues"] = [int(event_id)]

#         merged.append(p2)

#     # IMPORTANT: if your report template does NOT expose tag/event as parameters,
#     # the above will not inject new parameters (by design).
#     # In that case, you must adjust the report template or append a new param explicitly.
#     return merged


# @render_bp.route("/render", methods=["POST"])
# def handle_render():
#     sess = requests.Session()

#     try:
#         data = _payload_dict()
#         print(f"[RENDER] Incoming payload keys: {sorted(list(data.keys()))}")

#         app_id = data.get("app_id")
#         period_start = data.get("period_start")
#         period_end = data.get("period_end")
#         tag_id = data.get("tag_id")
#         report_id = data.get("report_id")
#         token = data.get("token")
#         base_url = data.get("base_url")
#         event_id = data.get("event_id")

#         # Keep strict requirements for render creation:
#         # We need a period; tag_id is needed if your report is group-filtered.
#         if not all([app_id, period_start, period_end, report_id, token, base_url]):
#             return jsonify({
#                 "error": "Missing required parameters",
#                 "required": ["app_id", "period_start", "period_end", "report_id", "token", "base_url"],
#                 "received_keys": sorted(list(data.keys()))
#             }), 400

#         # ------------------------------------------------------------------
#         # 1) DATABASE FIRST (IDEMPOTENCY)
#         # ------------------------------------------------------------------
#         existing = Render.query.filter_by(
#             app_id=str(app_id),
#             period_start=period_start,
#             period_end=period_end,
#             tag_id=str(tag_id) if tag_id else None,
#             report_id=str(report_id),
#             event_id=str(event_id) if event_id else None
#         ).first()

#         if existing and existing.render_id:
#             print(f"[RENDER] Using cached rendering_id={existing.render_id}")
#             return jsonify({"render_id": existing.render_id}), 200

#         # ------------------------------------------------------------------
#         # 2) READ REPORT MODEL (to get correct parameters)
#         # ------------------------------------------------------------------
#         headers = _headers(token)
#         report_url, renderings_url = _build_urls(str(base_url), str(app_id), str(report_id))

#         r_rep = sess.get(report_url, headers=headers, timeout=(10, 30))
#         if r_rep.status_code != 200:
#             return jsonify({
#                 "error": "Failed to read report model",
#                 "status_code": r_rep.status_code,
#                 "url": report_url,
#                 "response": (r_rep.text or "")[:800],
#             }), 502

#         report_model = r_rep.json() if r_rep.content else {}
#         report_parameters = report_model.get("parameters", [])

#         merged_params = _merge_parameters(
#             report_parameters=report_parameters,
#             period_start=period_start,
#             period_end=period_end,
#             tag_id=tag_id,
#             event_id=event_id
#         )

#         # ------------------------------------------------------------------
#         # 3) CREATE RENDERING (POST /renderings)
#         # ------------------------------------------------------------------
#         # GpsGate guide: use reportFormatId=2 for CSV :contentReference[oaicite:5]{index=5}
#         report_format_id = int(data.get("reportFormatId") or 2)

#         payload = {
#             "parameters": merged_params,
#             "reportFormatId": report_format_id,
#             "sendEmail": False
#         }

#         # Retry stabilization (some servers respond 201/202; treat as success and extract id)
#         rendering_id = None
#         last_status, last_body = None, None

#         for attempt in range(1, 4):
#             print(f"[RENDER] Attempt {attempt} → POST {renderings_url}")
#             resp = sess.post(renderings_url, json=payload, headers=headers, timeout=(10, 60))
#             last_status, last_body = resp.status_code, resp.text

#             print(f"[RENDER] Status={resp.status_code}")
#             print(f"[RENDER] Body={(resp.text or '')[:800]}")

#             if resp.status_code in (200, 201, 202):
#                 try:
#                     body = resp.json() if resp.content else {}
#                 except Exception:
#                     body = {}

#                 # GpsGate guide describes a renderID/renderingId; often returned as "id" :contentReference[oaicite:6]{index=6}
#                 rendering_id = body.get("id") or body.get("renderingId") or body.get("renderId") or body.get("render_id")
#                 if rendering_id:
#                     break

#             time.sleep(5)

#         if not rendering_id:
#             return jsonify({
#                 "error": "Failed to create rendering",
#                 "status_code": last_status,
#                 "url": renderings_url,
#                 "response": (last_body or "")[:1200],
#                 "request_body": payload
#             }), 502

#         rendering_id = str(rendering_id)

#         # ------------------------------------------------------------------
#         # 4) STORE RENDER RECORD
#         # ------------------------------------------------------------------
#         new_render = Render(
#             app_id=str(app_id),
#             period_start=period_start,
#             period_end=period_end,
#             tag_id=str(tag_id) if tag_id else None,
#             event_id=str(event_id) if event_id else None,
#             report_id=str(report_id),
#             render_id=rendering_id
#         )

#         db.session.add(new_render)
#         db.session.commit()

#         print(f"[RENDER] Stored rendering_id={rendering_id}")

#         return jsonify({
#             "render_id": rendering_id,
#             "app_id": str(app_id),
#             "report_id": str(report_id),
#             "reportFormatId": report_format_id
#         }), 200

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": "Render service error", "details": str(e)}), 500



import time
import requests
from flask import Blueprint, request, jsonify

render_bp = Blueprint("render", __name__)

DEFAULT_TIMEOUT = 45

@render_bp.route("/health")
def health():
    return "ok"

@render_bp.route("/render", methods=["POST"])
def render_report():
    """Handle render requests - accepts form data or JSON"""
    # Accept both JSON and form data
    if request.is_json:
        payload = request.get_json()
    else:
        payload = request.form.to_dict()

    print(f"[RENDER] Received payload keys: {sorted(list(payload.keys()))}")

    # Required fields
    base_url = (payload.get("base_url") or "").strip().rstrip("/")
    app_id = payload.get("app_id")
    report_id = payload.get("report_id")
    token = payload.get("token")
    period_start = payload.get("period_start")
    period_end = payload.get("period_end")
    
    # Optional
    tag_id = payload.get("tag_id")
    event_id = payload.get("event_id")

    if not all([base_url, app_id, report_id, token]):
        return jsonify({
            "ok": False,
            "error": "Missing required fields: base_url, app_id, report_id, token",
            "received": sorted(list(payload.keys()))
        }), 400

    url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"

    # Debug: Log token info (masked)
    token_preview = f"{token[:10]}...{token[-10:]}" if len(token) > 20 else "SHORT_TOKEN"
    print(f"[RENDER] Using token: {token_preview} (length: {len(token)})")
    
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": token
    }

    body = {
        "periodStart": period_start,
        "periodEnd": period_end,
        "reportFormatId": 2,
        "sendEmail": False
    }
    
    # Add optional parameters if provided
    if tag_id:
        body["tagId"] = str(tag_id)
    if event_id:
        body["eventId"] = str(event_id)

    print(f"[RENDER] Posting to: {url}")
    print(f"[RENDER] Body: {body}")

    # Retry with backoff
    last_status = None
    last_body = None

    for attempt in range(1, 4):
        try:
            resp = requests.post(url, headers=headers, json=body, timeout=DEFAULT_TIMEOUT)
            last_status = resp.status_code
            last_body = resp.text

            print(f"[RENDER] Attempt {attempt}: Status {resp.status_code}")
            if not resp.ok:
                print(f"[RENDER] Error response: {resp.text[:500]}")

            if resp.ok:
                data = resp.json() if resp.text else {}
                rendering_id = (
                    data.get("id")
                    or data.get("renderingId")
                    or data.get("rendering_id")
                )

                print(f"[RENDER] Success! rendering_id={rendering_id}")

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
            print(f"[RENDER] Attempt {attempt} exception: {e}")
            time.sleep(1.5 * attempt)

    print(f"[RENDER] Failed after retries. Status: {last_status}")
    print(f"[RENDER] Response body: {last_body[:1000] if last_body else 'None'}")
    return jsonify({
        "ok": False,
        "error": "Render failed",
        "status": last_status,
        "response": last_body[:500] if last_body else None
    }), 502
