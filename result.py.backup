# from flask import Blueprint, request, jsonify
# import requests
# from models import db, Result

# result_bp = Blueprint('result_bp', __name__)

# @result_bp.route('/result', methods=['POST'])
# def handle_result():
#     """
#     Get the result/download link for a completed render.
#     First checks the Result cache table, then polls GpsGate API if needed.
#     """
#     try:
#         data = request.form or request.get_json()
#         print(f"Result request data: {data}")

#         app_id = data.get('app_id')
#         render_id = data.get('render_id')
#         report_id = data.get('report_id')
#         token = data.get('token')
#         base_url = data.get('base_url')

#         if not all([app_id, render_id, report_id, token, base_url]):
#             return jsonify({"error": "Missing required parameters"}), 400

#         # FIRST: Check if we have a cached result
#         print(f"[DEBUG] Checking Result cache for render_id: {render_id}")
#         cached_result = Result.query.filter_by(render_id=str(render_id)).first()
        
#         if cached_result and cached_result.gdrive_link:
#             print(f"[OK] Found cached result with gdrive_link: {cached_result.gdrive_link[:60]}...")
#             return jsonify({
#                 "gdrive_link": cached_result.gdrive_link,
#                 "link": cached_result.gdrive_link
#             }), 200

#         # If no cache, try to poll GpsGate API
#         print(f"[DEBUG] No cached result, polling GpsGate API...")
        
#         # Poll the rendering status to get the outputFile
#         status_url = f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{render_id}"
#         headers = {
#             "Authorization": token
#         }

#         # Poll for up to 2 minutes
#         max_attempts = 120
#         for attempt in range(max_attempts):
#             status_resp = requests.get(status_url, headers=headers, timeout=(10, 30))
            
#             if status_resp.status_code != 200:
#                 print(f"[ERROR] Status check failed: {status_resp.status_code}")
#                 return jsonify({"error": "Failed to check render status"}), 502
            
#             status_data = status_resp.json()
            
#             if status_data.get("isReady"):
#                 # Report is ready, extract output file
#                 output_file = status_data.get("outputFile")
                
#                 if not output_file:
#                     print(f"[ERROR] No outputFile in ready response")
#                     return jsonify({"error": "No output file generated"}), 500
                
#                 # Build full download URL if it's a relative path
#                 if output_file.startswith("/"):
#                     gdrive_link = f"{base_url}{output_file}"
#                 else:
#                     gdrive_link = output_file
                
#                 print(f"[OK] Output file ready: {gdrive_link}")
#                 return jsonify({
#                     "gdrive_link": gdrive_link,
#                     "link": gdrive_link
#                 }), 200
            
#             # Not ready yet, wait and retry
#             if attempt > 0 and attempt % 10 == 0:
#                 print(f"[DEBUG] Waiting for render... {attempt}s elapsed")
            
#             import time
#             time.sleep(1)
        
#         # Timeout
#         print(f"[ERROR] Render not ready after {max_attempts}s")
#         return jsonify({"error": "Render timeout"}), 504

#     except Exception as e:
#         import traceback
#         print(f"[ERROR] Error in /result: {str(e)}")
#         print(traceback.format_exc())
#         return jsonify({"error": "Server error", "details": str(e)}), 500


# # result.py
# from flask import Blueprint, request, jsonify
# import requests
# import time
# from datetime import datetime, timezone

# from models import db, Result

# result_bp = Blueprint("result_bp", __name__)


# def _payload_dict():
#     """Accept JSON or x-www-form-urlencoded."""
#     data = request.get_json(silent=True)
#     if isinstance(data, dict):
#         return data
#     if request.form:
#         return request.form.to_dict(flat=True)
#     return {}


# def _headers(token: str) -> dict:
#     # GpsGate examples use Authorization header with the token value.
#     # If your server requires "Bearer <token>", adjust here.
#     return {
#         "accept": "application/json",
#         "Authorization": token,
#     }


# def _build_urls(base_url: str, app_id: str, report_id: str):
#     base = base_url.rstrip("/")
#     report_url = f"{base}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}"
#     renderings_url = f"{base}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings"
#     return report_url, renderings_url


# def _iso_to_period(ts: str) -> str:
#     # Keep the string as-is; GpsGate expects timestamps for Period parameters.
#     # Your pipeline already sends Zulu ISO strings.
#     return ts


# def _merge_parameters(default_params, period_start, period_end, tag_id=None, event_id=None):
#     """
#     Uses the report's default parameters, then overrides:
#       - Period => periodStart / periodEnd (parameterName == "Period" in docs)
#       - Tag/Group/Event params if present, via arrayValues (docs mention TagIDs, EventRuleIDs, etc.)
#     """
#     if not isinstance(default_params, list):
#         default_params = []

#     params = []
#     for p in default_params:
#         if not isinstance(p, dict):
#             continue
#         p2 = dict(p)

#         pname = (p2.get("parameterName") or "").lower()

#         # Period override (docs: periodStart/periodEnd used for the Period parameter)
#         if pname == "period":
#             if period_start and period_end:
#                 p2["periodStart"] = _iso_to_period(period_start)
#                 p2["periodEnd"] = _iso_to_period(period_end)

#         # Tag/Group override if the report supports it
#         # (Exact parameterName varies by report template; we set arrayValues only when caller provided tag_id)
#         if tag_id and ("tag" in pname or "group" in pname or "view" in pname):
#             p2["arrayValues"] = [int(tag_id)]

#         # Event Rule override if the report supports it
#         if event_id and ("event" in pname and ("rule" in pname or "id" in pname)):
#             p2["arrayValues"] = [int(event_id)]

#         params.append(p2)

#     # If the report has no explicit Tag param but you still want to try forcing it,
#     # you can optionally append a parameter here—however, that is template-specific.
#     return params


# @result_bp.route("/result", methods=["POST"])
# def handle_result():
#     sess = requests.Session()
#     try:
#         data = _payload_dict()

#         app_id = data.get("app_id")
#         report_id = data.get("report_id")
#         token = data.get("token")
#         base_url = data.get("base_url")

#         # Your pipeline currently sends tag_id + period_start/end (+ event_id for some reports)
#         tag_id = data.get("tag_id")
#         event_id = data.get("event_id")
#         period_start = data.get("period_start")
#         period_end = data.get("period_end")

#         # Allow caller to provide a real rendering id; otherwise we will create one.
#         rendering_id = data.get("render_id") or data.get("rendering_id")

#         if not all([app_id, report_id, token, base_url]):
#             return jsonify({
#                 "error": "Missing required parameters",
#                 "required": ["app_id", "report_id", "token", "base_url"],
#                 "received_keys": sorted(list(data.keys()))
#             }), 400

#         headers = _headers(token)
#         report_url, renderings_url = _build_urls(base_url, str(app_id), str(report_id))

#         # CACHE KEY:
#         # - If caller passes a true rendering_id, cache on that.
#         # - Otherwise cache on a deterministic composite key so repeated calls reuse output.
#         cache_key = str(rendering_id) if rendering_id else f"{app_id}:{report_id}:{tag_id}:{event_id}:{period_start}:{period_end}"

#         cached = Result.query.filter_by(render_id=cache_key).first()
#         if cached and cached.gdrive_link:
#             return jsonify({"gdrive_link": cached.gdrive_link, "link": cached.gdrive_link}), 200

#         # 1) CREATE RENDERING if needed (POST /renderings)
#         if not rendering_id:
#             # Fetch the report to obtain its parameter model (recommended by GpsGate docs)
#             # GET /applications/{appId}/reports/{reportId}
#             r_rep = sess.get(report_url, headers=headers, timeout=(10, 30))
#             if r_rep.status_code != 200:
#                 return jsonify({
#                     "error": "Failed to read report model",
#                     "status_code": r_rep.status_code,
#                     "url": report_url,
#                     "response": (r_rep.text or "")[:800],
#                 }), 502

#             report_model = r_rep.json() if r_rep.content else {}
#             default_params = report_model.get("parameters", [])

#             merged_params = _merge_parameters(
#                 default_params=default_params,
#                 period_start=period_start,
#                 period_end=period_end,
#                 tag_id=tag_id,
#                 event_id=event_id,
#             )

#             # Render request model (docs: can override parameters + reportFormatId)
#             # Docs also note CSV commonly uses reportFormatId = 2 (if your server uses that mapping)
#             render_body = {
#                 "reportFormatId": int(data.get("reportFormatId") or 2),
#                 "sendEmail": False,
#                 "parameters": merged_params,
#             }

#             r_create = sess.post(renderings_url, headers=headers, json=render_body, timeout=(10, 60))
#             if r_create.status_code not in (200, 201):
#                 return jsonify({
#                     "error": "Failed to create rendering",
#                     "status_code": r_create.status_code,
#                     "url": renderings_url,
#                     "response": (r_create.text or "")[:800],
#                     "request_body": render_body,
#                 }), 502

#             created = r_create.json() if r_create.content else {}
#             rendering_id = created.get("id") or created.get("renderingId") or created.get("render_id")
#             if not rendering_id:
#                 return jsonify({
#                     "error": "Rendering created but no rendering id returned",
#                     "url": renderings_url,
#                     "response": created,
#                 }), 502

#         # 2) POLL RENDER STATUS (GET /renderings/{renderingid})
#         status_url = f"{base_url.rstrip('/')}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}"

#         max_wait_s = int(data.get("max_wait_s") or 300)
#         waited = 0
#         sleep_s = 2

#         while waited < max_wait_s:
#             r_status = sess.get(status_url, headers=headers, timeout=(10, 30))
#             if r_status.status_code != 200:
#                 # Fail fast on auth/route errors
#                 if r_status.status_code in (401, 403, 404):
#                     return jsonify({
#                         "error": "Failed to check render status",
#                         "status_code": r_status.status_code,
#                         "url": status_url,
#                         "response": (r_status.text or "")[:800],
#                         "rendering_id": rendering_id,
#                     }), 502

#                 # Retry on throttling/server errors
#                 if r_status.status_code == 429 or 500 <= r_status.status_code <= 599:
#                     time.sleep(sleep_s)
#                     waited += sleep_s
#                     sleep_s = min(10, sleep_s * 2)
#                     continue

#                 return jsonify({
#                     "error": "Failed to check render status",
#                     "status_code": r_status.status_code,
#                     "url": status_url,
#                     "response": (r_status.text or "")[:800],
#                     "rendering_id": rendering_id,
#                 }), 502

#             status_data = r_status.json() if r_status.content else {}

#             if status_data.get("isReady") is True:
#                 output_file = status_data.get("outputFile")
#                 if not output_file:
#                     return jsonify({"error": "No output file generated", "status": status_data}), 500

#                 link = f"{base_url.rstrip('/')}{output_file}" if str(output_file).startswith("/") else str(output_file)

#                 # 3) CACHE WRITE
#                 row = cached or Result(render_id=cache_key, app_id=str(app_id), report_id=str(report_id))
#                 row.gdrive_link = link
#                 row.uploaded_at = datetime.now(timezone.utc)
#                 db.session.add(row)
#                 db.session.commit()

#                 return jsonify({
#                     "gdrive_link": link,
#                     "link": link,
#                     "rendering_id": str(rendering_id),
#                 }), 200

#             time.sleep(sleep_s)
#             waited += sleep_s
#             sleep_s = min(10, sleep_s * 2)

#         return jsonify({
#             "error": "Render timeout",
#             "url": status_url,
#             "rendering_id": str(rendering_id),
#             "waited_seconds": waited
#         }), 504

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({"error": "Server error", "details": str(e)}), 500

# result.py (UPDATED)
# routes/result.py

import time
import requests
from flask import Blueprint, request, jsonify, Response

result_bp = Blueprint("result", __name__)
@result_bp.route("/health")
def health():
    return "ok"

DEFAULT_TIMEOUT = 60


def _auth_header(token: str) -> dict:
    token = (token or "").strip()
    if not token:
        return {}
    return {"Authorization": token if token.lower().startswith("bearer ") else f"Bearer {token}"}


def _normalize_base_url(base_url: str) -> str:
    return (base_url or "").strip().rstrip("/")


def _pick_download_url(meta: dict):
    # Tries the common fields you may get back
    for k in ("downloadUrl", "resultUrl", "url", "href", "result_uri", "resultUri"):
        if meta.get(k):
            return meta[k]
    # Sometimes nested
    links = meta.get("_links") or meta.get("links") or {}
    for k in ("download", "result", "self"):
        v = links.get(k)
        if isinstance(v, dict) and v.get("href"):
            return v["href"]
        if isinstance(v, str):
            return v
    return None


@result_bp.post("/result")
def fetch_result():
    payload = request.get_json(force=True) or {}

    base_url = _normalize_base_url(payload.get("base_url"))
    token = payload.get("token")
    app_id = payload.get("app_id")

    rendering_id = payload.get("rendering_id") or payload.get("renderingId")
    report_id = payload.get("report_id")  # optional but helps fallbacks

    if not base_url or not token or app_id is None or not rendering_id:
        return jsonify({
            "ok": False,
            "error": "Missing required fields: base_url, token, app_id, rendering_id"
        }), 400

    headers = {
        "Accept": "*/*",
        **_auth_header(token)
    }

    # 1) Try to read rendering metadata (best option)
    meta_candidates = []
    if report_id is not None:
        meta_candidates.append(
            f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}"
        )
    meta_candidates.append(
        f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}"
    )

    meta = None
    for meta_url in meta_candidates:
        try:
            r = requests.get(meta_url, headers=headers, timeout=DEFAULT_TIMEOUT)
            if r.ok and r.text:
                meta = r.json()
                break
        except Exception:
            pass

    download_url = _pick_download_url(meta or {})

    # 2) Fallback to common “result” endpoints if meta didn't provide a URL
    fallback_urls = []
    if report_id is not None:
        fallback_urls.append(
            f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}/result"
        )
        fallback_urls.append(
            f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/reports/{report_id}/renderings/{rendering_id}/file"
        )
    fallback_urls.append(
        f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}/result"
    )
    fallback_urls.append(
        f"{base_url}/comGpsGate/api/v.1/applications/{app_id}/renderings/{rendering_id}/file"
    )

    candidates = [download_url] if download_url else []
    candidates += fallback_urls

    last_status, last_body = None, None

    for url in [u for u in candidates if u]:
        # some APIs return relative URLs
        if url.startswith("/"):
            url = f"{base_url}{url}"

        # Try multiple times because rendering may still be processing
        for attempt in range(1, 4):
            try:
                resp = requests.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
                last_status = resp.status_code
                last_body = resp.text[:1000] if resp.text else None

                # Not ready yet
                if resp.status_code in (202, 204):
                    time.sleep(1.5 * attempt)
                    continue

                # Success
                if resp.ok and resp.content:
                    content_type = resp.headers.get("Content-Type", "application/octet-stream")
                    return Response(
                        resp.content,
                        status=200,
                        mimetype=content_type,
                        headers={
                            "Content-Disposition": f'attachment; filename="rendering_{rendering_id}.bin"'
                        }
                    )

                # Retry transient errors
                if resp.status_code in (429, 500, 502, 503, 504):
                    time.sleep(1.5 * attempt)
                    continue

                break

            except Exception as e:
                last_body = str(e)
                time.sleep(1.5 * attempt)

    return jsonify({
        "ok": False,
        "error": "Result fetch failed",
        "last_status": last_status,
        "last_body": last_body,
        "rendering_id": str(rendering_id),
        "meta_found": bool(meta),
        "meta_sample_keys": list((meta or {}).keys())[:30]
    }), 502
