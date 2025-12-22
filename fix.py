from flask import Blueprint, request, jsonify
from models import db, Render, Result

fix_bp = Blueprint('fix_bp', __name__)

@fix_bp.route('/fix', methods=['POST'])
def handle_render():
    data = request.form or request.get_json()

    app_id = data.get('app_id')
    period_start = data.get('period_start')
    period_end = data.get('period_end')
    tag_id = data.get('tag_id')
    report_id = data.get('report_id')
    render_id = data.get('render_id')
    token = data.get('token')
    base_url = data.get('base_url')
    event_id = data.get('event_id')

    
    if render_id: # Check for existing record if render id is sent
        existing = Render.query.filter_by(
            render_id=render_id
        ).first()
        if existing:
            try:
                deleted_render_id = existing.id
                db.session.delete(existing)
                db.session.commit()
                return jsonify({
                    "ok": True,
                    "message": "Render row removed (if it existed).",
                    "render_id": render_id,
                    "deleted_render_id": deleted_render_id
                    }), 200
            except Exception as e:
                db.session.rollback()
                return jsonify({"ok": False, "error": str(e)}), 500
    else: # Check for existing record if app id is sent
        if app_id:
            existing = (
                db.session.query(Render)
                .outerjoin(Result, Result.render_id == Render.render_id)
                .filter(Render.app_id == app_id, Result.id.is_(None))
                .order_by(Render.created_at.asc())
            )

            target_rows = existing.all()
            if not target_rows:
                return jsonify({"ok": True, "message": "No missing renders found for this app.", "deleted": 0}), 200

            ids_to_delete = [r.id for r in target_rows]
            sample_render_ids = [r.render_id for r in target_rows[:10]]  # for response visibility

            try:
                (
                db.session.query(Render)
                .filter(Render.id.in_(ids_to_delete))
                .delete(synchronize_session=False)
                )
                db.session.commit()

                return jsonify({
                    "ok": True,
                    "message": "Missing-only renders removed for this app.",
                    "app_id": app_id,
                    "deleted": len(ids_to_delete),
                    "sample_render_ids": sample_render_ids  # just a small sample for logs/confirm
                }), 200

            except Exception as e:
                db.session.rollback()
                return jsonify({"ok": False, "error": str(e)}), 500