# health.py - Save this in your project root
from flask import Blueprint, jsonify
from models import db

health_bp = Blueprint('health_bp', __name__)

@health_bp.route('/health')
def health_check():
    return jsonify({
        "status": "healthy",
        "service": "GpsGate Power BI API"
    })

@health_bp.route('/db-check')
def db_check():
    try:
        # Try a simple query
        from models import ConsolidatedRequest
        count = ConsolidatedRequest.query.count()
        return jsonify({
            "status": "success",
            "database": "connected",
            "consolidated_requests_count": count
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "database": "not connected",
            "error": str(e)
        }), 500