#!/usr/bin/env python
"""
API endpoint for scheduled backfill (current week data)
Can be called by external cron services, GitHub Actions, or Render background jobs
"""
from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import os
import traceback as tb

from app.utils.logger import setup_logger

backfill_api = Blueprint('backfill_api', __name__, url_prefix='/api', template_folder='../templates')
logger = setup_logger("BACKFILL")


@backfill_api.route('/health', methods=['GET'])
def health():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'service': 'GPS Gate Data Pipeline',
        'timestamp': datetime.now().isoformat()
    })


@backfill_api.route('/test', methods=['GET'])
def test():
    """Test endpoint - simple response"""
    try:
        from app import create_app, db
        app = create_app()
        with app.app_context():
            return jsonify({
                'success': True,
                'message': 'API is working',
                'timestamp': datetime.now().isoformat()
            }), 200
    except Exception as e:
        logger.exception("test | ERROR")
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': tb.format_exc()
        }), 500


@backfill_api.route('/init-db', methods=['GET', 'POST'])
def init_db():
    """Initialize database schema - run migrations and create tables"""
    logger.info("init_db | TRIGGERED method=%s", request.method)
    try:
        from app import db, create_app
        import os

        app = create_app()
        with app.app_context():
            migration_status = "Skipped (no migrations folder)"
            migrations_path = os.path.join(os.path.dirname(__file__), 'migrations')

            if os.path.exists(migrations_path):
                try:
                    from flask_migrate import upgrade
                    upgrade()
                    migration_status = "Migrations completed successfully"
                    logger.info("init_db | migrations completed")
                except Exception as mig_err:
                    migration_status = f"Migration skipped: {str(mig_err)[:100]}"
                    logger.warning("init_db | migration skipped: %s", mig_err)

            db.create_all()
            logger.info("init_db | tables created | migration_status=%s", migration_status)

            return jsonify({
                'success': True,
                'message': 'Database initialized successfully',
                'migration_status': migration_status,
                'tables_created': True,
                'timestamp': datetime.now().isoformat()
            }), 200

    except Exception:
        logger.exception("init_db | FAILED")
        return jsonify({
            'success': False,
            'error': 'Database initialization failed',
            'traceback': tb.format_exc()
        }), 500


@backfill_api.route('/backfill/current-week', methods=['POST', 'GET'])
def backfill_current_week():
    """Trigger backfill for current week data"""
    logger.info("backfill_current_week | TRIGGERED method=%s", request.method)
    try:
        from app import create_app, db
        from app.services.backfill_helper import backfill_current_week

        app = create_app()
        with app.app_context():
            result = backfill_current_week()
            logger.info("backfill_current_week | DONE | success=%s", result.get('success'))
            return jsonify(result), 200

    except Exception:
        logger.exception("backfill_current_week | FAILED")
        return jsonify({
            'success': False,
            'error': 'Backfill failed',
            'error_type': 'Exception',
            'timestamp': datetime.now().isoformat()
        }), 500


@backfill_api.route('/backfill/status', methods=['GET'])
def backfill_status():
    """Get status of last backfill"""
    try:
        from app import db, create_app
        from app.models import FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU

        app = create_app()
        with app.app_context():
            tables = [
                ('Trip', FactTrip),
                ('Speeding', FactSpeeding),
                ('Idle', FactIdle),
                ('AWH', FactAWH),
                ('WH', FactWH),
                ('HA', FactHA),
                ('HB', FactHB),
                ('WU', FactWU),
            ]

            stats = {}
            total_records = 0

            for name, model in tables:
                try:
                    count = db.session.query(model).count()
                    duplicate_count = db.session.query(model).filter_by(is_duplicate=True).count()
                    stats[name] = {
                        'total': count,
                        'duplicates': duplicate_count,
                        'valid': count - duplicate_count
                    }
                    total_records += count
                except Exception:
                    logger.exception("backfill_status | table_error table=%s", name)
                    stats[name] = {'error': 'query failed'}

            return jsonify({
                'success': True,
                'total_records': total_records,
                'stats_by_type': stats,
                'timestamp': datetime.now().isoformat()
            }), 200

    except Exception:
        logger.exception("backfill_status | FAILED")
        return jsonify({
            'success': False,
            'error': 'Status query failed',
            'traceback': tb.format_exc()
        }), 500
