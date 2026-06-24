"""
Manual data fetch API endpoints for user-initiated backfill operations.
Allows triggering backfill for specific date ranges or week counts.
"""

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import threading
import subprocess
import os

from app.utils.logger import setup_logger

api_bp = Blueprint('api', __name__, url_prefix='/api')
logger = setup_logger("API")

# Track backfill operations
backfill_operations = {}


@api_bp.route('/backfill', methods=['POST'])
def manual_backfill():
    """
    Manual backfill endpoint - initiate data fetch for custom date range or weeks.

    Request JSON:
    {
        "weeks": 54,
        "start_date": "2025-01-01",
        "end_date": "2025-12-31",
        "event_types": ["Trip", "Speeding", ...]
    }
    """
    try:
        data = request.get_json() or {}
        weeks = data.get('weeks', 1)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        event_types = data.get('event_types')

        if weeks < 1 or weeks > 54:
            return jsonify({
                "status": "error",
                "message": "weeks must be between 1 and 54"
            }), 400

        operation_id = f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        env = os.environ.copy()
        if event_types:
            env['BACKFILL_EVENT_TYPES'] = ','.join(event_types)

        logger.info("manual_backfill | REQUEST | operation_id=%s weeks=%s start=%s end=%s event_types=%s",
                    operation_id, weeks, start_date, end_date, event_types)

        def run_backfill():
            try:
                backfill_operations[operation_id] = {
                    'status': 'running',
                    'start_time': datetime.now(),
                    'weeks': weeks,
                    'progress': 0
                }

                script_path = os.path.join(
                    os.path.dirname(__file__),
                    'backfill_direct_python.py'
                )

                logger.info("manual_backfill | THREAD_START | operation_id=%s script=%s",
                            operation_id, script_path)

                result = subprocess.run(
                    ['python', script_path],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=7200
                )

                backfill_operations[operation_id]['end_time'] = datetime.now()
                backfill_operations[operation_id]['output'] = result.stdout

                if result.returncode != 0:
                    backfill_operations[operation_id]['status'] = 'error'
                    backfill_operations[operation_id]['error'] = result.stderr
                    logger.error("manual_backfill | SCRIPT_ERROR | operation_id=%s returncode=%d stderr=%.300s",
                                 operation_id, result.returncode, result.stderr)
                else:
                    backfill_operations[operation_id]['status'] = 'completed'
                    logger.info("manual_backfill | SCRIPT_SUCCESS | operation_id=%s returncode=%d",
                                operation_id, result.returncode)

            except subprocess.TimeoutExpired:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = 'Script timeout (2 hours)'
                logger.error("manual_backfill | TIMEOUT | operation_id=%s", operation_id)
            except Exception:
                backfill_operations[operation_id]['status'] = 'error'
                logger.exception("manual_backfill | EXCEPTION | operation_id=%s", operation_id)

        thread = threading.Thread(target=run_backfill, daemon=True)
        thread.start()

        estimated_minutes = weeks
        return jsonify({
            "status": "started",
            "operation_id": operation_id,
            "message": f"Backfill started for {weeks} weeks",
            "weeks": weeks,
            "estimated_duration_minutes": estimated_minutes,
            "start_time": datetime.now().isoformat()
        }), 202

    except Exception:
        logger.exception("manual_backfill | UNEXPECTED_ERROR")
        return jsonify({
            "status": "error",
            "message": "Unexpected error"
        }), 500


@api_bp.route('/backfill/<operation_id>', methods=['GET'])
def get_backfill_status(operation_id):
    """Get status of a specific backfill operation."""
    if operation_id not in backfill_operations:
        return jsonify({
            "status": "error",
            "message": f"Operation {operation_id} not found"
        }), 404

    op = backfill_operations[operation_id]
    response = {
        "operation_id": operation_id,
        "status": op.get('status'),
        "weeks": op.get('weeks'),
        "start_time": op.get('start_time').isoformat() if op.get('start_time') else None,
        "end_time": op.get('end_time').isoformat() if op.get('end_time') else None,
    }

    if op.get('status') == 'completed':
        response['output'] = op.get('output', '')
        response['duration_seconds'] = (op['end_time'] - op['start_time']).total_seconds()

    if op.get('status') == 'error':
        response['error'] = op.get('error')

    return jsonify(response), 200


@api_bp.route('/backfill', methods=['GET'])
def list_backfill_operations():
    """List all backfill operations."""
    operations = []
    for op_id, op in backfill_operations.items():
        operations.append({
            "operation_id": op_id,
            "status": op.get('status'),
            "weeks": op.get('weeks'),
            "start_time": op.get('start_time').isoformat() if op.get('start_time') else None,
        })

    return jsonify({
        "total_operations": len(operations),
        "operations": operations
    }), 200


@api_bp.route('/fetch-current', methods=['POST'])
def fetch_current_data():
    """Fetch current week's data manually."""
    operation_id = f"current_week_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        logger.info("fetch_current | REQUEST | operation_id=%s", operation_id)

        backfill_operations[operation_id] = {
            'status': 'running',
            'start_time': datetime.now(),
            'weeks': 1,
            'type': 'current_week'
        }

        def run_current_fetch():
            try:
                script_path = os.path.join(os.path.dirname(__file__), 'backfill_direct_python.py')
                logger.info("fetch_current | THREAD_START | operation_id=%s script_exists=%s",
                            operation_id, os.path.exists(script_path))

                env = os.environ.copy()
                env['FETCH_CURRENT_WEEK'] = 'true'
                env['BACKFILL_MODE'] = 'true'

                result = subprocess.run(
                    ['python', script_path],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=1800
                )

                log_content = result.stdout + (f"\n--- STDERR ---\n{result.stderr}" if result.stderr else "")

                log_file_path = os.path.join(os.path.dirname(__file__), f'backfill_log_{operation_id}.txt')
                with open(log_file_path, 'w') as f:
                    f.write(log_content)

                backfill_operations[operation_id]['end_time'] = datetime.now()
                backfill_operations[operation_id]['output'] = log_content
                backfill_operations[operation_id]['log_file'] = log_file_path

                if result.returncode != 0:
                    backfill_operations[operation_id]['status'] = 'error'
                    backfill_operations[operation_id]['error'] = log_content[:500]
                    logger.error("fetch_current | SCRIPT_ERROR | operation_id=%s returncode=%d output=%.300s",
                                 operation_id, result.returncode, log_content)
                else:
                    backfill_operations[operation_id]['status'] = 'completed'
                    logger.info("fetch_current | SCRIPT_SUCCESS | operation_id=%s returncode=%d output_bytes=%d",
                                operation_id, result.returncode, len(log_content))

            except subprocess.TimeoutExpired:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = 'Script timeout (30 minutes)'
                logger.error("fetch_current | TIMEOUT | operation_id=%s", operation_id)
            except Exception:
                backfill_operations[operation_id]['status'] = 'error'
                logger.exception("fetch_current | EXCEPTION | operation_id=%s", operation_id)

        thread = threading.Thread(target=run_current_fetch, daemon=True)
        thread.start()

        logger.info("fetch_current | THREAD_LAUNCHED | operation_id=%s", operation_id)

        return jsonify({
            "status": "started",
            "operation_id": operation_id,
            "message": "Current week data fetch started in background",
            "estimated_duration_minutes": 1,
            "start_time": datetime.now().isoformat()
        }), 202

    except Exception:
        logger.exception("fetch_current | MAIN_ERROR | operation_id=%s", operation_id)
        return jsonify({
            "status": "error",
            "message": "Unexpected error",
            "operation_id": operation_id
        }), 500


@api_bp.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint for API."""
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "active_backfill_operations": len([op for op in backfill_operations.values() if op.get('status') == 'running'])
    }), 200


@api_bp.route('/fetch-current/<operation_id>', methods=['GET'])
def fetch_current_status(operation_id):
    """Get status of a fetch-current operation."""
    if operation_id not in backfill_operations:
        return jsonify({
            "error": f"Operation {operation_id} not found"
        }), 404

    op = backfill_operations[operation_id]
    return jsonify({
        "operation_id": operation_id,
        "status": op.get('status', 'unknown'),
        "start_time": op.get('start_time').isoformat() if op.get('start_time') else None,
        "end_time": op.get('end_time').isoformat() if op.get('end_time') else None,
        "output": op.get('output', '')[:2000],
        "error": op.get('error', ''),
        "type": op.get('type', 'unknown')
    }), 200
