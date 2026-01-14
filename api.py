"""
Manual data fetch API endpoints for user-initiated backfill operations.
Allows triggering backfill for specific date ranges or week counts.
"""

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import threading
import subprocess
import os

api_bp = Blueprint('api', __name__, url_prefix='/api')

# Track backfill operations
backfill_operations = {}


@api_bp.route('/backfill', methods=['POST'])
def manual_backfill():
    """
    Manual backfill endpoint - initiate data fetch for custom date range or weeks.
    
    Request JSON:
    {
        "weeks": 54,  # Optional: number of weeks to backfill (default 1)
        "start_date": "2025-01-01",  # Optional: start date (YYYY-MM-DD)
        "end_date": "2025-12-31",    # Optional: end date (YYYY-MM-DD)
        "event_types": ["Trip", "Speeding", "Idle", "AWH", "WH", "HA", "HB", "WU"]  # Optional: specific event types
    }
    
    Response:
    {
        "status": "started",
        "operation_id": "unique-id",
        "message": "Backfill started for X weeks",
        "estimated_duration_minutes": 45
    }
    """
    try:
        data = request.get_json() or {}
        weeks = data.get('weeks', 1)
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        event_types = data.get('event_types')
        
        # Validate input
        if weeks < 1 or weeks > 54:
            return jsonify({
                "status": "error",
                "message": "weeks must be between 1 and 54"
            }), 400
        
        # Create unique operation ID
        operation_id = f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Prepare environment for backfill
        env = os.environ.copy()
        if event_types:
            env['BACKFILL_EVENT_TYPES'] = ','.join(event_types)
        
        # Start backfill in background thread
        def run_backfill():
            try:
                backfill_operations[operation_id] = {
                    'status': 'running',
                    'start_time': datetime.now(),
                    'weeks': weeks,
                    'progress': 0
                }
                
                # Execute backfill script
                script_path = os.path.join(
                    os.path.dirname(__file__),
                    'backfill_direct_python.py'
                )
                
                result = subprocess.run(
                    ['python', script_path],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=7200  # 2 hour timeout
                )
                
                backfill_operations[operation_id]['status'] = 'completed'
                backfill_operations[operation_id]['end_time'] = datetime.now()
                backfill_operations[operation_id]['output'] = result.stdout
                
                if result.returncode != 0:
                    backfill_operations[operation_id]['status'] = 'error'
                    backfill_operations[operation_id]['error'] = result.stderr
                    
            except Exception as e:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = str(e)
        
        # Start backfill thread
        thread = threading.Thread(target=run_backfill, daemon=True)
        thread.start()
        
        # Estimate duration: ~1 minute per week
        estimated_minutes = weeks
        
        return jsonify({
            "status": "started",
            "operation_id": operation_id,
            "message": f"Backfill started for {weeks} weeks",
            "weeks": weeks,
            "estimated_duration_minutes": estimated_minutes,
            "start_time": datetime.now().isoformat()
        }), 202
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
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
    """
    Fetch current week's data manually.
    Useful for getting latest data without waiting for scheduled backfill.
    """
    try:
        operation_id = f"current_week_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        def run_current_fetch():
            try:
                print(f"\n{'='*80}")
                print(f"[FETCH-CURRENT] Starting operation {operation_id}", flush=True)
                print(f"{'='*80}", flush=True)
                
                backfill_operations[operation_id] = {
                    'status': 'running',
                    'start_time': datetime.now(),
                    'weeks': 1,
                    'type': 'current_week'
                }
                print(f"[FETCH-CURRENT] Operation status set to running", flush=True)
                print(f"[FETCH-CURRENT] Total backfill_operations: {len(backfill_operations)}", flush=True)
                
                script_path = os.path.join(
                    os.path.dirname(__file__),
                    'backfill_direct_python.py'
                )
                print(f"[FETCH-CURRENT] Running script: {script_path}", flush=True)
                print(f"[FETCH-CURRENT] Script exists: {os.path.exists(script_path)}", flush=True)
                
                # Set environment variable to fetch current week
                env = os.environ.copy()
                env['FETCH_CURRENT_WEEK'] = 'true'
                print(f"[FETCH-CURRENT] Environment FETCH_CURRENT_WEEK=true", flush=True)
                
                print(f"[FETCH-CURRENT] Starting subprocess.run()...", flush=True)
                result = subprocess.run(
                    ['python', script_path],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=1800  # 30 min timeout for current week
                )
                
                print(f"[FETCH-CURRENT] Script completed with return code: {result.returncode}", flush=True)
                print(f"[FETCH-CURRENT] STDOUT length: {len(result.stdout)} chars", flush=True)
                print(f"[FETCH-CURRENT] STDERR length: {len(result.stderr)} chars", flush=True)
                
                backfill_operations[operation_id]['status'] = 'completed'
                backfill_operations[operation_id]['end_time'] = datetime.now()
                backfill_operations[operation_id]['output'] = result.stdout
                
                if result.returncode != 0:
                    backfill_operations[operation_id]['status'] = 'error'
                    backfill_operations[operation_id]['error'] = result.stderr
                    print(f"[FETCH-CURRENT] ❌ Error output:\n{result.stderr[:1000]}", flush=True)
                else:
                    print(f"[FETCH-CURRENT] ✓ Success! Output (first 1000 chars):\n{result.stdout[:1000]}", flush=True)
                    
                print(f"{'='*80}", flush=True)
                print(f"[FETCH-CURRENT] Operation {operation_id} completed", flush=True)
                print(f"{'='*80}\n", flush=True)
                    
            except subprocess.TimeoutExpired:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = 'Script timeout (30 minutes)'
                print(f"[FETCH-CURRENT] ⏱️ TIMEOUT: Script did not complete within 30 minutes", flush=True)
            except Exception as e:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = str(e)
                print(f"[FETCH-CURRENT] ❌ Exception: {type(e).__name__}: {str(e)}", flush=True)
                import traceback
                print(f"[FETCH-CURRENT] Traceback:\n{traceback.format_exc()}", flush=True)
        
        print(f"[FETCH-CURRENT] Creating daemon thread for operation {operation_id}", flush=True)
        thread = threading.Thread(target=run_current_fetch, daemon=True)
        thread.start()
        print(f"[FETCH-CURRENT] Thread started, responding with 202", flush=True)
        
        return jsonify({
            "status": "started",
            "operation_id": operation_id,
            "message": "Current week data fetch started",
            "estimated_duration_minutes": 1,
            "start_time": datetime.now().isoformat()
        }), 202
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
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
        "output": op.get('output', '')[:2000],  # First 2000 chars
        "error": op.get('error', ''),
        "type": op.get('type', 'unknown')
    }), 200
