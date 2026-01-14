"""
Manual data fetch API endpoints for user-initiated backfill operations.
Allows triggering backfill for specific date ranges or week counts.
"""

from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import threading
import subprocess
import os
import sys

api_bp = Blueprint('api', __name__, url_prefix='/api')

# Track backfill operations
backfill_operations = {}

def log_operation(operation_id, message):
    """Log operation messages to both stderr and a file."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] [{operation_id}] {message}"
    
    # Write to stderr so it appears in Render logs
    print(log_msg, file=sys.stderr, flush=True)
    print(log_msg, flush=True)  # Also stdout
    
    # Write to file as backup
    try:
        log_file = os.path.join(os.path.dirname(__file__), 'operations.log')
        with open(log_file, 'a') as f:
            f.write(log_msg + '\n')
    except:
        pass


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
    operation_id = f"current_week_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        # Log to main thread stderr so it appears in Render logs
        print(f"[FETCH-CURRENT] {operation_id} - Request received", file=sys.stderr, flush=True)
        print(f"[FETCH-CURRENT] {operation_id} - Request received", flush=True)
        
        backfill_operations[operation_id] = {
            'status': 'running',
            'start_time': datetime.now(),
            'weeks': 1,
            'type': 'current_week'
        }
        
        print(f"[FETCH-CURRENT] {operation_id} - Operation created, starting background thread", file=sys.stderr, flush=True)
        
        def run_current_fetch():
            try:
                print(f"[FETCH-CURRENT] {operation_id} - Background thread started", file=sys.stderr, flush=True)
                
                script_path = os.path.join(os.path.dirname(__file__), 'backfill_direct_python.py')
                print(f"[FETCH-CURRENT] {operation_id} - Script: {script_path}, exists: {os.path.exists(script_path)}", file=sys.stderr, flush=True)
                
                # Set environment variable to fetch current week
                env = os.environ.copy()
                env['FETCH_CURRENT_WEEK'] = 'true'
                env['BACKFILL_MODE'] = 'true'  # Skip render/result calls for speed
                
                print(f"[FETCH-CURRENT] {operation_id} - Executing backfill script with BACKFILL_MODE=true...", file=sys.stderr, flush=True)
                
                # Always capture output
                result = subprocess.run(
                    ['python', script_path],
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=1800  # 30 min timeout
                )
                
                # Write captured output to file
                log_file_path = os.path.join(os.path.dirname(__file__), f'backfill_log_{operation_id}.txt')
                with open(log_file_path, 'w') as f:
                    f.write(result.stdout)
                    if result.stderr:
                        f.write(f"\n--- STDERR ---\n{result.stderr}")
                
                log_content = result.stdout + (f"\n--- STDERR ---\n{result.stderr}" if result.stderr else "")
                
                # Print key results to stderr so they appear in Render
                print(f"[FETCH-CURRENT] {operation_id} - Script completed with code: {result.returncode}", file=sys.stderr, flush=True)
                print(f"[FETCH-CURRENT] {operation_id} - Output size: {len(log_content)} bytes", file=sys.stderr, flush=True)
                
                backfill_operations[operation_id]['status'] = 'completed'
                backfill_operations[operation_id]['end_time'] = datetime.now()
                backfill_operations[operation_id]['output'] = log_content
                backfill_operations[operation_id]['log_file'] = log_file_path
                
                if result.returncode != 0:
                    backfill_operations[operation_id]['status'] = 'error'
                    backfill_operations[operation_id]['error'] = log_content[:500]
                    print(f"[FETCH-CURRENT] {operation_id} - ERROR: {log_content[:300]}", file=sys.stderr, flush=True)
                else:
                    print(f"[FETCH-CURRENT] {operation_id} - SUCCESS", file=sys.stderr, flush=True)
                    
            except subprocess.TimeoutExpired:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = 'Script timeout (30 minutes)'
                print(f"[FETCH-CURRENT] {operation_id} - TIMEOUT", file=sys.stderr, flush=True)
            except Exception as e:
                backfill_operations[operation_id]['status'] = 'error'
                backfill_operations[operation_id]['error'] = str(e)
                print(f"[FETCH-CURRENT] {operation_id} - EXCEPTION: {type(e).__name__}: {str(e)}", file=sys.stderr, flush=True)

        
        thread = threading.Thread(target=run_current_fetch, daemon=True)
        thread.start()
        
        print(f"[FETCH-CURRENT] {operation_id} - Responding with 202", file=sys.stderr, flush=True)
        
        return jsonify({
            "status": "started",
            "operation_id": operation_id,
            "message": "Current week data fetch started in background",
            "estimated_duration_minutes": 1,
            "start_time": datetime.now().isoformat()
        }), 202
        
    except Exception as e:
        print(f"[FETCH-CURRENT] {operation_id} - MAIN ERROR: {type(e).__name__}: {str(e)}", file=sys.stderr, flush=True)
        return jsonify({
            "status": "error",
            "message": str(e),
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
        "output": op.get('output', '')[:2000],  # First 2000 chars
        "error": op.get('error', ''),
        "type": op.get('type', 'unknown')
    }), 200
