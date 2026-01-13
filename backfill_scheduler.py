#!/usr/bin/env python
"""
API endpoint for scheduled backfill (current week data)
Can be called by external cron services, GitHub Actions, or Render background jobs
"""
from flask import Blueprint, jsonify, request, render_template
from datetime import datetime, timedelta
import os
import traceback as tb

backfill_api = Blueprint('backfill_api', __name__, url_prefix='/api', template_folder='../templates')

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
        from application import create_app, db
        app = create_app()
        with app.app_context():
            return jsonify({
                'success': True,
                'message': 'API is working',
                'timestamp': datetime.now().isoformat()
            }), 200
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': tb.format_exc()
        }), 500

@backfill_api.route('/init-db', methods=['GET', 'POST'])
def init_db():
    """Initialize database schema - run migrations and create tables"""
    try:
        from application import db, create_app
        import os
        
        app = create_app()
        with app.app_context():
            # Try to run migrations if they exist
            migration_status = "Skipped (no migrations folder)"
            migrations_path = os.path.join(os.path.dirname(__file__), 'migrations')
            
            if os.path.exists(migrations_path):
                try:
                    from flask_migrate import upgrade
                    upgrade()
                    migration_status = "Migrations completed successfully"
                except Exception as mig_err:
                    migration_status = f"Migration skipped: {str(mig_err)[:100]}"
            
            # Always create all tables if not exist
            db.create_all()
            
            return jsonify({
                'success': True,
                'message': 'Database initialized successfully',
                'migration_status': migration_status,
                'tables_created': True,
                'timestamp': datetime.now().isoformat()
            }), 200
            
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@backfill_api.route('/backfill/current-week', methods=['POST', 'GET'])
def backfill_current_week():
    """
    Trigger backfill for current week data
    
    GET: Simple trigger (no auth required for development, add key-based auth for production)
    POST: With optional week override
    
    Query/Body params:
    - api_key: (optional) Authentication key
    - week_offset: (optional) Days offset from today (0=current week, -7=last week)
    
    Returns:
    {
        "success": bool,
        "week": "2025-01-13 to 2025-01-19",
        "total_inserted": 12345,
        "total_duplicates": 10,
        "total_errors": 5,
        "stats_by_type": {...}
    }
    """
    
    # Wrap everything in try-catch to ensure JSON response
    try:
        # Optional: Validate API key
        try:
            api_key = request.args.get('api_key') or (request.get_json(silent=True).get('api_key') if request.is_json else None)
        except:
            api_key = request.args.get('api_key')
            
        expected_key = os.environ.get('BACKFILL_API_KEY')
        
        if expected_key and api_key != expected_key:
            return jsonify({'error': 'Unauthorized'}), 401
        
        try:
            from application import create_app, db
            from data_pipeline import process_event_data
            
            app = create_app()
            with app.app_context():
                # Get week dates
                today = datetime.now()
                try:
                    week_offset = int(request.args.get('week_offset', 0))
                except:
                    week_offset = 0
                    
                monday = today - timedelta(days=today.weekday() + week_offset)
                sunday = monday + timedelta(days=6)
                
                endpoints = [
                    ("Trip", "trip_data", "Trip"),
                    ("Speeding", "speeding_data", "Speeding"),
                    ("Idle", "idle_data", "Idle"),
                    ("AWH", "awh_data", "AWH"),
                    ("WH", "wh_data", "WH"),
                    ("HA", "ha_data", "HA"),
                    ("HB", "hb_data", "HB"),
                    ("WU", "wu_data", "WU"),
                ]
                
                all_stats = {}
                total_inserted = 0
                total_dupes = 0
                total_errors = 0
                
                for label, response_key, event_name in endpoints:
                    try:
                        result = process_event_data(event_name, response_key, week_start=monday, week_end=sunday)
                        
                        if result and 'db_stats' in result:
                            stats = result['db_stats']
                            inserted = stats.get('inserted', 0)
                            duplicates = stats.get('duplicates', 0)
                            errors = stats.get('errors', 0)
                            
                            all_stats[label] = {
                                'inserted': inserted,
                                'duplicates': duplicates,
                                'errors': errors
                            }
                            
                            total_inserted += inserted
                            total_dupes += duplicates
                            total_errors += errors
                        else:
                            all_stats[label] = {'inserted': 0, 'duplicates': 0, 'errors': 0}
                    except Exception as e:
                        all_stats[label] = {
                            'error': str(e)[:200]
                        }
                        total_errors += 1
                
                return jsonify({
                    'success': True,
                    'week': f"{monday.strftime('%Y-%m-%d')} to {sunday.strftime('%Y-%m-%d')}",
                    'total_inserted': total_inserted,
                    'total_duplicates': total_dupes,
                    'total_errors': total_errors,
                    'stats_by_type': all_stats,
                    'timestamp': datetime.now().isoformat()
                }), 200
        
        except Exception as main_error:
            return jsonify({
                'success': False,
                'error': str(main_error)[:500],
                'error_type': type(main_error).__name__,
                'timestamp': datetime.now().isoformat()
            }), 500
            
    except Exception as outer_error:
        # Ultimate fallback - ensure we always return JSON
        return jsonify({
            'success': False,
            'error': 'Unexpected error: ' + str(outer_error)[:300],
            'timestamp': datetime.now().isoformat()
        }), 500

@backfill_api.route('/backfill/status', methods=['GET'])
def backfill_status():
    """Get status of last backfill"""
    try:
        from application import db, create_app
        from models import FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
        
        # Ensure we're in app context
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
                except Exception as table_error:
                    stats[name] = {'error': str(table_error)}
            
            return jsonify({
                'success': True,
                'total_records': total_records,
                'stats_by_type': stats,
                'timestamp': datetime.now().isoformat()
            }), 200
        
    except Exception as e:
        import traceback
        return jsonify({
            'success': False,
            'error': str(e),
            'traceback': traceback.format_exc()
        }), 500

@backfill_api.route('/dashboard', methods=['GET'])
def dashboard():
    """Dashboard for monitoring and triggering backfills"""
    return render_template('backfill_dashboard.html')
