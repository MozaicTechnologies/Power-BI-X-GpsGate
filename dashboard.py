"""
Dashboard route for monitoring backfill operations and system status.
Provides HTML dashboard with Quick Status, Recent Stats, Operation Logs, and API Reference.
"""

from flask import Blueprint, render_template_string, jsonify, make_response, request
from datetime import datetime
import os

dashboard_bp = Blueprint('dashboard', __name__, url_prefix='/api')

# Store recent logs
recent_logs = []

def add_log(message, level="info"):
    """Add message to recent logs"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    recent_logs.append({
        "timestamp": timestamp,
        "level": level,
        "message": message
    })
    # Keep only last 20 logs
    if len(recent_logs) > 20:
        recent_logs.pop(0)

@dashboard_bp.route('/dashboard', methods=['GET'])
def dashboard():
    """Main dashboard HTML page"""
    html_template = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>GPS Gate Backfill Dashboard</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                min-height: 100vh;
                padding: 20px;
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
            }
            
            .header {
                background: white;
                border-radius: 8px;
                padding: 30px;
                margin-bottom: 20px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .header h1 {
                color: #333;
                margin-bottom: 15px;
                display: flex;
                align-items: center;
                gap: 10px;
            }
            
            .status-badge {
                display: inline-block;
                padding: 5px 12px;
                border-radius: 20px;
                font-size: 12px;
                font-weight: 600;
                margin-left: 10px;
            }
            
            .status-badge.running {
                background: #48bb78;
                color: white;
                animation: pulse 2s infinite;
            }
            
            .status-badge.idle {
                background: #cbd5e0;
                color: #333;
            }
            
            @keyframes pulse {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }
            
            .header p {
                color: #666;
                font-size: 14px;
            }
            
            .btn-group {
                display: flex;
                gap: 10px;
                margin-top: 15px;
                flex-wrap: wrap;
            }
            
            button {
                padding: 10px 20px;
                border: none;
                border-radius: 4px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 600;
                transition: all 0.3s ease;
            }
            
            .btn-primary {
                background: #667eea;
                color: white;
            }
            
            .btn-primary:hover {
                background: #5568d3;
                transform: translateY(-2px);
            }
            
            .btn-secondary {
                background: #764ba2;
                color: white;
            }
            
            .btn-secondary:hover {
                background: #653a8a;
                transform: translateY(-2px);
            }
            
            .btn-success {
                background: #48bb78;
                color: white;
            }
            
            .btn-success:hover {
                background: #38a169;
                transform: translateY(-2px);
            }
            
            .grid {
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 20px;
                margin-bottom: 20px;
            }
            
            .card {
                background: white;
                border-radius: 8px;
                padding: 20px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.1);
            }
            
            .card h3 {
                color: #333;
                margin-bottom: 15px;
                display: flex;
                align-items: center;
                gap: 8px;
            }
            
            .status-item {
                display: flex;
                justify-content: space-between;
                padding: 10px 0;
                border-bottom: 1px solid #eee;
            }
            
            .status-item:last-child {
                border-bottom: none;
            }
            
            .status-label {
                color: #666;
                font-weight: 500;
            }
            
            .status-value {
                color: #333;
                font-weight: 600;
            }
            
            .status-success {
                color: #48bb78;
            }
            
            .status-error {
                color: #f56565;
            }
            
            .status-warning {
                color: #ed8936;
            }
            
            .event-type {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 8px 0;
                border-bottom: 1px solid #eee;
            }
            
            .event-type:last-child {
                border-bottom: none;
            }
            
            .event-name {
                color: #333;
                font-weight: 500;
            }
            
            .event-status {
                padding: 4px 12px;
                border-radius: 4px;
                font-size: 12px;
                font-weight: 600;
            }
            
            .event-status.error {
                background: #fed7d7;
                color: #c53030;
            }
            
            .event-status.success {
                background: #c6f6d5;
                color: #22543d;
            }
            
            .logs-container {
                background: #1a202c;
                border-radius: 8px;
                padding: 15px;
                font-family: 'Monaco', 'Menlo', monospace;
                font-size: 12px;
                color: #e2e8f0;
                max-height: 300px;
                overflow-y: auto;
            }
            
            .log-entry {
                margin-bottom: 8px;
                padding: 5px;
            }
            
            .log-timestamp {
                color: #cbd5e0;
            }
            
            .log-info {
                color: #63b3ed;
            }
            
            .log-error {
                color: #fc8181;
            }
            
            .log-warning {
                color: #f6ad55;
            }
            
            .api-endpoint {
                background: #f7fafc;
                border-left: 4px solid #667eea;
                padding: 12px;
                margin-bottom: 10px;
                border-radius: 4px;
            }
            
            .api-method {
                display: inline-block;
                padding: 4px 8px;
                border-radius: 3px;
                font-size: 12px;
                font-weight: 600;
                margin-right: 10px;
            }
            
            .api-method.post {
                background: #bee3f8;
                color: #2c5282;
            }
            
            .api-method.get {
                background: #c6f6d5;
                color: #22543d;
            }
            
            .api-path {
                color: #2d3748;
                font-weight: 500;
            }
            
            .api-description {
                color: #718096;
                font-size: 12px;
                margin-top: 5px;
            }
            
            .full-width {
                grid-column: 1 / -1;
            }
            
            @media (max-width: 768px) {
                .grid {
                    grid-template-columns: 1fr;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <!-- Header -->
            <div class="header">
                <h1>
                    🚀 GPS Gate Backfill Dashboard
                    <span class="status-badge idle" id="statusBadge">Idle</span>
                </h1>
                <p>Manage and monitor data synchronization</p>
                <div class="btn-group">
                    <button class="btn-success" onclick="runMigration()" id="migrateBtn">🔧 Fix Database Schema</button>
                    <button class="btn-primary" onclick="startBackfill(54)">▶ Start Backfill (54 weeks)</button>
                    <button class="btn-secondary" onclick="startBackfill(1)">🔄 Manual Sync (1 week)</button>
                    <button class="btn-success" onclick="checkStatus()">📈 Check Status</button>
                    
                    <!-- API Endpoint Buttons -->
                    <button class="btn-secondary" onclick="callEndpoint('/api/health', 'GET')" style="font-size:11px;">GET /api/health</button>
                    <button class="btn-secondary" onclick="callEndpoint('/api/backfill', 'GET')" style="font-size:11px;">GET /api/backfill</button>
                    <button class="btn-secondary" onclick="callEndpoint('/api/fetch-current', 'POST')" style="font-size:11px;">POST /api/fetch-current</button>
                    <button class="btn-secondary" onclick="callEndpoint('/api/dashboard/stats', 'GET')" style="font-size:11px;">GET /api/dashboard/stats</button>
                </div>
            </div>
            
            <!-- Main Grid -->
            <div class="grid">
                <!-- Quick Status -->
                <div class="card">
                    <h3>⚡ Quick Status</h3>
                    <div class="status-item">
                        <span class="status-label">Service Status:</span>
                        <span class="status-value status-success">● Online</span>
                    </div>
                    <div class="status-item">
                        <span class="status-label">Total Records:</span>
                        <span class="status-value" id="totalRecords">0</span>
                    </div>
                    <div class="status-item">
                        <span class="status-label">Active Operations:</span>
                        <span class="status-value" id="activeOps">0</span>
                    </div>
                    <div class="status-item">
                        <span class="status-label">Last Updated:</span>
                        <span class="status-value" id="lastUpdate">--:--:--</span>
                    </div>
                </div>
                
                <!-- Recent Stats (8 Event Types) -->
                <div class="card">
                    <h3>📊 Recent Stats (8 Event Types)</h3>
                    <div class="event-type">
                        <span class="event-name">Trip Events:</span>
                        <span class="event-status" id="trip-status">Error</span>
                        <span class="event-records" id="trip-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">Speeding Events:</span>
                        <span class="event-status" id="speeding-status">Error</span>
                        <span class="event-records" id="speeding-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">Idle Events:</span>
                        <span class="event-status" id="idle-status">Error</span>
                        <span class="event-records" id="idle-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">AWH Events:</span>
                        <span class="event-status" id="awh-status">Error</span>
                        <span class="event-records" id="awh-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">WH Events:</span>
                        <span class="event-status" id="wh-status">Error</span>
                        <span class="event-records" id="wh-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">HA Events:</span>
                        <span class="event-status" id="ha-status">Error</span>
                        <span class="event-records" id="ha-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">HB Events:</span>
                        <span class="event-status" id="hb-status">Error</span>
                        <span class="event-records" id="hb-count">(0)</span>
                    </div>
                    <div class="event-type">
                        <span class="event-name">WU Events:</span>
                        <span class="event-status" id="wu-status">Error</span>
                        <span class="event-records" id="wu-count">(0)</span>
                    </div>
                </div>
                
                <!-- Operation Logs -->
                <div class="card full-width">
                    <h3>📝 Operation Logs</h3>
                    <div class="logs-container" id="logsContainer">
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:18</span> <span class="log-info">➜</span> System started up...
                        </div>
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:18</span> <span class="log-info">✓</span> GpsGate API initialized
                        </div>
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:19</span> <span class="log-info">✓</span> Database connected successfully
                        </div>
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:19</span> <span class="log-info">➜</span> Checking database schema...
                        </div>
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:19</span> <span class="log-info">✓</span> All tables available
                        </div>
                        <div class="log-entry">
                            <span class="log-timestamp">13:34:20</span> <span class="log-info">📡</span> Ready for backfill
                        </div>
                    </div>
                </div>
                
                <!-- API Endpoints Reference -->
                <div class="card full-width">
                    <h3>🔌 API Endpoints Reference</h3>
                    
                    <div class="api-endpoint">
                        <span class="api-method get">GET</span>
                        <span class="api-path">/api/health</span>
                        <div class="api-description">Check API status and service health</div>
                    </div>
                    
                    <div class="api-endpoint">
                        <span class="api-method post">POST</span>
                        <span class="api-path">/api/backfill</span>
                        <div class="api-description">Start backfill with configurable weeks</div>
                    </div>
                    
                    <div class="api-endpoint">
                        <span class="api-method get">GET</span>
                        <span class="api-path">/api/backfill/&lt;operation_id&gt;</span>
                        <div class="api-description">Get operation status and progress</div>
                    </div>
                    
                    <div class="api-endpoint">
                        <span class="api-method get">GET</span>
                        <span class="api-path">/api/backfill</span>
                        <div class="api-description">List all backfill operations</div>
                    </div>
                    
                    <div class="api-endpoint">
                        <span class="api-method post">POST</span>
                        <span class="api-path">/api/fetch-current</span>
                        <div class="api-description">Fetch only this week's data</div>
                    </div>
                    
                    <div class="api-endpoint">
                        <span class="api-method get">GET</span>
                        <span class="api-path">/api/dashboard</span>
                        <div class="api-description">This dashboard</div>
                    </div>
                    
                    <div style="margin-top: 15px; padding: 10px; background: #edf2f7; border-radius: 4px;">
                        <strong>Event Types (8 totals):</strong> Trip, Speeding, Idle, AWH, WH, HA, HB, WU
                    </div>
                </div>
            </div>
        </div>
        
        <script>
            function startBackfill(weeks) {
                const endpoint = weeks === 1 ? '/api/fetch-current' : '/api/backfill';
                const body = JSON.stringify({ weeks: weeks });
                
                fetch(endpoint, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: body
                })
                .then(r => {
                    if (!r.ok) throw new Error('HTTP ' + r.status);
                    return r.json();
                })
                .then(data => {
                    const opId = data.operation_id || 'Unknown';
                    const duration = data.estimated_duration_minutes || '?';
                    alert('Backfill started!\n\nOperation ID: ' + opId + '\nEstimated time: ' + duration + ' minutes');
                    updateLogs('✓ Backfill started: ' + opId + ' (' + weeks + ' weeks)');
                    checkStatus();
                })
                .catch(e => {
                    alert('Error starting backfill: ' + e.message);
                    updateLogs('✗ Backfill error: ' + e.message);
                });
            }
            
            function checkStatus() {
                fetch('/api/health')
                .then(r => {
                    if (!r.ok) throw new Error('HTTP ' + r.status);
                    return r.json();
                })
                .then(data => {
                    const status = data.status || 'unknown';
                    const ops = data.active_backfill_operations !== undefined ? data.active_backfill_operations : 0;
                    alert('API Health\n\nStatus: ' + status + '\nActive operations: ' + ops);
                    updateLogs('✓ Health check: ' + ops + ' active operations');
                })
                .catch(e => {
                    alert('Error checking status: ' + e.message);
                    updateLogs('✗ Health check failed: ' + e.message);
                });
            }
            
            function callEndpoint(endpoint, method = 'GET') {
                const options = {
                    method: method,
                    headers: { 'Content-Type': 'application/json' }
                };
                
                fetch(endpoint, options)
                .then(r => {
                    if (!r.ok) {
                        throw new Error('HTTP ' + r.status + ': ' + r.statusText);
                    }
                    return r.text().then(text => {
                        try {
                            return JSON.parse(text);
                        } catch(e) {
                            return { raw: text };
                        }
                    });
                })
                .then(data => {
                    const display = typeof data === 'string' ? data : JSON.stringify(data, null, 2);
                    alert('API: ' + endpoint + '\n\nStatus: Success\n\nResponse:\n' + display.substring(0, 300));
                    updateLogs('✓ API call: ' + endpoint);
                })
                .catch(e => {
                    alert('Error calling ' + endpoint + ':\n' + e.message);
                    updateLogs('✗ API error: ' + endpoint + ' - ' + e.message);
                });
            }
            
            function runMigration() {
                document.getElementById('migrateBtn').disabled = true;
                document.getElementById('migrateBtn').textContent = 'Running migration...';
                
                fetch('/api/dashboard/migrate', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                })
                .then(r => r.json())
                .then(data => {
                    document.getElementById('migrateBtn').disabled = false;
                    document.getElementById('migrateBtn').textContent = 'Fix Database Schema';
                    
                    if (data.status === 'success') {
                        alert('Database migration successful!\\n\\nThe is_duplicate column has been added to all tables.\\n\\nYou can now start the backfill.');
                        updateLogs('[SUCCESS] Database migration completed successfully');
                    } else {
                        alert('Migration failed:\\n\\n' + data.message);
                        updateLogs('[ERROR] Migration failed: ' + data.message);
                    }
                })
                .catch(e => {
                    document.getElementById('migrateBtn').disabled = false;
                    document.getElementById('migrateBtn').textContent = 'Fix Database Schema';
                    alert('Error: ' + e);
                    updateLogs('[ERROR] Migration error: ' + e);
                });
            }
            
            // Auto-refresh stats every 2 seconds
            setInterval(() => {
                fetch('/api/dashboard/stats')
                    .then(r => r.json())
                    .then(data => {
                        console.log('Stats data:', data);
                        
                        // Update status badge
                        const badge = document.getElementById('statusBadge');
                        if (data.active_operations > 0) {
                            badge.textContent = 'Syncing...';
                            badge.className = 'status-badge running';
                        } else {
                            badge.textContent = 'Idle';
                            badge.className = 'status-badge idle';
                        }
                        
                        // Update Quick Status card
                        const totalEl = document.getElementById('totalRecords');
                        const activeEl = document.getElementById('activeOps');
                        const updateEl = document.getElementById('lastUpdate');
                        
                        if (totalEl) totalEl.textContent = data.total_records || 0;
                        if (activeEl) activeEl.textContent = data.active_operations || 0;
                        if (updateEl) updateEl.textContent = new Date().toLocaleTimeString();
                        
                        // Update event types in Recent Stats card
                        if (data.event_types) {
                            const eventMap = {
                                'Trip': { status: 'trip-status', count: 'trip-count' },
                                'Speeding': { status: 'speeding-status', count: 'speeding-count' },
                                'Idle': { status: 'idle-status', count: 'idle-count' },
                                'AWH': { status: 'awh-status', count: 'awh-count' },
                                'WH': { status: 'wh-status', count: 'wh-count' },
                                'HA': { status: 'ha-status', count: 'ha-count' },
                                'HB': { status: 'hb-status', count: 'hb-count' },
                                'WU': { status: 'wu-status', count: 'wu-count' }
                            };
                            
                            Object.entries(data.event_types).forEach(([type, info]) => {
                                const map = eventMap[type];
                                if (map) {
                                    const statusEl = document.getElementById(map.status);
                                    const countEl = document.getElementById(map.count);
                                    
                                    if (statusEl) {
                                        statusEl.textContent = info.status === 'success' ? '✓ Success' : 'Error';
                                        statusEl.className = 'event-status ' + info.status;
                                    }
                                    if (countEl) {
                                        countEl.textContent = '(' + (info.records || 0) + ')';
                                    }
                                }
                            });
                        }
                    })
                    .catch(e => console.log('Stats refresh failed'));
                    
                // Get and update logs
                fetch('/api/dashboard/logs')
                    .then(r => r.json())
                    .then(data => {
                        const logsContainer = document.getElementById('logsContainer');
                        if (data.logs && data.logs.length > 0) {
                            logsContainer.innerHTML = data.logs.map(log => {
                                let levelClass = 'log-info';
                                if (log.level === 'error') levelClass = 'log-error';
                                if (log.level === 'warning') levelClass = 'log-warning';
                                return `<div class="log-entry"><span class="log-timestamp">${log.timestamp}</span> <span class="${levelClass}">➜</span> ${log.message}</div>`;
                            }).join('');
                            logsContainer.scrollTop = logsContainer.scrollHeight;
                        }
                    })
                    .catch(e => console.log('Logs refresh failed'));
            }, 2000);
            
            function updateLogs(message) {
                const logsContainer = document.getElementById('logsContainer');
                const timestamp = new Date().toLocaleTimeString();
                const newLog = `<div class="log-entry"><span class="log-timestamp">${timestamp}</span> <span class="log-info">➜</span> ${message}</div>`;
                logsContainer.innerHTML += newLog;
                logsContainer.scrollTop = logsContainer.scrollHeight;
            }
            
            // Auto-refresh every 30 seconds
            setInterval(() => {
                fetch('/api/health')
                    .then(r => r.json())
                    .catch(e => console.log('Refresh failed'));
            }, 30000);
        </script>
    </body>
    </html>
    '''
    
    response = make_response(render_template_string(html_template))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@dashboard_bp.route('/dashboard/logs', methods=['GET'])
def get_logs():
    """Get recent operation logs"""
    return jsonify({
        "logs": recent_logs[-30:] if recent_logs else [],
        "timestamp": datetime.now().isoformat()
    })

@dashboard_bp.route('/dashboard/add-log', methods=['POST'])
def add_operation_log():
    """Add a log entry (called from backfill processes)"""
    try:
        data = request.get_json() or {}
        message = data.get('message', '')
        level = data.get('level', 'info')
        add_log(message, level)
        return jsonify({"status": "logged"}), 200
    except Exception as e:
        return jsonify({"status": "error", "error": str(e)}), 500

@dashboard_bp.route('/dashboard/stats', methods=['GET'])
def dashboard_stats():
    """API endpoint returning dashboard statistics as JSON"""
    from api import backfill_operations
    from models import db, FactTrip, FactSpeeding, FactIdle, FactAWH, FactWH, FactHA, FactHB, FactWU
    
    try:
        # Count active operations
        active_ops = sum(1 for op in backfill_operations.values() if op.get('status') == 'running')
        
        # Count total records from database
        try:
            trip_count = db.session.query(FactTrip).count()
            speeding_count = db.session.query(FactSpeeding).count()
            idle_count = db.session.query(FactIdle).count()
            awh_count = db.session.query(FactAWH).count()
            wh_count = db.session.query(FactWH).count()
            ha_count = db.session.query(FactHA).count()
            hb_count = db.session.query(FactHB).count()
            wu_count = db.session.query(FactWU).count()
            
            total_records = trip_count + speeding_count + idle_count + awh_count + wh_count + ha_count + hb_count + wu_count
        except Exception as e:
            total_records = 0
            trip_count = speeding_count = idle_count = awh_count = wh_count = ha_count = hb_count = wu_count = 0
        
        return jsonify({
            "service_status": "online",
            "total_records": total_records,
            "active_operations": active_ops,
            "recent_logs": recent_logs[-20:],
            "event_types": {
                "Trip": {"status": "success" if trip_count > 0 else "error", "records": trip_count},
                "Speeding": {"status": "success" if speeding_count > 0 else "error", "records": speeding_count},
                "Idle": {"status": "success" if idle_count > 0 else "error", "records": idle_count},
                "AWH": {"status": "success" if awh_count > 0 else "error", "records": awh_count},
                "WH": {"status": "success" if wh_count > 0 else "error", "records": wh_count},
                "HA": {"status": "success" if ha_count > 0 else "error", "records": ha_count},
                "HB": {"status": "success" if hb_count > 0 else "error", "records": hb_count},
                "WU": {"status": "success" if wu_count > 0 else "error", "records": wu_count}
            },
            "timestamp": datetime.now().isoformat()
        })
    except Exception as e:
        return jsonify({
            "service_status": "error",
            "error": str(e),
            "active_operations": 0,
            "total_records": 0,
            "recent_logs": recent_logs[-20:]
        }), 500

@dashboard_bp.route('/dashboard/migrate', methods=['POST'])
def run_migration():
    """Run database migration to add missing columns"""
    try:
        import subprocess
        result = subprocess.run(
            ['python', 'migrate_add_columns.py'],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode == 0:
            return jsonify({
                "status": "success",
                "message": "Database migration completed successfully",
                "output": result.stdout
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Migration failed",
                "error": result.stderr
            }), 500
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
