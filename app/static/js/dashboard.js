// Set default dates (yesterday)
const today = new Date();
const yesterday = new Date(today);
yesterday.setDate(yesterday.getDate() - 1);
document.getElementById('fact-start').value = yesterday.toISOString().split('T')[0];
document.getElementById('fact-end').value = yesterday.toISOString().split('T')[0];

// Button loading state
function setLoading(btn, loading) {
    if (!btn) return;
    if (loading) {
        btn._originalText = btn.innerHTML;
        btn.innerHTML = '⏳ Running…';
        btn.disabled = true;
        btn.style.opacity = '0.7';
        btn.style.cursor = 'not-allowed';
    } else {
        btn.innerHTML = btn._originalText || btn.innerHTML;
        btn.disabled = false;
        btn.style.opacity = '';
        btn.style.cursor = '';
    }
}

// Result modal
function showMessage(type, text) {
    openResultModal(type, text);
}

function showCustomerConfigMessage(type, text) {
    openResultModal(type, text);
}

function openResultModal(type, content) {
    const modal  = document.getElementById('result-modal');
    const header = document.getElementById('result-modal-header');
    const title  = document.getElementById('result-modal-title');
    const body   = document.getElementById('result-modal-body');

    const isSuccess = type === 'success';
    header.style.background = isSuccess ? '#d4edda' : '#f8d7da';
    title.style.color       = isSuccess ? '#155724' : '#721c24';
    title.textContent       = isSuccess ? '✅ Success' : '❌ Error';

    // Pretty-print if JSON object, otherwise show as-is
    if (typeof content === 'object' && content !== null) {
        body.textContent = JSON.stringify(content, null, 2);
    } else {
        try {
            const parsed = JSON.parse(content);
            body.textContent = JSON.stringify(parsed, null, 2);
        } catch {
            body.textContent = content;
        }
    }

    modal.style.display = 'flex';
}

function closeResultModal() {
    document.getElementById('result-modal').style.display = 'none';
}

function getSelectedApplicationId() {
    return document.getElementById('manual-application-id').value.trim();
}

        const _customerTokenMap = {};
        let _allCustomerConfigs = [];

        function openCustomerConfigsModal() {
            const modal = document.getElementById('customer-configs-modal');
            const body = document.getElementById('customer-configs-modal-body');
            body.innerHTML = _allCustomerConfigs.map(customer => `
                <div style="background:#f8f9fa;border-radius:10px;padding:16px;margin-bottom:12px;border-left:4px solid #667eea;">
                    <div style="font-weight:700;font-size:0.98rem;color:#333;margin-bottom:10px;padding-bottom:8px;border-bottom:1px solid #e0e0e0;">
                        ${getApplicationLabel(customer.application_id)}
                    </div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 16px;font-size:0.82rem;">
                        <div><span style="color:#888;font-weight:600;">Token</span><br><span style="color:#333;font-family:monospace;word-break:break-all;">${customer.token}</span></div>
                        <div><span style="color:#888;font-weight:600;">Tag</span><br><span style="color:#333;">${customer.tag_name || '-'} <span style="color:#aaa;">#${customer.tag_id || '-'}</span></span></div>
                        <div><span style="color:#888;font-weight:600;">Trip Report</span><br><span style="color:#333;">${customer.trip_report_name || '-'} <span style="color:#aaa;">#${customer.trip_report_id || '-'}</span></span></div>
                        <div><span style="color:#888;font-weight:600;">Event Report</span><br><span style="color:#333;">${customer.event_report_name || '-'} <span style="color:#aaa;">#${customer.event_report_id || '-'}</span></span></div>
                        <div><span style="color:#888;font-weight:600;">Speed Rule</span><br><span style="color:#333;">${customer.speed_event_rule_name || '-'} <span style="color:#aaa;">#${customer.speed_event_id || '-'}</span></span></div>
                        <div><span style="color:#888;font-weight:600;">Idle Rule</span><br><span style="color:#333;">${customer.idle_event_rule_name || '-'} <span style="color:#aaa;">#${customer.idle_event_id || '-'}</span></span></div>
                    </div>
                </div>
            `).join('');
            modal.style.display = 'flex';
        }

        function closeCustomerConfigsModal() {
            document.getElementById('customer-configs-modal').style.display = 'none';
        }

        async function refreshCustomerConfigs() {
            try {
                const response = await fetch('/dashboard/customer-config');
                const data = await response.json();
                const list = document.getElementById('customer-config-list');
                const select = document.getElementById('manual-application-id');
                const cleanupSelect = document.getElementById('cleanup-application-id');
                const currentSelection = select.value;
                if (!data.success) {
                    list.innerHTML = '<em style="color: #666;">Failed to load customer config</em>';
                    return;
                }
                const customers = data.applications || [];
                if (!customers.length) {
                    list.innerHTML = '<em style="color: #666;">No customer_config rows yet</em>';
                    select.innerHTML = '<option value="">Select customer</option>';
                    cleanupSelect.innerHTML = '<option value="">Select customer</option>';
                    return;
                }
                const optionsHtml = customers.map(customer => `
                    <option value="${customer.application_id}">${getApplicationLabel(customer.application_id)}</option>
                `).join('');
                select.innerHTML = '<option value="">Select customer</option>' + optionsHtml;
                cleanupSelect.innerHTML = '<option value="">Select customer</option>' + optionsHtml;
                const statsFilter = document.getElementById('stats-filter-app');
                if (statsFilter) {
                    const prev = statsFilter.value;
                    statsFilter.innerHTML = '<option value="">All Customers</option>' + optionsHtml;
                    if (prev) statsFilter.value = prev;
                }
                customers.forEach(c => {
                    if (c.full_token) _customerTokenMap[String(c.application_id)] = c.full_token;
                });
                _allCustomerConfigs = customers;
                if (currentSelection && customers.some(customer => customer.application_id === currentSelection)) {
                    select.value = currentSelection;
                } else {
                    select.value = customers[0].application_id;
                }
                const first = customers[0];
                const extraCount = customers.length - 1;
                list.innerHTML = `
                    <div style="background:linear-gradient(135deg,#667eea11,#764ba211);border:1px solid #667eea33;border-radius:10px;padding:14px 16px;margin-bottom:8px;">
                        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
                            <span style="font-weight:700;font-size:0.95rem;color:#4a5568;">${getApplicationLabel(first.application_id)}</span>
                            <span style="font-size:0.75rem;background:#667eea;color:#fff;border-radius:12px;padding:2px 10px;">Active</span>
                        </div>
                        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px 12px;font-size:0.8rem;">
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Token</div>
                                <div style="color:#333;font-family:monospace;word-break:break-all;font-size:0.75rem;">${first.token}</div>
                            </div>
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Tag</div>
                                <div style="color:#333;">${first.tag_name || '-'} <span style="color:#bbb;">#${first.tag_id || '-'}</span></div>
                            </div>
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Trip Report</div>
                                <div style="color:#333;">${first.trip_report_name || '-'} <span style="color:#bbb;">#${first.trip_report_id || '-'}</span></div>
                            </div>
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Event Report</div>
                                <div style="color:#333;">${first.event_report_name || '-'} <span style="color:#bbb;">#${first.event_report_id || '-'}</span></div>
                            </div>
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Speed Rule</div>
                                <div style="color:#333;">${first.speed_event_rule_name || '-'} <span style="color:#bbb;">#${first.speed_event_id || '-'}</span></div>
                            </div>
                            <div style="background:#fff;border-radius:6px;padding:7px 10px;">
                                <div style="color:#999;font-weight:600;margin-bottom:2px;">Idle Rule</div>
                                <div style="color:#333;">${first.idle_event_rule_name || '-'} <span style="color:#bbb;">#${first.idle_event_id || '-'}</span></div>
                            </div>
                        </div>
                    </div>
                    ${extraCount > 0 ? `
                    <button onclick="openCustomerConfigsModal()" style="width:100%;padding:8px;background:none;border:1.5px dashed #667eea88;border-radius:8px;color:#667eea;font-size:0.82rem;cursor:pointer;font-weight:600;transition:background 0.2s;" onmouseover="this.style.background='#667eea11'" onmouseout="this.style.background='none'">
                        + ${extraCount} more customer${extraCount > 1 ? 's' : ''} — See All
                    </button>` : ''}`;
            } catch (error) {
                console.error('Failed to refresh customer config:', error);
            }
        }

async function saveCustomerConfig() {
    const payload = collectCustomerConfigPayload();
    const applicationId = payload.application_id;
    const token = payload.token;

    if (!applicationId || !token) {
        showCustomerConfigMessage('error', 'Please enter both application ID and token');
        return;
    }

    try {
        const response = await fetch('/dashboard/customer-config', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const data = await response.json();
        showCustomerConfigMessage(data.success ? 'success' : 'error', data.message || data.error);
        if (data.success) {
            clearCustomerConfigForm();
            refreshCustomerConfigs();
        }
    } catch (error) {
        showCustomerConfigMessage('error', 'Request failed: ' + error.message);
    }
}

// Trigger functions
async function triggerDimensionSync(event) {
    const btn = event?.currentTarget || event?.target;
    const applicationId = getSelectedApplicationId();
    if (!applicationId) { showMessage('error', 'Please select a customer'); return; }
    setLoading(btn, true);
    try {
        const response = await fetch('/dashboard/trigger/dimension-sync', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({application_id: applicationId})
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data);
        if (data.success) setTimeout(refreshJobs, 1000);
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    } finally {
        setLoading(btn, false);
    }
}

async function triggerFactSync(event) {
    const btn = event?.currentTarget || event?.target;
    const applicationId = getSelectedApplicationId();
    const startDate = document.getElementById('fact-start').value;
    const endDate   = document.getElementById('fact-end').value;
    if (!applicationId)      { showMessage('error', 'Please select a customer'); return; }
    if (!startDate || !endDate) { showMessage('error', 'Please select start and end dates'); return; }
    setLoading(btn, true);
    try {
        const response = await fetch('/dashboard/trigger/fact-sync', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({application_id: applicationId, start_date: startDate, end_date: endDate})
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data);
        if (data.success) setTimeout(refreshJobs, 1000);
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    } finally {
        setLoading(btn, false);
    }
}

async function triggerFullBackfill(event) {
    const btn = event?.currentTarget || event?.target;
    const applicationId = getSelectedApplicationId();
    const startDate = document.getElementById('fact-start').value;
    const endDate   = document.getElementById('fact-end').value;
    if (!applicationId)         { showMessage('error', 'Please select a customer'); return; }
    if (!startDate || !endDate) { showMessage('error', 'Please select start and end dates'); return; }
    setLoading(btn, true);
    try {
        const response = await fetch('/dashboard/trigger/full-backfill', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({application_id: applicationId, start_date: startDate, end_date: endDate})
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data);
        if (data.success) setTimeout(refreshJobs, 1000);
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    } finally {
        setLoading(btn, false);
    }
}

// Refresh functions
async function refreshStats() {
    try {
        const appId = document.getElementById('stats-filter-app')?.value || '';
        const url   = appId ? `/dashboard/stats/table-counts?application_id=${appId}` : '/dashboard/stats/table-counts';
        const response = await fetch(url);
        const data = await response.json();
        if (data.success) {
            document.getElementById('total-count').textContent = data.total.toLocaleString();
            document.getElementById('trip-count').textContent = data.counts.Trip.toLocaleString();
            document.getElementById('speeding-count').textContent = data.counts.Speeding.toLocaleString();
            document.getElementById('idle-count').textContent = data.counts.Idle.toLocaleString();
            document.getElementById('awh-count').textContent = data.counts.AWH.toLocaleString();
            document.getElementById('wh-count').textContent = data.counts.WH.toLocaleString();
            document.getElementById('ha-count').textContent = data.counts.HA.toLocaleString();
            document.getElementById('hb-count').textContent = data.counts.HB.toLocaleString();
            document.getElementById('wu-count').textContent = data.counts.WU.toLocaleString();
            if (data.dim_counts) {
                document.getElementById('dim-drivers-count').textContent = data.dim_counts.Drivers.toLocaleString();
                document.getElementById('dim-vehicles-count').textContent = data.dim_counts.Vehicles.toLocaleString();
                document.getElementById('dim-tags-count').textContent = data.dim_counts.Tags.toLocaleString();
                document.getElementById('dim-reports-count').textContent = data.dim_counts.Reports.toLocaleString();
                document.getElementById('dim-eventrules-count').textContent = data.dim_counts.EventRules.toLocaleString();
                document.getElementById('dim-customfields-count').textContent = data.dim_counts.CustomFields.toLocaleString();
            }
        }
    } catch (error) {
        console.error('Failed to refresh stats:', error);
    }
}

function formatJobMetadata(job) {
    const metadata = job.metadata || {};
    const details = [];

    if (metadata.application_id) {
        details.push(`Customer: ${metadata.application_id}`);
    }
    if (metadata.date) {
        details.push(`Date: ${metadata.date}`);
    }
    if (metadata.start_date && metadata.end_date) {
        details.push(`Range: ${metadata.start_date} to ${metadata.end_date}`);
    }
    if (metadata.weeks_processed) {
        details.push(`Weeks: ${metadata.weeks_processed}`);
    }
    if (metadata.dimension_records) {
        details.push(`Dimension Records: ${Number(metadata.dimension_records).toLocaleString()}`);
    }
    if (metadata.total_raw) {
        details.push(`Raw Records: ${Number(metadata.total_raw).toLocaleString()}`);
    }
    if (metadata.total_skipped) {
        details.push(`Skipped: ${Number(metadata.total_skipped).toLocaleString()}`);
    }
    if (metadata.total_records) {
        details.push(`Total Records: ${Number(metadata.total_records).toLocaleString()}`);
    }
    if (metadata.message) {
        details.push(metadata.message);
    }

    return details.length ? details.join('<br>') + '<br>' : '';
}

function _jobStatusBadge(status) {
    const map = {
        running:   { icon: '⚙️', label: 'Running',   color: '#0d6efd' },
        queued:    { icon: '⏳', label: 'Queued',    color: '#fd7e14' },
        completed: { icon: '✅', label: 'Completed', color: '#198754' },
        failed:    { icon: '❌', label: 'Failed',    color: '#dc3545' },
    };
    const s = map[status] || { icon: '❓', label: status, color: '#6c757d' };
    return `<span class="job-status ${status}" style="color:${s.color};font-weight:600;">${s.icon} ${s.label}</span>`;
}

function _elapsed(startedAt, completedAt) {
    if (!startedAt) return '';
    const start = new Date(startedAt);
    const end   = completedAt ? new Date(completedAt) : new Date();
    const secs  = Math.floor((end - start) / 1000);
    if (secs < 60)  return `${secs}s`;
    if (secs < 3600) return `${Math.floor(secs/60)}m ${secs%60}s`;
    return `${Math.floor(secs/3600)}h ${Math.floor((secs%3600)/60)}m`;
}

function _renderJobCard(job) {
    const meta     = job.metadata || {};
    const appId    = job.application_id || meta.application_id || null;
    const dateRange = (meta.start_date && meta.end_date)
        ? `${meta.start_date} → ${meta.end_date}`
        : (meta.date || '');

    // Header line
    let header = `<strong>${job.job_type.replace(/_/g, ' ')}</strong>`;
    if (appId)    header += `  <span style="color:#666;font-size:0.88rem;">App: <strong>${appId}</strong></span>`;
    if (dateRange) header += `  <span style="color:#888;font-size:0.85rem;">${dateRange}</span>`;

    // Status + timing
    const elapsed = _elapsed(job.started_at, job.completed_at);
    let timing = job.started_at ? `Started: ${new Date(job.started_at).toLocaleString()}` : '';
    if (job.status === 'running') timing += elapsed ? `  &nbsp;·&nbsp; running <strong>${elapsed}</strong>` : '';
    if (job.status === 'completed' && elapsed) timing += `  &nbsp;·&nbsp; took <strong>${elapsed}</strong>`;

    // Progress bar for running tasks
    let progressHtml = '';
    if (job.status === 'running') {
        const pct     = meta.percent || 0;
        const phase   = meta.phase_status || '';
        const evType  = meta.event_type ? ` [${meta.event_type}]` : '';
        const week    = meta.week ? ` · ${meta.week}` : '';
        const ins     = meta.inserted != null ? ` · ${Number(meta.inserted).toLocaleString()} inserted` : '';
        progressHtml = `
            <div style="margin:8px 0 4px;">
                <div style="background:#e9ecef;border-radius:4px;height:8px;overflow:hidden;">
                    <div style="background:#0d6efd;height:100%;width:${pct}%;transition:width .4s;"></div>
                </div>
                <div style="font-size:0.8rem;color:#555;margin-top:3px;">
                    ${pct}%${evType}${week}${ins}
                    ${phase ? `<br><em>${phase}</em>` : ''}
                </div>
            </div>`;
    }

    // Stats for completed
    let statsHtml = '';
    if (job.status === 'completed') {
        const parts = [];
        if (meta.dimension_records != null) parts.push(`Dims: <strong>${Number(meta.dimension_records).toLocaleString()}</strong>`);
        if (meta.total_inserted    != null) parts.push(`Inserted: <strong>${Number(meta.total_inserted).toLocaleString()}</strong>`);
        if (meta.total_skipped     != null) parts.push(`Skipped: ${Number(meta.total_skipped).toLocaleString()}`);
        if (meta.total_failed      != null && meta.total_failed > 0) parts.push(`<span style="color:#dc3545;">Failed: ${meta.total_failed}</span>`);
        if (job.records_processed  != null && !parts.length) parts.push(`Records: <strong>${Number(job.records_processed).toLocaleString()}</strong>`);
        if (parts.length) statsHtml = `<div style="font-size:0.85rem;color:#444;margin-top:5px;">${parts.join('&ensp;·&ensp;')}</div>`;
    }

    // Error
    const errorHtml = job.error_message
        ? `<div style="color:#dc3545;font-size:0.82rem;margin-top:5px;word-break:break-word;">⚠ ${job.error_message}</div>`
        : '';

    const borderColor = { running:'#0d6efd', queued:'#fd7e14', completed:'#198754', failed:'#dc3545' }[job.status] || '#ccc';

    const cancelBtn = (job.status === 'running' || job.status === 'queued')
        ? `<button onclick="cancelJob('${job.id}', this)"
               style="background:#dc3545;color:#fff;border:none;border-radius:4px;padding:3px 10px;font-size:0.78rem;cursor:pointer;margin-left:8px;"
               title="Cancel this job">✕ Cancel</button>`
        : '';

    const logButtons = job.job_type === 'full_backfill'
        ? `<button onclick="viewBackfillLog('${job.id}', this)"
               style="background:#6f42c1;color:#fff;border:none;border-radius:4px;padding:3px 10px;font-size:0.78rem;cursor:pointer;margin-left:8px;"
               title="View the dedicated Full Backfill log">📄 View Log</button>
           <button onclick="downloadBackfillLog('${job.id}')"
               style="background:#495057;color:#fff;border:none;border-radius:4px;padding:3px 10px;font-size:0.78rem;cursor:pointer;margin-left:5px;"
               title="Download the complete log file">⬇ Log</button>`
        : '';

    return `
        <div class="job-item ${job.status}" style="border-left:4px solid ${borderColor};padding:10px 14px;margin-bottom:8px;border-radius:4px;background:#fff;">
            <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:4px;">
                <div>${header}</div>
                <div style="display:flex;align-items:center;">${_jobStatusBadge(job.status)}${logButtons}${cancelBtn}</div>
            </div>
            ${progressHtml}
            ${statsHtml}
            ${errorHtml}
            <div style="font-size:0.78rem;color:#999;margin-top:6px;">${timing}</div>
        </div>`;
}

async function viewBackfillLog(taskId, btn) {
    const oldText = btn.textContent;
    btn.disabled = true;
    btn.textContent = 'Loading…';
    try {
        const response = await fetch(`/dashboard/task/${encodeURIComponent(taskId)}/log`);
        const contentType = response.headers.get('content-type') || '';
        if (!response.ok) {
            const message = contentType.includes('application/json')
                ? (await response.json()).error
                : await response.text();
            showMessage('error', message || 'Log is not available');
            return;
        }
        openResultModal('success', await response.text());
    } catch (error) {
        showMessage('error', 'Failed to load log: ' + error.message);
    } finally {
        btn.disabled = false;
        btn.textContent = oldText;
    }
}

function downloadBackfillLog(taskId) {
    window.location.href = `/dashboard/task/${encodeURIComponent(taskId)}/log?download=1`;
}

async function cancelJob(taskId, btn) {
    if (!confirm('Cancel this job?')) return;
    btn.disabled = true;
    btn.textContent = '…';
    try {
        const res  = await fetch(`/dashboard/task/${taskId}/cancel`, { method: 'POST' });
        const data = await res.json();
        if (data.success) {
            btn.textContent = 'Cancelled';
            btn.style.background = '#6c757d';
            setTimeout(refreshJobs, 800);
        } else {
            btn.disabled = false;
            btn.textContent = '✕ Cancel';
            showMessage('error', data.error || 'Cancel failed');
        }
    } catch (e) {
        btn.disabled = false;
        btn.textContent = '✕ Cancel';
        showMessage('error', 'Request failed: ' + e.message);
    }
}

async function refreshJobs() {
    try {
        const response = await fetch('/dashboard/status/recent');
        const data = await response.json();
        if (data.success) {
            const jobList = document.getElementById('job-list');
            if (!data.jobs.length) {
                jobList.innerHTML = '<em style="color:#888;font-size:0.9rem;">No recent jobs</em>';
            } else {
                jobList.innerHTML = data.jobs.map(_renderJobCard).join('');
            }
        }
    } catch (error) {
        console.error('Failed to refresh jobs:', error);
    }
}


async function refreshSchedulerStatus() {
    try {
        const response = await fetch('/dashboard/stats/scheduler-status');
        const data = await response.json();
        if (data.success) {
            // Daily syncs
            const dailyDiv = document.getElementById('daily-scheduler-status');
            if (data.daily_syncs.length > 0) {
                dailyDiv.innerHTML = data.daily_syncs.slice(0, 5).map(job => `
	                            <div style="padding: 8px; margin: 5px 0; background: ${job.status === 'completed' ? '#d4edda' : job.status === 'failed' ? '#f8d7da' : '#fff3cd'}; border-radius: 4px; font-size: 0.9rem;">
	                                <strong>${new Date(job.started_at).toLocaleString()}</strong>
	                                <span style="float: right; font-weight: 600; color: ${job.status === 'completed' ? '#28a745' : job.status === 'failed' ? '#dc3545' : '#ffc107'};">${job.status.toUpperCase()}</span><br>
	                                ${formatJobMetadata(job)}
	                                ${job.records_processed ? `Records: ${job.records_processed.toLocaleString()}` : ''}
	                                ${job.error_message ? `<br>Error: ${job.error_message}` : ''}
	                            </div>
	                        `).join('');
            } else {
                dailyDiv.innerHTML = '<em style="color: #666;">No daily syncs yet</em>';
            }

            // Weekly backfills
            const weeklyDiv = document.getElementById('weekly-scheduler-status');
            if (data.weekly_backfills.length > 0) {
                weeklyDiv.innerHTML = data.weekly_backfills.slice(0, 5).map(job => `
	                            <div style="padding: 8px; margin: 5px 0; background: ${job.status === 'completed' ? '#d4edda' : job.status === 'failed' ? '#f8d7da' : '#fff3cd'}; border-radius: 4px; font-size: 0.9rem;">
	                                <strong>${new Date(job.started_at).toLocaleString()}</strong>
	                                <span style="float: right; font-weight: 600; color: ${job.status === 'completed' ? '#28a745' : job.status === 'failed' ? '#dc3545' : '#ffc107'};">${job.status.toUpperCase()}</span><br>
	                                ${formatJobMetadata(job)}
	                                ${job.records_processed ? `Records: ${job.records_processed.toLocaleString()}` : ''}
	                                ${job.error_message ? `<br>Error: ${job.error_message}` : ''}
	                            </div>
	                        `).join('');
            } else {
                weeklyDiv.innerHTML = '<em style="color: #666;">No weekly backfills yet</em>';
            }

            // Show alert if any scheduler jobs are running
            if (data.running_jobs.length > 0) {
                console.log('Scheduler jobs currently running:', data.running_jobs.length);
            }
        }
    } catch (error) {
        console.error('Failed to refresh scheduler status:', error);
    }
}



// ── Admin Config Modal ─────────────────────────────────────────────────────────

async function openAdminConfig() {
    document.getElementById('admin-cfg-modal').style.display = 'flex';
    document.getElementById('admin-cfg-input').value = '';
    document.getElementById('admin-cfg-msg').textContent = '';
    document.getElementById('admin-cfg-masked').textContent = 'بارگذاری...';
    document.getElementById('admin-cfg-source').textContent = '';
    try {
        const res  = await fetch('/dashboard/admin-config');
        const data = await res.json();
        if (data.success) {
            document.getElementById('admin-cfg-masked').textContent = data.masked;
            document.getElementById('admin-cfg-source').textContent = `(${data.source})`;
        }
    } catch (e) {
        document.getElementById('admin-cfg-masked').textContent = 'خطا در بارگذاری';
    }
}

function closeAdminConfig() {
    document.getElementById('admin-cfg-modal').style.display = 'none';
}

function toggleAdminTokenVisibility() {
    const inp = document.getElementById('admin-cfg-input');
    const eye = document.getElementById('admin-cfg-eye');
    if (inp.type === 'password') { inp.type = 'text';     eye.textContent = '🙈'; }
    else                         { inp.type = 'password'; eye.textContent = '👁';  }
}

async function saveAdminConfig() {
    const token = document.getElementById('admin-cfg-input').value.trim();
    const msg   = document.getElementById('admin-cfg-msg');
    if (!token) { msg.style.color = '#dc3545'; msg.textContent = 'توکن نمی‌تواند خالی باشد'; return; }
    msg.style.color = '#888'; msg.textContent = 'در حال ذخیره...';
    try {
        const res  = await fetch('/dashboard/admin-config', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({token})
        });
        const data = await res.json();
        if (data.success) {
            msg.style.color = '#198754';
            msg.textContent = '✅ ' + data.message;
            document.getElementById('admin-cfg-masked').textContent = data.message.match(/\((.+)\)/)?.[1] || '---';
            document.getElementById('admin-cfg-source').textContent = '(database)';
            document.getElementById('admin-cfg-input').value = '';
        } else {
            msg.style.color = '#dc3545';
            msg.textContent = '❌ ' + (data.error || 'خطا');
        }
    } catch (e) {
        msg.style.color = '#dc3545';
        msg.textContent = '❌ ' + e.message;
    }
}

document.addEventListener('keydown', e => {
    if (e.key === 'Escape') closeAdminConfig();
});
document.getElementById('admin-cfg-modal')?.addEventListener('click', e => {
    if (e.target === document.getElementById('admin-cfg-modal')) closeAdminConfig();
});

// Initialize dashboard — staggered to avoid hammering the server at once
refreshJobs();
refreshCustomerConfigs();
setTimeout(refreshStats,           500);   // table counts after jobs
setTimeout(refreshSchedulerStatus, 1000);  // scheduler after table counts

setInterval(refreshJobs,            5000);
setInterval(refreshStats,          30000);
setInterval(refreshSchedulerStatus, 10000);
