// Set default dates (yesterday)
const today = new Date();
const yesterday = new Date(today);
yesterday.setDate(yesterday.getDate() - 1);
document.getElementById('fact-start').value = yesterday.toISOString().split('T')[0];
document.getElementById('fact-end').value = yesterday.toISOString().split('T')[0];

// Message display function
function showMessage(type, text) {
    const msg = document.getElementById('trigger-message');
    msg.className = 'message ' + type;
    msg.textContent = text;
    msg.style.display = 'block';
    setTimeout(() => {
        msg.style.display = 'none';
    }, 5000);
}

function showCustomerConfigMessage(type, text) {
    const msg = document.getElementById('customer-config-message');
    msg.className = 'message ' + type;
    msg.textContent = text;
    msg.style.display = 'block';
    setTimeout(() => {
        msg.style.display = 'none';
    }, 5000);
}

function getSelectedApplicationId() {
    return document.getElementById('manual-application-id').value.trim();
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
                if (!data.customers.length) {
                    list.innerHTML = '<em style="color: #666;">No customer_config rows yet</em>';
                    select.innerHTML = '<option value="">Select customer</option>';
                    cleanupSelect.innerHTML = '<option value="">Select customer</option>';
                    return;
                }
                const optionsHtml = data.customers.map(customer => `
                    <option value="${customer.application_id}">${getApplicationLabel(customer.application_id)}</option>
                `).join('');
                select.innerHTML = '<option value="">Select customer</option>' + optionsHtml;
                cleanupSelect.innerHTML = '<option value="">Select customer</option>' + optionsHtml;
                if (currentSelection && data.customers.some(customer => customer.application_id === currentSelection)) {
                    select.value = currentSelection;
                } else {
                    select.value = data.customers[0].application_id;
                }
                list.innerHTML = data.customers.map(customer => `
                    <div class="job-item" style="padding: 10px; margin-bottom: 8px;">
                        <div class="job-header">
                            <span class="job-type">${getApplicationLabel(customer.application_id)}</span>
                        </div>
                        <div class="job-details">
                            Token: ${customer.token}<br>
                            Tag Name: ${customer.tag_name || '-'}<br>
                            Tag ID: ${customer.tag_id || '-'}<br>
                            Trip Report Name: ${customer.trip_report_name || '-'}<br>
                            Trip Report ID: ${customer.trip_report_id || '-'}<br>
                            Event Report Name: ${customer.event_report_name || '-'}<br>
                            Event Report ID: ${customer.event_report_id || '-'}<br>
                            Speed Rule Name: ${customer.speed_event_rule_name || '-'}<br>
                            Speed Rule ID: ${customer.speed_event_id || '-'}<br>
                            Idle Rule Name: ${customer.idle_event_rule_name || '-'}<br>
                            Idle Rule ID: ${customer.idle_event_id || '-'}
                        </div>
                    </div>
                `).join('');
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
async function triggerDimensionSync() {
    const applicationId = getSelectedApplicationId();
    if (!applicationId) {
        showMessage('error', 'Please select a customer');
        return;
    }
    try {
        const response = await fetch('/dashboard/trigger/dimension-sync', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({application_id: applicationId})
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data.message || data.error);
        if (data.success) {
            setTimeout(refreshJobs, 1000);
        }
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    }
}

async function triggerFactSync() {
    const applicationId = getSelectedApplicationId();
    const startDate = document.getElementById('fact-start').value;
    const endDate = document.getElementById('fact-end').value;

    if (!applicationId) {
        showMessage('error', 'Please select a customer');
        return;
    }

    if (!startDate || !endDate) {
        showMessage('error', 'Please select start and end dates');
        return;
    }

    try {
        const response = await fetch('/dashboard/trigger/fact-sync', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                application_id: applicationId,
                start_date: startDate,
                end_date: endDate
            })
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data.message || data.error);
        if (data.success) {
            setTimeout(refreshJobs, 1000);
        }
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    }
}

async function triggerFullBackfill() {
    const applicationId = getSelectedApplicationId();
    const startDate = document.getElementById('fact-start').value;
    const endDate = document.getElementById('fact-end').value;

    if (!applicationId) {
        showMessage('error', 'Please select a customer');
        return;
    }

    if (!startDate || !endDate) {
        showMessage('error', 'Please select start and end dates');
        return;
    }

    try {
        const response = await fetch('/dashboard/trigger/full-backfill', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                application_id: applicationId,
                start_date: startDate,
                end_date: endDate
            })
        });
        const data = await response.json();
        showMessage(data.success ? 'success' : 'error', data.message || data.error);
        if (data.success) {
            setTimeout(refreshJobs, 1000);
        }
    } catch (error) {
        showMessage('error', 'Request failed: ' + error.message);
    }
}

// Refresh functions
async function refreshStats() {
    try {
        const response = await fetch('/dashboard/stats/table-counts');
        const data = await response.json();
        if (data.success) {
            // Fact tables
            document.getElementById('total-count').textContent = data.total.toLocaleString();
            document.getElementById('trip-count').textContent = data.counts.Trip.toLocaleString();
            document.getElementById('speeding-count').textContent = data.counts.Speeding.toLocaleString();
            document.getElementById('idle-count').textContent = data.counts.Idle.toLocaleString();
            document.getElementById('awh-count').textContent = data.counts.AWH.toLocaleString();
            document.getElementById('wh-count').textContent = data.counts.WH.toLocaleString();
            document.getElementById('ha-count').textContent = data.counts.HA.toLocaleString();
            document.getElementById('hb-count').textContent = data.counts.HB.toLocaleString();
            document.getElementById('wu-count').textContent = data.counts.WU.toLocaleString();

            // Dimension tables
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

async function refreshJobs() {
    try {
        const response = await fetch('/dashboard/status/recent');
        const data = await response.json();
        if (data.success) {
            const jobList = document.getElementById('job-list');
            jobList.innerHTML = data.jobs.map(job => `
	                        <div class="job-item ${job.status}">
	                            <div class="job-header">
	                                <span class="job-type">${job.job_type}</span>
	                                <span class="job-status ${job.status}">${job.status}</span>
	                            </div>
	                            <div class="job-details">
	                                Started: ${new Date(job.started_at).toLocaleString()}<br>
	                                ${job.completed_at ? 'Completed: ' + new Date(job.completed_at).toLocaleString() + '<br>' : ''}
	                                ${formatJobMetadata(job)}
	                                ${job.records_processed ? 'Records: ' + job.records_processed.toLocaleString() + '<br>' : ''}
	                                ${job.error_message ? 'Error: ' + job.error_message : ''}
	                            </div>
	                        </div>
	                    `).join('');
        }
    } catch (error) {
        console.error('Failed to refresh jobs:', error);
    }
}

async function refreshLastSync() {
    try {
        const response = await fetch('/dashboard/stats/last-sync');
        const data = await response.json();
        if (data.success) {
            const info = document.getElementById('last-sync-info');
            let html = '';
            if (data.daily_sync) {
                html += `<strong>Daily:</strong> ${new Date(data.daily_sync.completed_at).toLocaleString()}<br>`;
                html += `Records: ${data.daily_sync.records_processed.toLocaleString()}<br><br>`;
            }
            if (data.weekly_backfill) {
                html += `<strong>Weekly:</strong> ${new Date(data.weekly_backfill.completed_at).toLocaleString()}<br>`;
                html += `Records: ${data.weekly_backfill.records_processed.toLocaleString()}`;
            }
            info.innerHTML = html || 'No sync data available';
        }
    } catch (error) {
        console.error('Failed to refresh last sync:', error);
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



// Initialize dashboard
refreshJobs();
refreshCustomerConfigs();
refreshLastSync();
refreshSchedulerStatus();

setInterval(refreshJobs, 5000);  // Refresh jobs every 5 seconds
setInterval(refreshStats, 30000);  // Refresh stats every 30 seconds
setInterval(refreshSchedulerStatus, 10000);  // Refresh scheduler status every 10 seconds