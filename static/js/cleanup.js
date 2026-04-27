// Cleanup modal functions
function openCleanupModal() {
    document.getElementById('cleanup-modal').style.display = 'block';
    document.getElementById('cleanup-message').style.display = 'none';
    document.getElementById('cleanup-message').className = 'message';
}

function closeCleanupModal() {
    document.getElementById('cleanup-modal').style.display = 'none';
}

async function confirmCleanup() {
    const tableType = document.getElementById('cleanup-table-type').value;
    const applicationId = document.getElementById('cleanup-application-id').value;
    const apiKey = document.getElementById('cleanup-api-key').value;

    if (!tableType || !applicationId || !apiKey) {
        showCleanupMessage('error', 'Please fill in all fields');
        return;
    }

    if (!confirm('Are you absolutely sure you want to delete this data? This action cannot be undone!')) {
        return;
    }

    try {
        const response = await fetch('/dashboard/cleanup', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                table_type: tableType,
                application_id: applicationId,
                api_key: apiKey
            })
        });
        const data = await response.json();
        showCleanupMessage(data.success ? 'success' : 'error', data.message || data.error);

        // Show detailed operations and errors
        if (data.operations && data.operations.length > 0) {
            const details = data.operations.join('<br>');
            const messageDiv = document.getElementById('cleanup-message');
            messageDiv.innerHTML += '<br><br><strong>Details:</strong><br>' + details;
        }

        if (data.errors && data.errors.length > 0) {
            const errorDetails = data.errors.join('<br>');
            const messageDiv = document.getElementById('cleanup-message');
            messageDiv.innerHTML += '<br><br><strong>Errors:</strong><br>' + errorDetails;
        }

        if (data.success) {
            // Don't auto-close on success, let user see the results
            refreshStats();
        }
    } catch (error) {
        showCleanupMessage('error', 'Request failed: ' + error.message);
    }
}

function showCleanupMessage(type, text) {
    const msg = document.getElementById('cleanup-message');
    msg.className = 'message ' + type;
    msg.textContent = text;
    msg.style.display = 'block';
}