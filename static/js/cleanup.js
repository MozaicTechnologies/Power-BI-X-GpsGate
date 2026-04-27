// Cleanup modal functions
function openCleanupModal() {
    document.getElementById('cleanup-modal').style.display = 'block';
    document.getElementById('cleanup-message').style.display = 'none';
    document.getElementById('cleanup-message').className = 'message';
}

function closeCleanupModal() {
    document.getElementById('cleanup-modal').style.display = 'none';
}

function generateRandomKey() {
    const randomKey = Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    document.getElementById('generated-key').textContent = 'Generated Key: ' + randomKey;
    document.getElementById('cleanup-api-key').value = randomKey;
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
        if (data.success) {
            setTimeout(() => {
                closeCleanupModal();
                refreshStats();
            }, 2000);
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