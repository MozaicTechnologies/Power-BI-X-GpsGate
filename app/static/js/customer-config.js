const MAPPING_FIELDS = [
    {selectId: 'customer-tag-name',                nameField: 'tag_name',                idField: 'tag_id'},
    {selectId: 'customer-trip-report-name',        nameField: 'trip_report_name',        idField: 'trip_report_id'},
    {selectId: 'customer-event-report-name',       nameField: 'event_report_name',       idField: 'event_report_id'},
    {selectId: 'customer-speed-event-rule-name',   nameField: 'speed_event_rule_name',   idField: 'speed_event_id'},
    {selectId: 'customer-idle-event-rule-name',    nameField: 'idle_event_rule_name',    idField: 'idle_event_id'},
    {selectId: 'customer-awh-event-rule-name',     nameField: 'awh_event_rule_name',     idField: 'awh_event_id'},
    {selectId: 'customer-ha-event-rule-name',      nameField: 'ha_event_rule_name',      idField: 'ha_event_id'},
    {selectId: 'customer-hb-event-rule-name',      nameField: 'hb_event_rule_name',      idField: 'hb_event_id'},
    {selectId: 'customer-hc-event-rule-name',      nameField: 'hc_event_rule_name',      idField: 'hc_event_id'},
    {selectId: 'customer-wu-event-rule-name',      nameField: 'wu_event_rule_name',      idField: 'wu_event_id'},
    {selectId: 'customer-wh-event-rule-name',      nameField: 'wh_event_rule_name',      idField: 'wh_event_id'},
];

function escapeHtml(value) {
    return String(value == null ? '' : value).replace(/[&<>"']/g, ch => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    }[ch]));
}

function showMappingOptionsMessage(type, text) {
    const msg = document.getElementById('mapping-options-message');
    msg.className = 'message ' + type;
    msg.textContent = text;
    msg.style.display = 'block';
    if (type !== 'info') {
        setTimeout(() => { msg.style.display = 'none'; }, 5000);
    }
}

async function loadMappingOptions() {
    const applicationId = document.getElementById('customer-app-id').value.trim();
    const token = document.getElementById('customer-token').value.trim();
    if (!applicationId || !token) {
        showMappingOptionsMessage('error', 'Enter App ID and Token first');
        return;
    }
    showMappingOptionsMessage('info', 'Loading options from GpsGate...');
    try {
        const response = await fetch('/dashboard/customer-config/options', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({application_id: applicationId, token: token})
        });
        const data = await response.json();
        if (!data.success) {
            showMappingOptionsMessage('error', data.error || 'Failed to load options');
            return;
        }
        populateMappingSelects({
            tags: data.tags || [],
            event_rules: data.event_rules || [],
            reports: data.reports || []
        });
        showMappingOptionsMessage(
            'success',
            `Loaded ${data.tags.length} tags · ${data.reports.length} reports · ${data.event_rules.length} event rules`
        );
    } catch (error) {
        showMappingOptionsMessage('error', 'Request failed: ' + error.message);
    }
}

function populateMappingSelects(options) {
    MAPPING_FIELDS.forEach(({selectId}) => {
        const select = document.getElementById(selectId);
        const sourceKey = select.getAttribute('data-options-source');
        const items = options[sourceKey] || [];
        const previousValue = select.value;
        const optionsHtml = items.map(item =>
            `<option value="${escapeHtml(item.id)}" data-name="${escapeHtml(item.name)}">${escapeHtml(item.name)}</option>`
        ).join('');
        select.innerHTML = '<option value="">-- Select --</option>' + optionsHtml;
        if (previousValue && items.some(item => String(item.id) === String(previousValue))) {
            select.value = previousValue;
        }
    });
}

function collectCustomerConfigPayload() {
    const payload = {
        application_id: document.getElementById('customer-app-id').value.trim(),
        token: document.getElementById('customer-token').value.trim()
    };
    MAPPING_FIELDS.forEach(({selectId, nameField, idField}) => {
        const select = document.getElementById(selectId);
        const id = (select.value || '').trim();
        const selectedOption = id ? select.options[select.selectedIndex] : null;
        const name = selectedOption ? (selectedOption.dataset.name || selectedOption.textContent || '').trim() : '';
        payload[nameField] = name;
        payload[idField] = id;
    });
    return payload;
}

function clearCustomerConfigForm() {
    document.getElementById('customer-app-id').value = '';
    document.getElementById('customer-token').value = '';
    MAPPING_FIELDS.forEach(({selectId}) => {
        const select = document.getElementById(selectId);
        select.innerHTML = '<option value="">Load options first</option>';
    });
}

const applicationNamesById = {};

function getApplicationName(applicationId) {
    return applicationNamesById[String(applicationId)] || '';
}

function getApplicationLabel(applicationId) {
    const name = getApplicationName(applicationId);
    return name ? `${name} (${applicationId})` : `App ${applicationId}`;
}

async function loadEligibleApplications() {
    const select = document.getElementById('customer-app-id');
    const previousValue = select.value;
    try {
        const response = await fetch('/dashboard/eligible-applications');
        const data = await response.json();
        if (!data.success) {
            select.innerHTML = `<option value="">Failed: ${escapeHtml(data.error || 'unknown error')}</option>`;
            return;
        }
        const items = data.applications || [];
        items.forEach(item => {
            applicationNamesById[String(item.id)] = item.name;
        });
        if (!items.length) {
            select.innerHTML = '<option value="">No eligible applications</option>';
            return;
        }
        select.innerHTML = '<option value="">-- Select application --</option>' + items.map(item =>
            `<option value="${escapeHtml(item.id)}">${escapeHtml(item.name)} (${escapeHtml(item.id)})</option>`
        ).join('');
        if (previousValue && items.some(item => String(item.id) === String(previousValue))) {
            select.value = previousValue;
        }
        if (typeof refreshCustomerConfigs === 'function') {
            refreshCustomerConfigs();
        }
    } catch (error) {
        select.innerHTML = `<option value="">Failed: ${escapeHtml(error.message)}</option>`;
    }
}

loadEligibleApplications();
