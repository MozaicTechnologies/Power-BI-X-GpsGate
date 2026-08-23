// ── DB Browser ────────────────────────────────────────────────────────────────

const DB_TABLE_GROUPS = [
    { label: '⚙️ Config',     tables: ['gpsgate_application'] },
    { label: '📊 Fact Tables', tables: ['fact_trip','fact_speeding','fact_idle','fact_awh','fact_wh','fact_ha','fact_hb','fact_wu'] },
    { label: '📋 Dimensions',  tables: ['dim_tags','dim_event_rules','dim_reports','dim_vehicles','dim_drivers','dim_vehicle_custom_fields'] },
];

let _tableCounts   = {};
let _countsEstimated = false;
let _tableListLoadedAt = 0;
let _browserState  = { table: null, page: 1, per_page: 50, columns: [], totalPages: 1, total: 0 };

// ── Sidebar open/close ────────────────────────────────────────────────────────

function toggleSidebar() {
    const sidebar  = document.getElementById('db-sidebar');
    const overlay  = document.getElementById('db-sidebar-overlay');
    const isOpen   = sidebar.classList.contains('open');
    if (isOpen) {
        closeSidebar();
    } else {
        sidebar.classList.add('open');
        overlay.classList.add('open');
        // Paint the menu immediately; refresh counts only when cache is stale.
        renderSidebarGroups(document.getElementById('db-sidebar-body'));
        if (Date.now() - _tableListLoadedAt > 60000) loadTableList();
    }
}

function closeSidebar() {
    document.getElementById('db-sidebar').classList.remove('open');
    document.getElementById('db-sidebar-overlay').classList.remove('open');
}

// ── Load table list with counts ───────────────────────────────────────────────

async function loadTableList() {
    const body = document.getElementById('db-sidebar-body');
    try {
        const res  = await fetch('/dashboard/browse');
        const data = await res.json();
        if (!data.success) { body.innerHTML = '<p style="color:#f66;padding:10px;">Failed to load</p>'; return; }
        data.tables.forEach(t => { _tableCounts[t.name] = t.count; });
        _countsEstimated = Boolean(data.counts_estimated);
        _tableListLoadedAt = Date.now();
        renderSidebarGroups(body);
    } catch (e) {
        body.innerHTML = '<p style="color:#f66;padding:10px;">Error: ' + e.message + '</p>';
    }
}

function renderSidebarGroups(container) {
    container.innerHTML = DB_TABLE_GROUPS.map(group => `
        <div class="db-sidebar-group">
            <div class="db-sidebar-group-label">${group.label}</div>
            ${group.tables.map(t => {
                const count = _tableCounts[t];
                const countStr = count == null ? '…' : count >= 0 ? `${_countsEstimated ? '~' : ''}${count.toLocaleString()}` : 'err';
                return `<div class="db-sidebar-item" onclick="openBrowser('${t}')">
                    <span class="db-sidebar-table-name">${t}</span>
                    <span class="db-sidebar-count">${countStr}</span>
                </div>`;
            }).join('')}
        </div>
    `).join('');
}

// ── Open browser modal ────────────────────────────────────────────────────────

function openBrowser(tableName) {
    closeSidebar();
    _browserState = { table: tableName, page: 1, per_page: 50, columns: [], totalPages: 1, total: 0 };
    document.getElementById('db-browser-modal').style.display = 'flex';
    document.getElementById('db-browser-title').textContent   = tableName;
    document.getElementById('db-browser-per-page').value      = '50';
    document.getElementById('db-browser-search').value        = '';
    fetchBrowserPage();
}

function closeBrowser() {
    document.getElementById('db-browser-modal').style.display = 'none';
}

// ── Fetch & render page ───────────────────────────────────────────────────────

async function fetchBrowserPage() {
    const { table, page, per_page } = _browserState;
    const tbody  = document.getElementById('db-browser-tbody');
    const thead  = document.getElementById('db-browser-thead');
    const status = document.getElementById('db-browser-status');
    tbody.innerHTML = '<tr><td colspan="99" style="text-align:center;padding:20px;color:#999;">Loading…</td></tr>';
    status.textContent = '';
    try {
        const res  = await fetch(`/dashboard/browse/${table}?page=${page}&per_page=${per_page}`);
        const data = await res.json();
        if (!data.success) {
            tbody.innerHTML = `<tr><td colspan="99" style="color:#c00;padding:15px;">${data.error}</td></tr>`;
            return;
        }
        _browserState.columns    = data.columns;
        _browserState.totalPages = data.pages;
        _browserState.total      = data.total;
        _browserState.totalIsEstimate = Boolean(data.total_is_estimate);

        // Header
        thead.innerHTML = '<tr>' + data.columns.map(c => `<th>${c}</th>`).join('') + '<th style="width:50px;"></th></tr>';

        // Body
        const search = document.getElementById('db-browser-search').value.toLowerCase();
        const rows   = search
            ? data.rows.filter(r => Object.values(r).some(v => v && v.toLowerCase().includes(search)))
            : data.rows;

        if (!rows.length) {
            tbody.innerHTML = '<tr><td colspan="99" style="text-align:center;padding:20px;color:#999;">No rows found</td></tr>';
        } else {
            tbody.innerHTML = rows.map(row => {
                const id = row['id'];
                const deletBtn = id != null
                    ? `<td style="text-align:center;"><button onclick="deleteRow(${JSON.stringify(table)}, ${id})" title="Delete" style="background:none;border:none;cursor:pointer;color:#dc3545;font-size:1rem;padding:2px 6px;" onmouseover="this.style.background='#fde8e8';this.style.borderRadius='4px'" onmouseout="this.style.background='none'">🗑</button></td>`
                    : '<td></td>';
                return '<tr>' + data.columns.map(c => `<td>${row[c] ?? ''}</td>`).join('') + deletBtn + '</tr>';
            }).join('');
        }

        // Status
        const from = (page - 1) * per_page + 1;
        const to   = Math.min(page * per_page, data.total);
        const approx = data.total_is_estimate ? '~' : '';
        status.textContent = `Showing ${from}–${to} of ${approx}${data.total.toLocaleString()} rows  |  Page ${page} / ${data.pages}`;

        renderPagination();
    } catch (e) {
        tbody.innerHTML = `<tr><td colspan="99" style="color:#c00;padding:15px;">Error: ${e.message}</td></tr>`;
    }
}

function renderPagination() {
    const { page, totalPages } = _browserState;
    const container = document.getElementById('db-browser-pagination');
    const pages = [];
    const delta = 2;
    let prev = null;
    for (let p = 1; p <= totalPages; p++) {
        if (p === 1 || p === totalPages || (p >= page - delta && p <= page + delta)) {
            if (prev && p - prev > 1) pages.push('…');
            pages.push(p);
            prev = p;
        }
    }
    container.innerHTML = pages.map(p => {
        if (p === '…') return `<span class="db-page-ellipsis">…</span>`;
        return `<button class="db-page-btn${p === page ? ' active' : ''}" onclick="gotoPage(${p})">${p}</button>`;
    }).join('');
}

function gotoPage(p) {
    const { totalPages } = _browserState;
    if (p < 1 || p > totalPages) return;
    _browserState.page = p;
    fetchBrowserPage();
}

function browserPerPageChange() {
    _browserState.per_page = parseInt(document.getElementById('db-browser-per-page').value) || 50;
    _browserState.page = 1;
    fetchBrowserPage();
}

function browserSearch() {
    fetchBrowserPage();
}

// ── Delete row ────────────────────────────────────────────────────────────────

async function deleteRow(tableName, rowId) {
    if (!confirm(`Delete row #${rowId} from ${tableName}?\nThis cannot be undone.`)) return;
    try {
        const res  = await fetch(`/dashboard/browse/${tableName}/${rowId}`, { method: 'DELETE' });
        const data = await res.json();
        if (data.success) {
            fetchBrowserPage();
        } else {
            alert('Delete failed: ' + (data.error || 'Unknown error'));
        }
    } catch (e) {
        alert('Delete failed: ' + e.message);
    }
}

// ── Keyboard shortcuts ────────────────────────────────────────────────────────

document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
        closeBrowser();
        closeSidebar();
    }
    const modal = document.getElementById('db-browser-modal');
    if (modal.style.display === 'flex') {
        if (e.key === 'ArrowRight') gotoPage(_browserState.page + 1);
        if (e.key === 'ArrowLeft')  gotoPage(_browserState.page - 1);
    }
});
