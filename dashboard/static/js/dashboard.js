/* ================================================================
   Medicare KPI Dashboard — dashboard.js
   ================================================================ */

const API = '';
let trendChart = null;
let bifurBarChart = null;
let bifurDonutChart = null;
let allWeeks = [];
let currentClient = '';
let highDollarMode = false;

// Active filter state: {rfc: [...], rhp: [...]}
const activeFilters = { rfc: [], rhp: [] };

// ── FILTER PARAM BUILDER ──────────────────────────────────────
function filterParams() {
  const parts = [];
  if (activeFilters.rfc.length) parts.push('resp_fin_class=' + encodeURIComponent(activeFilters.rfc.join(',')));
  if (activeFilters.rhp.length) parts.push('resp_health_plan=' + encodeURIComponent(activeFilters.rhp.join(',')));
  if (highDollarMode) parts.push('high_dollar=true');
  return parts.join('&');
}

// ── MULTISELECT DROPDOWN COMPONENT ───────────────────────────
// fdId: 'fd-rfc' or 'fd-rhp', maps to activeFilters.rfc / .rhp
const FD_MAP = { 'fd-rfc': 'rfc', 'fd-rhp': 'rhp' };

function fdBuild(fdId, values) {
  const opts = document.getElementById(fdId + '-options');
  if (!opts) return;
  opts.innerHTML = '';
  values.forEach(v => {
    const row = document.createElement('label');
    row.className = 'fd-option';
    row.dataset.value = v;
    row.innerHTML = `<input type="checkbox" value="${v}" onchange="fdChange('${fdId}')"> ${v}`;
    opts.appendChild(row);
  });
}

function fdToggle(fdId) {
  const panel = document.getElementById(fdId + '-panel');
  const trigger = document.querySelector(`#${fdId} .fd-trigger`);
  const isOpen = panel.classList.contains('open');
  // Close all other panels first
  document.querySelectorAll('.fd-panel.open').forEach(p => {
    p.classList.remove('open');
    p.previousElementSibling?.classList.remove('open');
  });
  if (!isOpen) {
    panel.classList.add('open');
    trigger.classList.add('open');
    panel.querySelector('.fd-search')?.focus();
  }
}

function fdSearch(fdId, query) {
  const opts = document.querySelectorAll(`#${fdId}-options .fd-option`);
  const q = query.toLowerCase();
  opts.forEach(o => o.classList.toggle('hidden', !o.dataset.value.toLowerCase().includes(q)));
}

function fdSelectAll(fdId) {
  document.querySelectorAll(`#${fdId}-options .fd-option:not(.hidden) input`).forEach(cb => cb.checked = true);
  fdChange(fdId);
}

function fdClear(fdId) {
  document.querySelectorAll(`#${fdId}-options input`).forEach(cb => cb.checked = false);
  fdChange(fdId);
}

function fdChange(fdId) {
  const key = FD_MAP[fdId];
  const checked = [...document.querySelectorAll(`#${fdId}-options input:checked`)].map(cb => cb.value);
  activeFilters[key] = checked;

  const countEl = document.getElementById(fdId + '-count');
  const textEl = document.getElementById(fdId + '-text');
  if (checked.length === 0) {
    textEl.textContent = 'All';
    countEl.style.display = 'none';
  } else {
    textEl.textContent = checked.length === 1 ? checked[0] : `${checked.length} selected`;
    countEl.textContent = checked.length;
    countEl.style.display = '';
  }
  updateFilterActiveBadge();
  reloadActiveTab();
}

function clearAllFilters() {
  fdClear('fd-rfc');
  fdClear('fd-rhp');
}

function updateFilterActiveBadge() {
  const bar = document.getElementById('filter-active-bar');
  const tags = document.getElementById('filter-active-tags');
  const total = activeFilters.rfc.length + activeFilters.rhp.length;
  if (total === 0) {
    bar.classList.remove('visible');
    return;
  }
  bar.classList.add('visible');
  const parts = [];
  if (activeFilters.rfc.length) parts.push(`Fin.Class: ${activeFilters.rfc.length} selected`);
  if (activeFilters.rhp.length) parts.push(`Health Plan: ${activeFilters.rhp.length} selected`);
  tags.innerHTML = parts.map(p => `<span class="filter-tag">${p}</span>`).join('');
}

function loadFilterOptions() {
  fetch(apiUrl('/api/filters'))
    .then(r => r.json())
    .then(d => {
      if (d.resp_fin_class) fdBuild('fd-rfc', d.resp_fin_class);
      if (d.resp_health_plan) fdBuild('fd-rhp', d.resp_health_plan);
      // Restore previously checked values
      ['fd-rfc', 'fd-rhp'].forEach(fdId => {
        const key = FD_MAP[fdId];
        if (activeFilters[key].length) {
          document.querySelectorAll(`#${fdId}-options input`).forEach(cb => {
            cb.checked = activeFilters[key].includes(cb.value);
          });
        }
      });
    })
    .catch(() => {});
}

// Close dropdowns when clicking outside
document.addEventListener('click', e => {
  if (!e.target.closest('.fd-wrap')) {
    document.querySelectorAll('.fd-panel.open').forEach(p => p.classList.remove('open'));
    document.querySelectorAll('.fd-trigger.open').forEach(t => t.classList.remove('open'));
  }
});

// ── FORMATTERS ────────────────────────────────────────────────
const fmtDollar = v => {
  if (v == null) return '—';
  const abs = Math.abs(v);
  let s;
  if (abs >= 1e6) s = '$' + (abs / 1e6).toFixed(2) + 'M';
  else if (abs >= 1e3) s = '$' + (abs / 1e3).toFixed(1) + 'K';
  else s = '$' + abs.toFixed(2);
  return v < 0 ? '-' + s : s;
};
const fmtNum = v => v == null ? '—' : v.toLocaleString();
const fmtPct = v => v == null ? '—' : (v > 0 ? '+' : '') + v.toFixed(1) + '%';
const fmtDelta = v => v == null ? '—' : (v > 0 ? '+' : '') + v.toLocaleString();
const fmtWeek = w => {
  if (!w) return '—';
  const [mm, dd, yyyy] = w.split('.');
  return `WE ${mm}/${dd}/${yyyy}`;
};

// ── CLIENT + FILTER URL HELPERS ──────────────────────────────
function clientParam() {
  return currentClient ? `client=${encodeURIComponent(currentClient)}` : '';
}
function apiUrl(path, extra) {
  const params = [clientParam(), filterParams(), extra].filter(Boolean).join('&');
  return `${API}${path}${params ? '?' + params : ''}`;
}

function reloadActiveTab() {
  const active = document.querySelector('.tab-btn.active');
  switchTab(active ? active.dataset.tab : 'trending');
}

// ── CLIENT DISCOVERY + DROPDOWN ──────────────────────────────
function loadClients() {
  return fetch(`${API}/api/clients`)
    .then(r => r.json())
    .then(clients => {
      const sel = document.getElementById('sel-client');
      if (!sel) return clients;
      sel.innerHTML = '';
      clients.forEach(c => {
        const opt = document.createElement('option');
        opt.value = c.name;
        opt.textContent = c.name.replace(/_/g, ' ');
        if (c.loading) opt.textContent += ' (loading…)';
        sel.appendChild(opt);
      });
      if (clients.length && !currentClient) {
        currentClient = clients[0].name;
        sel.value = currentClient;
      }
      return clients;
    });
}

function switchClient(name) {
  if (name === currentClient) return;
  currentClient = name;
  // Clear filters when switching clients (new client has different payers)
  activeFilters.rfc = [];
  activeFilters.rhp = [];
  const sub = document.getElementById('page-title-sub');
  if (sub) sub.textContent = name.replace(/_/g, ' ');
  const sidebarSub = document.getElementById('sidebar-client-sub');
  if (sidebarSub) sidebarSub.textContent = name.replace(/_/g, ' ');
  const activeTab = document.querySelector('.tab-btn.active');
  const tabId = activeTab ? activeTab.dataset.tab : 'trending';
  pollClientStatus(tabId);
}

function pollClientStatus(tabId, onDone) {
  fetch(apiUrl('/api/status'))
    .then(r => r.json())
    .then(d => {
      const log = document.getElementById('loader-log');
      if (d.loading) {
        document.getElementById('loading-overlay').style.display = 'flex';
        if (log && d.log) {
          log.innerHTML = d.log.map(l => {
            const cls = l.startsWith('[DONE]') || l.startsWith('Loaded') ? 'ok' : l.startsWith('ERROR') ? 'err' : '';
            return `<p class="${cls}">${l}</p>`;
          }).join('');
          log.scrollTop = log.scrollHeight;
        }
        setTimeout(() => pollClientStatus(tabId, onDone), 2000);
      } else {
        document.getElementById('loading-overlay').style.display = 'none';
        allWeeks = d.weeks || [];
        populateWeekSelectors();
        loadFilterOptions();   // populate dropdowns once data is ready
        loadBillingEntities();
        switchTab(tabId || 'trending');
        if (highDollarMode) loadHDThreshold();
        if (typeof onDone === 'function') onDone();
      }
    })
    .catch(() => setTimeout(() => pollClientStatus(tabId, onDone), 3000));
}

// ── LOADING OVERLAY ───────────────────────────────────────────
function pollLoading() {
  // Load client list first, then poll status for default client
  loadClients().then(() => {
    pollClientStatus('trending');
  }).catch(() => setTimeout(pollLoading, 3000));
}

// ── TAB SWITCHING ─────────────────────────────────────────────
function switchTab(id) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === id));
  document.querySelectorAll('.tab-pane').forEach(p => p.style.display = p.id === 'tab-' + id ? 'block' : 'none');
  if (id === 'trending') loadTrending();
  if (id === 'rollover') loadRollover();
  if (id === 'bifurcation') loadBifurcation();
  if (id === 'contributors') loadContributors();
}

function switchSection(id) {
  document.querySelectorAll('.nav-item[data-section]').forEach(n => n.classList.toggle('active', n.dataset.section === id));
  // Both 'medicare' and 'highDollar' render the same content pane
  const paneId = (id === 'highDollar') ? 'medicare' : id;
  document.querySelectorAll('.section-pane').forEach(p => p.style.display = p.id === 'section-' + paneId ? 'block' : 'none');

  highDollarMode = (id === 'highDollar');
  const banner = document.getElementById('hd-threshold-banner');
  if (banner) banner.style.display = highDollarMode ? '' : 'none';

  const titles = {
    medicare: 'ATB Analysis', highDollar: 'High Dollar Analysis',
    workables: 'Workables', production: 'Production'
  };
  document.getElementById('page-title-text').textContent = titles[id] || id;

  if (!allWeeks.length) {
    // Data not loaded yet — poll until ready, then load the tab
    const activeTab = document.querySelector('.tab-btn.active');
    pollClientStatus(activeTab ? activeTab.dataset.tab : 'trending');
    return;
  }
  if (highDollarMode) loadHDThreshold();
  reloadActiveTab();
}

// ── HIGH DOLLAR THRESHOLD ─────────────────────────────────────
function loadHDThreshold() {
  const weeks = allWeeks;
  if (!weeks.length) return;
  const week = weeks[weeks.length - 1];
  fetch(apiUrl('/api/high-dollar-threshold', `week=${week}`))
    .then(r => r.json())
    .then(d => {
      const el = document.getElementById('hd-banner-text');
      if (!el) return;
      if (d.error) { el.textContent = 'High Dollar — ' + d.error; return; }
      const thresh = fmtDollar(d.threshold);
      el.innerHTML = `High Dollar threshold: <strong>${thresh}</strong> &nbsp;|&nbsp; `
        + `${fmtNum(d.enc_count)} encounters &nbsp;|&nbsp; `
        + `${fmtDollar(d.balance)} &nbsp;|&nbsp; `
        + `${d.pct}% of total balance &nbsp;<span style="opacity:.65">(top 60% by balance, week ${fmtWeek(d.week)})</span>`;
    })
    .catch(() => {});
}

// ── BILLING ENTITIES ──────────────────────────────────────────
function loadBillingEntities() {
  fetch(apiUrl('/api/billing-entities'))
    .then(r => r.json())
    .then(d => {
      const el = document.getElementById('billing-entities-list');
      if (!el) return;
      if (!d.entities || !d.entities.length) {
        el.textContent = 'No Billing Entity column found';
        return;
      }
      el.innerHTML = d.entities
        .map(e => `<div class="be-chip">${e}</div>`)
        .join('');
    })
    .catch(() => {});
}

// ── REFRESH DATA ─────────────────────────────────────────────
function refreshData() {
  const btn = document.getElementById('btn-refresh');
  if (!btn || btn.disabled) return;

  btn.disabled = true;
  btn.classList.add('refreshing');

  const url = currentClient
    ? `/api/reload?client=${encodeURIComponent(currentClient)}`
    : '/api/reload';

  fetch(url, { method: 'POST' })
    .then(r => r.json())
    .then(d => {
      if (d.error) {
        btn.disabled = false;
        btn.classList.remove('refreshing');
        return;
      }
      // Already loading — nothing queued, re-enable immediately
      if (d.skipped && d.skipped.length > 0 && (!d.reloaded || d.reloaded.length === 0)) {
        btn.disabled = false;
        btn.classList.remove('refreshing');
        return;
      }
      const activeTab = document.querySelector('.tab-btn.active');
      const tabId = activeTab ? activeTab.dataset.tab : 'trending';
      pollClientStatus(tabId, () => {
        btn.disabled = false;
        btn.classList.remove('refreshing');
      });
    })
    .catch(() => {
      btn.disabled = false;
      btn.classList.remove('refreshing');
    });
}

// ── INIT ─────────────────────────────────────────────────────
function initDashboard() {
  const sub = document.getElementById('page-title-sub');
  if (sub && currentClient) sub.textContent = currentClient.replace(/_/g, ' ');
  populateWeekSelectors();
  loadFilterOptions();
  switchSection('medicare');
  switchTab('trending');
}

// ── PAYER INFO ────────────────────────────────────────────────
function loadPayerInfo() {
  fetch(apiUrl('/api/medicare/payers'))
    .then(r => r.json())
    .then(d => {
      const wrap = document.getElementById('payer-tags');
      if (!wrap || !d.payers) return;
      wrap.innerHTML = d.payers.map(p =>
        `<span class="payer-tag">${p}</span>`
      ).join('');
    })
    .catch(() => {});
}

function populateWeekSelectors() {
  ['sel-from-week', 'sel-to-week', 'sel-bifur-week', 'sel-contrib-week'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = '';
    allWeeks.forEach((w, i) => {
      const opt = document.createElement('option');
      opt.value = w;
      opt.textContent = fmtWeek(w);
      el.appendChild(opt);
    });
  });

  const selFrom = document.getElementById('sel-from-week');
  const selTo = document.getElementById('sel-to-week');
  const selBifur = document.getElementById('sel-bifur-week');

  const selContrib = document.getElementById('sel-contrib-week');
  if (selFrom && allWeeks.length >= 2) selFrom.value = allWeeks[allWeeks.length - 2];
  if (selTo && allWeeks.length >= 1) selTo.value = allWeeks[allWeeks.length - 1];
  if (selBifur && allWeeks.length >= 1) selBifur.value = allWeeks[allWeeks.length - 1];
  if (selContrib && allWeeks.length >= 1) selContrib.value = allWeeks[allWeeks.length - 1];
}

// ── TAB 1: TRENDING ──────────────────────────────────────────
function loadTrending() {
  fetch(apiUrl('/api/trending'))
    .then(r => r.json())
    .then(data => {
      if (!Array.isArray(data)) return;
      renderKpiCards(data);
      renderTrendChart(data);
      renderTrendTable(data);
    })
    .catch(() => {});
}

function renderKpiCards(data) {
  if (!data.length) return;
  const latest = data[data.length - 1];
  const prev = data.length > 1 ? data[data.length - 2] : null;

  setKpi('kpi-encounters', fmtNum(latest.encounter_count),
    prev ? { v: latest.wow_count_delta, p: latest.wow_count_pct, up: latest.wow_count_delta > 0 } : null);
  setKpi('kpi-balance', fmtDollar(latest.balance_total),
    prev ? { v: fmtDollar(latest.wow_balance_delta), p: latest.wow_balance_pct, up: latest.wow_balance_delta > 0 } : null, true);
  setKpi('kpi-wow-enc', prev ? fmtDelta(latest.wow_count_delta) : '—',
    prev ? { label: fmtPct(latest.wow_count_pct), cls: latest.wow_count_delta > 0 ? 'up' : 'down' } : null);
  setKpi('kpi-wow-bal', prev ? fmtDollar(latest.wow_balance_delta) : '—',
    prev ? { label: fmtPct(latest.wow_balance_pct), cls: latest.wow_balance_delta > 0 ? 'up' : 'down' } : null);
}

function setKpi(id, value, delta, isDollar) {
  const card = document.getElementById(id);
  if (!card) return;
  card.querySelector('.kpi-value').textContent = value;
  const deltaEl = card.querySelector('.kpi-delta');
  if (!deltaEl) return;
  if (!delta) { deltaEl.textContent = 'vs prior week'; deltaEl.className = 'kpi-delta neutral'; return; }
  if (delta.label) {
    deltaEl.innerHTML = `${delta.label}`;
    deltaEl.className = 'kpi-delta ' + (delta.cls || 'neutral');
  } else {
    const arrow = delta.up ? '▲' : '▼';
    const cls = delta.up ? 'up' : 'down';
    deltaEl.innerHTML = `${arrow} ${isDollar ? delta.v : fmtDelta(delta.v)} (${fmtPct(delta.p)}) vs prior week`;
    deltaEl.className = 'kpi-delta ' + cls;
  }
}

function renderTrendChart(data) {
  const labels = data.map(d => fmtWeek(d.week));
  const balances = data.map(d => d.balance_total);
  const counts = data.map(d => d.encounter_count);
  const ctx = document.getElementById('trend-chart');
  if (!ctx) return;
  if (trendChart) trendChart.destroy();
  trendChart = new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type: 'bar',
          label: 'Total Balance ($)',
          data: balances,
          backgroundColor: 'rgba(14,165,233,0.25)',
          borderColor: 'rgba(14,165,233,0.8)',
          borderWidth: 1,
          borderRadius: 4,
          yAxisID: 'y1',
        },
        {
          type: 'line',
          label: 'Encounter Count',
          data: counts,
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245,158,11,0.15)',
          pointBackgroundColor: '#f59e0b',
          pointRadius: 5,
          tension: 0.3,
          fill: true,
          yAxisID: 'y2',
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 12 } } },
        tooltip: {
          callbacks: {
            label: ctx => ctx.dataset.yAxisID === 'y1'
              ? ` ${ctx.dataset.label}: ${fmtDollar(ctx.parsed.y)}`
              : ` ${ctx.dataset.label}: ${fmtNum(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(30,58,95,0.5)' } },
        y1: {
          type: 'linear', position: 'left',
          ticks: { color: '#0ea5e9', callback: v => fmtDollar(v) },
          grid: { color: 'rgba(30,58,95,0.5)' }
        },
        y2: {
          type: 'linear', position: 'right',
          ticks: { color: '#f59e0b', callback: v => fmtNum(v) },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

function renderTrendTable(data) {
  const tbody = document.querySelector('#trend-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  [...data].reverse().forEach(r => {
    const isFirst = r.wow_count_delta == null;
    const cntCls = !isFirst ? (r.wow_count_delta > 0 ? 'up' : 'down') : 'neutral';
    const balCls = !isFirst ? (r.wow_balance_delta > 0 ? 'up' : 'down') : 'neutral';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${fmtWeek(r.week)}</td>
      <td>${fmtNum(r.encounter_count)}</td>
      <td class="${cntCls}">${isFirst ? '—' : fmtDelta(r.wow_count_delta)}</td>
      <td class="${cntCls}">${isFirst ? '—' : fmtPct(r.wow_count_pct)}</td>
      <td>${fmtDollar(r.balance_total)}</td>
      <td class="${balCls}">${isFirst ? '—' : fmtDollar(r.wow_balance_delta)}</td>
      <td class="${balCls}">${isFirst ? '—' : fmtPct(r.wow_balance_pct)}</td>
    `;
    tbody.appendChild(tr);
  });
}

// ── TAB 2: AGING ROLLOVER ─────────────────────────────────────
function loadRollover() {
  const fromWeek = document.getElementById('sel-from-week')?.value;
  const toWeek = document.getElementById('sel-to-week')?.value;
  if (!fromWeek || !toWeek) return;

  document.getElementById('matrix-wrap').innerHTML = '<p style="color:#94a3b8;padding:20px;text-align:center">Loading...</p>';

  fetch(apiUrl('/api/migration', `from=${fromWeek}&to=${toWeek}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderMigrationMatrix(data, fromWeek, toWeek);
      renderMigrationSummary(data, fromWeek, toWeek);
      renderMigrationNarrative(data, fromWeek, toWeek);
    })
    .catch(() => {});
}

function renderMigrationNarrative(data, fromWeek, toWeek) {
  const el = document.getElementById('rollover-narrative');
  if (!el) return;
  const s = data.summary;
  const aged = s.aged_worse_count;
  const agedBal = fmtDollar(s.aged_worse_balance);
  const newCnt = data.new_encounters.count;
  const newBal = fmtDollar(data.new_encounters.balance);
  const resolved = data.resolved_encounters.count;
  const resolvedBal = fmtDollar(data.resolved_encounters.balance);
  el.innerHTML = `
    Comparing <strong>${fmtWeek(fromWeek)}</strong> → <strong>${fmtWeek(toWeek)}</strong>: &nbsp;
    <span class="danger-text">${fmtNum(aged)} encounters (${agedBal})</span> aged into older buckets. &nbsp;
    <span class="highlight">${fmtNum(newCnt)} new encounters (${newBal})</span> entered the ATB. &nbsp;
    <span class="success-text">${fmtNum(resolved)} encounters (${resolvedBal})</span> resolved / dropped off.
  `;
}

const BUCKET_COLORS = {
  diagonal: '#2563eb',
  improved: '#16a34a',
  worse1: '#d97706',
  worse2: '#dc2626',
};

function cellColor(fromIdx, toIdx, value) {
  if (!value || value === 0) return null;
  if (fromIdx === toIdx) return BUCKET_COLORS.diagonal;
  if (toIdx < fromIdx) return BUCKET_COLORS.improved;
  const steps = toIdx - fromIdx;
  return steps === 1 ? BUCKET_COLORS.worse1 : BUCKET_COLORS.worse2;
}

function cellOpacity(value, maxVal) {
  if (!maxVal || !value) return 0.15;
  return 0.2 + 0.75 * Math.min(value / maxVal, 1);
}

function renderMigrationMatrix(data, fromWeek, toWeek) {
  const wrap = document.getElementById('matrix-wrap');
  const buckets = data.buckets;
  const matrix = data.matrix;

  // Find max value for opacity scaling
  let maxVal = 0;
  buckets.forEach(fb => buckets.forEach(tb => {
    const v = matrix[fb]?.[tb]?.value || 0;
    if (v > maxVal) maxVal = v;
  }));

  let html = '<div class="matrix-container"><table class="matrix-table">';
  // Header row
  html += '<thead><tr><th class="matrix-table corner" style="background:#0a1628">FROM \\ TO</th>';
  buckets.forEach(b => { html += `<th class="col-header">${b}</th>`; });
  html += '</tr></thead><tbody>';

  buckets.forEach((fb, fi) => {
    html += `<tr><th class="row-header">${fb}</th>`;
    buckets.forEach((tb, ti) => {
      const cell = matrix[fb]?.[tb];
      const val = cell?.value || 0;
      const cnt = cell?.count || 0;
      const pct = cell?.pct;
      const baseColor = cellColor(fi, ti, val);
      if (!baseColor || val === 0) {
        html += `<td class="matrix-cell empty" data-from="${fb}" data-to="${tb}" data-val="0" data-cnt="0" data-pct="0">
          <span style="color:#334155;font-size:10px">—</span></td>`;
      } else {
        const opacity = cellOpacity(val, maxVal);
        const bg = hexWithOpacity(baseColor, opacity);
        const textColor = opacity > 0.5 ? '#fff' : '#e2e8f0';
        html += `<td class="matrix-cell"
          style="background:${bg};color:${textColor}"
          data-from="${fb}" data-to="${tb}" data-val="${val}" data-cnt="${cnt}" data-pct="${pct ?? 0}"
          onmouseenter="showMatrixTip(event,this)" onmouseleave="hideMatrixTip()">
          <div class="cell-val">${fmtDollar(val)}</div>
          <div class="cell-cnt">${fmtNum(cnt)} enc</div>
          ${pct != null ? `<div class="cell-pct">${pct}%</div>` : ''}
        </td>`;
      }
    });
    html += '</tr>';
  });

  html += '</tbody></table></div>';
  wrap.innerHTML = html;
}

function hexWithOpacity(hex, opacity) {
  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);
  return `rgba(${r},${g},${b},${opacity})`;
}

function showMatrixTip(e, cell) {
  const tt = document.getElementById('matrix-tooltip');
  const from = cell.dataset.from, to = cell.dataset.to;
  const val = parseFloat(cell.dataset.val);
  const cnt = parseInt(cell.dataset.cnt);
  const pct = parseFloat(cell.dataset.pct);
  tt.innerHTML = `
    <div class="tt-title">${from} &rarr; ${to}</div>
    <div class="tt-row"><span>Balance</span><span>${fmtDollar(val)}</span></div>
    <div class="tt-row"><span>Encounters</span><span>${fmtNum(cnt)}</span></div>
    <div class="tt-row"><span>% of From-Bucket</span><span>${pct}%</span></div>
    <div class="tt-row"><span>Movement</span><span>${from === to ? 'STAYED' : BUCKET_COLORS.improved === cellColor(0, -1, 1) ? '' : ''} ${
      from === to ? '(no change)' : `from ${from} → ${to}`
    }</span></div>
  `;
  tt.style.display = 'block';
  tt.style.left = (e.clientX + 14) + 'px';
  tt.style.top = (e.clientY - 10) + 'px';
}
function hideMatrixTip() {
  document.getElementById('matrix-tooltip').style.display = 'none';
}

function renderMigrationSummary(data, fromWeek, toWeek) {
  const s = data.summary;
  const ne = data.new_encounters;
  const re = data.resolved_encounters;

  setMCard('ms-aged', fmtDollar(s.aged_worse_balance), `${fmtNum(s.aged_worse_count)} encounters moved to older buckets`);
  setMCard('ms-new', fmtDollar(ne.balance), `${fmtNum(ne.count)} new encounters entered ATB`);
  setMCard('ms-resolved', fmtDollar(re.balance), `${fmtNum(re.count)} encounters resolved / removed`);
}

function setMCard(id, value, sub) {
  const el = document.getElementById(id);
  if (!el) return;
  el.querySelector('.ms-value').textContent = value;
  el.querySelector('.ms-sub').textContent = sub;
}

// ── TAB 3: ATB BIFURCATION ────────────────────────────────────
function loadBifurcation() {
  const week = document.getElementById('sel-bifur-week')?.value;
  if (!week) return;

  fetch(apiUrl('/api/bifurcation', `week=${week}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderBifurSummaryCards(data);
      renderBifurBarChart(data);
      renderBifurDonutChart(data);
      renderBifurTable(data);
    })
    .catch(() => {});
}

function renderBifurSummaryCards(data) {
  const t = data.totals;
  const cf = data.carried_forward_total;
  const nw = data.new_this_week_total;
  document.getElementById('bifur-total-enc').textContent = fmtNum(t.current.count);
  document.getElementById('bifur-total-bal').textContent = fmtDollar(t.current.balance);
  document.getElementById('bifur-carried').textContent = `${fmtNum(cf.count)} enc / ${fmtDollar(cf.balance)}`;
  document.getElementById('bifur-new').textContent = `${fmtNum(nw.count)} enc / ${fmtDollar(nw.balance)}`;
}

const BUCKET_PALETTE = [
  '#22c55e','#0ea5e9','#6366f1','#f59e0b','#ef4444',
  '#a855f7','#14b8a6','#f97316','#ec4899','#64748b',
  '#84cc16','#06b6d4','#8b5cf6','#fb923c','#e11d48'
];

function renderBifurBarChart(data) {
  const ctx = document.getElementById('bifur-bar-chart');
  if (!ctx) return;
  if (bifurBarChart) bifurBarChart.destroy();
  const buckets = data.buckets;
  const currentBal = data.rows.map(r => r.current_balance);
  const priorBal = data.rows.map(r => r.prior_balance);
  bifurBarChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: buckets,
      datasets: [
        {
          label: 'Current Week',
          data: currentBal,
          backgroundColor: 'rgba(14,165,233,0.7)',
          borderColor: '#0ea5e9',
          borderWidth: 1,
          borderRadius: 3,
        },
        {
          label: 'Prior Week',
          data: priorBal,
          backgroundColor: 'rgba(100,116,139,0.4)',
          borderColor: '#64748b',
          borderWidth: 1,
          borderRadius: 3,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 12 } } },
        tooltip: {
          callbacks: {
            label: ctx => ` ${ctx.dataset.label}: ${fmtDollar(ctx.parsed.y)}`
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8', maxRotation: 45 }, grid: { color: 'rgba(30,58,95,0.4)' } },
        y: { ticks: { color: '#94a3b8', callback: v => fmtDollar(v) }, grid: { color: 'rgba(30,58,95,0.4)' } }
      }
    }
  });
}

function renderBifurDonutChart(data) {
  const ctx = document.getElementById('bifur-donut-chart');
  if (!ctx) return;
  if (bifurDonutChart) bifurDonutChart.destroy();
  const buckets = data.buckets;
  const balances = data.rows.map(r => r.current_balance);
  const total = balances.reduce((a, b) => a + b, 0);
  bifurDonutChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: buckets,
      datasets: [{
        data: balances,
        backgroundColor: BUCKET_PALETTE.slice(0, buckets.length),
        borderColor: '#0d1b2a',
        borderWidth: 2,
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '60%',
      plugins: {
        legend: {
          position: 'right',
          labels: { color: '#94a3b8', font: { size: 11 }, padding: 10, boxWidth: 12 }
        },
        tooltip: {
          callbacks: {
            label: ctx => {
              const v = ctx.parsed;
              const pct = total ? (v / total * 100).toFixed(1) : 0;
              return ` ${fmtDollar(v)} (${pct}%)`;
            }
          }
        }
      }
    }
  });
}

function renderBifurTable(data) {
  const tbody = document.querySelector('#bifur-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  const total = data.totals.current.balance;
  data.rows.forEach(r => {
    const isAlert = r.delta_balance > 0 && r.delta_pct != null && r.delta_pct > 5;
    const dCls = r.delta_balance > 0 ? 'down' : r.delta_balance < 0 ? 'up' : 'neutral';
    const pctOfTotal = total ? (r.current_balance / total * 100).toFixed(1) + '%' : '—';
    const tr = document.createElement('tr');
    if (isAlert) tr.className = 'row-alert';
    tr.innerHTML = `
      <td>${r.bucket}</td>
      <td>${fmtNum(r.current_count)}</td>
      <td>${fmtDollar(r.current_balance)}</td>
      <td>${pctOfTotal}</td>
      <td>${fmtNum(r.prior_count)}</td>
      <td>${fmtDollar(r.prior_balance)}</td>
      <td class="${dCls}">${fmtDelta(r.delta_count)}</td>
      <td class="${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
      <td>${fmtNum(r.carried_count)} / ${fmtDollar(r.carried_balance)}</td>
      <td>${fmtNum(r.new_count)} / ${fmtDollar(r.new_balance)}</td>
    `;
    tbody.appendChild(tr);
  });

  // Totals row
  const t = data.totals;
  const cf = data.carried_forward_total;
  const nw = data.new_this_week_total;
  const dBal = t.current.balance - t.prior.balance;
  const dCls2 = dBal > 0 ? 'down' : dBal < 0 ? 'up' : 'neutral';
  const tr = document.createElement('tr');
  tr.style.fontWeight = '700';
  tr.style.borderTop = '2px solid #1e3a5f';
  tr.innerHTML = `
    <td>TOTAL</td>
    <td>${fmtNum(t.current.count)}</td>
    <td>${fmtDollar(t.current.balance)}</td>
    <td>100%</td>
    <td>${fmtNum(t.prior.count)}</td>
    <td>${fmtDollar(t.prior.balance)}</td>
    <td class="${dCls2}">${fmtDelta(t.current.count - t.prior.count)}</td>
    <td class="${dCls2}">${fmtDollar(dBal)}</td>
    <td class="${dCls2}">${t.prior.balance ? fmtPct(dBal / t.prior.balance * 100) : '—'}</td>
    <td>${fmtNum(cf.count)} / ${fmtDollar(cf.balance)}</td>
    <td>${fmtNum(nw.count)} / ${fmtDollar(nw.balance)}</td>
  `;
  tbody.appendChild(tr);
}

// ── TAB 4: 90+ CONTRIBUTORS ──────────────────────────────────
let contribRfcChart = null;
let contribRolledChart = null;

function loadContributors() {
  const week = document.getElementById('sel-contrib-week')?.value;
  const topN = document.getElementById('sel-contrib-topn')?.value || 15;
  if (!week) return;

  fetch(apiUrl('/api/aging-contributors', `week=${week}&top_n=${topN}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderKeyPoints(data.key_points || []);
      renderContribKpis(data.summary);
      renderRfcBarChart(data.by_fin_class || []);
      renderRolledChart(data.rolled_by_fin_class || []);
      renderRhpBars(data.by_health_plan || []);
      renderContribTable('contrib-rfc-table', data.by_fin_class || []);
      renderContribTable('contrib-rhp-table', data.by_health_plan || []);
      const lbl = document.getElementById('contrib-topn-label');
      if (lbl) lbl.textContent = topN;
    })
    .catch(() => {});
}

function renderKeyPoints(points) {
  const el = document.getElementById('key-points-list');
  if (!el) return;
  el.innerHTML = points.map(p => `
    <div class="kp-item">
      <div class="kp-dot ${p.type || 'info'}"></div>
      <div class="kp-text">${p.text}</div>
    </div>`).join('');
}

function renderContribKpis(s) {
  if (!s) return;
  const balCard = document.getElementById('kp-90-bal');
  const cntCard = document.getElementById('kp-90-cnt');
  const rolCard = document.getElementById('kp-rolled');
  const rolBal  = document.getElementById('kp-rolled-bal');

  if (balCard) {
    balCard.querySelector('.kpi-value').textContent = fmtDollar(s.curr_balance);
    const d = s.delta_balance, p = s.delta_pct;
    const cls = d > 0 ? 'down' : 'up';
    const arrow = d > 0 ? '▲' : '▼';
    balCard.querySelector('.kpi-delta').innerHTML = `${arrow} ${fmtDollar(d)} (${fmtPct(p)}) vs prior`;
    balCard.querySelector('.kpi-delta').className = 'kpi-delta ' + cls;
  }
  if (cntCard) {
    cntCard.querySelector('.kpi-value').textContent = fmtNum(s.curr_count);
    const d = s.curr_count - s.prev_count;
    const cls = d > 0 ? 'down' : 'up';
    cntCard.querySelector('.kpi-delta').innerHTML = (d > 0 ? '▲ +' : '▼ ') + fmtNum(d) + ' vs prior';
    cntCard.querySelector('.kpi-delta').className = 'kpi-delta ' + cls;
  }
  if (rolCard) {
    rolCard.querySelector('.kpi-value').textContent = fmtNum(s.rolled_in_count);
    rolCard.querySelector('.kpi-delta').textContent = 'crossed 90-day threshold';
  }
  if (rolBal) {
    rolBal.querySelector('.kpi-value').textContent = fmtDollar(s.rolled_in_balance);
    rolBal.querySelector('.kpi-delta').textContent = 'from 61-90 → 90+ buckets';
  }
}

function renderRfcBarChart(rows) {
  const ctx = document.getElementById('contrib-rfc-chart');
  if (!ctx) return;
  if (contribRfcChart) contribRfcChart.destroy();
  const labels = rows.map(r => r.name);
  contribRfcChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        { label: 'Current', data: rows.map(r => r.curr_balance),
          backgroundColor: 'rgba(239,68,68,0.65)', borderColor: '#ef4444', borderWidth: 1, borderRadius: 3 },
        { label: 'Prior',   data: rows.map(r => r.prev_balance),
          backgroundColor: 'rgba(100,116,139,0.35)', borderColor: '#64748b', borderWidth: 1, borderRadius: 3 },
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 11 } } },
        tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${fmtDollar(c.parsed.y)}` } }
      },
      scales: {
        x: { ticks: { color: '#94a3b8', maxRotation: 30 }, grid: { color: 'rgba(30,58,95,0.4)' } },
        y: { ticks: { color: '#94a3b8', callback: v => fmtDollar(v) }, grid: { color: 'rgba(30,58,95,0.4)' } }
      }
    }
  });
}

function renderRolledChart(rows) {
  const ctx = document.getElementById('contrib-rolled-chart');
  if (!ctx) return;
  if (contribRolledChart) contribRolledChart.destroy();
  if (!rows.length) {
    ctx.parentElement.innerHTML = '<p style="color:#64748b;padding:20px;text-align:center;font-size:12px">No 61-90 → 90+ rollovers this week.</p>';
    return;
  }
  contribRolledChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: rows.map(r => r.name),
      datasets: [{ data: rows.map(r => r.balance),
        backgroundColor: BUCKET_PALETTE.slice(0, rows.length),
        borderColor: '#0d1b2a', borderWidth: 2, hoverOffset: 5 }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, cutout: '55%',
      plugins: {
        legend: { position: 'bottom', labels: { color: '#94a3b8', font: { size: 10 }, padding: 8, boxWidth: 10 } },
        tooltip: { callbacks: { label: c => ` ${fmtDollar(c.parsed)} (${c.label})` } }
      }
    }
  });
}

function renderRhpBars(rows) {
  const wrap = document.getElementById('contrib-rhp-wrap');
  if (!wrap) return;
  if (!rows.length) { wrap.innerHTML = '<p style="color:#64748b;padding:20px">No data.</p>'; return; }
  const maxBal = Math.max(...rows.map(r => r.curr_balance), 1);
  const maxDelta = Math.max(...rows.map(r => Math.abs(r.delta_balance)), 1);
  let html = `<table class="rhp-bar-table">
    <thead style="font-size:10px;color:#64748b;text-transform:uppercase">
      <tr><td style="width:180px">Health Plan</td><td>Curr Balance</td><td style="width:180px">Balance Bar</td><td>WoW &Delta;</td><td>WoW %</td></tr>
    </thead><tbody>`;
  rows.forEach(r => {
    const barW = Math.max(4, Math.round(r.curr_balance / maxBal * 160));
    const dCls = r.delta_balance > 0 ? 'up' : 'down';
    const dBarW = Math.max(4, Math.round(Math.abs(r.delta_balance) / maxDelta * 80));
    html += `<tr>
      <td class="rhp-name" title="${r.name}">${r.name}</td>
      <td class="rhp-curr">${fmtDollar(r.curr_balance)}</td>
      <td><div style="display:flex;align-items:center;gap:4px">
        <div class="rhp-bar ${dCls}" style="width:${barW}px"></div>
        <div class="rhp-bar ${dCls}" style="width:${dBarW}px;opacity:0.4"></div>
      </div></td>
      <td class="rhp-delta ${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="rhp-delta ${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

function renderContribTable(tableId, rows) {
  const tbody = document.querySelector(`#${tableId} tbody`);
  if (!tbody) return;
  tbody.innerHTML = '';
  rows.forEach(r => {
    const dCls = r.delta_balance > 0 ? 'down' : r.delta_balance < 0 ? 'up' : 'neutral';
    const tr = document.createElement('tr');
    if (r.delta_balance > 0) tr.className = 'row-alert';
    tr.innerHTML = `
      <td>${r.name}</td>
      <td>${fmtNum(r.curr_count)}</td><td>${fmtDollar(r.curr_balance)}</td>
      <td>${fmtNum(r.prev_count)}</td><td>${fmtDollar(r.prev_balance)}</td>
      <td class="${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
    `;
    tbody.appendChild(tr);
  });
}

// ── BOOT ─────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  pollLoading();
});
