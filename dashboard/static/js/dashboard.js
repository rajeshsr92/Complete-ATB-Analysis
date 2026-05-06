/* ================================================================
   Medicare KPI Dashboard — dashboard.js
   ================================================================ */

const API = '';
let trendChart = null;
let bifurBarChart = null;
let bifurDonutChart = null;
let denialParetoChart = null;
let denialGroupChart = null;
let dvTrendChart = null;
let capPayerMatrixChart = null;
let capWaterfallChart   = null;
let capForecastChart    = null;
let allWeeks = [];
let currentClient = '';
let highDollarMode = false;

// Active filter state: {rfc: [...], rhp: [...], bt: [...]}
const activeFilters = { rfc: [], rhp: [], bt: [] };

// ── FILTER PARAM BUILDER ──────────────────────────────────────
function filterParams() {
  const parts = [];
  if (activeFilters.rfc.length) parts.push('resp_fin_class=' + encodeURIComponent(activeFilters.rfc.join(',')));
  if (activeFilters.rhp.length) parts.push('resp_health_plan=' + encodeURIComponent(activeFilters.rhp.join(',')));
  if (activeFilters.bt.length)  parts.push('balance_type=' + encodeURIComponent(activeFilters.bt.join(',')));
  if (highDollarMode) parts.push('high_dollar=true');
  return parts.join('&');
}

// ── MULTISELECT DROPDOWN COMPONENT ───────────────────────────
// fdId: 'fd-rfc', 'fd-rhp', or 'fd-bt', maps to activeFilters keys
const FD_MAP = { 'fd-rfc': 'rfc', 'fd-rhp': 'rhp', 'fd-bt': 'bt' };

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
  fdClear('fd-bt');
}

function updateFilterActiveBadge() {
  const bar = document.getElementById('filter-active-bar');
  const tags = document.getElementById('filter-active-tags');
  const total = activeFilters.rfc.length + activeFilters.rhp.length + activeFilters.bt.length;
  if (total === 0) {
    bar.classList.remove('visible');
    return;
  }
  bar.classList.add('visible');
  const parts = [];
  if (activeFilters.rfc.length) parts.push(`Fin.Class: ${activeFilters.rfc.length} selected`);
  if (activeFilters.rhp.length) parts.push(`Health Plan: ${activeFilters.rhp.length} selected`);
  if (activeFilters.bt.length)  parts.push(`Bal.Type: ${activeFilters.bt.join(', ')}`);
  tags.innerHTML = parts.map(p => `<span class="filter-tag">${p}</span>`).join('');
}

function loadFilterOptions() {
  fetch(apiUrl('/api/filters'))
    .then(r => r.json())
    .then(d => {
      if (d.resp_fin_class)  fdBuild('fd-rfc', d.resp_fin_class);
      if (d.resp_health_plan) fdBuild('fd-rhp', d.resp_health_plan);
      if (d.balance_type)    fdBuild('fd-bt', d.balance_type);

      // Restore previously checked values (or pre-select AR - Debit by default)
      ['fd-rfc', 'fd-rhp', 'fd-bt'].forEach(fdId => {
        const key = FD_MAP[fdId];
        if (activeFilters[key].length) {
          document.querySelectorAll(`#${fdId}-options input`).forEach(cb => {
            cb.checked = activeFilters[key].includes(cb.value);
          });
        } else if (fdId === 'fd-bt') {
          // Default: pre-select AR - Debit
          const arDebit = document.querySelector(`#fd-bt-options input[value="AR - Debit"]`);
          if (arDebit) { arDebit.checked = true; fdChange('fd-bt'); }
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

function _updateOverlayProgress(clients) {
  const total = clients.length;
  const loaded = clients.filter(c => !c.loading && c.weeks && c.weeks.length > 0).length;
  const errored = clients.filter(c => !c.loading && c.error).length;
  const pct = total > 0 ? Math.round(loaded / total * 100) : 0;

  const fill = document.getElementById('loader-progress-fill');
  const txt  = document.getElementById('loader-progress-text');
  if (fill) fill.style.width = pct + '%';
  if (txt)  txt.textContent  = `${loaded} of ${total} clients ready  —  ${pct}%${errored ? `  (${errored} error)` : ''}`;

  const wrap = document.getElementById('loader-clients');
  if (wrap) {
    wrap.innerHTML = clients.map(c => {
      if (!c.loading && c.weeks && c.weeks.length > 0) {
        return `<span class="lc-pill done">&#10003; ${c.name.replace(/_/g,' ')}</span>`;
      } else if (c.error) {
        return `<span class="lc-pill err">&#10007; ${c.name.replace(/_/g,' ')}</span>`;
      } else {
        return `<span class="lc-pill wait"><span class="lc-spin"></span>${c.name.replace(/_/g,' ')}</span>`;
      }
    }).join('');
  }
}

function _updateTopbarBadge(clients) {
  const badge = document.getElementById('topbar-load-badge');
  const txt   = document.getElementById('topbar-load-text');
  if (!badge) return;
  const still = clients.filter(c => c.loading).length;
  const total = clients.length;
  if (still > 0) {
    const done = total - still;
    badge.style.display = 'flex';
    if (txt) txt.textContent = `${done}/${total} clients loaded`;
  } else {
    badge.style.display = 'none';
  }
}

function _startBackgroundProgressPoll() {
  function tick() {
    fetch('/api/clients')
      .then(r => r.json())
      .then(clients => {
        _updateTopbarBadge(clients);
        if (clients.some(c => c.loading)) setTimeout(tick, 3000);
      })
      .catch(() => setTimeout(tick, 5000));
  }
  tick();
}

function pollClientStatus(tabId, onDone) {
  // Also refresh the global client list to update overlay progress
  Promise.all([
    fetch(apiUrl('/api/status')).then(r => r.json()),
    fetch('/api/clients').then(r => r.json()).catch(() => [])
  ]).then(([d, clients]) => {
    if (clients.length) _updateOverlayProgress(clients);

    const log = document.getElementById('loader-log');
    if (d.loading) {
      document.getElementById('loading-overlay').style.display = 'flex';
      const sub = document.getElementById('loader-sub');
      if (sub) sub.textContent = `Loading ${currentClient.replace(/_/g,' ')}…`;
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
      loadFilterOptions();
      loadBillingEntities();
      switchTab(tabId || 'trending');
      if (highDollarMode) loadHDThreshold();
      if (typeof onDone === 'function') onDone();
      // Keep updating topbar badge while other clients finish
      _startBackgroundProgressPoll();
    }
  }).catch(() => setTimeout(() => pollClientStatus(tabId, onDone), 3000));
}

// ── LOADING OVERLAY ───────────────────────────────────────────
function pollLoading() {
  loadClients().then(clients => {
    if (clients.length) _updateOverlayProgress(clients);
    pollClientStatus('trending');
  }).catch(() => setTimeout(pollLoading, 3000));
}

// ── TAB SWITCHING ─────────────────────────────────────────────
function switchTab(id) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === id));
  document.querySelectorAll('.tab-pane').forEach(p => p.style.display = p.id === 'tab-' + id ? 'block' : 'none');
  if (id === 'trending') loadTrending();
  if (id === 'rollover')   loadRollover();
  if (id === 'retention')  loadRetention();
  if (id === 'bifurcation') loadBifurcation();
  if (id === 'contributors') loadContributors();
  if (id === 'denials') loadDenials();
  if (id === 'denial-velocity') loadDenialVelocity();
  if (id === 'cash-action') loadCashActionPlan();
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
  ['sel-from-week','sel-to-week','ret-from-week','ret-to-week',
   'sel-bifur-week','sel-contrib-week','sel-denials-week'].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.innerHTML = '';
    allWeeks.forEach(w => {
      const opt = document.createElement('option');
      opt.value = w; opt.textContent = fmtWeek(w);
      el.appendChild(opt);
    });
  });

  const selFrom    = document.getElementById('sel-from-week');
  const selTo      = document.getElementById('sel-to-week');
  const selBifur   = document.getElementById('sel-bifur-week');
  const selContrib = document.getElementById('sel-contrib-week');
  const selDenials = document.getElementById('sel-denials-week');
  const retFrom    = document.getElementById('ret-from-week');
  const retTo      = document.getElementById('ret-to-week');

  if (selFrom  && allWeeks.length >= 2) selFrom.value  = allWeeks[allWeeks.length - 2];
  if (selTo    && allWeeks.length >= 1) selTo.value    = allWeeks[allWeeks.length - 1];
  if (retFrom  && allWeeks.length >= 2) retFrom.value  = allWeeks[allWeeks.length - 2];
  if (retTo    && allWeeks.length >= 1) retTo.value    = allWeeks[allWeeks.length - 1];
  if (selBifur && allWeeks.length >= 1) selBifur.value = allWeeks[allWeeks.length - 1];
  if (selContrib && allWeeks.length >= 1) selContrib.value = allWeeks[allWeeks.length - 1];
  if (selDenials && allWeeks.length >= 1) selDenials.value = allWeeks[allWeeks.length - 1];
}

// ── DOWNLOAD HELPERS ──────────────────────────────────────────
function triggerDownload(url) { window.location.href = url; }

function _dlWeek() {
  const ids = ['sel-bifur-week', 'sel-contrib-week', 'sel-denials-week', 'sel-to-week'];
  for (const id of ids) {
    const el = document.getElementById(id);
    if (el && el.value) return el.value;
  }
  return allWeeks.length ? allWeeks[allWeeks.length - 1] : '';
}

function downloadTrending() {
  const week = allWeeks.length ? allWeeks[allWeeks.length - 1] : '';
  triggerDownload(apiUrl('/api/download/trending', week ? `week=${week}` : ''));
}

function downloadMigration(fromBucket, toBucket) {
  const fw = document.getElementById('sel-from-week')?.value || '';
  const tw = document.getElementById('sel-to-week')?.value || '';
  if (!fw || !tw) return;
  let extra = `from=${encodeURIComponent(fw)}&to=${encodeURIComponent(tw)}`;
  if (fromBucket) extra += `&from_bucket=${encodeURIComponent(fromBucket)}`;
  if (toBucket)   extra += `&to_bucket=${encodeURIComponent(toBucket)}`;
  triggerDownload(apiUrl('/api/download/migration', extra));
}

function downloadBifurcation(bucket) {
  const week  = document.getElementById('sel-bifur-week')?.value || _dlWeek();
  let extra   = `week=${encodeURIComponent(week)}`;
  if (bucket) extra += `&bucket=${encodeURIComponent(bucket)}`;
  triggerDownload(apiUrl('/api/download/bifurcation', extra));
}

function downloadContributors(healthPlan, finClass) {
  const week = document.getElementById('sel-contrib-week')?.value || _dlWeek();
  let extra  = `week=${encodeURIComponent(week)}`;
  if (healthPlan) extra += `&health_plan=${encodeURIComponent(healthPlan)}`;
  if (finClass)   extra += `&fin_class=${encodeURIComponent(finClass)}`;
  triggerDownload(apiUrl('/api/download/aging-contributors', extra));
}

function downloadDenials(denialCode) {
  const week = document.getElementById('sel-denials-week')?.value || _dlWeek();
  let extra  = `week=${encodeURIComponent(week)}`;
  if (denialCode) extra += `&denial_code=${encodeURIComponent(denialCode)}`;
  triggerDownload(apiUrl('/api/download/denials', extra));
}

function downloadDenialVelocity() {
  const week = _dlWeek();
  triggerDownload(apiUrl('/api/download/denial-velocity', `week=${encodeURIComponent(week)}`));
}

function downloadCashActionPlan(payer) {
  const week = _dlWeek();
  let extra  = `week=${encodeURIComponent(week)}`;
  if (payer) extra += `&payer=${encodeURIComponent(payer)}`;
  triggerDownload(apiUrl('/api/download/cash-action-plan', extra));
}

// ── SHARED: TAB SUMMARY PANEL ────────────────────────────────
function renderTabSummary(panelId, points) {
  const el = document.getElementById(panelId);
  if (!el) return;
  const panel = el.parentElement;
  if (!points || !points.length) { if (panel) panel.style.display = 'none'; return; }
  if (panel) panel.style.display = '';
  el.innerHTML = points.map(p => `
    <div class="kp-item">
      <div class="kp-dot ${p.type || 'info'}"></div>
      <div class="kp-text">${p.text}</div>
    </div>`).join('');
}

// ── TAB 1: TRENDING ──────────────────────────────────────────
function loadTrending() {
  fetch(apiUrl('/api/trending'))
    .then(r => r.json())
    .then(data => {
      const rows = Array.isArray(data) ? data : (data.rows || []);
      if (!rows.length) return;
      renderKpiCards(rows);
      renderTrendChart(rows);
      renderTrendTable(rows);
      renderTabSummary('trending-summary', data.summary || []);
      const latestWeek = rows[rows.length - 1]?.week;
      if (latestWeek) loadBalanceGroups(latestWeek);
    })
    .catch(() => {});
}

function loadBalanceGroups(week) {
  fetch(apiUrl('/api/balance-groups', `week=${week}`))
    .then(r => r.json())
    .then(data => {
      if (!data || !data.available) return;
      renderBalanceGroupTable(data.groups || []);
    })
    .catch(() => {});
}

function renderBalanceGroupTable(groups) {
  const wrap = document.getElementById('balance-group-wrap');
  if (!wrap || !groups.length) { if (wrap) wrap.innerHTML = '<p style="color:#64748b;padding:12px;font-size:12px">No Balance Group data available.</p>'; return; }
  let html = '<table class="data-table"><thead><tr>' +
    '<th>Group</th><th>Encounters</th><th>Balance</th><th>% of Total</th><th>Prior Balance</th><th>WoW &Delta;</th><th>WoW &Delta; %</th>' +
    '</tr></thead><tbody>';
  groups.forEach(g => {
    const dCls = g.delta_balance > 0 ? 'up' : g.delta_balance < 0 ? 'down' : 'neutral';
    html += `<tr>
      <td>${g.name}</td>
      <td>${fmtNum(g.curr_count)}</td>
      <td>${fmtDollar(g.curr_balance)}</td>
      <td>${g.pct_of_total != null ? g.pct_of_total + '%' : '—'}</td>
      <td>${fmtDollar(g.prior_balance)}</td>
      <td class="${dCls}">${fmtDollar(g.delta_balance)}</td>
      <td class="${dCls}">${g.delta_pct != null ? fmtPct(g.delta_pct) : '—'}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
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
        },
        {
          type: 'line',
          label: '90+ % of Balance',
          data: data.map(d => d.over_90_pct || 0),
          borderColor: '#ef4444',
          backgroundColor: 'rgba(239,68,68,0.0)',
          pointBackgroundColor: '#ef4444',
          pointRadius: 4,
          borderDash: [4, 3],
          tension: 0.3,
          fill: false,
          yAxisID: 'yPct',
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
            label: ctx => {
              if (ctx.dataset.yAxisID === 'y1') return ` ${ctx.dataset.label}: ${fmtDollar(ctx.parsed.y)}`;
              if (ctx.dataset.yAxisID === 'yPct') return ` ${ctx.dataset.label}: ${ctx.parsed.y}%`;
              return ` ${ctx.dataset.label}: ${fmtNum(ctx.parsed.y)}`;
            }
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
        },
        yPct: {
          type: 'linear', position: 'right',
          min: 0, max: 100,
          ticks: { color: '#ef4444', callback: v => v + '%' },
          grid: { drawOnChartArea: false },
          display: false,
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
// ── ATB RETENTION TAB ─────────────────────────────────────────

let _retFromWeek = '', _retToWeek = '';

function loadRetention() {
  const fromWeek = document.getElementById('ret-from-week')?.value;
  const toWeek   = document.getElementById('ret-to-week')?.value;
  if (!fromWeek || !toWeek) return;
  _retFromWeek = fromWeek;
  _retToWeek   = toWeek;

  document.getElementById('ret-waterfall-wrap').innerHTML =
    '<div style="padding:40px;text-align:center;color:#94a3b8"><div class="spinner" style="margin:0 auto 12px"></div>Loading retention analysis…</div>';

  fetch(apiUrl('/api/retention', `from=${encodeURIComponent(fromWeek)}&to=${encodeURIComponent(toWeek)}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderRetentionKPI(data, fromWeek, toWeek);
      renderRetentionWaterfall(data, fromWeek, toWeek);
      renderRetentionSummaryTable(data);
    })
    .catch(() => {});
}

function renderRetentionKPI(data, fromWeek, toWeek) {
  const rateColor = data.retention_rate >= 80 ? '#4ade80' : data.retention_rate >= 60 ? '#facc15' : '#f87171';

  function setRetKPI(id, val, sub) {
    const el = document.getElementById(id);
    if (!el) return;
    el.querySelector('.ret-kpi-value').textContent = val;
    el.querySelector('.ret-kpi-sub').textContent   = sub;
  }

  setRetKPI('ret-kpi-cohort',   fmtNum(data.cohort_count),     `${fmtDollar(data.cohort_balance)} total balance in ${fmtWeek(fromWeek)}`);
  setRetKPI('ret-kpi-survived', fmtNum(data.survived_count),   `${fmtNum(data.resolved_count)} resolved / dropped off`);
  setRetKPI('ret-kpi-resolved', fmtNum(data.resolved_count),   `${Math.round(100 - data.retention_rate)}% of cohort cleared`);
  setRetKPI('ret-kpi-rate',     data.retention_rate + '%',     `of From-Week cohort still on ATB`);
  setRetKPI('ret-kpi-bal',      fmtDollar(data.survived_balance), `as of ${fmtWeek(toWeek)}`);

  const rateEl = document.getElementById('ret-kpi-rate');
  if (rateEl) rateEl.querySelector('.ret-kpi-value').style.color = rateColor;
}

function renderRetentionWaterfall(data, fromWeek, toWeek) {
  const wrap     = document.getElementById('ret-waterfall-wrap');
  const fromBkts = data.from_buckets;
  const toBkts   = data.to_buckets;
  const flows    = data.flows;

  if (!fromBkts.length) {
    wrap.innerHTML = '<p style="color:#94a3b8;text-align:center;padding:30px">No survived encounters found between the selected weeks.</p>';
    return;
  }

  const MOVE_COLOR = { stayed:'#2563eb', improved:'#16a34a', aged1:'#d97706', aged2:'#dc2626' };

  let html = '<div class="ret-waterfall">';
  fromBkts.forEach(fb => {
    const rowFlows = flows[fb] || {};
    const rowTotal = Object.values(rowFlows).reduce((s, c) => s + (c.balance || 0), 0);
    if (!rowTotal) return;

    // Build segments ordered by to_bucket
    const segments = toBkts
      .map(tb => ({ tb, ...rowFlows[tb] }))
      .filter(s => s.balance > 0);

    html += `<div class="ret-row">
      <div class="ret-row-label">
        <div class="ret-bucket-name">${fb}</div>
        <div class="ret-bucket-total">${fmtDollar(rowTotal)}</div>
      </div>
      <div class="ret-bar-track">`;

    segments.forEach(seg => {
      const pct   = seg.pct_bal || 0;
      const color = MOVE_COLOR[seg.move] || '#64748b';
      const fbe   = fb.replace(/'/g, "\\'");
      const tbe   = seg.tb.replace(/'/g, "\\'");
      html += `<div class="ret-bar-seg"
        style="width:${pct}%;background:${color};min-width:${pct > 0 ? '3px' : '0'}"
        data-from="${fb}" data-to="${seg.tb}" data-pct="${pct}"
        data-bal="${seg.balance}" data-cnt="${seg.count}"
        onmouseenter="showRetTip(event,this)" onmouseleave="hideMatrixTip()"
        onclick="openRetentionDrilldown('${fbe}','${tbe}')">
        ${pct >= 8 ? `<span class="ret-seg-label">${seg.tb}<br>${pct}%</span>` : ''}
      </div>`;
    });

    html += `</div>
      <div class="ret-row-right">
        <span class="ret-enc-count">${fmtNum(Object.values(rowFlows).reduce((s,c) => s + (c.count||0), 0))} enc</span>
      </div>
    </div>`;
  });

  html += '</div>';
  wrap.innerHTML = html;
}

function showRetTip(e, seg) {
  const tt = document.getElementById('matrix-tooltip');
  const from = seg.dataset.from, to = seg.dataset.to;
  const bal  = parseFloat(seg.dataset.bal);
  const cnt  = parseInt(seg.dataset.cnt);
  const pct  = parseFloat(seg.dataset.pct);
  tt.innerHTML = `
    <div class="tt-title">${from} &rarr; ${to}</div>
    <div class="tt-row"><span>Balance</span><span>${fmtDollar(bal)}</span></div>
    <div class="tt-row"><span>Encounters</span><span>${fmtNum(cnt)}</span></div>
    <div class="tt-row"><span>% of From-Row</span><span>${pct}%</span></div>
    <div style="margin-top:8px;font-size:10px;color:#38bdf8;text-align:center;border-top:1px solid rgba(255,255,255,0.1);padding-top:6px">
      &#128065; Click to drill into ${fmtNum(cnt)} encounters
    </div>`;
  tt.style.display = 'block';
  tt.style.left    = (e.clientX + 14) + 'px';
  tt.style.top     = (e.clientY - 10) + 'px';
}

function openRetentionDrilldown(fromBucket, toBucket) {
  // Temporarily swap rollover week vars so the shared drawer uses the retention weeks
  const savedFrom = _rolloverFromWeek, savedTo = _rolloverToWeek;
  _rolloverFromWeek = _retFromWeek;
  _rolloverToWeek   = _retToWeek;
  openMigrationDrawer(fromBucket, toBucket);
  // Restore after drawer sets its own copies
  _rolloverFromWeek = savedFrom;
  _rolloverToWeek   = savedTo;
}

function renderRetentionSummaryTable(data) {
  const toBkts  = data.to_buckets;
  const summary = data.to_summary;
  if (!toBkts.length) return;

  let html = `<table class="data-table" style="font-size:12px">
    <thead><tr>
      <th>To Week Bucket</th>
      <th style="text-align:right">Encounters</th>
      <th style="text-align:right">% of Cohort</th>
      <th style="text-align:right">Balance (To Week)</th>
    </tr></thead><tbody>`;

  toBkts.forEach((tb, i) => {
    const s = summary[tb];
    if (!s || !s.count) return;
    html += `<tr class="${i % 2 === 1 ? 'dt-row-alt' : ''}">
      <td><span class="bucket-pill">${tb}</span></td>
      <td style="text-align:right">${fmtNum(s.count)}</td>
      <td style="text-align:right">${s.pct}%</td>
      <td style="text-align:right;font-weight:700">${fmtDollar(s.balance)}</td>
    </tr>`;
  });

  const totalBal = toBkts.reduce((s, tb) => s + (summary[tb]?.balance || 0), 0);
  const totalCnt = toBkts.reduce((s, tb) => s + (summary[tb]?.count   || 0), 0);
  html += `<tr style="font-weight:700;border-top:2px solid rgba(255,255,255,0.1)">
    <td>TOTAL</td>
    <td style="text-align:right">${fmtNum(totalCnt)}</td>
    <td style="text-align:right">100%</td>
    <td style="text-align:right">${fmtDollar(totalBal)}</td>
  </tr></tbody></table>`;

  document.getElementById('ret-summary-wrap').innerHTML = html;
}

// ── AGING ROLLOVER / WATERFALL ────────────────────────────────

let _rolloverFromWeek = '', _rolloverToWeek = '';
let _drawerFromBucket = '', _drawerToBucket = '';
let _drawerAllRows = [];
let _drawerSortCol = 'to_balance', _drawerSortAsc = false;

function loadRollover() {
  const fromWeek = document.getElementById('sel-from-week')?.value;
  const toWeek   = document.getElementById('sel-to-week')?.value;
  if (!fromWeek || !toWeek) return;
  _rolloverFromWeek = fromWeek;
  _rolloverToWeek   = toWeek;

  document.getElementById('matrix-wrap').innerHTML =
    '<div style="padding:48px;text-align:center;color:#94a3b8"><div class="spinner" style="margin:0 auto 12px"></div>Loading waterfall…</div>';

  fetch(apiUrl('/api/migration', `from=${fromWeek}&to=${toWeek}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderMigrationMatrix(data, fromWeek, toWeek);
      renderMigrationSummary(data, fromWeek, toWeek);
      renderMigrationNarrative(data, fromWeek, toWeek);
      renderTabSummary('rollover-summary', data.summary_points || []);
      renderToWeekDistribution(data, toWeek);
    })
    .catch(() => {});
}

function renderMigrationNarrative(data, fromWeek, toWeek) {
  const el = document.getElementById('rollover-narrative');
  if (!el) return;
  const s   = data.summary;
  const pct = s.total_continued ? Math.round(s.aged_worse_count / s.total_continued * 100) : 0;
  el.innerHTML = `
    <span style="color:#94a3b8">Comparing</span>
    <strong style="color:#e2e8f0">${fmtWeek(fromWeek)}</strong>
    <span style="color:#64748b">→</span>
    <strong style="color:#e2e8f0">${fmtWeek(toWeek)}</strong> &nbsp;|&nbsp;
    <span class="danger-text">&#8599; ${fmtNum(s.aged_worse_count)} enc (${fmtDollar(s.aged_worse_balance)}) aged older</span> &nbsp;·&nbsp;
    <span style="color:#2563eb">&#8644; ${fmtNum(s.stayed_count)} stayed (${fmtDollar(s.stayed_balance)})</span> &nbsp;·&nbsp;
    <span class="highlight">&#43; ${fmtNum(data.new_encounters.count)} new (${fmtDollar(data.new_encounters.balance)})</span> &nbsp;·&nbsp;
    <span class="success-text">&#10003; ${fmtNum(data.resolved_encounters.count)} resolved (${fmtDollar(data.resolved_encounters.balance)})</span>
  `;
}

const BUCKET_COLORS = {
  diagonal: '#2563eb',
  improved: '#16a34a',
  worse1:   '#d97706',
  worse2:   '#dc2626',
};

function cellColor(fromIdx, toIdx, value) {
  if (!value || value === 0) return null;
  if (fromIdx === toIdx) return BUCKET_COLORS.diagonal;
  if (toIdx < fromIdx)   return BUCKET_COLORS.improved;
  const steps = toIdx - fromIdx;
  return steps === 1 ? BUCKET_COLORS.worse1 : BUCKET_COLORS.worse2;
}

function cellOpacity(value, maxVal) {
  if (!maxVal || !value) return 0.15;
  return 0.18 + 0.78 * Math.min(value / maxVal, 1);
}

function renderMigrationMatrix(data, fromWeek, toWeek) {
  renderMatrixInto('matrix-wrap', data, 'openMigrationDrawer');
}

function renderRetentionWaterfall(data, fromWeek, toWeek) {
  renderMatrixInto('ret-waterfall-wrap', data, 'openRetentionDrilldown');
}

function renderMatrixInto(wrapId, data, clickFn) {
  const wrap    = document.getElementById(wrapId);
  if (!wrap) return;
  const buckets = data.buckets;
  const matrix  = data.matrix;
  if (!buckets || !buckets.length) {
    wrap.innerHTML = '<p style="color:#94a3b8;text-align:center;padding:40px">No data for selected weeks.</p>';
    return;
  }

  const rowTotal = {}, colTotal = {};
  let grandTotal = 0;
  buckets.forEach(fb => {
    rowTotal[fb] = 0;
    buckets.forEach(tb => {
      const v = matrix[fb]?.[tb]?.value || 0;
      rowTotal[fb] += v;
      colTotal[tb]  = (colTotal[tb] || 0) + v;
      grandTotal   += v;
    });
  });

  let maxVal = 0;
  buckets.forEach(fb => buckets.forEach(tb => {
    const v = matrix[fb]?.[tb]?.value || 0;
    if (v > maxVal) maxVal = v;
  }));

  const maxRowTotal = Math.max(...Object.values(rowTotal), 1);

  let html = '<div class="matrix-container"><table class="matrix-table"><thead>';
  html += `<tr>
    <th class="matrix-corner">
      <div class="corner-from">PRIOR WEEK</div>
      <div class="corner-to">CURRENT WEEK &#8594;</div>
    </th>`;
  buckets.forEach(b => {
    html += `<th class="col-header"><div class="col-header-label">${b}</div></th>`;
  });
  html += '<th class="col-header total-col">ROW TOTAL</th></tr></thead><tbody>';

  buckets.forEach((fb, fi) => {
    const rt   = rowTotal[fb] || 0;
    const barW = rt > 0 ? Math.round(rt / maxRowTotal * 100) : 0;
    html += `<tr>
      <th class="row-header">
        <div class="row-header-label">${fb}</div>
        <div class="row-bar-wrap"><div class="row-bar" style="width:${barW}%"></div></div>
        <div class="row-header-total">${fmtDollar(rt)}</div>
      </th>`;

    buckets.forEach((tb, ti) => {
      const cell = matrix[fb]?.[tb];
      const val  = cell?.value || 0;
      const cnt  = cell?.count || 0;
      const pct  = cell?.pct ?? 0;
      const baseColor = cellColor(fi, ti, val);

      if (!baseColor || val === 0) {
        html += `<td class="matrix-cell empty"
          data-from="${fb}" data-to="${tb}"><span class="cell-empty-dash">—</span></td>`;
      } else {
        const opacity   = cellOpacity(val, maxVal);
        const bg        = hexWithOpacity(baseColor, opacity);
        const textColor = opacity > 0.45 ? '#fff' : '#cbd5e1';
        const diagClass = fi === ti ? ' cell-diagonal' : '';
        const fbe = fb.replace(/'/g, "\\'"), tbe = tb.replace(/'/g, "\\'");
        html += `<td class="matrix-cell${diagClass}"
          style="background:${bg};color:${textColor}"
          data-from="${fb}" data-to="${tb}" data-val="${val}" data-cnt="${cnt}" data-pct="${pct}"
          onmouseenter="showMatrixTip(event,this)" onmouseleave="hideMatrixTip()"
          onclick="${clickFn}('${fbe}','${tbe}')">
          <div class="cell-val">${fmtDollar(val)}</div>
          <div class="cell-cnt">${fmtNum(cnt)} enc</div>
          <div class="cell-pct">${pct}%</div>
        </td>`;
      }
    });

    html += `<td class="matrix-cell total-cell">
      <div class="cell-val">${fmtDollar(rt)}</div>
    </td></tr>`;
  });

  const twbb = data.to_week_by_bucket || null;
  html += '<tr><th class="row-header total-row-header">COL TOTAL</th>';
  buckets.forEach(tb => {
    const ct  = twbb ? (twbb[tb]?.total_balance || 0) : (colTotal[tb] || 0);
    const sub = twbb ? `<div class="ct-sub">${fmtNum(twbb[tb]?.total_count || 0)} enc</div>` : '';
    html += `<td class="matrix-cell total-cell"><div class="cell-val">${fmtDollar(ct)}</div>${sub}</td>`;
  });
  const grandDisplay = data.to_week_total_balance || grandTotal;
  html += `<td class="matrix-cell total-cell grand"><div class="cell-val">${fmtDollar(grandDisplay)}</div></td></tr>`;
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
  const tt  = document.getElementById('matrix-tooltip');
  const from = cell.dataset.from, to = cell.dataset.to;
  const val = parseFloat(cell.dataset.val);
  const cnt = parseInt(cell.dataset.cnt);
  const pct = parseFloat(cell.dataset.pct);
  const mov = from === to ? 'Stayed in bucket' : (to > from ? `${from} → ${to}` : `${from} → ${to} (improved)`);
  tt.innerHTML = `
    <div class="tt-title">${from} &rarr; ${to}</div>
    <div class="tt-row"><span>Balance</span><span>${fmtDollar(val)}</span></div>
    <div class="tt-row"><span>Encounters</span><span>${fmtNum(cnt)}</span></div>
    <div class="tt-row"><span>% of Total AR (ex. Self Pay)</span><span>${pct}%</span></div>
    <div style="margin-top:8px;font-size:10px;color:#38bdf8;text-align:center;border-top:1px solid rgba(255,255,255,0.1);padding-top:6px">
      &#128065; Click to drill into ${fmtNum(cnt)} encounters
    </div>
  `;
  tt.style.display = 'block';
  tt.style.left    = (e.clientX + 14) + 'px';
  tt.style.top     = (e.clientY - 10) + 'px';
}
function hideMatrixTip() {
  document.getElementById('matrix-tooltip').style.display = 'none';
}

function renderMigrationSummary(data, fromWeek, toWeek) {
  const s  = data.summary;
  const ne = data.new_encounters;
  const re = data.resolved_encounters;
  setMCard('ms-aged',       fmtDollar(s.aged_worse_balance),      `${fmtNum(s.aged_worse_count)} encounters moved to older buckets`);
  setMCard('ms-stayed',     fmtDollar(s.stayed_balance),           `${fmtNum(s.stayed_count)} encounters held in same bucket`);
  setMCard('ms-resolved',   fmtDollar(re.balance),                 `${fmtNum(re.count)} encounters resolved / removed`);
  setMCard('ms-new',        fmtDollar(ne.balance),                 `${fmtNum(ne.count)} new encounters entered ATB`);
  setMCard('ms-toweek-total', fmtDollar(data.to_week_total_balance), `Total ATB balance as of ${toWeek}`);
}

function setMCard(id, value, sub) {
  const el = document.getElementById(id);
  if (!el) return;
  el.querySelector('.ms-value').textContent = value;
  el.querySelector('.ms-sub').textContent   = sub;
}

function renderToWeekDistribution(data, toWeek) {
  const wrap = document.getElementById('toweek-dist-wrap');
  if (!wrap || !data.to_week_by_bucket) return;
  const byBkt   = data.to_week_by_bucket;
  const buckets = data.buckets;
  const totalAR = data.to_week_total_balance;
  const maxBal  = Math.max(...buckets.map(tb => byBkt[tb]?.total_balance || 0), 1);

  let rows = '';
  buckets.forEach(tb => {
    const row   = byBkt[tb] || {};
    const total = row.total_balance || 0;
    const roll  = row.rollover_balance || 0;
    const newB  = row.new_balance || 0;
    const rollW = total > 0 ? Math.round(roll / total * 100) : 0;
    const barW  = Math.round(total / maxBal * 100);
    rows += `<tr>
      <td class="dist-bucket">${tb}</td>
      <td class="dist-bar-cell">
        <div class="dist-bar-track">
          <div class="dist-bar-inner" style="width:${barW}%">
            <div class="dist-bar-roll" style="width:${rollW}%"></div>
            <div class="dist-bar-new" style="width:${100 - rollW}%"></div>
          </div>
        </div>
      </td>
      <td class="dist-num dist-total">${fmtDollar(total)}</td>
      <td class="dist-num">${row.pct_of_total || 0}%</td>
      <td class="dist-num dist-roll">${fmtDollar(roll)}</td>
      <td class="dist-num dist-new">${fmtDollar(newB)}</td>
      <td class="dist-num">${fmtNum(row.rollover_count || 0)}</td>
      <td class="dist-num">${fmtNum(row.new_count || 0)}</td>
    </tr>`;
  });

  wrap.innerHTML = `
    <div class="card" style="margin-top:18px">
      <div class="card-header">
        <span class="card-title">To-Week ATB — Full Balance Distribution</span>
        <span class="card-sub">As of ${fmtWeek(toWeek)} &nbsp;&middot;&nbsp; Total AR (ex. Self Pay): ${fmtDollar(totalAR)}</span>
      </div>
      <div class="dist-legend">
        <span class="dist-leg-roll">&#9646; Rollover (continued from prior week)</span>
        <span class="dist-leg-new">&#9646; New this week</span>
      </div>
      <div class="dist-table-wrap">
        <table class="dist-table">
          <thead><tr>
            <th>Bucket</th><th>Composition</th><th>Total Balance</th>
            <th>% of Total AR</th><th>Rollover $</th><th>New This Week $</th>
            <th>Rollover Enc</th><th>New Enc</th>
          </tr></thead>
          <tbody>${rows}</tbody>
        </table>
      </div>
    </div>`;
}

// ── MIGRATION DETAIL DRAWER ───────────────────────────────────

function openMigrationDrawer(fromBucket, toBucket) {
  hideMatrixTip();
  _drawerFromBucket = fromBucket;
  _drawerToBucket   = toBucket;
  _drawerAllRows    = [];
  _drawerSortCol    = 'to_balance';
  _drawerSortAsc    = false;

  const fi = _rolloverFromWeek, ti = _rolloverToWeek;

  // Badge + title
  const isSame    = fromBucket === toBucket;
  const fromIdx   = _BUCKET_ORDER.indexOf(fromBucket);
  const toIdx     = _BUCKET_ORDER.indexOf(toBucket);
  const worsened  = toIdx > fromIdx && fromIdx >= 0 && toIdx >= 0;
  const improved  = toIdx < fromIdx && fromIdx >= 0 && toIdx >= 0;
  const badgeText = isSame ? 'STAYED' : worsened ? 'AGED' : 'IMPROVED';
  const badgeCls  = isSame ? 'badge-stayed' : worsened ? 'badge-aged' : 'badge-improved';

  document.getElementById('mig-drawer-badge').textContent  = badgeText;
  document.getElementById('mig-drawer-badge').className    = `mig-drawer-badge ${badgeCls}`;
  document.getElementById('mig-drawer-title').textContent  = `${fromBucket}  →  ${toBucket}`;
  document.getElementById('mig-drawer-sub').textContent    = `${fmtWeek(fi)} → ${fmtWeek(ti)}`;
  document.getElementById('mig-drawer-body').innerHTML     =
    '<div style="padding:40px;text-align:center;color:#94a3b8"><div class="spinner" style="margin:0 auto 12px"></div>Loading encounters…</div>';
  document.getElementById('mig-drawer-search').value = '';

  // Reset stats
  ['mds-count','mds-total','mds-avg','mds-change'].forEach(id => {
    document.getElementById(id).querySelector('.mds-val').textContent = '—';
  });

  document.getElementById('mig-drawer').classList.add('open');
  document.getElementById('mig-drawer-overlay').classList.add('visible');

  const params = `from=${encodeURIComponent(fi)}&to=${encodeURIComponent(ti)}&from_bucket=${encodeURIComponent(fromBucket)}&to_bucket=${encodeURIComponent(toBucket)}`;
  fetch(apiUrl('/api/migration/detail', params))
    .then(r => r.json())
    .then(data => {
      if (data.error) { document.getElementById('mig-drawer-body').innerHTML = `<p style="color:#f87171;padding:20px">${data.error}</p>`; return; }
      _drawerAllRows = data.rows || [];
      renderDrawerStats(_drawerAllRows);
      renderDrawerTable(_drawerAllRows);
    })
    .catch(err => {
      document.getElementById('mig-drawer-body').innerHTML = `<p style="color:#f87171;padding:20px">Failed to load detail.</p>`;
    });
}

const _BUCKET_ORDER = ['Not Aged','DNFB','0-30','31-60','61-90','91-120','121-150','151-180','181-210','211-240','241-270','271-300','301-330','331-365','366+'];

function closeMigrationDrawer() {
  document.getElementById('mig-drawer').classList.remove('open');
  document.getElementById('mig-drawer-overlay').classList.remove('visible');
}

function renderDrawerStats(rows) {
  const count     = rows.length;
  const total     = rows.reduce((s, r) => s + (r.to_balance || 0), 0);
  const avg       = count ? total / count : 0;
  const netChange = rows.reduce((s, r) => s + (r.balance_change || 0), 0);
  document.getElementById('mds-count').querySelector('.mds-val').textContent  = fmtNum(count);
  document.getElementById('mds-total').querySelector('.mds-val').textContent  = fmtDollar(total);
  document.getElementById('mds-avg').querySelector('.mds-val').textContent    = fmtDollar(avg);
  const chEl = document.getElementById('mds-change').querySelector('.mds-val');
  chEl.textContent = (netChange >= 0 ? '+' : '') + fmtDollar(netChange);
  chEl.style.color = netChange >= 0 ? '#f87171' : '#4ade80';
}

function filterDrawerTable() {
  const q = (document.getElementById('mig-drawer-search')?.value || '').toLowerCase().trim();
  const filtered = q
    ? _drawerAllRows.filter(r =>
        r.enc.toLowerCase().includes(q) ||
        (r['Responsible Health Plan'] || '').toLowerCase().includes(q) ||
        (r['Responsible Financial Class'] || '').toLowerCase().includes(q) ||
        (r['First Claim Number'] || '').toLowerCase().includes(q) ||
        (r['Last Claim Number'] || '').toLowerCase().includes(q))
    : _drawerAllRows;
  renderDrawerTable(filtered);
}

function sortDrawerBy(col) {
  if (_drawerSortCol === col) { _drawerSortAsc = !_drawerSortAsc; }
  else { _drawerSortCol = col; _drawerSortAsc = col === 'enc'; }
  filterDrawerTable();
}

function renderDrawerTable(rows) {
  const col  = _drawerSortCol;
  const asc  = _drawerSortAsc;
  const sorted = [...rows].sort((a, b) => {
    const av = a[col] ?? '', bv = b[col] ?? '';
    const n = typeof av === 'number' ? (av - bv) : String(av).localeCompare(String(bv));
    return asc ? n : -n;
  });

  const hasFCN = rows.some(r => r['First Claim Number']);
  const hasLCN = rows.some(r => r['Last Claim Number']);

  const th = (label, col_key) => {
    const active = _drawerSortCol === col_key;
    const arrow  = active ? (_drawerSortAsc ? ' ▲' : ' ▼') : '';
    return `<th class="dt-th${active ? ' dt-th-active' : ''}" onclick="sortDrawerBy('${col_key}')">${label}${arrow}</th>`;
  };

  let html = `<table class="detail-table">
    <thead><tr>
      ${th('Enc #','enc')}
      ${hasFCN ? th('First Claim #','First Claim Number') : ''}
      ${hasLCN ? th('Last Claim #','Last Claim Number') : ''}
      ${th('From Bucket','from_bucket')}
      ${th('To Bucket','to_bucket')}
      ${th('Movement','movement')}
      ${th('Prior Balance','from_balance')}
      ${th('Current Balance','to_balance')}
      ${th('Balance Δ','balance_change')}
      ${th('Health Plan','Responsible Health Plan')}
      ${th('Fin Class','Responsible Financial Class')}
      ${th('Discharge Date','Discharge Date')}
    </tr></thead><tbody>`;

  sorted.forEach((r, i) => {
    const chg    = r.balance_change || 0;
    const chgCls = chg > 0 ? 'cell-worse' : chg < 0 ? 'cell-better' : '';
    const movCls = r.movement === 'Stayed' ? 'mov-stayed' : r.movement?.startsWith('Aged') ? 'mov-aged' : 'mov-improved';
    html += `<tr class="${i % 2 === 1 ? 'dt-row-alt' : ''}">
      <td class="dt-enc">${r.enc}</td>
      ${hasFCN ? `<td class="dt-claim">${r['First Claim Number'] || '—'}</td>` : ''}
      ${hasLCN ? `<td class="dt-claim">${r['Last Claim Number'] || '—'}</td>` : ''}
      <td><span class="bucket-pill">${r.from_bucket}</span></td>
      <td><span class="bucket-pill">${r.to_bucket}</span></td>
      <td><span class="mov-badge ${movCls}">${r.movement}</span></td>
      <td class="dt-num">${fmtDollar(r.from_balance)}</td>
      <td class="dt-num dt-bal">${fmtDollar(r.to_balance)}</td>
      <td class="dt-num ${chgCls}">${chg >= 0 ? '+' : ''}${fmtDollar(chg)}</td>
      <td class="dt-plan">${r['Responsible Health Plan'] || '—'}</td>
      <td class="dt-plan">${r['Responsible Financial Class'] || '—'}</td>
      <td class="dt-date">${r['Discharge Date'] || '—'}</td>
    </tr>`;
  });

  if (!sorted.length) {
    html += '<tr><td colspan="20" style="text-align:center;padding:40px;color:#94a3b8">No encounters match your search.</td></tr>';
  }
  html += '</tbody></table>';
  document.getElementById('mig-drawer-body').innerHTML = html;
}

function downloadMigrationFromDrawer() {
  downloadMigration(_drawerFromBucket, _drawerToBucket);
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
      renderTabSummary('bifur-summary', data.summary_points || []);
      renderUnbilledPanel(data.unbilled || {});
      loadVelocity(week);
    })
    .catch(() => {});
}

function renderUnbilledPanel(data) {
  if (!data.available) {
    const panel = document.getElementById('unbilled-panel');
    if (panel) panel.style.display = 'none';
    return;
  }
  const dnfbVal = document.getElementById('unbilled-dnfb-val');
  const dnfbDelta = document.getElementById('unbilled-dnfb-delta');
  const nonVal = document.getElementById('unbilled-non-val');
  const nonDelta = document.getElementById('unbilled-non-delta');

  if (dnfbVal) dnfbVal.textContent = fmtDollar(data.dnfb?.curr_balance);
  if (dnfbDelta) {
    const d = data.dnfb?.delta_balance;
    const p = data.dnfb?.delta_pct;
    const cls = d > 0 ? 'up' : d < 0 ? 'down' : 'neutral';
    dnfbDelta.innerHTML = d != null ? `${d > 0 ? '▲' : '▼'} ${fmtDollar(d)} (${fmtPct(p)}) vs prior` : 'vs prior week';
    dnfbDelta.className = 'kpi-delta ' + cls;
  }
  if (nonVal) nonVal.textContent = fmtDollar(data.non_dnfb?.curr_balance);
  if (nonDelta) {
    const d = data.non_dnfb?.delta_balance;
    const p = data.non_dnfb?.delta_pct;
    const cls = d > 0 ? 'up' : d < 0 ? 'down' : 'neutral';
    nonDelta.innerHTML = d != null ? `${d > 0 ? '▲' : '▼'} ${fmtDollar(d)} (${fmtPct(p)}) vs prior` : 'vs prior week';
    nonDelta.className = 'kpi-delta ' + cls;
  }

  const tbody = document.querySelector('#unbilled-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  (data.rows || []).forEach(r => {
    const dCls = r.delta_balance > 0 ? 'up' : r.delta_balance < 0 ? 'down' : 'neutral';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.bucket}</td>
      <td>${fmtNum(r.count)}</td>
      <td>${fmtDollar(r.balance)}</td>
      <td>${r.pct_of_total != null ? r.pct_of_total + '%' : '—'}</td>
      <td>${fmtDollar(r.prior_balance)}</td>
      <td class="${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
    `;
    tbody.appendChild(tr);
  });
}

function loadVelocity(week) {
  fetch(apiUrl('/api/aging-velocity', `week=${week}`))
    .then(r => r.json())
    .then(data => {
      if (!data || !data.available) {
        const panel = document.getElementById('velocity-panel');
        if (panel) panel.style.display = 'none';
        return;
      }
      const s = data.summary;
      const avgEl = document.getElementById('vel-avg-days');
      const pct90El = document.getElementById('vel-pct-90');
      const pct180El = document.getElementById('vel-pct-180');
      const noteEl = document.getElementById('vel-valid-note');
      if (avgEl) avgEl.textContent = s.avg_days != null ? s.avg_days + ' days' : '—';
      if (pct90El) pct90El.textContent = s.pct_over_90 != null ? s.pct_over_90 + '%' : '—';
      if (pct180El) pct180El.textContent = s.pct_over_180 != null ? s.pct_over_180 + '%' : '—';
      if (noteEl) noteEl.textContent = s.valid_count != null
        ? `${fmtNum(s.valid_count)} of ${fmtNum(s.total_count)} encounters` : 'all encounters';

      const tbody = document.querySelector('#velocity-table tbody');
      if (!tbody) return;
      tbody.innerHTML = '';
      (data.by_fin_class || []).forEach(r => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${r.name}</td>
          <td>${fmtNum(r.count)}</td>
          <td>${r.avg_days != null ? Math.round(r.avg_days) : '—'}</td>
          <td>${r.median_days != null ? Math.round(r.median_days) : '—'}</td>
          <td>${r.max_days != null ? r.max_days : '—'}</td>
          <td>${fmtDollar(r.balance)}</td>
        `;
        tbody.appendChild(tr);
      });
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
      renderTabSummary('key-points-list', data.key_points || []);
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

// ── TAB 5: OPEN DENIALS ──────────────────────────────────────
function loadDenials() {
  const week = document.getElementById('sel-denials-week')?.value;
  const topN = parseInt(document.getElementById('sel-denials-topn')?.value || 15);
  if (!week) return;

  const content   = document.getElementById('denial-content');
  const unavail   = document.getElementById('denial-unavailable');
  const summPanel = document.getElementById('denial-summary-panel');
  if (content)   content.style.display = 'none';
  if (unavail)   unavail.style.display = 'none';
  if (summPanel) summPanel.style.display = 'none';

  fetch(apiUrl('/api/denials', `week=${week}`))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      if (!data.available) {
        if (unavail) unavail.style.display = '';
        return;
      }
      if (content) content.style.display = '';
      renderTabSummary('denial-summary-list', data.summary_points || []);
      renderDenialKpis(data);
      renderDenialResolution(data.resolution || {});
      renderDenialParetoChart(data.by_code || []);
      renderDenialGroupDonut(data.by_group || []);
      renderDenialHpBars(data.by_health_plan || [], topN);
      renderDenialAgeTable(data.by_denial_age || [], data.has_date);
      renderDenialCodeTable(data.by_code || [], false, data.has_group);
      renderDenialHpTable(data.by_health_plan || [], topN);
      const lbl = document.getElementById('denials-topn-label');
      if (lbl) lbl.textContent = topN;
    })
    .catch(() => {});
}

function renderDenialKpis(data) {
  const k = data.kpis || {};
  const r = data.resolution || {};

  const setEl = (id, val) => { const el = document.getElementById(id); if (el) el.textContent = val; };
  const setHtml = (id, val) => { const el = document.getElementById(id); if (el) el.innerHTML = val; };
  const setClass = (id, cls) => { const el = document.getElementById(id); if (el) el.className = 'kpi-delta ' + cls; };

  setEl('dn-total-bal', fmtDollar(k.denied_balance));
  const bd = k.delta_balance, bp = k.delta_pct;
  const bCls = bd > 0 ? 'up' : bd < 0 ? 'down' : 'neutral';
  setHtml('dn-total-bal-delta', bd != null
    ? `${bd > 0 ? '▲' : '▼'} ${fmtDollar(bd)} (${fmtPct(bp)}) vs prior` : 'vs prior week');
  setClass('dn-total-bal-delta', bCls);

  setEl('dn-total-cnt', fmtNum(k.denied_count));
  const cd = k.delta_count;
  const cCls = cd > 0 ? 'up' : cd < 0 ? 'down' : 'neutral';
  setHtml('dn-total-cnt-delta', cd != null
    ? `${cd > 0 ? '▲ +' : '▼ '}${fmtNum(cd)} vs prior` : 'vs prior week');
  setClass('dn-total-cnt-delta', cCls);

  setEl('dn-pct-atb',  k.pct_of_atb  != null ? k.pct_of_atb + '%' : '—');
  setEl('dn-avg-bal',  fmtDollar(k.avg_balance));
  setEl('dn-new-cnt',  fmtNum(r.new_count));
  setEl('dn-new-bal',  fmtDollar(r.new_balance));
  setEl('dn-res-cnt',  fmtNum(r.resolved_count));
  setEl('dn-res-bal',  fmtDollar(r.resolved_balance));
}

function renderDenialResolution(r) {
  const setEl  = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  const setHtml = (id, v) => { const el = document.getElementById(id); if (el) el.innerHTML = v; };
  setEl('res-new-val',      fmtDollar(r.new_balance));
  setEl('res-new-sub',      `${fmtNum(r.new_count)} encounters`);
  setEl('res-resolved-val', fmtDollar(r.resolved_balance));
  setEl('res-resolved-sub', `${fmtNum(r.resolved_count)} encounters cleared`);
  setEl('res-continued-val', fmtDollar(r.continued_balance));
  setEl('res-continued-sub', `${fmtNum(r.continued_count)} still open from prior week`);
}

function renderDenialParetoChart(by_code) {
  const ctx = document.getElementById('denial-pareto-chart');
  if (!ctx) return;
  if (denialParetoChart) denialParetoChart.destroy();

  // Show top 20 codes for readability
  const rows   = by_code.slice(0, 20);
  const labels = rows.map(r => {
    const code = r.code.length > 30 ? r.code.substring(0, 30) + '…' : r.code;
    return code + (r.group && r.group !== 'nan' && r.group !== 'Unknown' ? ` [${r.group}]` : '');
  });
  const balances = rows.map(r => r.balance);
  const cumPcts  = rows.map(r => r.cumulative_pct);
  const refLine  = rows.map(() => 90);

  denialParetoChart = new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type: 'bar',
          label: 'Denied Balance',
          data: balances,
          backgroundColor: rows.map(r => r.is_top90
            ? 'rgba(239,68,68,0.65)' : 'rgba(100,116,139,0.45)'),
          borderColor: rows.map(r => r.is_top90 ? '#ef4444' : '#64748b'),
          borderWidth: 1,
          borderRadius: 3,
          yAxisID: 'y1',
          order: 2,
        },
        {
          type: 'line',
          label: 'Cumulative %',
          data: cumPcts,
          borderColor: '#0ea5e9',
          backgroundColor: 'transparent',
          pointBackgroundColor: '#0ea5e9',
          pointRadius: 4,
          tension: 0.2,
          fill: false,
          yAxisID: 'yPct',
          order: 1,
        },
        {
          type: 'line',
          label: '90% Threshold',
          data: refLine,
          borderColor: '#f59e0b',
          borderDash: [6, 4],
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
          yAxisID: 'yPct',
          order: 0,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 11 } } },
        tooltip: {
          callbacks: {
            label: c => {
              if (c.dataset.yAxisID === 'y1')   return ` ${c.dataset.label}: ${fmtDollar(c.parsed.y)}`;
              if (c.dataset.label === '90% Threshold') return null;
              return ` ${c.dataset.label}: ${c.parsed.y}%`;
            }
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8', maxRotation: 50, font: { size: 10 } }, grid: { color: 'rgba(30,58,95,0.4)' } },
        y1: {
          type: 'linear', position: 'left',
          ticks: { color: '#ef4444', callback: v => fmtDollar(v) },
          grid: { color: 'rgba(30,58,95,0.4)' }
        },
        yPct: {
          type: 'linear', position: 'right',
          min: 0, max: 105,
          ticks: { color: '#0ea5e9', callback: v => v + '%' },
          grid: { drawOnChartArea: false }
        }
      }
    }
  });
}

function renderDenialGroupDonut(by_group) {
  const wrap = document.getElementById('denial-group-chart-wrap');
  const ctx  = document.getElementById('denial-group-chart');
  if (!ctx) return;
  if (denialGroupChart) denialGroupChart.destroy();

  if (!by_group.length) {
    if (wrap) wrap.innerHTML = '<p style="color:#64748b;padding:40px;text-align:center;font-size:12px">No denial group data available.<br>Requires "Last Denial Group" column.</p>';
    return;
  }

  denialGroupChart = new Chart(ctx, {
    type: 'doughnut',
    data: {
      labels: by_group.map(r => r.name),
      datasets: [{
        data: by_group.map(r => r.balance),
        backgroundColor: BUCKET_PALETTE.slice(0, by_group.length),
        borderColor: '#0d1b2a',
        borderWidth: 2,
        hoverOffset: 6,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: '55%',
      plugins: {
        legend: {
          position: 'bottom',
          labels: { color: '#94a3b8', font: { size: 10 }, padding: 8, boxWidth: 10 }
        },
        tooltip: {
          callbacks: {
            label: c => {
              const total = by_group.reduce((s, r) => s + r.balance, 0);
              const pct   = total ? (c.parsed / total * 100).toFixed(1) : 0;
              return ` ${fmtDollar(c.parsed)} (${pct}%)`;
            }
          }
        }
      }
    }
  });
}

function renderDenialHpBars(rows, topN) {
  const wrap = document.getElementById('denial-hp-wrap');
  if (!wrap) return;
  const visible = rows.slice(0, topN);
  if (!visible.length) { wrap.innerHTML = '<p style="color:#64748b;padding:20px">No health plan denial data.</p>'; return; }

  const maxBal   = Math.max(...visible.map(r => r.balance), 1);
  const maxDelta = Math.max(...visible.map(r => Math.abs(r.delta_balance)), 1);

  let html = `<table class="rhp-bar-table">
    <thead style="font-size:10px;color:#64748b;text-transform:uppercase">
      <tr>
        <td style="width:180px">Health Plan</td>
        <td>Denied Balance</td>
        <td>% of Denials</td>
        <td style="width:200px">Balance Bar</td>
        <td>WoW &Delta;</td>
        <td>WoW %</td>
        <td>Top Denial Group</td>
      </tr>
    </thead><tbody>`;

  visible.forEach(r => {
    const barW  = Math.max(4, Math.round(r.balance / maxBal * 150));
    const dCls  = r.delta_balance > 0 ? 'up' : 'down';
    const dBarW = Math.max(4, Math.round(Math.abs(r.delta_balance) / maxDelta * 60));
    const topGrp = r.by_group && r.by_group.length ? r.by_group[0].group : '—';
    html += `<tr>
      <td class="rhp-name" title="${r.name}">${r.name}</td>
      <td class="rhp-curr">${fmtDollar(r.balance)}</td>
      <td style="color:#94a3b8;text-align:right">${r.pct_of_denied != null ? r.pct_of_denied + '%' : '—'}</td>
      <td><div style="display:flex;align-items:center;gap:4px">
        <div class="rhp-bar ${dCls}" style="width:${barW}px"></div>
        <div class="rhp-bar ${dCls}" style="width:${dBarW}px;opacity:0.35"></div>
      </div></td>
      <td class="rhp-delta ${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="rhp-delta ${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
      <td style="color:#94a3b8;font-size:11px">${topGrp}</td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

function renderDenialAgeTable(rows, hasDate) {
  const panel = document.getElementById('denial-age-panel');
  const tbody = document.querySelector('#denial-age-table tbody');
  if (!tbody) return;
  if (!hasDate || !rows.length) {
    if (panel) panel.style.display = 'none';
    return;
  }
  if (panel) panel.style.display = '';

  tbody.innerHTML = '';
  rows.forEach(r => {
    const isCritical = ['90-119 days', '120-179 days', '180+ days'].includes(r.bucket);
    const isWarning  = r.bucket === '60-89 days';
    const cls        = isCritical ? 'denial-age-critical' : isWarning ? 'denial-age-warning' : '';
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td class="${cls}">${r.bucket}</td>
      <td>${fmtNum(r.count)}</td>
      <td class="${cls}">${fmtDollar(r.balance)}</td>
      <td>${r.pct_of_denied != null ? r.pct_of_denied + '%' : '—'}</td>
      <td class="${cls}">${r.avg_age_days != null ? r.avg_age_days + ' d' : '—'}</td>
    `;
    tbody.appendChild(tr);
  });
}

function renderDenialCodeTable(rows, hasReason, hasGroup) {
  const tbody = document.querySelector('#denial-code-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  rows.forEach(r => {
    const dCls = r.delta_balance > 0 ? 'up' : r.delta_balance < 0 ? 'down' : 'neutral';
    const tr   = document.createElement('tr');
    if (r.is_top90) tr.className = 'top90-row';
    const codeEsc = r.code.replace(/'/g, "\\'");
    tr.innerHTML = `
      <td>
        ${r.code}
        ${r.is_top90 ? '<span class="pareto-90-label">TOP 90%</span>' : ''}
      </td>
      <td style="font-size:11px;color:#64748b">${hasGroup && r.group && r.group !== 'nan' && r.group !== 'Unknown' ? r.group : '—'}</td>
      <td>${fmtNum(r.count)}</td>
      <td>${fmtDollar(r.balance)}</td>
      <td>${r.pct_of_denied != null ? r.pct_of_denied + '%' : '—'}</td>
      <td style="font-weight:600;color:${r.cumulative_pct <= 90 ? '#ef4444' : '#64748b'}">${r.cumulative_pct != null ? r.cumulative_pct + '%' : '—'}</td>
      <td>${fmtDollar(r.prior_balance)}</td>
      <td class="${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td style="text-align:center"><button class="btn-download-sm" onclick="downloadDenials('${codeEsc}')" title="Download encounters for this denial code">&#8659;</button></td>
    `;
    tbody.appendChild(tr);
  });
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:#64748b;padding:20px">No denial code data found.</td></tr>';
  }
}

function renderDenialHpTable(rows, topN) {
  const tbody = document.querySelector('#denial-hp-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  const visible = rows.slice(0, topN);
  visible.forEach(r => {
    const dCls   = r.delta_balance > 0 ? 'up' : r.delta_balance < 0 ? 'down' : 'neutral';
    const isTop  = r.cumulative_pct <= 90;
    const topGrp = r.by_group && r.by_group.length
      ? `${r.by_group[0].group} (${fmtDollar(r.by_group[0].balance)})` : '—';
    const tr = document.createElement('tr');
    if (r.delta_balance > 0) tr.className = 'row-alert';
    tr.innerHTML = `
      <td>${r.name}</td>
      <td>${fmtNum(r.count)}</td>
      <td>${fmtDollar(r.balance)}</td>
      <td>${r.pct_of_denied != null ? r.pct_of_denied + '%' : '—'}</td>
      <td style="font-weight:600;color:${isTop ? '#ef4444' : '#64748b'}">${r.cumulative_pct != null ? r.cumulative_pct + '%' : '—'}</td>
      <td>${fmtDollar(r.prior_balance)}</td>
      <td class="${dCls}">${fmtDollar(r.delta_balance)}</td>
      <td class="${dCls}">${r.delta_pct != null ? fmtPct(r.delta_pct) : '—'}</td>
      <td style="font-size:11px;color:#94a3b8">${topGrp}</td>
    `;
    tbody.appendChild(tr);
  });
  if (!visible.length) {
    tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:#64748b;padding:20px">No health plan denial data.</td></tr>';
  }
}

// ── TAB 6: DENIAL VELOCITY ───────────────────────────────────
function loadDenialVelocity() {
  const content   = document.getElementById('dv-content');
  const unavail   = document.getElementById('dv-unavailable');
  const summPanel = document.getElementById('dv-summary-panel');
  if (content)   content.style.display = 'none';
  if (unavail)   unavail.style.display = 'none';
  if (summPanel) summPanel.style.display = 'none';

  fetch(apiUrl('/api/denial-velocity'))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      if (!data.available) {
        if (unavail) unavail.style.display = '';
        return;
      }
      if (content) content.style.display = '';
      renderTabSummary('dv-summary-list', data.summary_points || []);
      renderDvKpis(data.kpis || {});
      renderDvTrendChart(data.trend || []);
      renderDvHeatTable(data);
      renderDvAgedTable(data.aged_denials || [], data.has_date);
    })
    .catch(() => {});
}

function renderDvKpis(kpis) {
  const setEl  = (id, v) => { const el = document.getElementById(id); if (el) el.textContent = v; };
  const setHtml = (id, v) => { const el = document.getElementById(id); if (el) el.innerHTML = v; };
  setEl('dv-open-bal',      fmtDollar(kpis.open_denied_balance));
  setEl('dv-open-cnt-val',  fmtNum(kpis.open_denied_count));
  setEl('dv-avg-age',       kpis.avg_age_days != null ? kpis.avg_age_days + ' days' : '—');
  setEl('dv-over90-bal',    fmtDollar(kpis.over_90_balance));
  setHtml('dv-over90-pct',  kpis.pct_over_90_days != null ? kpis.pct_over_90_days + '% of denied' : '—');
  setEl('dv-over180-bal',   fmtDollar(kpis.over_180_balance));
  setHtml('dv-over180-pct', kpis.pct_over_180_days != null ? kpis.pct_over_180_days + '% write-off risk' : '—');
}

function renderDvTrendChart(trend) {
  const ctx = document.getElementById('dv-trend-chart');
  if (!ctx) return;
  if (dvTrendChart) dvTrendChart.destroy();
  const labels = trend.map(d => fmtWeek(d.week));
  dvTrendChart = new Chart(ctx, {
    data: {
      labels,
      datasets: [
        {
          type: 'bar',
          label: 'Denied Balance ($)',
          data: trend.map(d => d.balance),
          backgroundColor: 'rgba(239,68,68,0.3)',
          borderColor: '#ef4444',
          borderWidth: 1,
          borderRadius: 4,
          yAxisID: 'y1',
          order: 2,
        },
        {
          type: 'line',
          label: 'Denied Encounters',
          data: trend.map(d => d.count),
          borderColor: '#f59e0b',
          backgroundColor: 'rgba(245,158,11,0.1)',
          pointBackgroundColor: '#f59e0b',
          pointRadius: 5,
          tension: 0.3,
          fill: true,
          yAxisID: 'y2',
          order: 1,
        },
        {
          type: 'line',
          label: '% of ATB',
          data: trend.map(d => d.pct_of_atb || 0),
          borderColor: '#0ea5e9',
          backgroundColor: 'transparent',
          pointBackgroundColor: '#0ea5e9',
          pointRadius: 4,
          borderDash: [4, 3],
          tension: 0.3,
          fill: false,
          yAxisID: 'yPct',
          order: 0,
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
            label: c => {
              if (c.dataset.yAxisID === 'y1')   return ` ${c.dataset.label}: ${fmtDollar(c.parsed.y)}`;
              if (c.dataset.yAxisID === 'yPct') return ` ${c.dataset.label}: ${c.parsed.y}%`;
              return ` ${c.dataset.label}: ${fmtNum(c.parsed.y)}`;
            }
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(30,58,95,0.5)' } },
        y1: {
          type: 'linear', position: 'left',
          ticks: { color: '#ef4444', callback: v => fmtDollar(v) },
          grid: { color: 'rgba(30,58,95,0.5)' }
        },
        y2: {
          type: 'linear', position: 'right',
          ticks: { color: '#f59e0b', callback: v => fmtNum(v) },
          grid: { drawOnChartArea: false }
        },
        yPct: {
          type: 'linear', position: 'right',
          min: 0, max: 100,
          ticks: { color: '#0ea5e9', callback: v => v + '%' },
          grid: { drawOnChartArea: false },
          display: false,
        }
      }
    }
  });
}

const DV_AGE_BUCKETS = ['0-29 days','30-59 days','60-89 days','90-119 days','120-179 days','180+ days'];

function renderDvHeatTable(data) {
  const wrap = document.getElementById('dv-heat-wrap');
  if (!wrap) return;
  const by_code  = data.by_code || [];
  const has_date = data.has_date;

  if (!by_code.length) {
    wrap.innerHTML = '<p style="color:#64748b;padding:20px">No denial code data available for latest ATB.</p>';
    return;
  }

  // Compute heat thresholds from all non-zero age-bucket values
  const allVals = [];
  by_code.forEach(r => DV_AGE_BUCKETS.forEach(b => {
    const v = (r.age_dist && r.age_dist[b]) || 0;
    if (v > 0) allVals.push(v);
  }));
  allVals.sort((a, b) => a - b);
  const p25 = allVals[Math.floor(allVals.length * 0.25)] || 1;
  const p75 = allVals[Math.floor(allVals.length * 0.75)] || 1;

  function heatCls(v) {
    if (!v || v === 0) return 'heat-cell-0';
    if (v < p25) return 'heat-cell-low';
    if (v < p75) return 'heat-cell-mid';
    return 'heat-cell-high';
  }

  function ageBarHtml(avgDays) {
    if (avgDays == null) return '<span style="color:#64748b">—</span>';
    const cls = avgDays <= 30 ? 'res-speed-fast' : avgDays <= 90 ? 'res-speed-mid' : 'res-speed-slow';
    const w   = Math.min(60, Math.max(6, Math.round(avgDays / 200 * 60)));
    return `<div style="display:flex;align-items:center;gap:6px">
      <span style="min-width:36px;font-size:11px">${avgDays}d</span>
      <span class="res-speed-bar ${cls}" style="width:${w}px"></span>
    </div>`;
  }

  let html = '<table class="data-table dv-heat-table"><thead><tr>';
  html += '<th style="min-width:190px">Denial Code &amp; Reason</th>';
  html += '<th>Group</th>';
  if (has_date) {
    DV_AGE_BUCKETS.forEach(b => { html += `<th style="text-align:center;white-space:nowrap">${b}</th>`; });
  }
  html += '<th style="text-align:center;white-space:nowrap">Avg Age<br>(Days)</th>';
  html += '<th style="text-align:center;white-space:nowrap">Max Age<br>(Days)</th>';
  html += '<th style="text-align:center;white-space:nowrap">% Over<br>90d</th>';
  html += '<th style="text-align:right;white-space:nowrap">Enc</th>';
  html += '<th style="text-align:right;white-space:nowrap">Balance</th>';
  html += '</tr></thead><tbody>';

  by_code.forEach(r => {
    const codeLabel = r.code.length > 55 ? r.code.substring(0, 55) + '…' : r.code;
    const isCritical = (r.avg_age_days || 0) >= 90;
    html += `<tr${isCritical ? ' style="border-left:3px solid #ef4444"' : ''}>`;
    html += `<td title="${r.code}" style="font-size:11px">${codeLabel}</td>`;
    html += `<td style="font-size:11px;color:#64748b">${r.group && r.group !== 'nan' ? r.group : '—'}</td>`;
    if (has_date) {
      DV_AGE_BUCKETS.forEach(b => {
        const v = (r.age_dist && r.age_dist[b]) || 0;
        html += `<td class="${heatCls(v)}" style="text-align:center;font-size:11px;padding:5px 8px">${v > 0 ? fmtDollar(v) : '—'}</td>`;
      });
    }
    html += `<td style="text-align:center">${ageBarHtml(r.avg_age_days)}</td>`;
    html += `<td style="text-align:center;font-size:11px;color:${(r.max_age_days||0)>=180?'#ef4444':'#94a3b8'}">${r.max_age_days != null ? r.max_age_days + 'd' : '—'}</td>`;
    html += `<td style="text-align:center;font-size:11px;color:${(r.pct_over_90||0)>=50?'#ef4444':'#94a3b8'}">${r.pct_over_90 != null ? r.pct_over_90 + '%' : '—'}</td>`;
    html += `<td style="text-align:right">${fmtNum(r.count)}</td>`;
    html += `<td style="text-align:right;font-weight:600">${fmtDollar(r.balance)}</td>`;
    html += '</tr>';
  });
  // Totals row
  const totEnc = by_code.reduce((s, r) => s + (r.count || 0), 0);
  const totBal = by_code.reduce((s, r) => s + (r.balance || 0), 0);
  const totBuckets = {};
  DV_AGE_BUCKETS.forEach(b => {
    totBuckets[b] = by_code.reduce((s, r) => s + ((r.age_dist && r.age_dist[b]) || 0), 0);
  });
  const maxAge = by_code.reduce((m, r) => Math.max(m, r.max_age_days || 0), 0);
  const wtdAge = totBal > 0
    ? Math.round(by_code.reduce((s, r) => s + (r.avg_age_days || 0) * (r.balance || 0), 0) / totBal)
    : null;
  const over90Sum = DV_AGE_BUCKETS.filter(b => b !== '0-29 days' && b !== '30-59 days' && b !== '60-89 days')
    .reduce((s, b) => s + (totBuckets[b] || 0), 0);
  const pct90Tot = totBal > 0 ? Math.round(over90Sum / totBal * 100) : null;

  html += '<tr style="border-top:2px solid #334155;background:#1e293b;font-weight:700">';
  html += `<td colspan="2" style="font-size:12px;color:#f1f5f9">Total (90% of denials)</td>`;
  if (has_date) {
    DV_AGE_BUCKETS.forEach(b => {
      const v = totBuckets[b];
      html += `<td class="${heatCls(v)}" style="text-align:center;font-size:11px;padding:5px 8px;font-weight:700">${v > 0 ? fmtDollar(v) : '—'}</td>`;
    });
  }
  html += `<td style="text-align:center">${ageBarHtml(wtdAge)}</td>`;
  html += `<td style="text-align:center;font-size:11px;color:${maxAge>=180?'#ef4444':'#94a3b8'}">${maxAge > 0 ? maxAge + 'd' : '—'}</td>`;
  html += `<td style="text-align:center;font-size:11px;color:${(pct90Tot||0)>=50?'#ef4444':'#94a3b8'}">${pct90Tot != null ? pct90Tot + '%' : '—'}</td>`;
  html += `<td style="text-align:right">${fmtNum(totEnc)}</td>`;
  html += `<td style="text-align:right;font-weight:700">${fmtDollar(totBal)}</td>`;
  html += '</tr>';

  html += '</tbody></table>';
  wrap.innerHTML = html;
}

function renderDvAgedTable(rows, hasDate) {
  const tbody = document.querySelector('#dv-aged-table tbody');
  if (!tbody) return;
  if (!hasDate) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#64748b;padding:20px">Last Denial Date column not available in this ATB file.</td></tr>';
    return;
  }
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#22c55e;padding:30px">&#10003; No denials over 90 days old in the latest ATB.</td></tr>';
    return;
  }
  tbody.innerHTML = '';
  rows.forEach(r => {
    const tr = document.createElement('tr');
    if (r.age_days >= 180) tr.style.borderLeft = '3px solid #ef4444';
    else if (r.age_days >= 90) tr.style.borderLeft = '3px solid #f59e0b';
    const codeShort = r.code.length > 55 ? r.code.substring(0, 55) + '…' : r.code;
    const planShort = r.plan && r.plan.length > 28 ? r.plan.substring(0, 28) + '…' : (r.plan || '—');
    tr.innerHTML = `
      <td style="font-weight:600">${r.encounter}</td>
      <td style="font-size:11px" title="${r.code}">${codeShort}</td>
      <td style="font-size:11px;color:#64748b">${r.group && r.group !== 'nan' ? r.group : '—'}</td>
      <td style="font-size:11px;color:#94a3b8" title="${r.plan}">${planShort}</td>
      <td style="font-size:11px;color:#64748b">${r.denial_date}</td>
      <td style="text-align:center;font-weight:700;color:${r.age_days >= 180 ? '#ef4444' : '#f59e0b'}">${r.age_days}</td>
      <td style="text-align:right;font-weight:600">${fmtDollar(r.balance)}</td>
    `;
    tbody.appendChild(tr);
  });
}

// ══════════════════════════════════════════════════════════════
// CASH COLLECTION ACTION PLAN TAB
// ══════════════════════════════════════════════════════════════

function loadCashActionPlan() {
  const panel = document.getElementById('cap-insights-panel');
  if (panel) panel.style.display = 'none';
  fetch(apiUrl('/api/cash-action-plan'))
    .then(r => r.json())
    .then(data => {
      if (!data || data.error) return;
      renderCapUrgencyBanner(data.urgency_alert || {});
      renderCapKpis(data.kpis || {});
      renderCapPriorityTable(data.priority_table || []);
      renderCapPayerMatrix(data.payer_matrix || []);
      renderCapWaterfall(data.waterfall || []);
      renderCapForecast(data.forecast || {});
      renderCapFinClassRankings(data.fin_class_rankings || []);
      renderTabSummary('cap-insights-list', data.action_insights || []);
      if (panel && (data.action_insights || []).length) panel.style.display = '';
    })
    .catch(() => {});
}

function renderCapUrgencyBanner(alert) {
  const banner = document.getElementById('cap-urgency-banner');
  const text   = document.getElementById('cap-urgency-text');
  if (!banner || !text) return;
  if (!alert.show) { banner.style.display = 'none'; return; }
  banner.style.display = '';
  text.innerHTML = `<strong>${fmtDollar(alert.at_risk_balance)} in ${fmtNum(alert.at_risk_count)} encounters must be worked THIS WEEK or lost forever</strong>
    &nbsp;— timely filing deadline within ${alert.threshold_days} days`;
}

function renderCapKpis(kpis) {
  const setCard = (id, val, note) => {
    const el = document.getElementById(id);
    if (!el) return;
    const v = el.querySelector('.kpi-value');
    const d = el.querySelector('.kpi-delta');
    if (v) v.textContent = val;
    if (d && note !== undefined) d.textContent = note;
  };
  setCard('cap-kpi-opportunity', fmtDollar(kpis.recoverable_opportunity),
    `${fmtNum(kpis.recoverable_count)} encounters (90+ or denied)`);
  setCard('cap-kpi-quickwin', fmtDollar(kpis.quick_win_balance),
    `${fmtNum(kpis.quick_win_count)} newly denied 91-120d enc.`);
  setCard('cap-kpi-tf-risk', fmtDollar(kpis.tf_at_risk_balance));
  const tfNote = document.getElementById('cap-tf-note');
  if (tfNote) tfNote.textContent = kpis.tf_column_available
    ? `${fmtNum(kpis.tf_at_risk_count)} enc. — <30 days to filing limit`
    : 'Estimated from 91-150d bucket';
  setCard('cap-kpi-forecast', fmtDollar(kpis.projected_4wk_recovery));
  const fRate = document.getElementById('cap-forecast-rate');
  if (fRate) fRate.textContent = kpis.avg_weekly_resolution_rate != null
    ? `${kpis.avg_weekly_resolution_rate.toFixed(1)}% avg weekly resolution`
    : 'at current pace';
}

function renderCapPriorityTable(rows) {
  const tbody = document.querySelector('#cap-priority-table tbody');
  if (!tbody) return;
  tbody.innerHTML = '';
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="11" style="text-align:center;color:#64748b;padding:20px">No data available.</td></tr>';
    return;
  }
  rows.forEach(r => {
    const sc = r.priority_score;
    const scoreClass = sc >= 70 ? 'score-badge-critical' : sc >= 45 ? 'score-badge-high' : 'score-badge-medium';
    const scoreColor = sc >= 70 ? '#ef4444' : sc >= 45 ? '#f59e0b' : '#0ea5e9';
    const tfRisk = r.tf_risk_score != null ? (r.tf_risk_score * 100).toFixed(0) + '%' : '—';
    const da     = r.avg_denial_age != null ? r.avg_denial_age + 'd' : '—';
    const daColor = (r.avg_denial_age || 0) >= 90 ? '#ef4444' : '#94a3b8';
    const tfColor = (r.tf_risk_score || 0) >= 0.7 ? '#f59e0b' : '#64748b';
    const payerEsc = r.payer.replace(/'/g, "\\'");
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td style="text-align:center;font-weight:700;color:${scoreColor};white-space:nowrap">${r.rank}</td>
      <td style="text-align:left;font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:150px" title="${r.payer}">${r.payer}</td>
      <td style="text-align:left;color:#94a3b8;font-size:var(--fs-sm);white-space:nowrap">${r.fin_class}</td>
      <td style="text-align:right;white-space:nowrap">${fmtNum(r.encounter_count)}</td>
      <td style="text-align:right;font-weight:600;white-space:nowrap">${fmtDollar(r.balance)}</td>
      <td style="text-align:right;white-space:nowrap">${r.avg_ar_days != null ? r.avg_ar_days + 'd' : '—'}</td>
      <td style="text-align:right;color:${daColor};white-space:nowrap">${da}</td>
      <td style="text-align:right;color:${tfColor};white-space:nowrap">${tfRisk}</td>
      <td style="text-align:center;white-space:nowrap"><span class="priority-score-badge ${scoreClass}">${sc.toFixed(1)}</span></td>
      <td style="text-align:left;color:#94a3b8;font-size:var(--fs-sm);overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:220px" title="${r.recommended_action}">${r.recommended_action}</td>
      <td style="text-align:center;white-space:nowrap"><button class="btn-download-sm" onclick="downloadCashActionPlan('${payerEsc}')" title="Download encounters for ${r.payer}">&#8659;</button></td>
    `;
    tbody.appendChild(tr);
  });
}

function renderCapPayerMatrix(rows) {
  const el = document.getElementById('cap-payer-matrix-chart');
  if (!el) return;
  if (capPayerMatrixChart) { capPayerMatrixChart.destroy(); capPayerMatrixChart = null; }

  const wrap = el.parentElement;
  wrap.style.height   = 'auto';
  wrap.style.overflow = 'auto';
  wrap.style.maxHeight = '340px';

  if (!rows.length) {
    wrap.innerHTML = '<p style="color:#64748b;padding:40px;text-align:center">No payer data.</p>';
    return;
  }

  const xSorted = [...rows.map(r => r.x)].sort((a,b)=>a-b);
  const ySorted = [...rows.map(r => r.y)].sort((a,b)=>a-b);
  const midX = xSorted[Math.floor(xSorted.length/2)] || 30;
  const midY = ySorted[Math.floor(ySorted.length/2)] || 0;

  const getQ = (x, y) => {
    if (x <  midX && y >= midY) return 'strategic_focus';  // fresh + high balance = work now
    if (x >= midX && y >= midY) return 'urgent_recovery';  // aging + high balance = escalate
    if (x <  midX)              return 'monitor';
    return 'deprioritize';
  };
  const QC = {
    strategic_focus: { bg: 'rgba(34,197,94,0.14)',   border: '#22c55e', label: 'STRATEGIC FOCUS' },
    urgent_recovery: { bg: 'rgba(245,158,11,0.14)',  border: '#f59e0b', label: 'URGENT RECOVERY'  },
    monitor:         { bg: 'rgba(100,116,139,0.10)', border: '#64748b', label: 'MONITOR'           },
    deprioritize:    { bg: 'rgba(239,68,68,0.10)',   border: '#ef4444', label: 'DEPRIORITIZE'      },
  };
  const QORDER = { strategic_focus: 0, urgent_recovery: 1, monitor: 2, deprioritize: 3 };

  const maxBal  = Math.max(...rows.map(r => r.y), 1);
  const maxAge  = Math.max(...rows.map(r => r.x), 1);
  const maxRate = Math.max(...rows.map(r => r.denial_rate || 0), 0.01);
  const maxEnc  = Math.max(...rows.map(r => r.r), 1);
  const heat = (t, r, g, b) => `rgba(${r},${g},${b},${Math.min(0.55, t * 0.7).toFixed(2)})`;

  const sorted = [...rows].sort((a, b) => {
    const qa = QORDER[getQ(a.x, a.y)] ?? 9;
    const qb = QORDER[getQ(b.x, b.y)] ?? 9;
    return qa !== qb ? qa - qb : b.y - a.y;
  });

  const tbody = sorted.map(r => {
    const qc   = QC[getQ(r.x, r.y)];
    const tBal  = r.y / maxBal;
    const tAge  = r.x / maxAge;
    const tRate = (r.denial_rate || 0) / maxRate;
    const tEnc  = r.r / maxEnc;
    return `<tr style="border-bottom:1px solid rgba(30,58,95,0.5)">
      <td style="padding:5px 10px;text-align:left;font-weight:500;font-size:12px;white-space:nowrap;max-width:150px;overflow:hidden;text-overflow:ellipsis" title="${r.name}">${r.name}</td>
      <td style="padding:5px 8px;white-space:nowrap"><span style="background:${qc.bg};color:${qc.border};border:1px solid ${qc.border};border-radius:4px;padding:2px 6px;font-size:10px;font-weight:600">${qc.label}</span></td>
      <td style="padding:5px 10px;text-align:right;font-size:12px;font-weight:600;background:${heat(tBal,14,165,233)};color:#e2e8f0">${fmtDollar(r.y)}</td>
      <td style="padding:5px 10px;text-align:right;font-size:12px;background:${heat(tAge,245,158,11)};color:#e2e8f0">${r.x}d</td>
      <td style="padding:5px 10px;text-align:right;font-size:12px;background:${heat(tRate,239,68,68)};color:#e2e8f0">${r.denial_rate != null ? (r.denial_rate*100).toFixed(0)+'%' : '—'}</td>
      <td style="padding:5px 10px;text-align:right;font-size:12px;background:${heat(tEnc,139,92,246)};color:#e2e8f0">${fmtNum(r.r)}</td>
    </tr>`;
  }).join('');

  wrap.innerHTML = `
    <div style="display:flex;gap:16px;flex-wrap:wrap;padding:0 2px 8px;font-size:10px;color:#94a3b8">
      <span><span style="color:#22c55e;font-weight:700">●</span> Strategic Focus — fresh denials, high balance (act now)</span>
      <span><span style="color:#f59e0b;font-weight:700">●</span> Urgent Recovery — aging denials, high balance (escalate)</span>
      <span><span style="color:#64748b;font-weight:700">●</span> Monitor</span>
      <span><span style="color:#ef4444;font-weight:700">●</span> Deprioritize</span>
    </div>
    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="border-bottom:1px solid #1e3a5f;position:sticky;top:0;background:#1a2d42;z-index:1">
          <th style="padding:6px 10px;text-align:left;font-size:11px;color:#64748b;font-weight:600">Payer</th>
          <th style="padding:6px 8px;text-align:left;font-size:11px;color:#64748b;font-weight:600">Priority</th>
          <th style="padding:6px 10px;text-align:right;font-size:11px;color:#0ea5e9;font-weight:600">Balance</th>
          <th style="padding:6px 10px;text-align:right;font-size:11px;color:#f59e0b;font-weight:600">Denial Age</th>
          <th style="padding:6px 10px;text-align:right;font-size:11px;color:#ef4444;font-weight:600">Denial Rate</th>
          <th style="padding:6px 10px;text-align:right;font-size:11px;color:#8b5cf6;font-weight:600">Enc.</th>
        </tr>
      </thead>
      <tbody>${tbody}</tbody>
    </table>`;
}

function renderCapWaterfall(steps) {
  const ctx = document.getElementById('cap-waterfall-chart');
  if (!ctx) return;
  if (capWaterfallChart) { capWaterfallChart.destroy(); capWaterfallChart = null; }
  if (!steps.length) return;
  const typeColor = {
    total:    'rgba(14,165,233,0.75)',
    deduct:   'rgba(34,197,94,0.65)',
    warning:  'rgba(245,158,11,0.75)',
    critical: 'rgba(239,68,68,0.6)',
    danger:   'rgba(239,68,68,0.88)',
  };
  capWaterfallChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: steps.map(s => s.label),
      datasets: [{
        data: steps.map(s => Math.abs(s.value)),
        backgroundColor: steps.map(s => typeColor[s.type] || 'rgba(100,116,139,0.5)'),
        borderWidth: 1,
        borderRadius: 3,
      }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: c => {
              const s = steps[c.dataIndex];
              const sign = s.type === 'deduct' ? '−' : s.type === 'total' ? '' : '+';
              return ` ${sign}${fmtDollar(Math.abs(s.value))} [${s.type.toUpperCase()}]`;
            }
          }
        }
      },
      scales: {
        x: { ticks: { color: '#94a3b8', callback: v => fmtDollar(v) }, grid: { color: 'rgba(30,58,95,0.4)' } },
        y: { ticks: { color: '#e2e8f0', font: { size: 11 } }, grid: { color: 'rgba(30,58,95,0.25)' } }
      }
    }
  });
}

function renderCapForecast(forecast) {
  const ctx = document.getElementById('cap-forecast-chart');
  if (!ctx) return;
  if (capForecastChart) { capForecastChart.destroy(); capForecastChart = null; }
  const bw = forecast.baseline_weeks    || [];
  const aw = forecast.accelerated_weeks || [];
  if (!bw.length) return;
  capForecastChart = new Chart(ctx, {
    type: 'line',
    data: {
      labels: bw.map(w => 'Wk ' + w.week),
      datasets: [
        {
          label: 'Current Pace',
          data: bw.map(w => w.cumulative_recovered),
          borderColor: '#0ea5e9', backgroundColor: 'rgba(14,165,233,0.08)',
          pointBackgroundColor: '#0ea5e9', pointRadius: 4, tension: 0.3, fill: true,
        },
        {
          label: 'Accelerated (2× Priority)',
          data: aw.map(w => w.cumulative_recovered),
          borderColor: '#22c55e', backgroundColor: 'rgba(34,197,94,0.05)',
          pointBackgroundColor: '#22c55e', pointRadius: 4, borderDash: [6,3], tension: 0.3, fill: false,
        }
      ]
    },
    options: {
      responsive: true, maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 12 } } },
        tooltip: { callbacks: { label: c => ` ${c.dataset.label}: ${fmtDollar(c.parsed.y)}` } }
      },
      scales: {
        x: { ticks: { color: '#94a3b8' }, grid: { color: 'rgba(30,58,95,0.4)' } },
        y: { ticks: { color: '#94a3b8', callback: v => fmtDollar(v) }, grid: { color: 'rgba(30,58,95,0.4)' } }
      }
    }
  });
  const el = document.getElementById('cap-forecast-summary');
  if (el && bw.length === 8) {
    const b8 = bw[7].cumulative_recovered;
    const a8 = aw[7] ? aw[7].cumulative_recovered : b8;
    const delta = a8 - b8;
    const rateNote = forecast.avg_weekly_rate != null
      ? `(avg ${(forecast.avg_weekly_rate*100).toFixed(1)}% weekly resolution, ${forecast.weeks_of_data_used || 0} weeks of data)`
      : '';
    el.innerHTML = `At current pace: <strong style="color:#0ea5e9">${fmtDollar(b8)}</strong> recovered by week 8 ${rateNote}.`
      + (delta > 0 ? ` &nbsp;|&nbsp; If prioritized: <strong style="color:#22c55e">${fmtDollar(a8)}</strong>`
        + ` — <span style="color:#22c55e">+${fmtDollar(delta)} additional cash</span> with focused effort.` : '');
  }
}

function renderCapFinClassRankings(rows) {
  const wrap = document.getElementById('cap-finclass-chart-wrap');
  if (!wrap) return;
  if (!rows.length) { wrap.innerHTML = '<p style="color:#64748b;padding:20px">No data available.</p>'; return; }
  const maxBal = Math.max(...rows.map(r => r.total_balance), 1);
  const prColor = { CRITICAL: '#ef4444', HIGH: '#f59e0b', MEDIUM: '#0ea5e9', MONITOR: '#64748b' };
  const prBg    = { CRITICAL: 'rgba(239,68,68,0.10)', HIGH: 'rgba(245,158,11,0.08)',
                    MEDIUM: 'rgba(14,165,233,0.07)', MONITOR: 'transparent' };
  let html = `<table class="data-table">
    <thead><tr>
      <th>Financial Class</th>
      <th style="text-align:right">Total Balance</th>
      <th style="width:180px">Balance Bar</th>
      <th style="text-align:right">90%+ of AR</th>
      <th style="text-align:right">Denial Rate</th>
      <th style="text-align:right">Avg Days</th>
      <th style="text-align:center">Action Priority</th>
    </tr></thead><tbody>`;
  rows.forEach(r => {
    const barW  = Math.max(4, Math.round(r.total_balance / maxBal * 160));
    const color = prColor[r.action_priority] || '#64748b';
    const bg    = prBg[r.action_priority]    || 'transparent';
    html += `<tr style="background:${bg}">
      <td style="font-weight:500" title="${r.name}">${r.name}</td>
      <td style="text-align:right;font-weight:600">${fmtDollar(r.total_balance)}</td>
      <td><div style="width:${barW}px;height:10px;background:${color};border-radius:3px;opacity:0.8"></div></td>
      <td style="text-align:right;color:${r.pct_over_90 >= 50 ? '#ef4444' : '#94a3b8'}">${r.pct_over_90 != null ? r.pct_over_90.toFixed(1)+'%' : '—'}</td>
      <td style="text-align:right;color:${r.avg_denial_rate >= 30 ? '#f59e0b' : '#94a3b8'}">${r.avg_denial_rate != null ? r.avg_denial_rate.toFixed(1)+'%' : '—'}</td>
      <td style="text-align:right">${r.avg_collection_velocity != null ? r.avg_collection_velocity+'d' : '—'}</td>
      <td style="text-align:center"><span class="action-priority-badge priority-${(r.action_priority||'monitor').toLowerCase()}">${r.action_priority||'—'}</span></td>
    </tr>`;
  });
  html += '</tbody></table>';
  wrap.innerHTML = html;
}

// ── BOOT ─────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  pollLoading();
});
