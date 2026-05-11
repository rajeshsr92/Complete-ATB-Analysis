# ATB Analysis Dashboard — Developer Reference

## Project Overview
Flask + Pandas single-page application that loads Aged Trial Balance (ATB) claim data from a Windows network share, caches it locally, and serves an interactive dashboard with trending, migration, denial, high-dollar, and workables analysis.

---

## Quick Start (Local)
```
start_dashboard.bat          # double-click — starts server on http://localhost:5000
```
Or manually:
```
cd dashboard
python app.py
```
Hard-refresh browser after restart: **Ctrl + Shift + R**

**Important:** Always stop the existing server before restarting. Two `app.py` processes can both bind to port 5000, causing requests to hit the old process. Check with:
```powershell
Get-NetTCPConnection -LocalPort 5000 -State Listen
Stop-Process -Id <PID> -Force
```

---

## Architecture

```
Complete-ATB-Analysis/
├── dashboard/
│   ├── app.py            # Flask app, all API routes
│   ├── analytics.py      # Pure-pandas analysis functions (no I/O)
│   ├── data_loader.py    # File loading, caching, discovery
│   ├── templates/index.html
│   └── static/
│       ├── js/dashboard.js   (v17)
│       └── css/dashboard.css (v8)
├── Data/
│   ├── {ClientName}/
│   │   └── ATB/          # ATB xlsx files (EOM or Weekly)
│   └── Seneca_Health_SNCA_CA/
│       └── Production/   # Production + Work Queue files (gitignored xlsx)
├── start_dashboard.bat
└── CLAUDE.md
```

### Static asset versioning
CSS and JS are cache-busted via `?v=N` in `index.html`. Bump both when changing those files.
- Current: `dashboard.css?v=8`, `dashboard.js?v=17`

---

## Data Loading

### Network share (primary)
```
\\bopsprdwfil01.accretivehealth.local\CBOS_Reporting\ClientReporting
```
- Scans `{client}/2026/{MM_MonthName}/EOM/` for `Claim Level ATB EOM` xlsx
- Falls back to latest `MM_DD_YYYY` weekly subfolder if no EOM
- Clients without the global-latest month are treated as termed and excluded
- `ATB_LOCAL_ONLY=1` env var forces local `Data/` scan (used on Railway)

### Local cache
Parsed DataFrames are pickled to `%LOCALAPPDATA%\atb_eom_cache\` keyed by `md5(path)_{PKL_VERSION}.pkl`.
- **PKL_VERSION = `v12`** — bump this constant in `data_loader.py` whenever column schema changes to force cache rebuild
- Cache hit = pkl newer than source xlsx → skips openpyxl parse (very fast)

### Key columns
| Set | Columns |
|---|---|
| NEEDED_COLS | Encounter Number, Primary Health Plan, Responsible Financial Class, Responsible Health Plan, Balance Amount, Balance Type, Discharge Aging Category, Unbilled Aging Category, Balance Group, REPORT_DATE, Discharge Date |
| OPTIONAL_COLS | Billing Entity, First Claim Number, Last Claim Number, Last Denial Code and Reason, Last Denial Date, Last Denial Group, Claim Status, Claim Transmission Age Category |

ATB rows are filtered to `Balance Amount > 0` on load.

---

## Data Folder Structure — Client Naming Convention

Each client has a named folder under `Data/`:
```
Data/
  Seneca_Health_SNCA_CA/
    ATB/            ← ATB xlsx files (for local mode)
    Production/     ← SNCA_Sanitized Production.xlsx + Work Queue Weekly *.xlsx
  {NewClientName}/
    ATB/
    Production/     ← drop files here for new clients
```

**Adding a new client:** create `Data/{ClientName}/Production/` and drop the Production xlsx and Work Queue Weekly xlsx there. The loaders pick them up automatically via `load_production_file(client_name)` and `load_work_queue_file(client_name)`.

The loaders fall back to `Data/Production/` for backward compat, but new clients must use the named folder.

`.gitignore` excludes `Data/*/Production/*.xlsx` — these are live client files and must never be committed.

---

## API Routes

| Route | Purpose |
|---|---|
| `GET /api/clients` | List all loaded clients |
| `GET /api/status?client=` | Loading progress |
| `GET /api/trending?client=&week=` | WoW trending data |
| `GET /api/migration?client=&week=&prior=` | Aging migration |
| `GET /api/bifurcation?client=&week=` | ATB bifurcation + unbilled |
| `GET /api/denials?client=&week=` | Open denials analysis |
| `GET /api/denial-velocity?client=` | Denial velocity trending |
| `GET /api/cash-action-plan?client=` | Cash collection action plan |
| `GET /api/workables/untouched-claims?client=&date=&week=&exclude_wq=` | Untouched ATB claims |
| `GET /api/download/workables-untouched?client=&date=&week=&exclude_wq=` | Download untouched claims xlsx |
| `GET /api/download/*` | Excel downloads for all tabs |
| `GET /api/reload?client=` | Force-reload client data |

All data routes accept standard ATB filters as query params: `rfc=`, `rhp=`, `bt=`, `cs=`, `dac=`, `ctac=`

---

## Workables Section

**Production file** (`SNCA_Sanitized Production.xlsx`):
- Located at `Data/Seneca_Health_SNCA_CA/Production/`
- Key columns: `Claim#`, `First Claim#`, `Worked Date`, `Balance Amount`, `Billed Amount`
- Currently SNCA-only — other clients see a lock message

**Work Queue Weekly file** (`Work Queue Weekly *.xlsx`):
- Located at `Data/Seneca_Health_SNCA_CA/Production/`
- Matched to ATB via **`Encounter Number`** (NOT `Claim Number` — that column is blank for WQ-state rows)
- Only rows with these `Work Flow State` values are loaded:
  - R1 Credentialing, SNCA Coding Denials, SNCA Registration Edits, SNCA Authorizations
  - SNCA Adjustment Request, SNCA Registration Denials, SNCA Medical Necissity Edits Review, SNCA Coding Edits

**Matching logic:**
- "Unworked" = ATB claim whose `First Claim Number` or `Last Claim Number` is NOT in any production record `Claim#`/`First Claim#` from the last 30 days
- Production window date is user-selectable (defaults to today)
- All 6 ATB filters apply to narrow the ATB dataset used for matching

**View toggle (Mode 1 / Mode 2):**
- **All Untouched** (default): all unworked ATB claims; Work Flow State column shows WQ assignment if present
- **Excl. Work Queue**: further removes claims already in the Work Queue — shows truly unassigned claims

---

## Known Issues & Fixes Applied

### Windows AVD session isolation
Background bash tools run in session 6; user's browser runs in session 2. In some AVD environments loopback TCP does not cross sessions. Always start the server from the user's own session via `start_dashboard.bat` — do not rely on background-launched servers for the user's browser.

### Stale server / dual-process collision
When restarting `start_dashboard.bat`, the old process may not die, leaving two `app.py` processes both bound to port 5000. Requests randomly hit the old one. **Always kill all port-5000 processes before restarting:**
```powershell
$p = Get-NetTCPConnection -LocalPort 5000 -State Listen
Stop-Process -Id $p.OwningProcess -Force
```

### Python bytecode cache after code changes
If code changes aren't reflected after server restart, delete `dashboard/__pycache__/` to force recompilation:
```bash
find dashboard/__pycache__ -name "*.pyc" -delete
```

### JSON serialization (NaN / NaT / numpy scalars)
ATB DataFrames contain Categorical dtypes, float64 NaN, datetime64, and numpy scalars. `pandas.to_json()` emits bare `NaN` tokens (invalid JSON). The safe serialization pattern in `analytics.py`:
```python
def _safe(v):
    try:
        if pd.isnull(v):          # catches NaN, NaT, NA, None
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.strftime('%m/%d/%Y')
    if hasattr(v, 'item'):        # numpy scalar → Python native
        return v.item()
    return v

rows_records = [
    {col: _safe(val) for col, val in rec.items()}
    for rec in df.astype(object).to_dict(orient='records')
]
```
Drop `REPORT_DATE` column before serializing — it's always a datetime column not needed in row output.

### Work Queue matching key
The Work Queue Weekly file's `Claim Number` column is **blank** for all WQ-state rows. Matching must be done on `Encounter Number` (present in both WQ file and ATB data). If Work Flow State is not appearing in the table, verify `wq_map` is built from `Encounter Number`, not `Claim Number`.

### ATB Categorical dtype filter performance
Columns like `Responsible Financial Class`, `Responsible Health Plan` are stored as `category` dtype for memory efficiency. When filtering with `.isin()`, always pass a list not a set — some pandas versions handle set membership differently on Categorical. Use `df[df[col].isin(list(values))]`.

### Railway deployment
- Uses gunicorn with `app:app` entrypoint
- `ATB_LOCAL_ONLY=1` env var set on Railway to skip network share discovery
- Footer is shown only on Railway (detected via `RAILWAY_ENVIRONMENT` env var)
- `Procfile`: `web: gunicorn --chdir dashboard app:app --bind 0.0.0.0:$PORT`

---

## Module-level Caches in app.py

```python
_clients          = {}   # {client_name: {loading, weeks, weekly_data, ...}}
_production_dfs   = {}   # {client_name: DataFrame} — production file per client
_work_queue_dfs   = {}   # {client_name: DataFrame} — WQ file per client
```

All caches are populated lazily on first request and held for the server lifetime. Force reload via `GET /api/reload?client=`.

---

## Filter Bar (Shared)

The `#filter-bar` div lives **outside** all section panes, directly in `#content`. It is shown/hidden in `switchSection()` based on which section is active:
- Shown for: `medicare` (ATB Analysis), `highDollar`, `workables`
- Hidden for: all other sections

`filterParams()` in `dashboard.js` serializes active filters and `apiUrl()` appends them to every data fetch automatically.

---

## Adding a New Client with Workables Data

1. Create `Data/{NewClientName}/Production/`
2. Drop the Production xlsx and Work Queue Weekly xlsx there
3. Update `_check_snca_client()` in `app.py` if the guard logic needs to change (currently checks for `'SNCA'` in client name)
4. Add the client's specific `Work Flow State` values to `WQ_WORKFLOW_STATES` in `data_loader.py` if they differ

---

## git Commit History (Key Milestones)

| Commit | Description |
|---|---|
| `b4deaf3` | Initial commit: ATB KPI Dashboard |
| `47268c2` | New analyses + tab insight panels |
| `290e5eb` | Open Denials tab |
| `69d310a` | Denial Velocity tab + HD filter fix |
| `5c3a882` | Cash Collection Action Plan + PDF/screenshot utils |
| `c64c63c` | Railway deployment + gunicorn |
| `4ad3225` | Network share data source + launcher |
| `30e9827` | Balance Type filter, To-Week distribution, weekly folder fallback, Friday auto-refresh |
| `14ee871` | Fix Railway data loading (gunicorn startup race) |
| `bf0c2a2` | Footer on Railway only |
| `4a6f569` | **Workables dashboard** — untouched claims, WQ toggle, client folder structure |
