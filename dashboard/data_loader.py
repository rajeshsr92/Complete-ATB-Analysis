import sys, os
_LIB = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
if _LIB not in sys.path:
    sys.path.insert(0, _LIB)

def _patch_pyarrow():
    try:
        import pyarrow as pa
        missing = ['__version__', 'Array', 'ChunkedArray', 'Table', 'RecordBatch',
                   'Schema', 'Field', 'DataType', 'lib', 'types']
        for attr in missing:
            if not hasattr(pa, attr):
                if attr == '__version__':
                    pa.__version__ = '0.0.0'
                elif attr in ('Array', 'ChunkedArray', 'Table', 'RecordBatch',
                               'Schema', 'Field', 'DataType'):
                    setattr(pa, attr, type(attr, (), {}))
                elif attr == 'lib':
                    import types as _types
                    stub = _types.ModuleType('pyarrow.lib')
                    stub.is_pyarrow_array = lambda x: False
                    setattr(pa, attr, stub)
                else:
                    setattr(pa, attr, None)
    except ImportError:
        pass

_patch_pyarrow()

import pandas as pd
import glob
import re
import threading
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from calendar import monthrange

# Network share root
NETWORK_BASE = r'\\bopsprdwfil01.accretivehealth.local\CBOS_Reporting\ClientReporting'

# Month folder pattern: MM_MonthName  e.g. 04_April
_MONTH_RE = re.compile(r'^(\d{2})_\w+$')

# Local machine cache for network-share pkls (avoids writing to UNC path and is much faster to read)
_CACHE_DIR = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'atb_eom_cache')

# Limit concurrent raw Excel parses — each consumes ~400 MB RAM during parse
_excel_semaphore = threading.Semaphore(4)

NEEDED_COLS = [
    'Encounter Number', 'Primary Health Plan',
    'Responsible Financial Class', 'Responsible Health Plan',
    'Balance Amount', 'Discharge Aging Category', 'Unbilled Aging Category',
    'Balance Group', 'REPORT_DATE', 'Discharge Date'
]

OPTIONAL_COLS = [
    'Billing Entity',
    'Last Denial Code and Reason', 'Last Denial Date', 'Last Denial Group',
]

CAT_COLS = [
    'Primary Health Plan', 'Responsible Financial Class', 'Responsible Health Plan',
    'Discharge Aging Category', 'Unbilled Aging Category', 'Balance Group',
    'Billing Entity',
]

DENIAL_CAT_COLS = ['Last Denial Code and Reason', 'Last Denial Group']

PKL_VERSION = 'v7'


def _data_root():
    if 'ATB_DATA_ROOT' in os.environ:
        return os.environ['ATB_DATA_ROOT']
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base, '..', 'Data'))


def _pkl_path_for(xlsx_path):
    """Return local pkl cache path. Network files are cached in LOCALAPPDATA."""
    if xlsx_path.startswith('\\\\'):
        os.makedirs(_CACHE_DIR, exist_ok=True)
        h = hashlib.md5(xlsx_path.lower().encode()).hexdigest()[:20]
        return os.path.join(_CACHE_DIR, f'{h}_{PKL_VERSION}.pkl')
    return xlsx_path + f'.atb_{PKL_VERSION}.pkl'


# ── discovery ──────────────────────────────────────────────────────────────────

def discover_clients():
    """
    Scan network share for clients that have a 'Claim Level ATB EOM' file in
    their latest 2026 EOM month folder.  Clients without the global-latest month
    are treated as termed and excluded.  Falls back to local Data/ scan when the
    network share is unreachable.

    Returns [{name, atb_folder}] where atb_folder is:
      - dict {month_label: file_path}  for network clients
      - str path                        for local clients
    """
    if os.environ.get('ATB_LOCAL_ONLY') == '1':
        return _discover_local()
    if os.path.isdir(NETWORK_BASE):
        clients = _discover_network()
        if clients:
            return clients
    return _discover_local()


def _discover_local():
    root = _data_root()
    clients = []
    if os.path.isdir(root):
        for name in sorted(os.listdir(root)):
            atb_path = os.path.join(root, name, 'ATB')
            if os.path.isdir(atb_path):
                clients.append({'name': name, 'atb_folder': atb_path})
    return clients


def _discover_network():
    """
    Walk NETWORK_BASE and collect, for each client:
      {client_name: {month_num: (label, xlsx_path)}}

    Then keep only clients whose latest month == global latest month.
    Clients with non-standard folder structure are silently skipped.
    """
    all_client_months = {}
    latest_month = 0

    try:
        entries = os.listdir(NETWORK_BASE)
    except Exception:
        return []

    for entry in sorted(entries):
        client_path = os.path.join(NETWORK_BASE, entry)
        if not os.path.isdir(client_path):
            continue
        y2026 = os.path.join(client_path, '2026')
        if not os.path.isdir(y2026):
            continue

        found = {}
        try:
            for mf in os.listdir(y2026):
                m = _MONTH_RE.match(mf)
                if not m:
                    continue  # Non-standard month folder — different format, skip
                mnum = int(m.group(1))
                eom_dir = os.path.join(y2026, mf, 'EOM')
                if not os.path.isdir(eom_dir):
                    continue
                for f in sorted(os.listdir(eom_dir)):
                    if 'Claim Level ATB EOM' in f and f.lower().endswith('.xlsx'):
                        found[mnum] = (mf, os.path.join(eom_dir, f))
                        break  # one file per month is enough
        except Exception:
            continue

        if found:
            all_client_months[entry] = found
            latest_month = max(latest_month, max(found.keys()))

    if not latest_month:
        return []

    result = []
    for name in sorted(all_client_months):
        months = all_client_months[name]
        if latest_month not in months:
            continue  # No current-month file → termed → skip
        # Build ordered dict: month_label -> file_path (all available months)
        eom_files = {lbl: fp for _, (lbl, fp) in sorted(months.items())}
        result.append({'name': name, 'atb_folder': eom_files})

    return result


# ── file loading ───────────────────────────────────────────────────────────────

def load_atb_file(path, progress_cb=None):
    """
    Load one ATB Excel file.  Returns a filtered DataFrame (Balance > 0).
    Results are cached as a local pkl for fast subsequent loads.
    """
    fname = os.path.basename(path)
    pkl_path = _pkl_path_for(path)

    # Cache hit: pkl newer than source file
    try:
        if os.path.exists(pkl_path) and os.path.getmtime(pkl_path) > os.path.getmtime(path):
            if progress_cb:
                progress_cb(f'[CACHE] {fname}')
            return pd.read_pickle(pkl_path)
    except Exception:
        pass

    if progress_cb:
        progress_cb(f'[READ]  {fname} (first load — may take 1-3 min)...')

    _wanted = set(NEEDED_COLS + OPTIONAL_COLS)
    with _excel_semaphore:
        try:
            try:
                import python_calamine  # noqa: F401  (faster engine when available)
                df = pd.read_excel(path, usecols=lambda c: c in _wanted, engine='calamine')
            except (ImportError, Exception):
                df = pd.read_excel(path, usecols=lambda c: c in _wanted, engine='openpyxl')
        except PermissionError:
            # File locked (open in Excel / OneDrive sync) — use any existing cached pkl
            if path.startswith('\\\\'):
                h = hashlib.md5(path.lower().encode()).hexdigest()[:20]
                pattern = os.path.join(_CACHE_DIR, f'{h}_*.pkl')
            else:
                pattern = path + '.atb_*.pkl'
            existing = sorted(glob.glob(pattern), key=os.path.getmtime, reverse=True)
            if existing:
                if progress_cb:
                    progress_cb(f'[FALLBACK] {fname} — file locked, using cached version')
                return pd.read_pickle(existing[0])
            raise

    df['Encounter Number'] = pd.to_numeric(df['Encounter Number'], errors='coerce')
    df['Balance Amount'] = pd.to_numeric(df['Balance Amount'], errors='coerce').fillna(0.0)
    for col in CAT_COLS:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown').astype('category')
    for col in DENIAL_CAT_COLS:
        if col in df.columns:
            df[col] = df[col].astype('category')

    df = df[df['Balance Amount'] > 0].copy()
    df.to_pickle(pkl_path)

    if progress_cb:
        progress_cb(f'[DONE]  {fname} — {len(df):,} rows')
    return df


def extract_week_from_df(df):
    """Derive period-end date from REPORT_DATE column."""
    if 'REPORT_DATE' not in df.columns:
        return None
    dates = pd.to_datetime(df['REPORT_DATE'], errors='coerce').dropna()
    if dates.empty:
        return None
    return dates.max().strftime('%m.%d.%Y')


def extract_week_date(filename):
    """Fallback: parse date from filename pattern 'WE MM.DD.YYYY'."""
    m = re.search(r'WE (\d{2}\.\d{2}\.\d{4})', filename)
    return m.group(1) if m else None


def _eom_date_from_label(label):
    """'04_April' → '04.30.2026' (last calendar day of that 2026 month)."""
    m = _MONTH_RE.match(label)
    if not m:
        return None
    mnum = int(m.group(1))
    last_day = monthrange(2026, mnum)[1]
    return f'{mnum:02d}.{last_day:02d}.2026'


def load_all_atb_files(atb_folder, progress_cb=None):
    """
    Load ATB data for a client.
      - dict {label: path}  → EOM network mode, parallel load, returns {eom_date: df}
      - str folder path     → local folder mode, sequential load, returns {week_date: df}
    """
    if isinstance(atb_folder, dict):
        return _load_eom_parallel(atb_folder, progress_cb)

    pattern = os.path.join(atb_folder, '*.xlsx')
    files = sorted(glob.glob(pattern))
    result = {}
    for f in files:
        df = load_atb_file(f, progress_cb=progress_cb)
        week = extract_week_from_df(df) or extract_week_date(os.path.basename(f))
        if week:
            result[week] = df
        elif progress_cb:
            progress_cb(f'[SKIP]  {os.path.basename(f)} — could not determine week date')
    return result


def _load_eom_parallel(eom_files, progress_cb=None):
    """Load all EOM files concurrently. Returns {eom_date: df}."""
    result = {}
    lock = threading.Lock()

    def _load_one(label, path):
        try:
            df = load_atb_file(path, progress_cb=progress_cb)
            key = extract_week_from_df(df) or _eom_date_from_label(label)
            if key:
                with lock:
                    result[key] = df
        except Exception as e:
            if progress_cb:
                progress_cb(f'[ERROR] {label}: {e}')

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(_load_one, lbl, path)
                   for lbl, path in eom_files.items()]
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception:
                pass

    return result


# ── filter helpers ─────────────────────────────────────────────────────────────

def get_billing_entities(weekly_data):
    entities = set()
    for df in weekly_data.values():
        if 'Billing Entity' in df.columns:
            entities.update(df['Billing Entity'].dropna().unique())
    return sorted(str(v) for v in entities if str(v) not in ('Unknown', 'nan', ''))


def get_filter_values(weekly_data):
    fin_classes, health_plans = set(), set()
    for df in weekly_data.values():
        if 'Responsible Financial Class' in df.columns:
            fin_classes.update(df['Responsible Financial Class'].dropna().unique())
        if 'Responsible Health Plan' in df.columns:
            health_plans.update(df['Responsible Health Plan'].dropna().unique())
    return {
        'resp_fin_class': sorted(str(v) for v in fin_classes if str(v) != 'Unknown'),
        'resp_health_plan': sorted(str(v) for v in health_plans if str(v) != 'Unknown'),
    }
