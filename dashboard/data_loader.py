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
from concurrent.futures import ThreadPoolExecutor, as_completed

# Limit concurrent Excel reads to avoid memory spikes (each file ~300-500 MB during parse)
_excel_semaphore = threading.Semaphore(2)

NEEDED_COLS = [
    'Encounter Number', 'Primary Health Plan',
    'Responsible Financial Class', 'Responsible Health Plan',
    'Balance Amount', 'Discharge Aging Category', 'Unbilled Aging Category',
    'Balance Group', 'REPORT_DATE', 'Discharge Date'
]

# Optional columns loaded when present in the source file
OPTIONAL_COLS = [
    'Billing Entity',
    'Last Denial Code and Reason', 'Last Denial Date', 'Last Denial Group',
]

# String columns stored as categoricals — filled with 'Unknown' when empty
CAT_COLS = [
    'Primary Health Plan', 'Responsible Financial Class', 'Responsible Health Plan',
    'Discharge Aging Category', 'Unbilled Aging Category', 'Balance Group',
    'Billing Entity',
]

# Denial columns: categoricals but NOT filled with 'Unknown' — empty stays NaN
DENIAL_CAT_COLS = ['Last Denial Code and Reason', 'Last Denial Group']

# Bump when filter/column logic changes — forces pkl regeneration
PKL_VERSION = 'v6'


def _data_root():
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.normpath(os.path.join(base, '..', 'Data'))


def discover_clients():
    """
    Scan Data/ for client folders that contain an ATB/ subfolder.
    Folder name IS the client name (e.g. Littleton_Regional_LTTL_NH).
    Returns list of dicts: [{name, atb_folder}]
    """
    root = _data_root()
    clients = []
    if os.path.isdir(root):
        for name in sorted(os.listdir(root)):
            atb_path = os.path.join(root, name, 'ATB')
            if os.path.isdir(atb_path):
                clients.append({'name': name, 'atb_folder': atb_path})
    return clients


def get_atb_folder(client_name=None):
    clients = discover_clients()
    if not clients:
        raise FileNotFoundError('No client folders found under Data/')
    if client_name:
        for c in clients:
            if c['name'] == client_name:
                return c['atb_folder']
        raise FileNotFoundError(f'Client not found: {client_name}')
    return clients[0]['atb_folder']


def load_atb_file(path, progress_cb=None):
    """Load ATB Excel (Balance > 0, all payers). Uses versioned pkl cache."""
    fname = os.path.basename(path)
    pkl_path = path + f'.atb_{PKL_VERSION}.pkl'

    # Check current-version cache
    try:
        xlsx_mtime = os.path.getmtime(path)
        if os.path.exists(pkl_path) and os.path.getmtime(pkl_path) > xlsx_mtime:
            if progress_cb:
                progress_cb(f'[CACHE] {fname}')
            return pd.read_pickle(pkl_path)
    except Exception:
        pass

    if progress_cb:
        progress_cb(f'[READ]  {fname} (first time: 1-3 min)...')

    _wanted = set(NEEDED_COLS + OPTIONAL_COLS)
    try:
        try:
            import python_calamine  # noqa: F401
            df = pd.read_excel(path, usecols=lambda c: c in _wanted, engine='calamine')
        except (ImportError, Exception):
            df = pd.read_excel(path, usecols=lambda c: c in _wanted, engine='openpyxl')
    except PermissionError:
        # File locked (open in Excel / OneDrive sync) — fall back to any existing pkl
        existing = sorted(glob.glob(path + '.atb_*.pkl'), key=os.path.getmtime, reverse=True)
        if existing:
            if progress_cb:
                progress_cb(f'[FALLBACK] {fname} — file locked, using cached version')
            return pd.read_pickle(existing[0])
        raise

    # Coerce types
    df['Encounter Number'] = pd.to_numeric(df['Encounter Number'], errors='coerce')
    df['Balance Amount'] = pd.to_numeric(df['Balance Amount'], errors='coerce').fillna(0.0)
    for col in CAT_COLS:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown').astype('category')
    # Denial columns: keep NaN for empty so absence of denial is detectable
    for col in DENIAL_CAT_COLS:
        if col in df.columns:
            df[col] = df[col].astype('category')

    df = df[df['Balance Amount'] > 0].copy()
    df.to_pickle(pkl_path)

    if progress_cb:
        progress_cb(f'[DONE]  {fname} — {len(df):,} rows (Balance > $0, all payers)')
    return df


def extract_week_from_df(df):
    """Derive week-ending date from the REPORT_DATE column (primary method)."""
    if 'REPORT_DATE' not in df.columns:
        return None
    dates = pd.to_datetime(df['REPORT_DATE'], errors='coerce').dropna()
    if dates.empty:
        return None
    return dates.max().strftime('%m.%d.%Y')


def extract_week_date(filename):
    """Fallback: parse week-ending date from filename (e.g. 'WE 04.04.2026')."""
    m = re.search(r'WE (\d{2}\.\d{2}\.\d{4})', filename)
    return m.group(1) if m else None


def load_all_atb_files(atb_folder, progress_cb=None):
    # Match all xlsx files — filename format is not assumed
    pattern = os.path.join(atb_folder, '*.xlsx')
    files = sorted(glob.glob(pattern))
    result = {}
    for f in files:
        df = load_atb_file(f, progress_cb=progress_cb)
        # Always try REPORT_DATE first; fall back to filename parsing
        week = extract_week_from_df(df) or extract_week_date(os.path.basename(f))
        if week:
            result[week] = df
        else:
            if progress_cb:
                progress_cb(f'[SKIP]  {os.path.basename(f)} — could not determine week date')
    return result


def get_billing_entities(weekly_data):
    """Return sorted unique Billing Entity values across all weeks."""
    entities = set()
    for df in weekly_data.values():
        if 'Billing Entity' in df.columns:
            entities.update(df['Billing Entity'].dropna().unique())
    return sorted(str(v) for v in entities if str(v) not in ('Unknown', 'nan', ''))


def get_filter_values(weekly_data):
    """Return unique sorted values for both filter dropdowns across all weeks."""
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
