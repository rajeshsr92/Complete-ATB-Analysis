import sys
import os

def _find_and_add_lib():
    candidates = [
        os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'medicare_dash_lib'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'),
        os.environ.get('MEDICARE_LIB_PATH', ''),
    ]
    for path in candidates:
        path = os.path.normpath(path)
        if os.path.isdir(path) and path not in sys.path:
            if os.path.exists(os.path.join(path, 'flask', '__init__.py')):
                sys.path.insert(0, path)
                return path
    return None

_find_and_add_lib()

try:
    import pyarrow as _pa
    _STUBS = ['__version__', 'Array', 'ChunkedArray', 'Table', 'RecordBatch', 'Schema', 'Field', 'DataType']
    if not hasattr(_pa, '__version__'):
        _pa.__version__ = '0.0.0'
    for _a in _STUBS[1:]:
        if not hasattr(_pa, _a):
            setattr(_pa, _a, type(_a, (), {}))
except ImportError:
    pass

import threading
from flask import Flask, render_template, jsonify, request
from data_loader import (load_all_atb_files, discover_clients, get_filter_values,
                          get_billing_entities)
from analytics import (wow_trending, trending_summary,
                        aging_migration, rollover_summary,
                        atb_bifurcation, bifurcation_summary,
                        unbilled_analysis, balance_group_breakdown, aging_velocity,
                        aging_contributors, compute_high_dollar_threshold,
                        denial_analysis, denial_velocity,
                        cash_collection_action_plan)

app = Flask(__name__)

@app.after_request
def no_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response

_clients = {}
_clients_lock = threading.Lock()


def _get_state(name):
    with _clients_lock:
        return _clients.get(name)


def _load_client(client_name, atb_folder):
    with _clients_lock:
        if client_name not in _clients:
            _clients[client_name] = {
                'weekly_data': {}, 'weeks': [], 'loading': True,
                'load_log': [], 'error': None
            }
    state = _clients[client_name]

    def log(msg):
        print(f'[{client_name}] {msg}')
        state['load_log'].append(msg)

    try:
        log('Starting data load...')
        if not os.path.isdir(atb_folder):
            raise FileNotFoundError(f'ATB folder not found: {atb_folder}')
        data = load_all_atb_files(atb_folder, progress_cb=log)
        state['weekly_data'] = data
        state['weeks'] = sorted(data.keys())
        log(f'Loaded {len(data)} weeks: {state["weeks"]}')
    except Exception as e:
        state['error'] = str(e)
        log(f'ERROR: {e}')
    finally:
        state['loading'] = False


def _load_all_clients():
    clients = discover_clients()
    if not clients:
        print('WARNING: No client folders found under Data/')
        return
    for c in clients:
        t = threading.Thread(target=_load_client,
                             args=(c['name'], c['atb_folder']), daemon=True)
        t.start()


def _reload_client(client_name, atb_folder):
    """Force-replaces client state and reloads all files. Safe to call while old load is running."""
    new_state = {'weekly_data': {}, 'weeks': [], 'loading': True, 'load_log': [], 'error': None}
    with _clients_lock:
        _clients[client_name] = new_state
    state = new_state

    def log(msg):
        print(f'[{client_name}] {msg}')
        state['load_log'].append(msg)

    try:
        log('Starting data reload...')
        if not os.path.isdir(atb_folder):
            raise FileNotFoundError(f'ATB folder not found: {atb_folder}')
        data = load_all_atb_files(atb_folder, progress_cb=log)
        state['weekly_data'] = data
        state['weeks'] = sorted(data.keys())
        log(f'Loaded {len(data)} weeks: {state["weeks"]}')
    except Exception as e:
        state['error'] = str(e)
        log(f'ERROR: {e}')
    finally:
        state['loading'] = False


def _resolve_client(name=None):
    """Return (client_name, state) or (None, error_tuple)."""
    if not name:
        with _clients_lock:
            for k, v in _clients.items():
                return k, v
        return None, (jsonify({'error': 'No clients loaded'}), 503)
    state = _get_state(name)
    if state is None:
        return None, (jsonify({'error': f'Client not found: {name}'}), 404)
    return name, state


def _apply_filters(df, req):
    """Apply Responsible Financial Class and Responsible Health Plan filters from request args."""
    rfc = [v for v in req.args.get('resp_fin_class', '').split(',') if v]
    rhp = [v for v in req.args.get('resp_health_plan', '').split(',') if v]
    if rfc:
        df = df[df['Responsible Financial Class'].astype(str).isin(rfc)]
    if rhp:
        df = df[df['Responsible Health Plan'].astype(str).isin(rhp)]
    return df


def _apply_all_filters(df, req):
    """Apply fin/plan filters, then optional high dollar filter."""
    df = _apply_filters(df, req)
    if req.args.get('high_dollar') == 'true':
        try:
            pct = float(req.args.get('hd_pct', 0.60))
            threshold = compute_high_dollar_threshold(df, pct)['threshold']
            if threshold > 0:
                df = df[df['Balance Amount'] >= threshold]
        except Exception as e:
            print(f'[HD filter error] {e}')
    return df


# ── routes ───────────────────────────────────────────────────────

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/clients')
def api_clients():
    discovered = discover_clients()
    result = []
    for c in discovered:
        state = _get_state(c['name']) or {}
        result.append({
            'name': c['name'],
            'loading': state.get('loading', True),
            'error': state.get('error'),
            'weeks': state.get('weeks', []),
        })
    return jsonify(result)


@app.route('/api/status')
def api_status():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    return jsonify({
        'loading': state['loading'],
        'error': state['error'],
        'weeks_loaded': len(state['weeks']),
        'weeks': state['weeks'],
        'log': state['load_log'][-5:],
    })


@app.route('/api/reload', methods=['POST'])
def api_reload():
    client_param = request.args.get('client')
    discovered = {c['name']: c for c in discover_clients()}

    if client_param:
        if client_param not in discovered:
            return jsonify({'error': f'Client not found: {client_param}'}), 404
        targets = [discovered[client_param]]
    else:
        targets = list(discovered.values())

    reloaded, skipped = [], []
    for c in targets:
        name = c['name']
        with _clients_lock:
            existing = _clients.get(name)
            currently_loading = existing is not None and existing.get('loading', False)
        if currently_loading:
            skipped.append(name)
        else:
            t = threading.Thread(target=_reload_client, args=(name, c['atb_folder']), daemon=True)
            t.start()
            reloaded.append(name)

    return jsonify({'reloaded': reloaded, 'skipped': skipped})


@app.route('/api/weeks')
def api_weeks():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    return jsonify({'weeks': state['weeks']})


@app.route('/api/filters')
def api_filters():
    """Return unique Responsible Financial Class and Health Plan values for dropdowns."""
    client = request.args.get('client')
    name, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    return jsonify(get_filter_values(state['weekly_data']))


@app.route('/api/trending')
def api_trending():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    filtered = {w: _apply_all_filters(df, request) for w, df in state['weekly_data'].items()}
    rows = wow_trending(filtered)
    return jsonify({'rows': rows, 'summary': trending_summary(rows)})


@app.route('/api/migration')
def api_migration():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    from_week = request.args.get('from')
    to_week = request.args.get('to')
    if not from_week or not to_week:
        return jsonify({'error': 'from and to parameters required'}), 400
    wd = state['weekly_data']
    if from_week not in wd or to_week not in wd:
        return jsonify({'error': 'Week not found'}), 404
    a = _apply_all_filters(wd[from_week], request)
    b = _apply_all_filters(wd[to_week], request)
    result = aging_migration(a, b)
    result['summary_points'] = rollover_summary(result)
    return jsonify(result)


@app.route('/api/bifurcation')
def api_bifurcation():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    if not week:
        return jsonify({'error': 'week parameter required'}), 400
    wd = state['weekly_data']
    weeks = state['weeks']
    if week not in wd:
        return jsonify({'error': 'Week not found'}), 404
    idx = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    curr = _apply_all_filters(wd[week], request)
    prior = _apply_all_filters(wd[prior_week], request)
    bifur_result = atb_bifurcation(curr, prior)
    unbilled = unbilled_analysis(curr, prior)
    bifur_result['unbilled'] = unbilled
    bifur_result['summary_points'] = bifurcation_summary(bifur_result, unbilled)
    return jsonify(bifur_result)


@app.route('/api/aging-contributors')
def api_aging_contributors():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    wd = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    idx = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    curr = _apply_all_filters(wd[week], request)
    prior = _apply_all_filters(wd[prior_week], request)
    top_n = int(request.args.get('top_n', 15))
    return jsonify(aging_contributors(curr, prior, top_n=top_n))


@app.route('/api/high-dollar-threshold')
def api_hd_threshold():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    week = request.args.get('week')
    if not week or week not in state['weekly_data']:
        week = weeks[-1]
    df = _apply_filters(state['weekly_data'][week], request)
    pct = float(request.args.get('hd_pct', 0.60))
    result = compute_high_dollar_threshold(df, pct)
    result['week'] = week
    return jsonify(result)


@app.route('/api/billing-entities')
def api_billing_entities():
    client = request.args.get('client')
    name, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    return jsonify({'entities': get_billing_entities(state['weekly_data'])})


@app.route('/api/unbilled')
def api_unbilled():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    wd = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    idx = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    curr = _apply_all_filters(wd[week], request)
    prior = _apply_all_filters(wd[prior_week], request)
    return jsonify(unbilled_analysis(curr, prior))


@app.route('/api/balance-groups')
def api_balance_groups():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    wd = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    idx = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    curr = _apply_all_filters(wd[week], request)
    prior = _apply_all_filters(wd[prior_week], request)
    return jsonify(balance_group_breakdown(curr, prior))


@app.route('/api/aging-velocity')
def api_aging_velocity():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    wd = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    curr = _apply_all_filters(wd[week], request)
    return jsonify(aging_velocity(curr))


@app.route('/api/denials')
def api_denials():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week = request.args.get('week')
    wd = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    idx = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    curr  = _apply_all_filters(wd[week], request)
    prior = _apply_all_filters(wd[prior_week], request)
    return jsonify(denial_analysis(curr, prior))


@app.route('/api/denial-velocity')
def api_denial_velocity():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    filtered = {w: _apply_all_filters(df, request) for w, df in state['weekly_data'].items()}
    return jsonify(denial_velocity(filtered))


@app.route('/api/cash-action-plan')
def api_cash_action_plan():
    client = request.args.get('client')
    _, state = _resolve_client(client)
    if isinstance(state, tuple):
        return state
    if state['loading']:
        return jsonify({'error': 'Data still loading'}), 503
    week  = request.args.get('week')
    wd    = state['weekly_data']
    weeks = state['weeks']
    if not weeks:
        return jsonify({'error': 'No data'}), 404
    if not week or week not in wd:
        week = weeks[-1]
    idx        = weeks.index(week)
    prior_week = weeks[idx - 1] if idx > 0 else week
    filtered_all = {w: _apply_all_filters(df, request) for w, df in wd.items()}
    curr  = filtered_all[week]
    prior = filtered_all[prior_week]
    return jsonify(cash_collection_action_plan(filtered_all, curr, prior))


# Keep old /api/medicare/* paths as aliases so cached bookmarks still work
app.add_url_rule('/api/medicare/trending', view_func=api_trending)
app.add_url_rule('/api/medicare/migration', view_func=api_migration, endpoint='api_migration_alias')
app.add_url_rule('/api/medicare/bifurcation', view_func=api_bifurcation, endpoint='api_bifurcation_alias')


if __name__ == '__main__':
    t = threading.Thread(target=_load_all_clients, daemon=True)
    t.start()
    app.run(debug=False, port=5000, use_reloader=False)
