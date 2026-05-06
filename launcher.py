import sys
import os
import threading
import webbrowser
import time

# ── path setup ────────────────────────────────────────────────────────────────
if getattr(sys, 'frozen', False):
    _BUNDLE_DIR = sys._MEIPASS
    _EXE_DIR    = os.path.dirname(sys.executable)
else:
    _BUNDLE_DIR = os.path.dirname(os.path.abspath(__file__))
    _EXE_DIR    = _BUNDLE_DIR

# Data lives next to the .exe
os.environ['ATB_DATA_ROOT']  = os.path.join(_EXE_DIR, 'Data')

# Add dashboard package to path
_dash = os.path.join(_BUNDLE_DIR, 'dashboard')
if os.path.isdir(_dash):
    sys.path.insert(0, _dash)
else:
    sys.path.insert(0, _BUNDLE_DIR)

from app import app, _load_all_clients

PORT = 5050


def _open_browser():
    time.sleep(3)
    webbrowser.open(f'http://localhost:{PORT}')


if __name__ == '__main__':
    print('=' * 60)
    print('  ATB Analysis Dashboard  —  Director Showcase')
    print('  Clients: MCH_TX | LTTL_NH | MODO_CA | LCHN_CO')
    print('=' * 60)
    print(f'  URL : http://localhost:{PORT}')
    print('  NOTE: First launch reads Excel files (~3-5 min).')
    print('        Keep this window open while using the dashboard.')
    print('=' * 60)

    threading.Thread(target=_load_all_clients, daemon=True).start()
    threading.Thread(target=_open_browser,     daemon=True).start()

    app.run(debug=False, host='127.0.0.1', port=PORT, use_reloader=False)
