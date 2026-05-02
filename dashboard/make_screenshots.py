"""
make_screenshots.py — capture all ATB dashboard tabs as high-res PNGs.
Client name is masked before any screenshot is taken.
Run with Flask already running on http://localhost:5000
"""

import os, time
from pathlib import Path
from playwright.sync_api import sync_playwright

OUT_DIR = Path(__file__).parent / 'screenshots'
OUT_DIR.mkdir(exist_ok=True)

BASE_URL = 'http://localhost:5000'
FAKE_CLIENT = 'Healthcare System'

# (filename, section, tab_id, extra_wait_ms)
SHOTS = [
    ('01_wow_trending.png',     'medicare',     'trending',        2000),
    ('02_aging_migration.png',  'medicare',     'rollover',        2000),
    ('03_atb_bifurcation.png',  'medicare',     'bifurcation',     2500),
    ('04_aging_contributors.png','medicare',    'contributors',    2000),
    ('05_open_denials.png',     'medicare',     'denials',         2500),
    ('06_denial_velocity.png',  'medicare',     'denial-velocity', 2500),
    ('07_hd_trending.png',      'highDollar',   'trending',        2000),
    ('08_hd_denials.png',       'highDollar',   'denials',         2500),
]

MASK_JS = f"""
() => {{
    const sub = document.getElementById('page-title-sub');
    if (sub) sub.textContent = '{FAKE_CLIENT}';
    const sbar = document.getElementById('sidebar-client-sub');
    if (sbar) sbar.textContent = '{FAKE_CLIENT}';
    const sel = document.getElementById('sel-client');
    if (sel && sel.selectedIndex >= 0) {{
        sel.options[sel.selectedIndex].text = '{FAKE_CLIENT}';
    }}
    // Also mask the page title text if it has client name
    document.querySelectorAll('.page-title-sub, .client-name').forEach(el => {{
        el.textContent = '{FAKE_CLIENT}';
    }});
}}
"""

def wait_for_data(page, timeout=30):
    """Wait until KPI cards have real values (not '—' or empty)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        val = page.evaluate("""
            () => {
                const el = document.getElementById('kpi-balance');
                if (!el) return '';
                return el.querySelector('.kpi-value')?.textContent || '';
            }
        """)
        if val and val not in ('—', '', 'Loading...'):
            return True
        time.sleep(0.8)
    return False

def take_screenshots():
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={'width': 1600, 'height': 900},
            device_scale_factor=2,           # retina-quality
        )
        page = ctx.new_page()

        print(f'Navigating to {BASE_URL} ...')
        page.goto(BASE_URL, wait_until='networkidle')

        # Wait for initial data load
        print('Waiting for data to load...')
        if not wait_for_data(page):
            print('WARNING: data may not be fully loaded')
        time.sleep(1)

        current_section = 'medicare'

        for fname, section, tab_id, extra_wait in SHOTS:
            print(f'  Capturing {fname} (section={section}, tab={tab_id}) ...')

            # Switch section if needed
            if section != current_section:
                page.evaluate(f"() => switchSection('{section}')")
                time.sleep(1.5)
                current_section = section

            # Switch tab
            page.evaluate(f"() => switchTab('{tab_id}')")
            time.sleep(extra_wait / 1000)

            # Mask client name every time (re-apply after each render)
            page.evaluate(MASK_JS)
            time.sleep(0.3)

            out_path = OUT_DIR / fname
            page.screenshot(path=str(out_path), full_page=True)
            print(f'    -> saved {out_path} ({os.path.getsize(out_path)//1024} KB)')

        browser.close()
    print(f'\nDone. Screenshots saved to: {OUT_DIR}')

if __name__ == '__main__':
    take_screenshots()
