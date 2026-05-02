"""
make_pdf.py -- assemble ATB Dashboard portfolio PDF from screenshots.
Produces ATB_Dashboard_Portfolio.pdf in the dashboard/ folder.
Run after make_screenshots.py has populated the screenshots/ folder.
"""

from pathlib import Path
from PIL import Image as PILImage
from fpdf import FPDF

SCREENSHOTS = Path(__file__).parent / 'screenshots'
OUT_PDF     = Path(__file__).parent / 'ATB_Dashboard_Portfolio.pdf'

# Colour palettes (R, G, B)
NAVY  = (15,  23,  42)
BLUE  = (14, 165, 233)
AMBER = (245, 158,  11)
RED   = (239,  68,  68)
GREEN = ( 34, 197,  94)
SLATE = (100, 116, 139)
WHITE = (255, 255, 255)
LGRAY = (241, 245, 249)
DARK  = ( 30,  41,  59)

# ── Tab content (pure ASCII / latin-1 safe) ──────────────────────────────────
TABS = [
    {
        'file':   '01_wow_trending.png',
        'title':  '01 | WoW Trending -- AR Balance & Encounter Volume',
        'accent': BLUE,
        'what': (
            'The Week-over-Week (WoW) Trending view is the primary operational pulse check for '
            'the entire AR portfolio. It tracks total billed balance and encounter count across '
            'every ATB aging bucket (Not Aged, DNFB, 0-30 days through 366+ days) for each '
            'weekly ATB snapshot. A three-axis chart overlays balance bars, encounter count '
            'line, and the over-90-day percentage to reveal trend direction at a glance.'
        ),
        'metrics': [
            'Total AR balance ($) -- current week vs prior week',
            'Total encounter count -- current week vs prior week',
            'WoW balance delta ($) and percentage change',
            'Over-90-day balance ($) and percentage of total AR',
            'Per-bucket balance breakdown: Not Aged, DNFB, 0-30 ... 366+',
            'KPI insight panel with 6-8 auto-generated bullet points',
        ],
        'why': (
            'This is the executive-level KPI most revenue cycle leaders review first. A growing '
            'total balance with a rising over-90-day % signals that collections are falling behind '
            'new volume -- the earliest warning of a systemic billing or follow-up failure. '
            'Conversely, a shrinking balance with a stable or declining over-90-day % confirms '
            'that recovery efforts are working. The bucket breakdown pinpoints WHERE the aging '
            'is accumulating (e.g., a spike in 61-90 days today becomes a 91-120 days crisis '
            'next week if unaddressed). Tracking WoW removes the noise of month-end rushes and '
            'gives a true rolling view of AR health.'
        ),
    },
    {
        'file':   '02_aging_migration.png',
        'title':  '02 | Aging Migration -- Rollover & Resolution Matrix',
        'accent': AMBER,
        'what': (
            'The Aging Migration (Rollover) view shows how individual encounters moved between '
            'aging buckets across two selected ATB weeks. A colour-coded migration matrix maps '
            'FROM bucket (rows) to TO bucket (columns): cells on the diagonal mean the encounter '
            'stayed in the same bucket; cells below the diagonal mean it aged further; cells '
            'above the diagonal mean it paid down or resolved. New encounters and fully resolved '
            'encounters are tracked separately.'
        ),
        'metrics': [
            'Net aged-worse balance ($) and encounter count',
            'Net improved/resolved balance ($) and encounter count',
            'New encounters entering the ATB this week (balance + count)',
            'Resolved encounters (paid/written-off) since prior week',
            'Per-cell migration dollar amounts in the matrix',
            'Rollover rate: % of prior-week balance that aged one or more buckets',
        ],
        'why': (
            'The rollover matrix answers the question revenue cycle directors ask most urgently: '
            '"Are we winning or losing against the AR?" A high rollover from 61-90 into 91-120 '
            'days is the clearest early indicator of a follow-up gap -- claims are sitting '
            'untouched as appeal windows narrow. By quantifying exactly how much balance crosses '
            'each threshold each week, leadership can set performance targets (e.g., "keep '
            'rollover from 61-90 below $500K per week") and hold teams accountable with a '
            'single number. The new vs resolved split reveals whether billing throughput is '
            'keeping pace with inflow, directly linking billing team productivity to AR trend '
            'direction.'
        ),
    },
    {
        'file':   '03_atb_bifurcation.png',
        'title':  '03 | ATB Bifurcation -- Billed vs Unbilled, New vs Carried-Forward',
        'accent': GREEN,
        'what': (
            'The ATB Bifurcation view splits the current week\'s AR into four meaningful '
            'segments: billed vs. unbilled, and new-this-week vs. carried-forward from prior '
            'weeks. A donut chart displays the billed/unbilled share of total balance, while a '
            'stacked bar chart breaks each segment down by aging bucket. A dedicated Unbilled '
            'analysis panel highlights unbilled AR by age -- the most time-sensitive data on '
            'the dashboard.'
        ),
        'metrics': [
            'Total billed balance and encounter count',
            'Total unbilled balance and encounter count',
            'New-this-week billed vs unbilled split',
            'Carried-forward billed vs unbilled split',
            'Unbilled by aging bucket (DNFB, 0-30 ... 90+)',
            'Week-over-week change in unbilled balance',
        ],
        'why': (
            'Unbilled AR is revenue at risk of permanent loss due to timely filing limits -- '
            'most commercial payers enforce 90-365 day windows, after which claims are denied '
            'as "not timely filed" with no appeal right. This view makes unbilled AR visible '
            'as a distinct category with its own aging clock, rather than buried inside the '
            'overall ATB. A large or growing unbilled DNFB balance signals a coding or '
            'charge-capture backlog; unbilled 31-60-day AR suggests a billing workflow '
            'bottleneck. The carried-forward vs new split further diagnoses whether the team '
            'is clearing the backlog (carried-forward shrinking) or accumulating it week over '
            'week. Every unbilled dollar in this view represents a specific, actionable billing '
            'task with a hard deadline.'
        ),
    },
    {
        'file':   '04_aging_contributors.png',
        'title':  '04 | Aging Contributors -- Top Accounts Driving AR Growth',
        'accent': RED,
        'what': (
            'The Aging Contributors view ranks individual encounters by their contribution to '
            'current-week AR balance, with a week-over-week delta showing whether each account '
            'is growing, stable, or paying down. The top-N encounters (default 15, configurable) '
            'are displayed with payer, financial class, aging bucket, current balance, and WoW '
            'change. Accounts that increased significantly since last week are highlighted to '
            'guide immediate follow-up prioritisation.'
        ),
        'metrics': [
            'Per-encounter balance ($) -- current week',
            'WoW balance delta ($) per encounter',
            'Aging bucket for each encounter',
            'Responsible health plan and financial class',
            'Rank by balance or by delta (configurable)',
            'Sum of top-N as % of total AR',
        ],
        'why': (
            'The Pareto principle consistently holds in healthcare AR: typically 20% of '
            'encounters represent 80% of total balance. This view operationalises that insight '
            '-- instead of managing thousands of accounts equally, teams can focus daily work '
            'effort on the few high-balance, high-growth encounters that move the needle most. '
            'An encounter growing by $50K in a single week while sitting at 91-120 days is a '
            'clear escalation candidate. Equally, identifying accounts that are actively paying '
            'down validates that prior follow-up activity is working. This view bridges '
            'analytics and operations: it produces a daily work list, not just a report.'
        ),
    },
    {
        'file':   '05_open_denials.png',
        'title':  '05 | Open Denials -- Current Week Denial Snapshot',
        'accent': RED,
        'what': (
            'The Open Denials tab provides a complete snapshot of the current week\'s denied AR '
            'based on the Last Denial Code and Reason field. A Pareto bar chart ranks the top '
            'denial codes by total denied balance; a donut chart shows denial group distribution '
            '(clinical, authorisation, eligibility, etc.); a detailed table breaks down each '
            'code by health plan with balance, encounter count, and percentage of total denials. '
            'Self-pay accounts are automatically excluded.'
        ),
        'metrics': [
            'Total denied balance ($) and denied encounter count',
            'Denial rate as % of total AR balance',
            'Top denial codes by balance -- Pareto concentration',
            'Denial group breakdown (clinical, auth, eligibility, etc.)',
            'Per-code, per-payer denial balance table',
            'WoW change in denial balance and encounter count',
        ],
        'why': (
            'Denials are the single largest controllable driver of AR aging and write-offs in '
            'healthcare revenue cycle. An unworked denial becomes uncollectable the moment its '
            'appeal window closes -- often 60-180 days from the denial date. This view enables '
            'denial triage by ROI: focus appeals on the highest-balance codes first. The Pareto '
            'chart immediately reveals whether denials are systemic (one or two codes dominate) '
            'or diffuse (many small codes), which determines whether the fix is a process change '
            'or individual account work. Payer-level breakdown identifies which health plans '
            'deny most aggressively, supporting contract renegotiation and payer-specific appeal '
            'strategy development.'
        ),
    },
    {
        'file':   '06_denial_velocity.png',
        'title':  '06 | Denial Velocity -- Denial Age & Resolution Speed',
        'accent': AMBER,
        'what': (
            'The Denial Velocity tab measures how OLD the current open denials are, using the '
            'Last Denial Date field to compute denial age in days relative to the ATB report '
            'date. A heat table maps each denial code (rows) against six age buckets -- 0-29 '
            'days through 180+ days -- colour-coded by balance magnitude. An Aged Denials table '
            'lists every encounter denied 90+ days ago, sorted by age descending. The WoW trend '
            'chart provides historical denial volume context across all available weeks.'
        ),
        'metrics': [
            'Average denial age (days) -- across all open denials',
            '90+ day denied balance ($) and percentage',
            '180+ day denied balance ($) -- near-certain write-off risk',
            'Per-code average age, max age, and % of balance over 90 days',
            'Heat table: balance per code per age bucket (0-29 ... 180+)',
            'Aged denials table: encounter #, code, payer, denial date, age, balance',
        ],
        'why': (
            'Velocity adds the time dimension that a static denial snapshot misses. A $2M '
            'denial balance is very different depending on whether it is 15 days old (fully '
            'actionable) or 175 days old (likely unrecoverable). This view separates those '
            'two scenarios instantly. The 180+ day denied balance is the most operationally '
            'urgent number on the entire dashboard -- it represents revenue that is about to '
            'be permanently lost unless escalated immediately. The heat table identifies which '
            'denial codes are chronically slow to resolve, pointing directly at root causes: '
            'a code with $500K consistently sitting in the 120-179-day bucket indicates a '
            'systematic appeal process failure for that specific code or payer. This analysis '
            'drives payer escalations, appeal template redesign, and staffing reallocation '
            'before windows close.'
        ),
    },
    {
        'file':   '07_hd_trending.png',
        'title':  '07 | High Dollar -- WoW Trending (Threshold-Filtered)',
        'accent': BLUE,
        'what': (
            'The High Dollar section applies the same full suite of ATB analyses -- WoW '
            'Trending, Aging Migration, Bifurcation, Aging Contributors, Open Denials, and '
            'Denial Velocity -- but restricts the data to encounters above the computed '
            'high-dollar balance threshold. The threshold is the minimum balance required to '
            'capture 60% of the total ATB balance, isolating the high-impact minority of '
            'accounts. This WoW Trending view shows how that high-dollar cohort is trending '
            'week over week.'
        ),
        'metrics': [
            'High-dollar threshold value ($) displayed in the banner',
            'Total high-dollar AR balance and encounter count',
            'WoW delta for the HD cohort only',
            'Over-90-day % within the HD cohort',
            'HD balance as % of total ATB (concentration ratio)',
            'Per-bucket HD balance breakdown',
        ],
        'why': (
            'High-dollar accounts demand a separate lens because they behave differently from '
            'the general AR population. A single $200K account has more revenue impact than '
            '200 $1K accounts -- it warrants senior staff attention, direct payer contact, '
            'and potentially legal escalation if it ages past 180 days. The HD filter '
            'isolates this cohort so leadership can track its trajectory independently, set '
            'specific HD-focused performance targets, and ensure that high-value accounts are '
            'not hidden by the noise of smaller claims in the overall AR view. The '
            'concentration ratio (HD balance as % of total AR) is a key portfolio risk metric '
            '-- a high concentration means that losing one or two accounts to write-off has '
            'an outsized impact on total revenue.'
        ),
    },
    {
        'file':   '08_hd_denials.png',
        'title':  '08 | High Dollar -- Open Denials (Threshold-Filtered)',
        'accent': RED,
        'what': (
            'This view applies the Open Denials analysis exclusively to high-dollar encounters '
            '(above the computed balance threshold). It shows which denial codes and payers are '
            'responsible for the largest denied balances within the high-dollar cohort -- the '
            'accounts where an unworked denial has the greatest revenue impact. The same Pareto '
            'bar chart, denial group donut, and detail table are presented, scoped to HD '
            'accounts only.'
        ),
        'metrics': [
            'Total denied balance within the HD cohort ($)',
            'HD denial rate as % of HD total AR',
            'Top denial codes by HD denied balance',
            'Payer breakdown of HD denials',
            'HD denial count vs total HD encounter count',
            'Comparison to overall denial rate (context from banner)',
        ],
        'why': (
            'Combining the high-dollar filter with denial analysis creates the highest-priority '
            'work queue in the entire revenue cycle operation: large-balance accounts that are '
            'currently denied and therefore at risk of write-off. A $150K account denied for '
            '"prior authorisation not obtained" with a 60-day appeal window is an emergency -- '
            'it requires same-day escalation to clinical documentation and the payer\'s provider '
            'relations team. This view ensures those accounts are never invisible in the noise '
            'of a large ATB. Regularly reviewing HD denials also reveals whether high-dollar '
            'accounts are being denied at a higher rate than the general population -- a pattern '
            'that often indicates payer-specific tactics targeting large claims for administrative '
            'denial, which should be flagged to contract management.'
        ),
    },
]


class PDF(FPDF):

    def header(self):
        pass

    def footer(self):
        self.set_y(-12)
        self.set_font('Helvetica', 'I', 8)
        self.set_text_color(*SLATE)
        self.cell(0, 6,
                  'ATB KPI Dashboard  *  Revenue Cycle Analytics Portfolio  *  '
                  f'Page {self.page_no()}',
                  align='C')

    # ── Cover page ────────────────────────────────────────────────────────────
    def cover(self):
        self.add_page()
        W, H = self.w, self.h

        self.set_fill_color(*NAVY)
        self.rect(0, 0, W, H, 'F')

        self.set_fill_color(*BLUE)
        self.rect(0, H * 0.38, W, 3, 'F')

        self.set_font('Helvetica', 'B', 42)
        self.set_text_color(*WHITE)
        self.set_xy(0, H * 0.22)
        self.cell(W, 20, 'ATB KPI Dashboard', align='C')

        self.set_font('Helvetica', '', 20)
        self.set_text_color(*BLUE)
        self.set_xy(0, H * 0.30)
        self.cell(W, 12, 'Revenue Cycle Analytics', align='C')
        self.set_xy(0, H * 0.335)
        self.cell(W, 12, 'Accounts Receivable Intelligence Platform', align='C')

        self.set_font('Helvetica', 'I', 13)
        self.set_text_color(148, 163, 184)
        self.set_xy(0, H * 0.42)
        self.cell(W, 10, 'Built with Python  *  Flask  *  Chart.js  *  Pandas', align='C')

        self.set_font('Helvetica', '', 11)
        self.set_text_color(203, 213, 225)
        desc = (
            'An end-to-end revenue cycle analytics platform that transforms weekly ATB Excel '
            'snapshots into actionable intelligence -- tracking AR aging, migration, denial '
            'patterns, and high-dollar exposure across multiple healthcare clients in real time.'
        )
        self.set_xy(30, H * 0.52)
        self.multi_cell(W - 60, 7, desc, align='C')

        features = [
            'WoW Trending', 'Aging Migration', 'ATB Bifurcation',
            'Aging Contributors', 'Open Denials', 'Denial Velocity',
        ]
        pill_w = 44
        total_w = len(features) * pill_w + (len(features) - 1) * 4
        start_x = (W - total_w) / 2
        y_pill = H * 0.68
        for i, feat in enumerate(features):
            x = start_x + i * (pill_w + 4)
            self.set_fill_color(*DARK)
            self.set_draw_color(*BLUE)
            self.rect(x, y_pill, pill_w, 9, 'FD')
            self.set_font('Helvetica', 'B', 7)
            self.set_text_color(*BLUE)
            self.set_xy(x, y_pill + 1.5)
            self.cell(pill_w, 6, feat, align='C')

        self.set_font('Helvetica', 'I', 9)
        self.set_text_color(*SLATE)
        self.set_xy(0, H - 20)
        self.cell(W, 6,
                  'Confidential -- Portfolio Document  *  Client data anonymised',
                  align='C')

    # ── Executive summary ─────────────────────────────────────────────────────
    def exec_summary(self):
        self.add_page()
        W = self.w
        self.set_font('Helvetica', 'B', 22)
        self.set_text_color(*NAVY)
        self.set_xy(18, 16)
        self.cell(0, 12, 'Executive Summary')

        self.set_fill_color(*BLUE)
        self.rect(18, 30, 130, 0.8, 'F')

        self.set_font('Helvetica', '', 11)
        self.set_text_color(*DARK)
        intro = (
            'This dashboard was designed to give revenue cycle leadership and operations teams '
            'a single, consolidated view of AR health across multiple healthcare client '
            'portfolios. It ingests weekly ATB (Aged Trial Balance) Excel files and computes '
            'a suite of interconnected analyses -- from high-level WoW trends to account-level '
            'denial velocity. Each view answers a specific operational question that drives '
            'collection decisions.'
        )
        self.set_xy(18, 34)
        self.multi_cell(W - 36, 6.5, intro)

        summaries = [
            (BLUE,  'WoW Trending',
             'Tracks total AR balance and encounter volume week-over-week, broken down by '
             'aging bucket. The primary leadership KPI -- identifies whether AR is growing '
             'or shrinking and which buckets are accumulating.'),
            (AMBER, 'Aging Migration',
             'Maps how encounters migrate between aging buckets week-over-week. Quantifies '
             'the rollover rate into critical 90+ day territory and tracks new vs resolved '
             'encounters to measure team throughput.'),
            (GREEN, 'ATB Bifurcation',
             'Splits AR into billed vs unbilled and new vs carried-forward. Unbilled AR '
             'faces timely filing write-off risk -- this view makes it visible with its own '
             'aging clock and urgency signals.'),
            (RED,   'Aging Contributors',
             'Ranks the top individual encounters by balance and WoW growth. Operationalises '
             'the Pareto principle -- 20% of accounts drive 80% of balance. Produces a daily '
             'prioritised work list for follow-up teams.'),
            (RED,   'Open Denials',
             'Snapshot of current denied AR by code, group, and payer. Enables triage by '
             'ROI (highest balance first) and identifies whether denials are systemic or '
             'diffuse across many small codes.'),
            (AMBER, 'Denial Velocity',
             'Measures the age of open denials using Last Denial Date. Flags 90+ and 180+ '
             'day denied balance -- the highest-urgency write-off risk in the entire AR '
             'portfolio.'),
        ]

        y = self.get_y() + 6
        for colour, title, text in summaries:
            self.set_fill_color(*colour)
            self.rect(18, y, 3, 16, 'F')
            self.set_font('Helvetica', 'B', 11)
            self.set_text_color(*NAVY)
            self.set_xy(24, y + 1)
            self.cell(0, 6, title)
            self.set_font('Helvetica', '', 10)
            self.set_text_color(51, 65, 85)
            self.set_xy(24, y + 7)
            self.multi_cell(W - 42, 5.5, text)
            y = self.get_y() + 4

    # ── Per-tab pages (screenshot + explanation) ──────────────────────────────
    def tab_page(self, tab):
        img_path = SCREENSHOTS / tab['file']
        if not img_path.exists():
            print(f"  WARNING: screenshot not found: {img_path}")
            return

        # Page 1 -- screenshot
        self.add_page()
        W, H = self.w, self.h

        self.set_fill_color(*tab['accent'])
        self.rect(0, 0, W, 1.5, 'F')

        self.set_fill_color(*NAVY)
        self.rect(0, 1.5, W, 12, 'F')
        self.set_font('Helvetica', 'B', 11)
        self.set_text_color(*WHITE)
        self.set_xy(8, 3)
        self.cell(0, 8, tab['title'])

        with PILImage.open(img_path) as im:
            iw, ih = im.size
        avail_w = W - 16
        avail_h = H - 26
        scale   = min(avail_w / iw, avail_h / ih)
        draw_w  = iw * scale
        draw_h  = ih * scale
        x_img   = (W - draw_w) / 2
        self.image(str(img_path), x=x_img, y=15, w=draw_w, h=draw_h)

        # Page 2 -- explanations
        self.add_page()
        W, H = self.w, self.h

        self.set_fill_color(*tab['accent'])
        self.rect(0, 0, 5, H, 'F')

        self.set_fill_color(*NAVY)
        self.rect(5, 0, W - 5, 14, 'F')
        self.set_font('Helvetica', 'B', 12)
        self.set_text_color(*WHITE)
        self.set_xy(12, 2)
        self.cell(0, 10, tab['title'])

        y = 18

        def section_header(label):
            nonlocal y
            self.set_fill_color(*LGRAY)
            self.rect(8, y, W - 16, 7, 'F')
            self.set_font('Helvetica', 'B', 10)
            self.set_text_color(*tab['accent'])
            self.set_xy(12, y + 0.5)
            self.cell(0, 6, label)
            y += 9

        section_header('WHAT THIS VIEW SHOWS')
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*DARK)
        self.set_xy(12, y)
        self.multi_cell(W - 22, 5.8, tab['what'])
        y = self.get_y() + 5

        section_header('KEY METRICS TRACKED')
        for bullet in tab['metrics']:
            self.set_fill_color(*tab['accent'])
            self.ellipse(12, y + 1.8, 2.2, 2.2, 'F')
            self.set_font('Helvetica', '', 10)
            self.set_text_color(*DARK)
            self.set_xy(17, y)
            self.multi_cell(W - 27, 5.5, bullet)
            y = self.get_y() + 1.5

        y += 3
        section_header('WHY THIS MATTERS -- BUSINESS SIGNIFICANCE')
        self.set_font('Helvetica', '', 10)
        self.set_text_color(*DARK)
        self.set_xy(12, y)
        self.multi_cell(W - 22, 5.8, tab['why'])


# ── Build ─────────────────────────────────────────────────────────────────────
def build_pdf():
    pdf = PDF(orientation='L', unit='mm', format='A4')
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.set_margins(0, 0, 0)

    print('Building PDF...')
    pdf.cover()
    print('  cover page done')

    pdf.exec_summary()
    print('  executive summary done')

    for tab in TABS:
        if (SCREENSHOTS / tab['file']).exists():
            pdf.tab_page(tab)
            print(f"  {tab['file']} done")
        else:
            print(f"  SKIP (no screenshot): {tab['file']}")

    pdf.output(str(OUT_PDF))
    size_kb = OUT_PDF.stat().st_size // 1024
    print(f'\nPDF saved: {OUT_PDF}  ({size_kb} KB)')


if __name__ == '__main__':
    build_pdf()
