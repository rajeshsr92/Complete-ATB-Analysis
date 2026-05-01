import sys, os
_LIB = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'lib'))
if _LIB not in sys.path:
    sys.path.insert(0, _LIB)

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

import pandas as pd

BUCKET_ORDER = [
    'Not Aged', 'DNFB', '0-30', '31-60', '61-90', '91-120',
    '121-150', '151-180', '181-210', '211-240', '241-270',
    '271-300', '301-330', '331-365', '366+'
]

BUCKET_INDEX = {b: i for i, b in enumerate(BUCKET_ORDER)}

OVER_90_BUCKETS = ['91-120', '121-150', '151-180', '181-210', '211-240',
                   '241-270', '271-300', '301-330', '331-365', '366+']
FEEDS_INTO_90  = ['61-90']


def _decat(df):
    """Convert any categorical columns to plain strings in-place (returns copy)."""
    cat_cols = [c for c in df.columns if hasattr(df[c], 'cat')]
    if not cat_cols:
        return df
    df = df.copy()
    for c in cat_cols:
        df[c] = df[c].astype(str)
    return df


def _safe_pct(numer, denom):
    return round(float(numer) / float(denom) * 100, 1) if denom else None


def _fmt_dollar(v):
    v = float(v)
    if abs(v) >= 1_000_000:
        return f'${v/1_000_000:.1f}M'
    if abs(v) >= 1_000:
        return f'${v/1_000:.1f}K'
    return f'${v:,.0f}'


def compute_high_dollar_threshold(df: pd.DataFrame, pct: float = 0.60) -> dict:
    df = _decat(df)
    deduped = df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')
    sorted_bal = deduped['Balance Amount'].sort_values(ascending=False).values
    total = float(sorted_bal.sum())
    if total == 0 or len(sorted_bal) == 0:
        return {'threshold': 0.0, 'enc_count': 0, 'balance': 0.0, 'pct': 0.0,
                'total_balance': round(total, 2), 'pct_target': round(pct * 100, 1)}
    target = total * pct
    cumsum = 0.0
    threshold = float(sorted_bal[-1])
    for bal in sorted_bal:
        cumsum += bal
        if cumsum >= target:
            threshold = float(bal)
            break
    hd = deduped[deduped['Balance Amount'] >= threshold]
    hd_bal = float(hd['Balance Amount'].sum())
    return {
        'threshold': round(threshold, 2),
        'enc_count': int(len(hd)),
        'balance': round(hd_bal, 2),
        'pct': round(hd_bal / total * 100, 1),
        'total_balance': round(total, 2),
        'pct_target': round(pct * 100, 1),
    }


def wow_trending(weekly_data: dict) -> list:
    """WoW trending with per-bucket breakdown and 90+ metrics added."""
    weekly_data = {w: _decat(df) for w, df in weekly_data.items()}
    rows = []
    sorted_weeks = sorted(weekly_data.keys())
    for i, week in enumerate(sorted_weeks):
        df = weekly_data[week]
        count = int(df['Encounter Number'].nunique())
        balance = float(df['Balance Amount'].sum())

        if i > 0:
            prev = sorted_weeks[i - 1]
            p_df = weekly_data[prev]
            p_count = int(p_df['Encounter Number'].nunique())
            p_balance = float(p_df['Balance Amount'].sum())
            d_count = count - p_count
            d_balance = round(balance - p_balance, 2)
            pct_count = _safe_pct(d_count, p_count)
            pct_balance = _safe_pct(d_balance, p_balance)
        else:
            d_count = d_balance = pct_count = pct_balance = None

        # Per-bucket breakdown
        bucket_breakdown = {}
        for b in BUCKET_ORDER:
            sub = df[df['Discharge Aging Category'] == b]
            if len(sub):
                bucket_breakdown[b] = {
                    'count': int(sub['Encounter Number'].nunique()),
                    'balance': round(float(sub['Balance Amount'].sum()), 2),
                }

        over_90_bal = sum(
            bucket_breakdown.get(b, {}).get('balance', 0.0) for b in OVER_90_BUCKETS
        )
        over_90_pct = _safe_pct(over_90_bal, balance)

        rows.append({
            'week': week,
            'encounter_count': count,
            'balance_total': round(balance, 2),
            'wow_count_delta': d_count,
            'wow_count_pct': pct_count,
            'wow_balance_delta': d_balance,
            'wow_balance_pct': pct_balance,
            'bucket_breakdown': bucket_breakdown,
            'over_90_balance': round(over_90_bal, 2),
            'over_90_pct': over_90_pct,
        })
    return rows


def trending_summary(rows: list) -> list:
    """Generate 6 bullet insights from wow_trending rows."""
    if len(rows) < 2:
        return [{'type': 'info', 'text': 'Insufficient weeks of data to compute trend insights.'}]

    points = []
    latest = rows[-1]
    prior = rows[-2]

    # 1. Consecutive streak
    streak = 0
    direction = None
    for r in reversed(rows[1:]):
        delta = r.get('wow_balance_delta')
        if delta is None:
            break
        curr_dir = 'up' if delta > 0 else ('down' if delta < 0 else None)
        if direction is None:
            direction = curr_dir
        if curr_dir == direction:
            streak += 1
        else:
            break

    if streak >= 2 and direction:
        cumulative = sum(
            r.get('wow_balance_delta') or 0 for r in rows[-streak:]
        )
        pct_cum = _safe_pct(cumulative, rows[-streak - 1]['balance_total']) if streak < len(rows) else None
        pct_str = f' ({("+" if cumulative > 0 else "")}{pct_cum}%)' if pct_cum is not None else ''
        word = 'increased' if direction == 'up' else 'decreased'
        points.append({
            'type': 'danger' if direction == 'up' else 'success',
            'text': f'Balance has {word} for {streak} consecutive weeks '
                    f'(cumulative {("+" if cumulative > 0 else "")}{_fmt_dollar(abs(cumulative))}{pct_str}).'
        })
    else:
        delta = latest.get('wow_balance_delta') or 0
        pct = latest.get('wow_balance_pct')
        pct_str = f' ({("+" if delta > 0 else "")}{pct}%)' if pct is not None else ''
        points.append({
            'type': 'danger' if delta > 0 else ('success' if delta < 0 else 'info'),
            'text': f'Balance {"increased" if delta > 0 else ("decreased" if delta < 0 else "unchanged")} '
                    f'this week by {_fmt_dollar(abs(delta))}{pct_str} vs prior week.'
        })

    # 2. Dataset high/low
    max_row = max(rows, key=lambda r: r['balance_total'])
    min_row = min(rows, key=lambda r: r['balance_total'])
    n = len(rows)
    if latest['balance_total'] == max_row['balance_total']:
        points.append({
            'type': 'danger',
            'text': f'Current week is the dataset high across all {n} weeks at {_fmt_dollar(latest["balance_total"])}.'
        })
    elif latest['balance_total'] == min_row['balance_total']:
        points.append({
            'type': 'success',
            'text': f'Current week is the dataset low across all {n} weeks at {_fmt_dollar(latest["balance_total"])}.'
        })
    else:
        points.append({
            'type': 'info',
            'text': f'{n}-week range: low {_fmt_dollar(min_row["balance_total"])} ({min_row["week"]}) '
                    f'— high {_fmt_dollar(max_row["balance_total"])} ({max_row["week"]}).'
        })

    # 3. Acceleration
    if len(rows) >= 3:
        this_delta = latest.get('wow_balance_delta') or 0
        prev_delta = prior.get('wow_balance_delta') or 0
        if this_delta != 0 and prev_delta != 0 and (this_delta > 0) == (prev_delta > 0):
            if abs(this_delta) > abs(prev_delta):
                points.append({
                    'type': 'danger' if this_delta > 0 else 'success',
                    'text': f'Balance movement is accelerating — this week\'s change '
                            f'({_fmt_dollar(this_delta)}) exceeded last week\'s ({_fmt_dollar(prev_delta)}).'
                })
            else:
                points.append({
                    'type': 'success' if this_delta > 0 else 'info',
                    'text': f'Balance movement is decelerating — this week\'s change '
                            f'({_fmt_dollar(this_delta)}) was smaller than last week\'s ({_fmt_dollar(prev_delta)}).'
                })

    # 4. 90+ share trend
    latest_pct = latest.get('over_90_pct')
    lookback = rows[-5] if len(rows) >= 5 else rows[0]
    old_pct = lookback.get('over_90_pct')
    if latest_pct is not None and old_pct is not None:
        diff = round(latest_pct - old_pct, 1)
        weeks_back = min(4, len(rows) - 1)
        sign = '+' if diff > 0 else ''
        points.append({
            'type': 'danger' if diff > 0 else 'success',
            'text': f'90+ share of total balance: {latest_pct}% now vs {old_pct}% '
                    f'{weeks_back} week{"s" if weeks_back > 1 else ""} ago ({sign}{diff}pp).'
        })

    # 5. Encounter/balance divergence
    curr_enc = latest['encounter_count']
    prev_enc = prior['encounter_count']
    curr_bal = latest['balance_total']
    prev_bal = prior['balance_total']
    if curr_enc < prev_enc and curr_bal > prev_bal and curr_enc > 0:
        avg_bal = round(curr_bal / curr_enc, 0)
        prev_avg = round(prev_bal / prev_enc, 0) if prev_enc else 0
        diff_avg = avg_bal - prev_avg
        points.append({
            'type': 'warning',
            'text': f'Encounter count fell ({prev_enc:,} → {curr_enc:,}) while balance rose — '
                    f'average balance per encounter is ${avg_bal:,.0f} (up ${diff_avg:,.0f}).'
        })
    elif curr_enc > 0:
        avg_bal = round(curr_bal / curr_enc, 0)
        points.append({
            'type': 'info',
            'text': f'Average balance per encounter this week: ${avg_bal:,.0f} '
                    f'({curr_enc:,} encounters, {_fmt_dollar(curr_bal)} total).'
        })

    # 6. Momentum score
    up_weeks = sum(1 for r in rows[1:] if (r.get('wow_balance_delta') or 0) > 0)
    total_comparable = len(rows) - 1
    if total_comparable > 0:
        points.append({
            'type': 'danger' if up_weeks / total_comparable >= 0.6 else (
                'success' if up_weeks / total_comparable <= 0.3 else 'info'),
            'text': f'Balance increased in {up_weeks} of {total_comparable} measurable weeks '
                    f'({round(up_weeks / total_comparable * 100)}% upward momentum).'
        })

    return points


def aging_migration(week_a_df: pd.DataFrame, week_b_df: pd.DataFrame) -> dict:
    week_a_df, week_b_df = _decat(week_a_df), _decat(week_b_df)

    def _dedup(df):
        return (df[['Encounter Number', 'Discharge Aging Category', 'Balance Amount']]
                .sort_values('Balance Amount', ascending=False)
                .drop_duplicates('Encounter Number'))

    a = _dedup(week_a_df).copy()
    b = _dedup(week_b_df).copy()
    a.columns = ['enc', 'from_bucket', 'from_balance']
    b.columns = ['enc', 'to_bucket', 'to_balance']

    merged = pd.merge(a, b, on='enc', how='inner')

    new_b = b[~b['enc'].isin(a['enc'])].rename(columns={'enc': 'Encounter Number', 'to_bucket': 'Discharge Aging Category', 'to_balance': 'Balance Amount'})
    resolved_a = a[~a['enc'].isin(b['enc'])].rename(columns={'enc': 'Encounter Number', 'from_bucket': 'Discharge Aging Category', 'from_balance': 'Balance Amount'})
    new_encs = new_b
    resolved_encs = resolved_a

    all_buckets_in_data = set(merged['from_bucket'].unique()) | set(merged['to_bucket'].unique())
    buckets = [bkt for bkt in BUCKET_ORDER if bkt in all_buckets_in_data]
    unknown = sorted(all_buckets_in_data - set(BUCKET_ORDER))
    buckets = buckets + unknown

    matrix = {}
    for fb in buckets:
        sub = merged[merged['from_bucket'] == fb]
        from_total_bal = float(sub['to_balance'].sum())
        from_total_cnt = len(sub)
        matrix[fb] = {}
        for tb in buckets:
            cell = sub[sub['to_bucket'] == tb]
            val = float(cell['to_balance'].sum())
            cnt = int(len(cell))
            pct_bal = _safe_pct(val, from_total_bal)
            pct_cnt = _safe_pct(cnt, from_total_cnt)
            matrix[fb][tb] = {
                'value': round(val, 2),
                'count': cnt,
                'pct': pct_bal,
                'pct_count': pct_cnt,
            }

    def _bucket_summary(df):
        if df.empty:
            return []
        g = df.groupby('Discharge Aging Category').agg(
            count=('Encounter Number', 'count'),
            balance=('Balance Amount', 'sum')
        ).reset_index()
        return [{'bucket': r['Discharge Aging Category'], 'count': int(r['count']),
                 'balance': round(float(r['balance']), 2)} for _, r in g.iterrows()]

    stayed = merged[merged['from_bucket'] == merged['to_bucket']]
    moved_worse = merged.apply(
        lambda r: BUCKET_INDEX.get(r['to_bucket'], 99) > BUCKET_INDEX.get(r['from_bucket'], 99), axis=1
    )
    aged_out = merged[moved_worse]

    return {
        'buckets': buckets,
        'matrix': matrix,
        'summary': {
            'total_continued': int(len(merged)),
            'continued_balance': round(float(merged['to_balance'].sum()), 2),
            'stayed_count': int(len(stayed)),
            'stayed_balance': round(float(stayed['to_balance'].sum()), 2),
            'aged_worse_count': int(len(aged_out)),
            'aged_worse_balance': round(float(aged_out['to_balance'].sum()), 2),
        },
        'new_encounters': {
            'count': int(len(new_encs)),
            'balance': round(float(new_encs['Balance Amount'].sum()), 2),
            'by_bucket': _bucket_summary(new_encs),
        },
        'resolved_encounters': {
            'count': int(len(resolved_encs)),
            'balance': round(float(resolved_encs['Balance Amount'].sum()), 2),
            'by_bucket': _bucket_summary(resolved_encs),
        },
    }


def rollover_summary(migration_data: dict) -> list:
    """Generate 6 bullet insights from aging_migration result."""
    points = []
    summary = migration_data.get('summary', {})
    new_enc = migration_data.get('new_encounters', {})
    resolved_enc = migration_data.get('resolved_encounters', {})
    matrix = migration_data.get('matrix', {})
    buckets = migration_data.get('buckets', [])

    continued = summary.get('total_continued', 0)
    resolved_cnt = resolved_enc.get('count', 0)
    aged_worse_cnt = summary.get('aged_worse_count', 0)
    aged_worse_bal = summary.get('aged_worse_balance', 0.0)
    continued_bal = summary.get('continued_balance', 0.0)
    new_bal = new_enc.get('balance', 0.0)
    resolved_bal = resolved_enc.get('balance', 0.0)

    # 1. Retention rate
    total_prior = continued + resolved_cnt
    ret_pct = _safe_pct(continued, total_prior)
    if ret_pct is not None:
        points.append({
            'type': 'warning' if ret_pct > 80 else 'info',
            'text': f'{ret_pct}% of prior-week encounters were still on the ATB this week '
                    f'({continued:,} retained, {resolved_cnt:,} resolved/dropped).'
        })

    # 2. Net aging direction
    improved_cnt = 0
    for fb in buckets:
        for tb in buckets:
            if BUCKET_INDEX.get(tb, 99) < BUCKET_INDEX.get(fb, 99):
                improved_cnt += matrix.get(fb, {}).get(tb, {}).get('count', 0)
    net = aged_worse_cnt - improved_cnt
    direction = 'deteriorating' if net > 0 else 'improving'
    points.append({
        'type': 'danger' if net > 0 else 'success',
        'text': f'Net aging direction is {direction}: {aged_worse_cnt:,} encounters aged into older buckets '
                f'vs {improved_cnt:,} that moved to younger buckets (net {abs(net):,} {"worse" if net > 0 else "better"}).'
    })

    # 3. Largest single migration flow (worst off-diagonal movement)
    max_flow = {'val': 0.0, 'from': None, 'to': None, 'cnt': 0}
    for fb in buckets:
        for tb in buckets:
            if BUCKET_INDEX.get(tb, 99) > BUCKET_INDEX.get(fb, 99):
                cell = matrix.get(fb, {}).get(tb, {})
                if cell.get('value', 0) > max_flow['val']:
                    max_flow = {'val': cell['value'], 'from': fb, 'to': tb, 'cnt': cell.get('count', 0)}
    if max_flow['from']:
        points.append({
            'type': 'danger',
            'text': f'Largest aging movement: {_fmt_dollar(max_flow["val"])} ({max_flow["cnt"]:,} enc) '
                    f'flowed from {max_flow["from"]} → {max_flow["to"]}.'
        })

    # 4. % balance that worsened
    worsened_pct = _safe_pct(aged_worse_bal, continued_bal)
    if worsened_pct is not None:
        points.append({
            'type': 'danger' if worsened_pct > 20 else 'warning',
            'text': f'{worsened_pct}% of continued balance ({_fmt_dollar(aged_worse_bal)}) '
                    f'aged into older buckets this week.'
        })

    # 5. New vs resolved net inflow
    net_inflow = new_bal - resolved_bal
    points.append({
        'type': 'danger' if net_inflow > 0 else 'success',
        'text': f'New encounters ({_fmt_dollar(new_bal)}) {"exceeded" if net_inflow > 0 else "fell short of"} '
                f'resolved encounters ({_fmt_dollar(resolved_bal)}) — '
                f'net ATB {"inflow" if net_inflow > 0 else "reduction"} of {_fmt_dollar(abs(net_inflow))}.'
    })

    # 6. New encounters entering 90+ directly
    new_by_bucket = {r['bucket']: r for r in new_enc.get('by_bucket', [])}
    direct_90_bal = sum(new_by_bucket.get(b, {}).get('balance', 0.0) for b in OVER_90_BUCKETS)
    direct_90_cnt = sum(new_by_bucket.get(b, {}).get('count', 0) for b in OVER_90_BUCKETS)
    if direct_90_cnt > 0:
        points.append({
            'type': 'danger',
            'text': f'{direct_90_cnt:,} new encounters ({_fmt_dollar(direct_90_bal)}) entered the ATB '
                    f'directly into 90+ day buckets this week.'
        })
    else:
        points.append({
            'type': 'success',
            'text': 'No new encounters entered directly into 90+ day buckets this week.'
        })

    return points


def atb_bifurcation(latest_df: pd.DataFrame, prior_df: pd.DataFrame) -> dict:
    """Latest ATB vs prior week: counts + balances by Discharge Aging Category."""
    latest_df, prior_df = _decat(latest_df), _decat(prior_df)

    def _summarize(df):
        if df.empty:
            return {}
        g = df.groupby('Discharge Aging Category').agg(
            count=('Encounter Number', 'count'),
            balance=('Balance Amount', 'sum')
        )
        return {idx: {'count': int(row['count']), 'balance': round(float(row['balance']), 2)}
                for idx, row in g.iterrows()}

    latest = _summarize(latest_df)
    prior = _summarize(prior_df)

    all_buckets_in_data = set(latest.keys()) | set(prior.keys())
    buckets = [b for b in BUCKET_ORDER if b in all_buckets_in_data]
    unknown = sorted(all_buckets_in_data - set(BUCKET_ORDER))
    buckets = buckets + unknown

    latest_encs = set(latest_df['Encounter Number'].dropna())
    prior_encs = set(prior_df['Encounter Number'].dropna())
    carried_df = latest_df[latest_df['Encounter Number'].isin(prior_encs)]
    new_df = latest_df[~latest_df['Encounter Number'].isin(prior_encs)]

    carried = _summarize(carried_df)
    new_this = _summarize(new_df)

    rows = []
    for b in buckets:
        l = latest.get(b, {'count': 0, 'balance': 0.0})
        p = prior.get(b, {'count': 0, 'balance': 0.0})
        d_count = l['count'] - p['count']
        d_balance = round(l['balance'] - p['balance'], 2)
        rows.append({
            'bucket': b,
            'current_count': l['count'],
            'current_balance': l['balance'],
            'prior_count': p['count'],
            'prior_balance': p['balance'],
            'delta_count': d_count,
            'delta_balance': d_balance,
            'delta_pct': _safe_pct(d_balance, p['balance']),
            'carried_count': carried.get(b, {}).get('count', 0),
            'carried_balance': carried.get(b, {}).get('balance', 0.0),
            'new_count': new_this.get(b, {}).get('count', 0),
            'new_balance': new_this.get(b, {}).get('balance', 0.0),
        })

    total_l = {'count': int(latest_df['Encounter Number'].count()),
               'balance': round(float(latest_df['Balance Amount'].sum()), 2)}
    total_p = {'count': int(prior_df['Encounter Number'].count()),
               'balance': round(float(prior_df['Balance Amount'].sum()), 2)}

    return {
        'buckets': buckets,
        'rows': rows,
        'totals': {'current': total_l, 'prior': total_p},
        'carried_forward_total': {'count': int(len(carried_df)),
                                  'balance': round(float(carried_df['Balance Amount'].sum()), 2)},
        'new_this_week_total': {'count': int(len(new_df)),
                                'balance': round(float(new_df['Balance Amount'].sum()), 2)},
    }


def unbilled_analysis(curr_df: pd.DataFrame, prior_df: pd.DataFrame) -> dict:
    """DNFB vs billed breakdown and full Unbilled Aging Category distribution."""
    curr_df, prior_df = _decat(curr_df), _decat(prior_df)

    def _dedup(df):
        return df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')

    curr = _dedup(curr_df)
    prior = _dedup(prior_df)

    if 'Unbilled Aging Category' not in curr.columns:
        return {'available': False}

    total_bal = float(curr['Balance Amount'].sum())
    total_prior_bal = float(prior['Balance Amount'].sum()) if 'Unbilled Aging Category' in prior.columns else 0.0

    def _group_stats(df, col_filter=None):
        if col_filter is not None:
            sub = df[df['Unbilled Aging Category'] == col_filter]
        else:
            sub = df[df['Unbilled Aging Category'].notna() & (df['Unbilled Aging Category'] != 'nan')]
        return int(len(sub)), round(float(sub['Balance Amount'].sum()), 2)

    curr_dnfb_cnt, curr_dnfb_bal = _group_stats(curr, 'DNFB')
    prior_dnfb_cnt, prior_dnfb_bal = _group_stats(prior, 'DNFB') if 'Unbilled Aging Category' in prior.columns else (0, 0.0)

    curr_non_cnt, curr_non_bal = _group_stats(
        curr[curr['Unbilled Aging Category'] != 'DNFB'], None
    ) if 'Unbilled Aging Category' in curr.columns else (0, 0.0)

    non_sub_prior = prior[prior['Unbilled Aging Category'] != 'DNFB'] if 'Unbilled Aging Category' in prior.columns else prior.iloc[0:0]
    prior_non_cnt, prior_non_bal = _group_stats(non_sub_prior, None)

    def _build_stat(cc, cb, pc, pb, total):
        d_bal = round(cb - pb, 2)
        return {
            'curr_count': cc, 'curr_balance': cb,
            'prior_count': pc, 'prior_balance': round(pb, 2),
            'delta_balance': d_bal,
            'delta_pct': _safe_pct(d_bal, pb),
            'pct_of_total': _safe_pct(cb, total),
        }

    # Full unbilled distribution
    valid = curr[curr['Unbilled Aging Category'].notna() & (curr['Unbilled Aging Category'] != 'nan')]
    unbilled_total_bal = float(valid['Balance Amount'].sum())

    g = valid.groupby('Unbilled Aging Category').agg(
        count=('Encounter Number', 'count'),
        balance=('Balance Amount', 'sum')
    ).reset_index()

    prior_by_bucket = {}
    if 'Unbilled Aging Category' in prior.columns:
        valid_p = prior[prior['Unbilled Aging Category'].notna() & (prior['Unbilled Aging Category'] != 'nan')]
        pg = valid_p.groupby('Unbilled Aging Category').agg(balance=('Balance Amount', 'sum')).reset_index()
        prior_by_bucket = {r['Unbilled Aging Category']: round(float(r['balance']), 2) for _, r in pg.iterrows()}

    # Sort rows by BUCKET_ORDER then alpha
    def _bucket_sort_key(bkt):
        return (BUCKET_INDEX.get(bkt, 999), bkt)

    rows = []
    for _, r in g.sort_values('Unbilled Aging Category', key=lambda s: s.map(_bucket_sort_key)).iterrows():
        bkt = r['Unbilled Aging Category']
        cb = round(float(r['balance']), 2)
        pb = prior_by_bucket.get(bkt, 0.0)
        d_bal = round(cb - pb, 2)
        rows.append({
            'bucket': bkt,
            'count': int(r['count']),
            'balance': cb,
            'pct_of_total': _safe_pct(cb, unbilled_total_bal),
            'prior_balance': pb,
            'delta_balance': d_bal,
            'delta_pct': _safe_pct(d_bal, pb),
        })

    return {
        'available': True,
        'dnfb': _build_stat(curr_dnfb_cnt, curr_dnfb_bal, prior_dnfb_cnt, prior_dnfb_bal, total_bal),
        'non_dnfb': _build_stat(curr_non_cnt, curr_non_bal, prior_non_cnt, prior_non_bal, total_bal),
        'rows': rows,
        'total_unbilled_balance': round(unbilled_total_bal, 2),
        'total_unbilled_count': int(len(valid)),
        'unbilled_pct_of_atb': _safe_pct(unbilled_total_bal, total_bal),
    }


def bifurcation_summary(bifur_data: dict, unbilled_data: dict) -> list:
    """Generate 6 bullet insights from atb_bifurcation + unbilled_analysis results."""
    points = []
    rows = bifur_data.get('rows', [])
    totals = bifur_data.get('totals', {})
    carried = bifur_data.get('carried_forward_total', {})
    new_total = bifur_data.get('new_this_week_total', {})
    curr_total_bal = totals.get('current', {}).get('balance', 0.0)

    # 1. Carry-forward rate
    cf_bal = carried.get('balance', 0.0)
    cf_pct = _safe_pct(cf_bal, curr_total_bal)
    new_bal = new_total.get('balance', 0.0)
    if cf_pct is not None:
        points.append({
            'type': 'info',
            'text': f'{cf_pct}% of current ATB balance ({_fmt_dollar(cf_bal)}) is carried forward '
                    f'from prior week — {_fmt_dollar(new_bal)} is newly introduced this week.'
        })

    # 2. Fastest growing bucket by delta_pct
    growing = [r for r in rows if (r.get('delta_pct') or 0) > 0 and r.get('prior_balance', 0) > 0]
    if growing:
        fastest = max(growing, key=lambda r: r.get('delta_pct') or 0)
        points.append({
            'type': 'danger',
            'text': f'Fastest-growing bucket: {fastest["bucket"]} — up {fastest["delta_pct"]}% '
                    f'({_fmt_dollar(fastest["delta_balance"])}) vs prior week.'
        })

    # 3. 90+ concentration
    over_90_bal = sum(r['current_balance'] for r in rows if r['bucket'] in OVER_90_BUCKETS)
    over_90_pct = _safe_pct(over_90_bal, curr_total_bal)
    if over_90_pct is not None:
        points.append({
            'type': 'danger' if over_90_pct > 40 else 'warning',
            'text': f'90+ day buckets represent {over_90_pct}% of total ATB balance ({_fmt_dollar(over_90_bal)}).'
        })

    # 4. DNFB share from unbilled data
    if unbilled_data.get('available'):
        dnfb = unbilled_data.get('dnfb', {})
        dnfb_bal = dnfb.get('curr_balance', 0.0)
        dnfb_pct = dnfb.get('pct_of_total')
        dnfb_delta = dnfb.get('delta_balance', 0.0)
        direction = 'increasing' if dnfb_delta > 0 else 'decreasing'
        if dnfb_pct is not None:
            points.append({
                'type': 'warning' if dnfb_pct > 20 else ('success' if dnfb_delta < 0 else 'info'),
                'text': f'DNFB accounts for {dnfb_pct}% of total ATB ({_fmt_dollar(dnfb_bal)}) '
                        f'— {direction} by {_fmt_dollar(abs(dnfb_delta))} vs prior week.'
            })

    # 5. Bucket with highest new-encounter concentration
    new_heavy = [r for r in rows if r.get('current_balance', 0) > 0]
    if new_heavy:
        def _new_ratio(r):
            return r.get('new_balance', 0) / r['current_balance'] if r['current_balance'] else 0
        most_new = max(new_heavy, key=_new_ratio)
        ratio_pct = round(_new_ratio(most_new) * 100, 1)
        if ratio_pct > 0:
            points.append({
                'type': 'info',
                'text': f'The {most_new["bucket"]} bucket is {ratio_pct}% net-new this week '
                        f'({_fmt_dollar(most_new["new_balance"])}) — highest new-encounter concentration.'
            })

    # 6. Largest absolute balance bucket
    if rows:
        largest = max(rows, key=lambda r: r.get('current_balance', 0))
        lg_pct = _safe_pct(largest['current_balance'], curr_total_bal)
        points.append({
            'type': 'info',
            'text': f'Largest balance bucket: {largest["bucket"]} at {_fmt_dollar(largest["current_balance"])} '
                    f'({lg_pct}% of total ATB).'
        })

    return points


def balance_group_breakdown(curr_df: pd.DataFrame, prior_df: pd.DataFrame) -> dict:
    """IP/OP (Balance Group) breakdown by encounters and balance."""
    curr_df, prior_df = _decat(curr_df), _decat(prior_df)

    if 'Balance Group' not in curr_df.columns:
        return {'available': False, 'groups': [], 'by_bucket': {}, 'total_balance': 0.0}

    total_bal = float(curr_df['Balance Amount'].sum())

    def _agg(df):
        return df.groupby('Balance Group').agg(
            count=('Encounter Number', 'count'),
            balance=('Balance Amount', 'sum')
        ).reset_index()

    cg = _agg(curr_df)
    pg = _agg(prior_df) if 'Balance Group' in prior_df.columns else curr_df.iloc[0:0].pipe(_agg)

    merged = pd.merge(cg, pg, on='Balance Group', how='outer', suffixes=('_c', '_p')).fillna(0)
    merged['delta_balance'] = merged['balance_c'] - merged['balance_p']
    merged = merged.sort_values('balance_c', ascending=False)

    groups = []
    for _, r in merged.iterrows():
        d_bal = round(float(r['delta_balance']), 2)
        groups.append({
            'name': str(r['Balance Group']),
            'curr_count': int(r['count_c']),
            'curr_balance': round(float(r['balance_c']), 2),
            'prior_count': int(r['count_p']),
            'prior_balance': round(float(r['balance_p']), 2),
            'delta_balance': d_bal,
            'delta_pct': _safe_pct(d_bal, float(r['balance_p'])),
            'pct_of_total': _safe_pct(float(r['balance_c']), total_bal),
        })

    # Cross-tab: group × discharge bucket
    by_bucket = {}
    if 'Discharge Aging Category' in curr_df.columns:
        cross = curr_df.groupby(['Balance Group', 'Discharge Aging Category']).agg(
            count=('Encounter Number', 'count'),
            balance=('Balance Amount', 'sum')
        ).reset_index()
        for grp_name, sub in cross.groupby('Balance Group'):
            bucket_rows = []
            for _, row in sub.iterrows():
                bucket_rows.append({
                    'bucket': row['Discharge Aging Category'],
                    'count': int(row['count']),
                    'balance': round(float(row['balance']), 2),
                })
            bucket_rows.sort(key=lambda x: BUCKET_INDEX.get(x['bucket'], 999))
            by_bucket[str(grp_name)] = bucket_rows

    return {
        'available': True,
        'groups': groups,
        'by_bucket': by_bucket,
        'total_balance': round(total_bal, 2),
    }


def aging_velocity(curr_df: pd.DataFrame) -> dict:
    """True days outstanding from Discharge Date vs REPORT_DATE per financial class."""
    curr_df = _decat(curr_df)

    if 'Discharge Date' not in curr_df.columns or 'REPORT_DATE' not in curr_df.columns:
        return {'available': False}

    df = curr_df.copy()
    df['discharge_dt'] = pd.to_datetime(df['Discharge Date'], errors='coerce')
    df['report_dt'] = pd.to_datetime(df['REPORT_DATE'], errors='coerce')
    df['days_outstanding'] = (df['report_dt'] - df['discharge_dt']).dt.days

    total_count = int(df['Encounter Number'].nunique())
    df = df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')
    df = df[df['days_outstanding'].notna() & (df['days_outstanding'] > 0)]
    valid_count = int(len(df))

    if valid_count == 0:
        return {'available': True, 'summary': {
            'avg_days': None, 'median_days': None, 'pct_over_90': None,
            'pct_over_180': None, 'valid_count': 0, 'total_count': total_count
        }, 'by_fin_class': []}

    avg_days = round(float(df['days_outstanding'].mean()), 1)
    median_days = round(float(df['days_outstanding'].median()), 1)
    pct_over_90 = _safe_pct((df['days_outstanding'] >= 90).sum(), valid_count)
    pct_over_180 = _safe_pct((df['days_outstanding'] >= 180).sum(), valid_count)

    by_rfc = df.groupby('Responsible Financial Class').agg(
        count=('Encounter Number', 'count'),
        avg_days=('days_outstanding', 'mean'),
        median_days=('days_outstanding', 'median'),
        max_days=('days_outstanding', 'max'),
        balance=('Balance Amount', 'sum')
    ).reset_index().sort_values('avg_days', ascending=False)

    rfc_rows = []
    for _, r in by_rfc.iterrows():
        rfc_rows.append({
            'name': str(r['Responsible Financial Class']),
            'count': int(r['count']),
            'avg_days': round(float(r['avg_days']), 1),
            'median_days': round(float(r['median_days']), 1),
            'max_days': int(r['max_days']),
            'balance': round(float(r['balance']), 2),
        })

    return {
        'available': True,
        'summary': {
            'avg_days': avg_days,
            'median_days': median_days,
            'pct_over_90': pct_over_90,
            'pct_over_180': pct_over_180,
            'valid_count': valid_count,
            'total_count': total_count,
        },
        'by_fin_class': rfc_rows,
    }


def aging_contributors(latest_df: pd.DataFrame, prior_df: pd.DataFrame, top_n: int = 15) -> dict:
    """90+ aging contributor analysis — expanded to 9 key insights."""
    def _dedup(df):
        return df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')

    latest_df, prior_df = _decat(latest_df), _decat(prior_df)

    curr_90  = latest_df[latest_df['Discharge Aging Category'].isin(OVER_90_BUCKETS)]
    prev_90  = prior_df[prior_df['Discharge Aging Category'].isin(OVER_90_BUCKETS)]
    prev_feed = prior_df[prior_df['Discharge Aging Category'].isin(FEEDS_INTO_90)]

    c_bal  = float(curr_90['Balance Amount'].sum())
    p_bal  = float(prev_90['Balance Amount'].sum())
    c_cnt  = int(_dedup(curr_90)['Encounter Number'].nunique())
    p_cnt  = int(_dedup(prev_90)['Encounter Number'].nunique())

    rolled_merge = pd.merge(
        _dedup(prev_feed)[['Encounter Number', 'Responsible Financial Class',
                            'Responsible Health Plan', 'Balance Amount']]
            .rename(columns={'Balance Amount': 'prev_bal',
                             'Responsible Financial Class': 'rfc',
                             'Responsible Health Plan': 'rhp'}),
        _dedup(latest_df)[latest_df['Discharge Aging Category'].astype(str).isin(OVER_90_BUCKETS)]
            [['Encounter Number', 'Balance Amount', 'Discharge Aging Category']]
            .rename(columns={'Balance Amount': 'curr_bal', 'Discharge Aging Category': 'curr_bucket'}),
        on='Encounter Number', how='inner'
    )

    rolled_cnt = int(len(rolled_merge))
    rolled_bal = round(float(rolled_merge['curr_bal'].sum()), 2)

    def _by_group(col, df_curr, df_prev, top=None):
        c = df_curr.groupby(col).agg(
            curr_count=('Encounter Number', 'count'),
            curr_balance=('Balance Amount', 'sum')
        ).reset_index()
        p = df_prev.groupby(col).agg(
            prev_count=('Encounter Number', 'count'),
            prev_balance=('Balance Amount', 'sum')
        ).reset_index()
        mg = pd.merge(c, p, on=col, how='outer').fillna(0)
        mg['delta_balance'] = mg['curr_balance'] - mg['prev_balance']
        mg['delta_pct'] = mg.apply(
            lambda r: round(float(r['delta_balance']) / float(r['prev_balance']) * 100, 1)
                      if r['prev_balance'] else None, axis=1)
        mg = mg.sort_values('curr_balance', ascending=False)
        if top:
            mg = mg.head(top)
        rows = []
        for _, r in mg.iterrows():
            rows.append({
                'name': str(r[col]),
                'curr_count': int(r['curr_count']),
                'curr_balance': round(float(r['curr_balance']), 2),
                'prev_count': int(r['prev_count']),
                'prev_balance': round(float(r['prev_balance']), 2),
                'delta_balance': round(float(r['delta_balance']), 2),
                'delta_pct': r['delta_pct'],
            })
        return rows

    by_rfc = _by_group('Responsible Financial Class', curr_90, prev_90)
    by_rhp = _by_group('Responsible Health Plan', curr_90, prev_90, top=top_n)

    rolled_by_rfc = []
    if not rolled_merge.empty:
        rb = rolled_merge.groupby('rfc').agg(
            count=('Encounter Number', 'count'),
            balance=('curr_bal', 'sum')
        ).reset_index().sort_values('balance', ascending=False)
        for _, r in rb.iterrows():
            rolled_by_rfc.append({'name': str(r['rfc']),
                                   'count': int(r['count']),
                                   'balance': round(float(r['balance']), 2)})

    # ── Key points (expanded: 4 original + 5 new = up to 9) ──────
    key_points = []
    delta = round(c_bal - p_bal, 2)
    delta_pct = _safe_pct(delta, p_bal)

    # 1. 90+ balance WoW
    key_points.append({
        'type': 'danger' if delta > 0 else 'success',
        'text': f'90+ balance {"increased" if delta > 0 else "decreased"} by '
                f'${abs(delta):,.0f} ({("+" if delta > 0 else "")}{delta_pct}%) this week.'
    })

    # 2. Rolled-in volume
    key_points.append({
        'type': 'warning',
        'text': f'{rolled_cnt:,} encounters (${rolled_bal:,.0f}) rolled from 61-90 days into 90+ territory.'
    })

    # 3. Top fin class contributor
    if by_rfc:
        top_rfc = by_rfc[0]
        key_points.append({
            'type': 'info',
            'text': f'Largest 90+ contributor by Fin. Class: '
                    f'{top_rfc["name"]} — ${top_rfc["curr_balance"]:,.0f} '
                    f'({round(top_rfc["curr_balance"] / c_bal * 100, 1) if c_bal else 0}% of total 90+).'
        })

    # 4. Biggest health plan jump
    if by_rhp:
        biggest_increase = max(by_rhp, key=lambda r: r['delta_balance'], default=None)
        if biggest_increase and biggest_increase['delta_balance'] > 0:
            key_points.append({
                'type': 'danger',
                'text': f'Highest 90+ balance jump: {biggest_increase["name"]} '
                        f'+${biggest_increase["delta_balance"]:,.0f}'
                        f'{(" (" + str(biggest_increase["delta_pct"]) + "%)") if biggest_increase["delta_pct"] else ""}.'
            })

    # 5. Top-3 health plan concentration
    all_rhp = _by_group('Responsible Health Plan', curr_90, prev_90)
    if len(all_rhp) >= 3 and c_bal > 0:
        top3_bal = sum(r['curr_balance'] for r in all_rhp[:3])
        top3_pct = round(top3_bal / c_bal * 100, 1)
        names = ', '.join(r['name'] for r in all_rhp[:3])
        key_points.append({
            'type': 'warning',
            'text': f'Top 3 health plans account for {top3_pct}% of 90+ balance '
                    f'({_fmt_dollar(top3_bal)}): {names}.'
        })

    # 6. Most improved fin class
    improved = [r for r in by_rfc if r['delta_balance'] < 0]
    if improved:
        best = min(improved, key=lambda r: r['delta_balance'])
        key_points.append({
            'type': 'success',
            'text': f'Most improved financial class: {best["name"]} — 90+ balance reduced by '
                    f'{_fmt_dollar(abs(best["delta_balance"]))} ({best["delta_pct"]}%).'
        })

    # 7. 61-90 at-risk pool (next week's rollover candidates)
    at_risk = latest_df[latest_df['Discharge Aging Category'] == '61-90']
    at_risk_cnt = int(_dedup(at_risk)['Encounter Number'].nunique())
    at_risk_bal = round(float(at_risk['Balance Amount'].sum()), 2)
    if at_risk_cnt > 0:
        key_points.append({
            'type': 'warning',
            'text': f'{at_risk_cnt:,} encounters ({_fmt_dollar(at_risk_bal)}) currently in 61-90 days '
                    f'are at risk of entering 90+ next week.'
        })

    # 8. Highest average balance per encounter by fin class
    if by_rfc:
        avg_bal_list = [(r, r['curr_balance'] / r['curr_count']) for r in by_rfc if r['curr_count'] > 0]
        if avg_bal_list:
            top_avg = max(avg_bal_list, key=lambda x: x[1])
            key_points.append({
                'type': 'info',
                'text': f'Highest average 90+ balance per encounter: {top_avg[0]["name"]} '
                        f'at {_fmt_dollar(top_avg[1])} per encounter ({top_avg[0]["curr_count"]:,} enc).'
            })

    # 9. 61-90 → 90+ crossover rate
    prev_61_90_cnt = int(_dedup(prev_feed)['Encounter Number'].nunique())
    if prev_61_90_cnt > 0:
        crossover_pct = _safe_pct(rolled_cnt, prev_61_90_cnt)
        key_points.append({
            'type': 'danger' if (crossover_pct or 0) > 50 else 'warning',
            'text': f'{crossover_pct}% of last week\'s 61-90 encounters '
                    f'({rolled_cnt:,} of {prev_61_90_cnt:,}) crossed into 90+ this week.'
        })

    return {
        'summary': {
            'curr_balance': round(c_bal, 2), 'prev_balance': round(p_bal, 2),
            'delta_balance': round(c_bal - p_bal, 2),
            'delta_pct': _safe_pct(c_bal - p_bal, p_bal),
            'curr_count': c_cnt, 'prev_count': p_cnt,
            'rolled_in_count': rolled_cnt, 'rolled_in_balance': rolled_bal,
        },
        'key_points': key_points,
        'by_fin_class': by_rfc,
        'by_health_plan': by_rhp,
        'rolled_by_fin_class': rolled_by_rfc,
    }
