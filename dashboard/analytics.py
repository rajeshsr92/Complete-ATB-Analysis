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


_SELF_PAY_TERMS = frozenset([
    'self pay', 'self-pay', 'self_pay', 'selfpay', 'cash pay', 'private pay',
    'self', 'selfpay/private pay', 'private', 'uninsured'
])

_DENIAL_EMPTY = frozenset(['', 'nan', 'unknown', 'none', 'n/a', '#n/a', 'null'])


def _get_denied_df(df):
    """Return rows with genuine denial codes (non-empty, non-self-pay). Expects decat'd df."""
    _CODE = 'Last Denial Code and Reason'
    _RHP  = 'Responsible Health Plan'
    if _CODE not in df.columns:
        return df.iloc[0:0]
    mask = (
        df[_CODE].notna() &
        (~df[_CODE].astype(str).str.lower().str.strip().isin(_DENIAL_EMPTY))
    )
    sub = df[mask].copy()
    if _RHP in sub.columns:
        sp = sub[_RHP].astype(str).str.lower().str.strip()
        sub = sub[~sp.isin(_SELF_PAY_TERMS)]
    return sub

_DENIAL_AGE_ORDER = [
    '0-29 days', '30-59 days', '60-89 days', '90-119 days',
    '120-179 days', '180+ days', 'Unknown'
]


def _denial_age_bucket(d):
    if pd.isna(d) or d < 0:
        return 'Unknown'
    if d < 30:
        return '0-29 days'
    if d < 60:
        return '30-59 days'
    if d < 90:
        return '60-89 days'
    if d < 120:
        return '90-119 days'
    if d < 180:
        return '120-179 days'
    return '180+ days'


def denial_analysis(curr_df: pd.DataFrame, prior_df: pd.DataFrame) -> dict:
    """Complete open denial analysis: resolution tracking, Pareto, health plan, denial group, and age."""
    curr_df, prior_df = _decat(curr_df), _decat(prior_df)

    CODE_COL  = 'Last Denial Code and Reason'
    DATE_COL  = 'Last Denial Date'
    GROUP_COL = 'Last Denial Group'
    RHP_COL   = 'Responsible Health Plan'

    if CODE_COL not in curr_df.columns:
        return {'available': False}

    has_date  = DATE_COL  in curr_df.columns
    has_group = GROUP_COL in curr_df.columns

    _DENIAL_EMPTY = frozenset(['', 'nan', 'unknown', 'none', 'n/a', '#n/a', 'null'])

    def _get_denied(df):
        mask = (
            df[CODE_COL].notna() &
            (~df[CODE_COL].astype(str).str.lower().str.strip().isin(_DENIAL_EMPTY))
        )
        sub = df[mask].copy()
        if RHP_COL in sub.columns:
            sp = sub[RHP_COL].astype(str).str.lower().str.strip()
            sub = sub[~sp.isin(_SELF_PAY_TERMS)]
        return sub

    curr_denied = _get_denied(curr_df)
    prior_denied = _get_denied(prior_df)

    def _dedup(df):
        if df.empty:
            return df
        return df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')

    curr_d = _dedup(curr_denied)
    prior_d = _dedup(prior_denied)

    total_atb_bal   = float(curr_df['Balance Amount'].sum())
    denied_bal      = float(curr_d['Balance Amount'].sum())
    denied_cnt      = int(len(curr_d))
    prior_denied_bal = float(prior_d['Balance Amount'].sum())
    prior_denied_cnt = int(len(prior_d))
    delta_bal       = round(denied_bal - prior_denied_bal, 2)

    # ── Resolution tracking ──────────────────────────────────────
    curr_encs     = set(curr_d['Encounter Number'].dropna())
    prior_encs    = set(prior_d['Encounter Number'].dropna())
    new_encs      = curr_encs - prior_encs
    resolved_encs = prior_encs - curr_encs
    continued_encs = curr_encs & prior_encs

    new_df       = curr_d[curr_d['Encounter Number'].isin(new_encs)]
    resolved_df  = prior_d[prior_d['Encounter Number'].isin(resolved_encs)]
    continued_df = curr_d[curr_d['Encounter Number'].isin(continued_encs)]

    # ── By denial code+reason (+ group) — Pareto ─────────────────
    group_cols = [CODE_COL]
    if has_group:
        group_cols.append(GROUP_COL)

    by_code_rows = []
    if not curr_d.empty:
        cg = (curr_d.groupby(group_cols, dropna=False)
              .agg(count=('Encounter Number', 'count'), balance=('Balance Amount', 'sum'))
              .reset_index()
              .sort_values('balance', ascending=False))

        prior_code_map = {}
        if not prior_d.empty:
            pg = (prior_d.groupby(group_cols, dropna=False)
                  .agg(balance=('Balance Amount', 'sum'))
                  .reset_index())
            for _, r in pg.iterrows():
                key = tuple(str(r[c]) for c in group_cols)
                prior_code_map[key] = round(float(r['balance']), 2)

        denom = denied_bal if denied_bal > 0 else 1.0
        cumulative_pct = 0.0
        for _, r in cg.iterrows():
            cb  = round(float(r['balance']), 2)
            pct = round(cb / denom * 100, 1)
            cumulative_pct = round(cumulative_pct + pct, 1)
            key = tuple(str(r[c]) for c in group_cols)
            pb  = prior_code_map.get(key, 0.0)
            by_code_rows.append({
                'code':           str(r[CODE_COL]),
                'group':          str(r[GROUP_COL]) if has_group else '',
                'count':          int(r['count']),
                'balance':        cb,
                'pct_of_denied':  pct,
                'cumulative_pct': min(cumulative_pct, 100.0),
                'prior_balance':  pb,
                'delta_balance':  round(cb - pb, 2),
                'is_top90':       cumulative_pct <= 90.5,
            })

    # ── By denial group ──────────────────────────────────────────
    by_group_rows = []
    if has_group and not curr_d.empty:
        gg = (curr_d.groupby(GROUP_COL, dropna=False)
              .agg(count=('Encounter Number', 'count'), balance=('Balance Amount', 'sum'))
              .reset_index()
              .sort_values('balance', ascending=False))

        prior_grp = {}
        if not prior_d.empty and GROUP_COL in prior_d.columns:
            for _, r in (prior_d.groupby(GROUP_COL, dropna=False)
                         .agg(balance=('Balance Amount', 'sum'),
                              count=('Encounter Number', 'count'))
                         .reset_index().iterrows()):
                prior_grp[str(r[GROUP_COL])] = {
                    'balance': round(float(r['balance']), 2),
                    'count':   int(r['count']),
                }

        for _, r in gg.iterrows():
            cb  = round(float(r['balance']), 2)
            gn  = str(r[GROUP_COL])
            pb  = prior_grp.get(gn, {}).get('balance', 0.0)
            d_b = round(cb - pb, 2)
            by_group_rows.append({
                'name':          gn,
                'count':         int(r['count']),
                'balance':       cb,
                'pct_of_denied': _safe_pct(cb, denied_bal),
                'prior_balance': pb,
                'delta_balance': d_b,
                'delta_pct':     _safe_pct(d_b, pb),
            })

    # ── By health plan ────────────────────────────────────────────
    by_hp_rows = []
    if not curr_d.empty and RHP_COL in curr_d.columns:
        hg = (curr_d.groupby(RHP_COL, dropna=False)
              .agg(count=('Encounter Number', 'count'), balance=('Balance Amount', 'sum'))
              .reset_index()
              .sort_values('balance', ascending=False))

        prior_hp = {}
        if not prior_d.empty and RHP_COL in prior_d.columns:
            for _, r in (prior_d.groupby(RHP_COL, dropna=False)
                         .agg(balance=('Balance Amount', 'sum'))
                         .reset_index().iterrows()):
                prior_hp[str(r[RHP_COL])] = round(float(r['balance']), 2)

        cum_hp = 0.0
        for _, r in hg.iterrows():
            cb   = round(float(r['balance']), 2)
            name = str(r[RHP_COL])
            pb   = prior_hp.get(name, 0.0)
            d_b  = round(cb - pb, 2)
            pct  = _safe_pct(cb, denied_bal) or 0.0
            cum_hp = round(cum_hp + pct, 1)

            # denial group breakdown for this plan
            grp_sub = curr_d[curr_d[RHP_COL].astype(str) == name] if GROUP_COL in curr_d.columns else curr_d.iloc[0:0]
            grp_detail = []
            if has_group and not grp_sub.empty:
                for _, gr in (grp_sub.groupby(GROUP_COL, dropna=False)
                              .agg(count=('Encounter Number', 'count'), balance=('Balance Amount', 'sum'))
                              .reset_index().sort_values('balance', ascending=False).iterrows()):
                    grp_detail.append({
                        'group':   str(gr[GROUP_COL]),
                        'count':   int(gr['count']),
                        'balance': round(float(gr['balance']), 2),
                    })

            by_hp_rows.append({
                'name':           name,
                'count':          int(r['count']),
                'balance':        cb,
                'pct_of_denied':  round(pct, 1),
                'cumulative_pct': min(cum_hp, 100.0),
                'prior_balance':  pb,
                'delta_balance':  d_b,
                'delta_pct':      _safe_pct(d_b, pb),
                'by_group':       grp_detail,
            })

    # ── Denial age breakdown ──────────────────────────────────────
    by_age_rows = []
    if has_date and not curr_d.empty:
        df_age = curr_d.copy()
        if 'REPORT_DATE' in df_age.columns:
            report_dts = pd.to_datetime(df_age['REPORT_DATE'], errors='coerce').dropna()
            ref_date   = report_dts.max() if len(report_dts) else pd.Timestamp.now()
        else:
            ref_date = pd.Timestamp.now()

        df_age['denial_dt']       = pd.to_datetime(df_age[DATE_COL], errors='coerce')
        df_age['denial_age_days'] = (ref_date - df_age['denial_dt']).dt.days
        df_age['age_bucket']      = df_age['denial_age_days'].apply(_denial_age_bucket)

        ag = (df_age.groupby('age_bucket')
              .agg(count=('Encounter Number', 'count'),
                   balance=('Balance Amount', 'sum'),
                   avg_age=('denial_age_days', 'mean'))
              .reset_index())
        age_map = {r['age_bucket']: r for _, r in ag.iterrows()}

        for bkt in _DENIAL_AGE_ORDER:
            if bkt in age_map:
                r = age_map[bkt]
                by_age_rows.append({
                    'bucket':       bkt,
                    'count':        int(r['count']),
                    'balance':      round(float(r['balance']), 2),
                    'pct_of_denied': _safe_pct(float(r['balance']), denied_bal),
                    'avg_age_days': round(float(r['avg_age']), 0) if not pd.isna(r['avg_age']) else None,
                })

    result = {
        'available': True,
        'kpis': {
            'denied_balance':      round(denied_bal, 2),
            'denied_count':        denied_cnt,
            'avg_balance':         round(denied_bal / denied_cnt, 2) if denied_cnt else 0.0,
            'pct_of_atb':          _safe_pct(denied_bal, total_atb_bal),
            'prior_denied_balance': round(prior_denied_bal, 2),
            'prior_denied_count':  prior_denied_cnt,
            'delta_balance':       delta_bal,
            'delta_count':         denied_cnt - prior_denied_cnt,
            'delta_pct':           _safe_pct(delta_bal, prior_denied_bal),
        },
        'resolution': {
            'new_count':        int(len(new_df)),
            'new_balance':      round(float(new_df['Balance Amount'].sum()), 2),
            'resolved_count':   int(len(resolved_df)),
            'resolved_balance': round(float(resolved_df['Balance Amount'].sum()), 2),
            'continued_count':  int(len(continued_df)),
            'continued_balance': round(float(continued_df['Balance Amount'].sum()), 2),
        },
        'by_code':        by_code_rows,
        'by_group':       by_group_rows,
        'by_health_plan': by_hp_rows,
        'by_denial_age':  by_age_rows,
        'has_group':      has_group,
        'has_date':       has_date,
    }
    result['summary_points'] = denial_summary(result)
    return result


def denial_summary(data: dict) -> list:
    """Generate 8+ deep key insights from denial_analysis result."""
    points = []
    kpis       = data.get('kpis', {})
    resolution = data.get('resolution', {})
    by_code    = data.get('by_code', [])
    by_group   = data.get('by_group', [])
    by_hp      = data.get('by_health_plan', [])
    by_age     = data.get('by_denial_age', [])

    denied_bal      = kpis.get('denied_balance', 0.0)
    prior_denied_bal = kpis.get('prior_denied_balance', 0.0)
    delta_bal       = kpis.get('delta_balance', 0.0)
    denied_cnt      = kpis.get('denied_count', 0)
    pct_atb         = kpis.get('pct_of_atb')

    # 1. WoW denial balance
    delta_pct = kpis.get('delta_pct')
    pct_str   = f' ({("+" if delta_bal > 0 else "")}{delta_pct}%)' if delta_pct is not None else ''
    points.append({
        'type': 'danger' if delta_bal > 0 else 'success',
        'text': f'Open denial balance {"increased" if delta_bal > 0 else ("decreased" if delta_bal < 0 else "unchanged")} '
                f'by {_fmt_dollar(abs(delta_bal))}{pct_str} this week '
                f'(total {_fmt_dollar(denied_bal)}, {denied_cnt:,} encounters).'
    })

    # 2. % of ATB
    if pct_atb is not None:
        avg_bal = kpis.get('avg_balance', 0.0)
        points.append({
            'type': 'danger' if pct_atb > 20 else 'warning',
            'text': f'Open denials represent {pct_atb}% of total ATB balance. '
                    f'Average denied balance per encounter: {_fmt_dollar(avg_bal)}.'
        })

    # 3. Resolution rate
    resolved_bal = resolution.get('resolved_balance', 0.0)
    resolved_cnt = resolution.get('resolved_count', 0)
    new_bal      = resolution.get('new_balance', 0.0)
    new_cnt      = resolution.get('new_count', 0)
    if prior_denied_bal > 0:
        res_rate = _safe_pct(resolved_bal, prior_denied_bal)
        points.append({
            'type': 'success' if (res_rate or 0) >= 10 else 'warning',
            'text': f'Resolution rate: {res_rate}% of prior-week denial balance resolved '
                    f'({_fmt_dollar(resolved_bal)}, {resolved_cnt:,} encounters cleared).'
        })

    # 4. New denial inflow vs resolved
    net_inflow = new_bal - resolved_bal
    points.append({
        'type': 'danger' if net_inflow > 0 else 'success',
        'text': f'New denial inflow {_fmt_dollar(new_bal)} ({new_cnt:,} enc) '
                f'{"exceeded" if net_inflow > 0 else "was offset by"} '
                f'resolutions {_fmt_dollar(resolved_bal)} ({resolved_cnt:,} enc) — '
                f'net {"increase" if net_inflow > 0 else "reduction"} of {_fmt_dollar(abs(net_inflow))}.'
    })

    # 5. Pareto — how many codes cover 90%
    top90_codes = [r for r in by_code if r.get('is_top90')]
    if top90_codes and by_code:
        total_codes = len(by_code)
        pct_codes   = round(len(top90_codes) / total_codes * 100, 0) if total_codes else 0
        top3        = by_code[:3]
        top3_labels = [r['code'][:50] for r in top3]
        top3_str = '; '.join(top3_labels)
        points.append({
            'type': 'danger',
            'text': f'{len(top90_codes)} of {total_codes} denial codes account for 90% of denied balance '
                    f'({pct_codes:.0f}% of unique codes drive 90% of value). '
                    f'Top 3: {top3_str}.'
        })

    # 6. Worst health plan
    if by_hp:
        worst = by_hp[0]
        worst_delta = worst.get('delta_balance', 0.0)
        d_str = f', {"+" if worst_delta > 0 else ""}{_fmt_dollar(worst_delta)} WoW' if worst_delta != 0 else ''
        points.append({
            'type': 'danger',
            'text': f'Highest denial balance health plan: {worst["name"]} — '
                    f'{_fmt_dollar(worst["balance"])} ({worst.get("pct_of_denied")}% of all denials{d_str}).'
        })

    # 7. Top-3 health plan concentration
    if len(by_hp) >= 3:
        top3_hp_bal = sum(r['balance'] for r in by_hp[:3])
        top3_hp_pct = _safe_pct(top3_hp_bal, denied_bal)
        top3_hp_names = ', '.join(r['name'] for r in by_hp[:3])
        points.append({
            'type': 'warning',
            'text': f'Top 3 health plans concentrate {top3_hp_pct}% of total denial balance '
                    f'({_fmt_dollar(top3_hp_bal)}): {top3_hp_names}.'
        })

    # 8. Worst denial group
    if by_group:
        lg = by_group[0]
        fastest = max(by_group, key=lambda r: r.get('delta_balance') or 0)
        grp_pct = lg.get('pct_of_denied')
        points.append({
            'type': 'info',
            'text': f'Largest denial group: "{lg["name"]}" — '
                    f'{_fmt_dollar(lg["balance"])} ({grp_pct}% of denials, {lg["count"]:,} enc).'
        })
        if fastest.get('delta_balance', 0) > 0 and fastest['name'] != lg['name']:
            points.append({
                'type': 'danger',
                'text': f'Fastest-growing denial group this week: "{fastest["name"]}" '
                        f'— up {_fmt_dollar(fastest["delta_balance"])} '
                        f'({fastest.get("delta_pct", 0)}%) vs prior week.'
            })

    # 9. Aging of denials — most exposed bucket
    aged_critical = [r for r in by_age if r['bucket'] in ('90-119 days', '120-179 days', '180+ days')]
    if aged_critical:
        aged_bal = sum(r['balance'] for r in aged_critical)
        aged_cnt = sum(r['count'] for r in aged_critical)
        aged_pct = _safe_pct(aged_bal, denied_bal)
        oldest   = max(by_age, key=lambda r: r.get('avg_age_days') or 0)
        points.append({
            'type': 'danger',
            'text': f'{aged_pct}% of denial balance ({_fmt_dollar(aged_bal)}, {aged_cnt:,} enc) '
                    f'is 90+ days old — oldest avg age in "{oldest["bucket"]}" bucket '
                    f'({oldest.get("avg_age_days", 0):.0f} avg days).'
        })

    return points


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


def denial_velocity(weekly_data: dict) -> dict:
    """Denial velocity: latest ATB open denials + denial age analysis + WoW trend."""
    _CODE  = 'Last Denial Code and Reason'
    _GROUP = 'Last Denial Group'
    _RHP   = 'Responsible Health Plan'
    _DATE  = 'Last Denial Date'

    sorted_weeks = sorted(weekly_data.keys())
    if not sorted_weeks:
        return {'available': False}

    latest_week = sorted_weeks[-1]
    latest_df   = _decat(weekly_data[latest_week])

    if _CODE not in latest_df.columns:
        return {'available': False}

    # ── WoW trend — all weeks for trend chart only ────────────────
    trend = []
    for week in sorted_weeks:
        df = _decat(weekly_data[week])
        denied_w  = _get_denied_df(df)
        total_bal = float(df['Balance Amount'].sum())
        bal       = float(denied_w['Balance Amount'].sum())
        trend.append({
            'week':       week,
            'count':      int(denied_w['Encounter Number'].nunique()),
            'balance':    round(bal, 2),
            'pct_of_atb': _safe_pct(bal, total_bal),
        })

    # ── Latest ATB only — core velocity analysis ──────────────────
    denied = _get_denied_df(latest_df)
    if denied.empty:
        return {
            'available': True, 'weeks': sorted_weeks, 'latest_week': latest_week,
            'kpis': {}, 'trend': trend, 'by_code': [],
            'by_age': [], 'aged_denials': [], 'has_date': False,
            'summary_points': [],
        }

    denied = (denied
              .sort_values('Balance Amount', ascending=False)
              .drop_duplicates('Encounter Number')
              .copy())

    has_date = _DATE in denied.columns and 'REPORT_DATE' in denied.columns
    has_group = _GROUP in denied.columns

    if has_date:
        rpt_dts  = pd.to_datetime(denied['REPORT_DATE'], errors='coerce').dropna()
        ref_date = rpt_dts.max() if len(rpt_dts) else pd.Timestamp.now()
        denied   = denied.copy()
        denied['_denial_dt']  = pd.to_datetime(denied[_DATE], errors='coerce')
        denied['_age_days']   = (ref_date - denied['_denial_dt']).dt.days
        denied['_age_bucket'] = denied['_age_days'].apply(
            lambda d: _denial_age_bucket(d) if pd.notna(d) else 'Unknown'
        )
    else:
        denied['_age_days']   = None
        denied['_age_bucket'] = 'Unknown'

    total_denied_bal = float(denied['Balance Amount'].sum())
    denied_cnt       = int(len(denied))

    # ── By denial code — sorted by avg denial age (slowest first) ─
    group_cols = [_CODE] + ([_GROUP] if has_group else [])
    by_code = []
    for keys, sub in denied.groupby(group_cols, dropna=False):
        code  = str(keys[0]) if isinstance(keys, tuple) else str(keys)
        group = str(keys[1]) if isinstance(keys, tuple) and len(keys) > 1 else ''
        bal   = float(sub['Balance Amount'].sum())
        cnt   = int(len(sub))

        # Per-age-bucket balance breakdown for heat columns
        age_dist = {}
        for bucket in _DENIAL_AGE_ORDER:
            sub_b = sub[sub['_age_bucket'] == bucket] if has_date else sub.iloc[0:0]
            age_dist[bucket] = round(float(sub_b['Balance Amount'].sum()), 2)

        avg_age = None
        max_age = None
        pct_90  = None
        if has_date:
            valid_ages = sub['_age_days'].dropna()
            if len(valid_ages):
                avg_age = round(float(valid_ages.mean()), 0)
                max_age = int(valid_ages.max())
            over_90_bal = float(sub[sub['_age_days'] >= 90]['Balance Amount'].sum())
            pct_90 = _safe_pct(over_90_bal, bal)

        by_code.append({
            'code':          code,
            'group':         group,
            'count':         cnt,
            'balance':       round(bal, 2),
            'avg_age_days':  avg_age,
            'max_age_days':  max_age,
            'pct_over_90':   pct_90,
            'pct_of_denied': _safe_pct(bal, total_denied_bal),
            'age_dist':      age_dist,
        })

    # Keep codes covering 90% of total denied balance, then sort by oldest avg age
    by_code.sort(key=lambda r: -r['balance'])
    cutoff = total_denied_bal * 0.90
    cumsum = 0.0
    keep = []
    for r in by_code:
        keep.append(r)
        cumsum += r['balance']
        if cumsum >= cutoff:
            break
    by_code = sorted(keep, key=lambda r: (-(r['avg_age_days'] or 0), -r['balance']))

    # ── Overall by-age-bucket distribution ───────────────────────
    by_age = []
    if has_date:
        for bucket in _DENIAL_AGE_ORDER:
            sub = denied[denied['_age_bucket'] == bucket]
            if len(sub):
                by_age.append({
                    'bucket':        bucket,
                    'count':         int(len(sub)),
                    'balance':       round(float(sub['Balance Amount'].sum()), 2),
                    'pct_of_denied': _safe_pct(float(sub['Balance Amount'].sum()), total_denied_bal),
                    'avg_age_days':  round(float(sub['_age_days'].mean()), 0),
                })

    # ── Aged denials — top 90+ day encounters ────────────────────
    aged_denials = []
    if has_date:
        aged = (denied[denied['_age_days'] >= 90]
                .sort_values('_age_days', ascending=False)
                .head(100))
        for _, row in aged.iterrows():
            aged_denials.append({
                'encounter':   int(row['Encounter Number']) if not pd.isna(row.get('Encounter Number')) else 0,
                'code':        str(row[_CODE]),
                'group':       str(row[_GROUP]) if has_group and _GROUP in row.index else '',
                'plan':        str(row[_RHP])   if _RHP in row.index else '',
                'denial_date': str(row['_denial_dt'].date()) if pd.notna(row.get('_denial_dt')) else '—',
                'age_days':    int(row['_age_days']) if pd.notna(row.get('_age_days')) else 0,
                'balance':     round(float(row['Balance Amount']), 2),
            })

    # ── KPIs ─────────────────────────────────────────────────────
    over_90_bal  = float(denied[denied['_age_days'] >= 90]['Balance Amount'].sum())  if has_date else 0.0
    over_180_bal = float(denied[denied['_age_days'] >= 180]['Balance Amount'].sum()) if has_date else 0.0
    avg_age_all  = None
    if has_date:
        valid = denied['_age_days'].dropna()
        if len(valid):
            avg_age_all = round(float(valid.mean()), 0)

    kpis = {
        'open_denied_balance': round(total_denied_bal, 2),
        'open_denied_count':   denied_cnt,
        'avg_age_days':        avg_age_all,
        'pct_over_90_days':    _safe_pct(over_90_bal,  total_denied_bal),
        'over_90_balance':     round(over_90_bal, 2),
        'pct_over_180_days':   _safe_pct(over_180_bal, total_denied_bal),
        'over_180_balance':    round(over_180_bal, 2),
    }

    result = {
        'available':    True,
        'weeks':        sorted_weeks,
        'latest_week':  latest_week,
        'kpis':         kpis,
        'trend':        trend,
        'by_code':      by_code,
        'by_age':       by_age,
        'aged_denials': aged_denials,
        'has_date':     has_date,
    }
    result['summary_points'] = denial_velocity_summary(result)
    return result


def denial_velocity_summary(data: dict) -> list:
    """Generate 8 key insights from denial_velocity result."""
    points  = []
    kpis    = data.get('kpis', {})
    trend   = data.get('trend', [])
    by_code = data.get('by_code', [])
    by_age  = data.get('by_age', [])
    has_date = data.get('has_date', False)

    open_bal   = kpis.get('open_denied_balance', 0.0)
    open_cnt   = kpis.get('open_denied_count', 0)
    avg_age    = kpis.get('avg_age_days')
    pct_90     = kpis.get('pct_over_90_days')
    over_90_bal = kpis.get('over_90_balance', 0.0)
    pct_180    = kpis.get('pct_over_180_days')
    over_180_bal = kpis.get('over_180_balance', 0.0)

    # 1. Open denial snapshot
    points.append({
        'type': 'info',
        'text': f'Latest ATB has {open_cnt:,} open denied encounters totalling {_fmt_dollar(open_bal)}. '
                f'Analysis is based on current snapshot only — resolved denials are excluded.',
    })

    # 2. Average denial age
    if avg_age is not None:
        t = 'danger' if avg_age > 90 else ('warning' if avg_age > 45 else 'info')
        points.append({
            'type': t,
            'text': f'Average denial age: {avg_age:.0f} days since Last Denial Date. '
                    f'Denials older than 90 days are at high risk of write-off.',
        })

    # 3. 90+ day exposure
    if pct_90 is not None:
        points.append({
            'type': 'danger' if pct_90 > 30 else 'warning',
            'text': f'{pct_90}% of denied balance ({_fmt_dollar(over_90_bal)}) has been in denial status '
                    f'for 90+ days — these require immediate escalation.',
        })

    # 4. 180+ day write-off risk
    if pct_180 is not None and pct_180 > 0:
        points.append({
            'type': 'danger',
            'text': f'{pct_180}% of denied balance ({_fmt_dollar(over_180_bal)}) is 180+ days old — '
                    f'critical write-off risk; timely filing deadlines likely breached.',
        })

    # 5. WoW trend
    if len(trend) >= 2:
        d_bal = trend[-1]['balance'] - trend[0]['balance']
        d_pct = _safe_pct(d_bal, trend[0]['balance'])
        sign  = '+' if d_pct and d_pct > 0 else ''
        points.append({
            'type': 'danger' if d_bal > 0 else 'success',
            'text': f'Denied balance {"grew" if d_bal > 0 else "declined"} by {_fmt_dollar(abs(d_bal))} '
                    f'({sign}{d_pct}%) from {trend[0]["week"]} to {trend[-1]["week"]}.',
        })

    # 6. Code with highest average denial age (most stuck)
    aged_codes = [r for r in by_code if r.get('avg_age_days') is not None]
    if aged_codes:
        oldest = max(aged_codes, key=lambda r: r['avg_age_days'])
        points.append({
            'type': 'danger',
            'text': f'Oldest average denial age: "{oldest["code"][:60]}" — '
                    f'{oldest["avg_age_days"]:.0f} days avg ({oldest["count"]:,} enc, '
                    f'{_fmt_dollar(oldest["balance"])} balance).',
        })

    # 7. Top balance code with 90%+ over 90 days
    stuck = [r for r in by_code if (r.get('pct_over_90') or 0) >= 90 and r['balance'] > 0]
    if stuck:
        worst = max(stuck, key=lambda r: r['balance'])
        points.append({
            'type': 'danger',
            'text': f'"{worst["code"][:60]}" — {worst["pct_over_90"]}% of its {_fmt_dollar(worst["balance"])} '
                    f'denial balance is 90+ days old ({worst["count"]:,} encounters).',
        })

    # 8. Largest single-week denial spike from trend
    max_jump_w, max_jump_v = None, 0.0
    for i in range(1, len(trend)):
        delta = trend[i]['balance'] - trend[i - 1]['balance']
        if delta > max_jump_v:
            max_jump_v = delta
            max_jump_w = trend[i]['week']
    if max_jump_w and max_jump_v > 0:
        points.append({
            'type': 'warning',
            'text': f'Largest single-week denial spike: +{_fmt_dollar(max_jump_v)} entering week {max_jump_w}.',
        })

    return points


def cash_collection_action_plan(weekly_data: dict, curr_df: pd.DataFrame, prior_df: pd.DataFrame) -> dict:
    """Prescriptive cash collection analysis: prioritized actions, waterfall, forecast, and scoring."""
    import statistics as _stats

    curr = _decat(curr_df)

    _BAL   = 'Balance Amount'
    _ENC   = 'Encounter Number'
    _DAC   = 'Discharge Aging Category'
    _RFC   = 'Responsible Financial Class'
    _RHP   = 'Responsible Health Plan'
    _DD    = 'Discharge Date'
    _RD    = 'REPORT_DATE'
    _LDD   = 'Last Denial Date'
    TF_COL = 'Days to Timely Filing Limit'

    deduped  = curr.sort_values(_BAL, ascending=False).drop_duplicates(_ENC)
    total_ar = float(deduped[_BAL].sum())

    report_dt = None
    if _RD in deduped.columns:
        try:
            report_dt = pd.to_datetime(deduped[_RD], errors='coerce').max()
        except Exception:
            pass
    if report_dt is None or pd.isna(report_dt):
        report_dt = pd.Timestamp.now()

    denied_ded      = _get_denied_df(deduped)
    denied_encs_set = set(denied_ded[_ENC].astype(str))
    has_denial_mask = deduped[_ENC].astype(str).isin(denied_encs_set)

    # ── SECTION 1: URGENCY ALERT ────────────────────────────
    has_tf = TF_COL in deduped.columns
    urgency_alert = {'show': False, 'at_risk_count': 0, 'at_risk_balance': 0.0,
                     'threshold_days': 14, 'column_available': has_tf}
    if has_tf:
        try:
            tf_vals    = pd.to_numeric(deduped[TF_COL], errors='coerce')
            tf_risk_df = deduped[tf_vals.between(0, 14)]
            urgency_alert.update({
                'show': int(tf_risk_df[_ENC].nunique()) > 0,
                'at_risk_count': int(tf_risk_df[_ENC].nunique()),
                'at_risk_balance': round(float(tf_risk_df[_BAL].sum()), 2),
            })
        except Exception:
            pass

    # ── SECTION 2: KPIs ─────────────────────────────────────
    over90_encs_set = set(deduped[deduped[_DAC].isin(OVER_90_BUCKETS)][_ENC].astype(str))
    recoverable_df  = deduped[deduped[_ENC].astype(str).isin(over90_encs_set | denied_encs_set)]
    recoverable_bal = float(recoverable_df[_BAL].sum())
    recoverable_cnt = int(recoverable_df[_ENC].nunique())

    b9120_denied = _get_denied_df(deduped[deduped[_DAC] == '91-120'])
    if len(b9120_denied) and _LDD in b9120_denied.columns:
        try:
            ldd_qw = pd.to_datetime(b9120_denied[_LDD], errors='coerce')
            da_qw  = (report_dt - ldd_qw).dt.days
            qw_df  = b9120_denied[da_qw.fillna(9999) < 30]
        except Exception:
            qw_df = b9120_denied
    else:
        qw_df = b9120_denied
    quick_win_bal = float(qw_df[_BAL].sum())
    quick_win_cnt = int(qw_df[_ENC].nunique())

    tf_col_available = has_tf
    if has_tf:
        try:
            tf30_vals      = pd.to_numeric(deduped[TF_COL], errors='coerce')
            tf_30_df       = deduped[tf30_vals.between(0, 30)]
            tf_at_risk_bal = float(tf_30_df[_BAL].sum())
            tf_at_risk_cnt = int(tf_30_df[_ENC].nunique())
        except Exception:
            tf_col_available = False
            tf_est         = deduped[deduped[_DAC].isin(['91-120', '121-150'])]
            tf_at_risk_bal = float(tf_est[_BAL].sum())
            tf_at_risk_cnt = int(tf_est[_ENC].nunique())
    else:
        tf_est         = deduped[deduped[_DAC].isin(['91-120', '121-150'])]
        tf_at_risk_bal = float(tf_est[_BAL].sum())
        tf_at_risk_cnt = int(tf_est[_ENC].nunique())

    sorted_weeks     = sorted(weekly_data.keys())
    resolution_rates = []
    for i in range(1, len(sorted_weeks)):
        try:
            pw_ded        = _decat(weekly_data[sorted_weeks[i - 1]])
            pw_ded        = pw_ded.sort_values(_BAL, ascending=False).drop_duplicates(_ENC)
            cw_ded        = _decat(weekly_data[sorted_weeks[i]])
            cw_ded        = cw_ded.sort_values(_BAL, ascending=False).drop_duplicates(_ENC)
            prev_denied_w = _get_denied_df(pw_ded)
            curr_denied_w = _get_denied_df(cw_ded)
            prev_bal_w    = float(prev_denied_w[_BAL].sum())
            if prev_bal_w > 0:
                curr_encs_w = set(curr_denied_w[_ENC].astype(str))
                resolved_w  = prev_denied_w[~prev_denied_w[_ENC].astype(str).isin(curr_encs_w)]
                rate = max(0.0, min(1.0, float(resolved_w[_BAL].sum()) / prev_bal_w))
                resolution_rates.append(rate)
        except Exception:
            continue

    avg_rate     = float(sum(resolution_rates[-8:])) / len(resolution_rates[-8:]) if resolution_rates else 0.05
    denied_pool  = float(denied_ded[_BAL].sum())
    projected_4wk = round(denied_pool * (1 - (1 - avg_rate) ** 4), 2)

    kpis = {
        'recoverable_opportunity': round(recoverable_bal, 2),
        'recoverable_count': recoverable_cnt,
        'quick_win_balance': round(quick_win_bal, 2),
        'quick_win_count': quick_win_cnt,
        'tf_at_risk_balance': round(tf_at_risk_bal, 2),
        'tf_at_risk_count': tf_at_risk_cnt,
        'tf_column_available': tf_col_available,
        'projected_4wk_recovery': projected_4wk,
        'avg_weekly_resolution_rate': round(avg_rate * 100, 1),
    }

    # ── SECTION 3: PRIORITY SCORE TABLE ─────────────────────
    raw_rows = []
    for (payer, fin_class), g in deduped.groupby([_RHP, _RFC], sort=False):
        bal = float(g[_BAL].sum())
        if bal <= 0:
            continue
        enc_count = int(g[_ENC].nunique())
        if _DD in g.columns:
            dd_g = pd.to_datetime(g[_DD], errors='coerce')
            ard  = (report_dt - dd_g).dt.days.dropna()
            avg_ar = round(float(ard.mean()), 0) if len(ard) else None
        else:
            avg_ar = None
        denied_g = _get_denied_df(g)
        if len(denied_g) and _LDD in denied_g.columns:
            ldd_g  = pd.to_datetime(denied_g[_LDD], errors='coerce')
            da_g   = (report_dt - ldd_g).dt.days.dropna()
            avg_da = round(float(da_g.mean()), 0) if len(da_g) else None
        else:
            avg_da = None
        if has_tf:
            try:
                tf_g  = pd.to_numeric(g[TF_COL], errors='coerce').dropna()
                avg_tf = float(tf_g.mean()) if len(tf_g) else 90.0
            except Exception:
                avg_tf = 90.0
        else:
            bucket_tf_map = {'0-30': 150, '31-60': 120, '61-90': 90, '91-120': 60,
                             '121-150': 30, '151-180': 15, 'DNFB': 180, 'Not Aged': 180}
            dom_bucket = None
            if _DAC in g.columns and len(g):
                dom_bucket = g.groupby(_DAC)[_BAL].sum().idxmax()
            avg_tf = bucket_tf_map.get(dom_bucket, 90)
        raw_rows.append({
            'payer': str(payer), 'fin_class': str(fin_class),
            'encounter_count': enc_count, 'balance': bal,
            'avg_ar_days': avg_ar, 'avg_denial_age': avg_da,
            'avg_tf_remaining': avg_tf,
            'payer_denial_rate': len(denied_g) / enc_count if enc_count else 0.0,
        })

    priority_table = []
    if raw_rows:
        max_bal  = max(r['balance'] for r in raw_rows) or 1.0
        max_days = max(r['avg_ar_days'] or 0 for r in raw_rows) or 1.0
        max_da   = max(r['avg_denial_age'] or 0 for r in raw_rows) or 1.0
        max_pdr  = max(r['payer_denial_rate'] for r in raw_rows) or 1.0
        for r in raw_rows:
            b_score  = r['balance'] / max_bal
            d_score  = (r['avg_ar_days'] or 0) / max_days
            tf_rem   = r['avg_tf_remaining']
            tf_score = max(0.0, 1.0 - tf_rem / 90.0) if tf_rem <= 90 else 0.0
            da_score = (r['avg_denial_age'] or 0) / max_da
            pdr_s    = r['payer_denial_rate'] / max_pdr
            score    = (0.35 * b_score + 0.25 * d_score + 0.20 * tf_score + 0.15 * da_score + 0.05 * pdr_s) * 100
            if tf_score >= 0.8:
                action = 'URGENT: File appeal — timely filing deadline near'
            elif da_score >= 0.7 and b_score >= 0.4:
                action = 'Escalate to denial specialist — aged denial'
            elif b_score >= 0.8 and d_score <= 0.3:
                action = 'Priority billing follow-up — high balance, recent'
            elif d_score >= 0.7 and da_score <= 0.2:
                action = 'Chase payment — outstanding, no active denial'
            elif r['payer_denial_rate'] >= 0.5:
                action = 'Denial management — high payer denial rate'
            else:
                action = 'Standard follow-up and status check'
            r['priority_score'] = round(score, 1)
            r['tf_risk_score']  = round(tf_score, 2)
            r['action']         = action
        raw_rows.sort(key=lambda r: r['priority_score'], reverse=True)
        for i, r in enumerate(raw_rows[:20]):
            priority_table.append({
                'rank': i + 1,
                'payer': r['payer'],
                'fin_class': r['fin_class'],
                'encounter_count': r['encounter_count'],
                'balance': round(r['balance'], 2),
                'avg_ar_days': int(r['avg_ar_days']) if r['avg_ar_days'] is not None else None,
                'avg_denial_age': int(r['avg_denial_age']) if r['avg_denial_age'] is not None else None,
                'tf_risk_score': r['tf_risk_score'],
                'priority_score': r['priority_score'],
                'recommended_action': r['action'],
            })

    # ── SECTION 4: PAYER ACTION MATRIX ──────────────────────
    payer_rows = []
    for payer, pg in deduped.groupby(_RHP, sort=False):
        pg_bal = float(pg[_BAL].sum())
        if pg_bal <= 0:
            continue
        pg_enc    = int(pg[_ENC].nunique())
        pg_denied = _get_denied_df(pg)
        if len(pg_denied) and _LDD in pg_denied.columns:
            ldd_p = pd.to_datetime(pg_denied[_LDD], errors='coerce')
            da_p  = (report_dt - ldd_p).dt.days.dropna()
            avg_da_p = round(float(da_p.mean()), 1) if len(da_p) else 0.0
        else:
            avg_da_p = 0.0
        payer_rows.append({
            'name': str(payer), 'x': avg_da_p, 'y': round(pg_bal, 2), 'r': pg_enc,
            'denial_rate': round(len(pg_denied) / pg_enc if pg_enc else 0.0, 3),
        })
    if payer_rows:
        x_vals = [r['x'] for r in payer_rows]
        y_vals = [r['y'] for r in payer_rows]
        mid_x  = _stats.median(x_vals) if len(x_vals) > 1 else (x_vals[0] if x_vals else 30.0)
        mid_y  = _stats.median(y_vals) if len(y_vals) > 1 else (y_vals[0] / 2 if y_vals else 0.0)
        if mid_x == 0:
            mid_x = 30.0
        for r in payer_rows:
            if r['x'] < mid_x and r['y'] >= mid_y:
                r['quadrant'] = 'quick_win'
            elif r['x'] >= mid_x and r['y'] >= mid_y:
                r['quadrant'] = 'strategic_focus'
            elif r['x'] < mid_x:
                r['quadrant'] = 'monitor'
            else:
                r['quadrant'] = 'deprioritize'
    payer_matrix = sorted(payer_rows, key=lambda r: r['y'], reverse=True)[:30]

    # ── SECTION 5: CASH RECOVERY WATERFALL ──────────────────
    clean_pipeline = float(deduped[(deduped[_DAC].isin(['0-30', '31-60'])) & ~has_denial_mask][_BAL].sum())
    dnfb_pool      = float(deduped[deduped[_DAC] == 'DNFB'][_BAL].sum())
    at_risk_pool   = float(deduped[deduped[_DAC] == '61-90'][_BAL].sum())
    over90_df      = deduped[deduped[_DAC].isin(OVER_90_BUCKETS)]
    over90_denied_df   = _get_denied_df(over90_df)
    over90_denied_encs = set(over90_denied_df[_ENC].astype(str))
    if len(over90_denied_df) and _LDD in over90_denied_df.columns:
        ldd_o = pd.to_datetime(over90_denied_df[_LDD], errors='coerce')
        da_o  = (report_dt - ldd_o).dt.days.fillna(9999)
        denied_recoverable = float(over90_denied_df[da_o < 90][_BAL].sum())
        denied_hard        = float(over90_denied_df[da_o >= 90][_BAL].sum())
    else:
        denied_recoverable = float(over90_denied_df[_BAL].sum())
        denied_hard        = 0.0
    non_denied_90plus = float(over90_df[~over90_df[_ENC].astype(str).isin(over90_denied_encs)][_BAL].sum())

    waterfall = [
        {'label': 'Total AR Balance',                  'value': round(total_ar, 2),           'type': 'total'},
        {'label': 'Clean Pipeline (0-60, no denial)',   'value': round(clean_pipeline, 2),     'type': 'deduct'},
        {'label': 'DNFB (Internal Billing Fix)',        'value': round(dnfb_pool, 2),          'type': 'deduct'},
        {'label': 'At-Risk Pool (61-90)',               'value': round(at_risk_pool, 2),       'type': 'warning'},
        {'label': 'Denied & Recoverable (<90d denial)', 'value': round(denied_recoverable, 2), 'type': 'critical'},
        {'label': 'Denied & Hard (90d+ denial)',        'value': round(denied_hard, 2),        'type': 'danger'},
        {'label': 'Non-Denied 90+ (Billing/Auth)',      'value': round(non_denied_90plus, 2),  'type': 'danger'},
    ]

    # ── SECTION 6: 8-WEEK FORECAST ──────────────────────────
    accelerated_rate = min(avg_rate * 2, 0.40)
    baseline_weeks, accelerated_weeks = [], []
    pool_b = pool_a = denied_pool
    cum_b  = cum_a  = 0.0
    for wn in range(1, 9):
        rec_b = pool_b * avg_rate;      pool_b -= rec_b;  cum_b += rec_b
        rec_a = pool_a * accelerated_rate; pool_a -= rec_a; cum_a += rec_a
        baseline_weeks.append({'week': wn, 'cumulative_recovered': round(cum_b, 2), 'remaining': round(pool_b, 2)})
        accelerated_weeks.append({'week': wn, 'cumulative_recovered': round(cum_a, 2), 'remaining': round(pool_a, 2)})

    forecast = {
        'avg_weekly_rate': round(avg_rate, 4),
        'accelerated_rate': round(accelerated_rate, 4),
        'current_outstanding': round(denied_pool, 2),
        'baseline_8wk_recovery': baseline_weeks[-1]['cumulative_recovered'] if baseline_weeks else 0.0,
        'accelerated_8wk_recovery': accelerated_weeks[-1]['cumulative_recovered'] if accelerated_weeks else 0.0,
        'weeks_of_data_used': len(resolution_rates),
        'baseline_weeks': baseline_weeks,
        'accelerated_weeks': accelerated_weeks,
    }

    # ── SECTION 7: FINANCIAL CLASS RANKINGS ─────────────────
    fc_rankings = []
    for fc, fg in deduped.groupby(_RFC, sort=False):
        fc_bal = float(fg[_BAL].sum())
        if fc_bal <= 0:
            continue
        over90_fc   = float(fg[fg[_DAC].isin(OVER_90_BUCKETS)][_BAL].sum())
        pct_over_90 = round(_safe_pct(over90_fc, fc_bal) or 0.0, 1)
        denied_fg   = _get_denied_df(fg)
        fc_enc      = int(fg[_ENC].nunique())
        denial_rate = round(_safe_pct(int(denied_fg[_ENC].nunique()), fc_enc) or 0.0, 1)
        if _DD in fg.columns:
            dd_fc  = pd.to_datetime(fg[_DD], errors='coerce')
            ard_fc = (report_dt - dd_fc).dt.days.dropna()
            avg_vel = int(ard_fc.mean()) if len(ard_fc) else None
        else:
            avg_vel = None
        if pct_over_90 >= 60 and denial_rate >= 30:
            priority = 'CRITICAL'
        elif pct_over_90 >= 40 or denial_rate >= 40:
            priority = 'HIGH'
        elif pct_over_90 >= 20 or denial_rate >= 20:
            priority = 'MEDIUM'
        else:
            priority = 'MONITOR'
        fc_rankings.append({
            'name': str(fc), 'total_balance': round(fc_bal, 2),
            'pct_over_90': pct_over_90, 'avg_denial_rate': denial_rate,
            'avg_collection_velocity': avg_vel, 'action_priority': priority,
        })
    fc_rankings.sort(key=lambda r: r['total_balance'], reverse=True)

    # ── SECTION 8: KEY ACTION INSIGHTS ──────────────────────
    insights = []
    total_90_denied = denied_recoverable + denied_hard
    if total_90_denied > 0:
        top_payers_90d = (_get_denied_df(over90_df).groupby(_RHP)[_BAL].sum().sort_values(ascending=False))
        top3_pct = round(float(top_payers_90d.head(3).sum()) / total_90_denied * 100, 0)
        insights.append({'type': 'danger',
            'text': f'{_fmt_dollar(total_90_denied)} locked in 90+ DENIED claims — '
                    f'top 3 payers account for {top3_pct:.0f}% of this. Escalate immediately.'})
    if at_risk_pool > 0:
        cnt_61_90 = int(deduped[deduped[_DAC] == '61-90'][_ENC].nunique())
        insights.append({'type': 'warning',
            'text': f'{cnt_61_90:,} encounters in 61-90 bucket ({_fmt_dollar(at_risk_pool)}) '
                    f'roll into 90+ next week — intervening now prevents permanent aging.'})
    if quick_win_bal > 0:
        insights.append({'type': 'success',
            'text': f'Quick win: {quick_win_cnt:,} newly denied 91-120d claims worth {_fmt_dollar(quick_win_bal)} '
                    f'— denial is fresh (<30 days), high reversal probability.'})
    if payer_matrix:
        slowest = max(payer_matrix, key=lambda r: r['x'])
        if slowest['x'] >= 45:
            insights.append({'type': 'warning',
                'text': f'Payer "{slowest["name"][:50]}" averages {slowest["x"]:.0f} days from denial to resolution '
                        f'({_fmt_dollar(slowest["y"])} outstanding) — reassign to a denial specialist.'})
    if payer_matrix:
        top5_bal = sum(r['y'] for r in payer_matrix[:5])
        top5_pct = round(top5_bal / total_ar * 100, 0) if total_ar else 0
        insights.append({'type': 'info',
            'text': f'Working top 5 payers by priority covers {top5_pct:.0f}% of total AR '
                    f'({_fmt_dollar(top5_bal)}) — concentrate team effort here first.'})
    if urgency_alert['show']:
        insights.append({'type': 'danger',
            'text': f'URGENT: {_fmt_dollar(urgency_alert["at_risk_balance"])} in '
                    f'{urgency_alert["at_risk_count"]:,} encounters faces timely filing deadline '
                    f'within {urgency_alert["threshold_days"]} days — must file THIS WEEK.'})
    gap = forecast['accelerated_8wk_recovery'] - forecast['baseline_8wk_recovery']
    if gap > 0:
        insights.append({'type': 'success',
            'text': f'Doubling resolution pace could yield an extra {_fmt_dollar(gap)} over 8 weeks '
                    f'(total {_fmt_dollar(forecast["accelerated_8wk_recovery"])} vs '
                    f'current-pace {_fmt_dollar(forecast["baseline_8wk_recovery"])}).'})
    if dnfb_pool > 0:
        insights.append({'type': 'info',
            'text': f'{_fmt_dollar(dnfb_pool)} is in DNFB — internal billing issue, not a payer problem. '
                    f'Same-day resolution frees this cash immediately.'})
    crit_fcs = [r for r in fc_rankings if r['action_priority'] == 'CRITICAL']
    if crit_fcs:
        c = crit_fcs[0]
        insights.append({'type': 'danger',
            'text': f'Financial class "{c["name"][:40]}" is CRITICAL: '
                    f'{c["pct_over_90"]}% over 90 days, {c["avg_denial_rate"]}% denial rate, '
                    f'{_fmt_dollar(c["total_balance"])} total balance.'})
    if denied_hard > 0:
        insights.append({'type': 'danger',
            'text': f'{_fmt_dollar(denied_hard)} in denials aged 90+ days — approaching write-off territory. '
                    f'Immediate escalation required before revenue is permanently lost.'})

    return {
        'urgency_alert': urgency_alert,
        'kpis': kpis,
        'priority_table': priority_table,
        'payer_matrix': payer_matrix,
        'waterfall': waterfall,
        'forecast': forecast,
        'fin_class_rankings': fc_rankings,
        'action_insights': insights,
    }


def get_priority_encounter_df(df_curr: pd.DataFrame) -> pd.DataFrame:
    """Return encounter-level DataFrame for Cash Action Plan download (90+ or denied pool)."""
    curr = _decat(df_curr)

    _BAL   = 'Balance Amount'
    _ENC   = 'Encounter Number'
    _DAC   = 'Discharge Aging Category'
    _DD    = 'Discharge Date'
    _RD    = 'REPORT_DATE'
    _LDD   = 'Last Denial Date'
    _CODE  = 'Last Denial Code and Reason'
    TF_COL = 'Days to Timely Filing Limit'

    deduped = curr.sort_values(_BAL, ascending=False).drop_duplicates(_ENC)

    report_dt = pd.Timestamp.now()
    if _RD in deduped.columns:
        try:
            rd = pd.to_datetime(deduped[_RD], errors='coerce').max()
            if pd.notna(rd):
                report_dt = rd
        except Exception:
            pass

    denied_df   = _get_denied_df(deduped)
    denied_encs = set(denied_df[_ENC].astype(str))
    pool = deduped[
        deduped[_DAC].isin(OVER_90_BUCKETS) |
        deduped[_ENC].astype(str).isin(denied_encs)
    ].copy()

    if _DD in pool.columns:
        dd = pd.to_datetime(pool[_DD], errors='coerce')
        pool['Days Outstanding'] = (report_dt - dd).dt.days.clip(lower=0).astype('Int64')
    else:
        pool['Days Outstanding'] = None

    if _LDD in pool.columns:
        ldd = pd.to_datetime(pool[_LDD], errors='coerce')
        pool['Denial Age (Days)'] = (report_dt - ldd).dt.days.clip(lower=0).astype('Int64')
    else:
        pool['Denial Age (Days)'] = None

    if TF_COL in pool.columns:
        tf_vals = pd.to_numeric(pool[TF_COL], errors='coerce')
        conditions = [
            tf_vals <= 14,
            tf_vals <= 30,
            tf_vals <= 60,
        ]
        choices = ['CRITICAL (<14d)', 'High (<30d)', 'Medium (<60d)']
        pool['TF Risk'] = pd.Series(
            pd.cut(tf_vals, bins=[-1, 14, 30, 60, float('inf')],
                   labels=['CRITICAL (<14d)', 'High (<30d)', 'Medium (<60d)', 'Low']),
            index=pool.index
        ).astype(str).replace('nan', 'N/A')
    else:
        pool['TF Risk'] = 'N/A'

    def _action(row):
        tf_risk  = str(row.get('TF Risk', ''))
        da       = row.get('Denial Age (Days)', None)
        dac      = str(row.get(_DAC, ''))
        code_val = str(row.get(_CODE, '')).strip().lower() if _CODE in row.index else ''
        has_denial = code_val not in _DENIAL_EMPTY and code_val not in ('', 'nan')
        if 'CRITICAL' in tf_risk or 'High' in tf_risk:
            return 'URGENT: File appeal — timely filing deadline near'
        if has_denial and da is not None and da >= 90:
            return 'Escalate to denial specialist — aged denial'
        if has_denial and da is not None and da < 30:
            return 'Quick Win — recent denial, act now'
        if dac in OVER_90_BUCKETS and not has_denial:
            return 'Chase payment — outstanding, no active denial'
        if has_denial:
            return 'Denial management — follow up on denial'
        return 'Standard follow-up and status check'

    pool['Recommended Action'] = pool.apply(_action, axis=1)
    return pool.sort_values(_BAL, ascending=False)
