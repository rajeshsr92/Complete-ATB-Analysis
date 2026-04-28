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


def compute_high_dollar_threshold(df: pd.DataFrame, pct: float = 0.60) -> dict:
    """
    Find the minimum balance where claims >= that amount account for pct% of total balance.
    Returns threshold value, encounter count, balance sum, and actual coverage pct.
    """
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
    """WoW trending: encounter count + balance total per week with deltas."""
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

        rows.append({
            'week': week,
            'encounter_count': count,
            'balance_total': round(balance, 2),
            'wow_count_delta': d_count,
            'wow_count_pct': pct_count,
            'wow_balance_delta': d_balance,
            'wow_balance_pct': pct_balance,
        })
    return rows


def aging_migration(week_a_df: pd.DataFrame, week_b_df: pd.DataFrame) -> dict:
    """
    Migration matrix: for each Encounter Number present in both weeks,
    show movement from Discharge Aging Category (week A) → (week B).
    Cell values = Balance Amount from week B, count of encounters, % of from-bucket total.
    """
    week_a_df, week_b_df = _decat(week_a_df), _decat(week_b_df)

    # Deduplicate: one row per encounter (keep highest balance where duplicates exist)
    def _dedup(df):
        return (df[['Encounter Number', 'Discharge Aging Category', 'Balance Amount']]
                .sort_values('Balance Amount', ascending=False)
                .drop_duplicates('Encounter Number'))

    a = _dedup(week_a_df).copy()
    b = _dedup(week_b_df).copy()
    a.columns = ['enc', 'from_bucket', 'from_balance']
    b.columns = ['enc', 'to_bucket', 'to_balance']

    merged = pd.merge(a, b, on='enc', how='inner')

    # Use deduped sets for new/resolved — avoids double-counting
    new_b = b[~b['enc'].isin(a['enc'])].rename(columns={'enc': 'Encounter Number', 'to_bucket': 'Discharge Aging Category', 'to_balance': 'Balance Amount'})
    resolved_a = a[~a['enc'].isin(b['enc'])].rename(columns={'enc': 'Encounter Number', 'from_bucket': 'Discharge Aging Category', 'from_balance': 'Balance Amount'})
    new_encs = new_b
    resolved_encs = resolved_a

    all_buckets_in_data = set(merged['from_bucket'].unique()) | set(merged['to_bucket'].unique())
    buckets = [b for b in BUCKET_ORDER if b in all_buckets_in_data]
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


# ── Buckets considered "90+ days" ────────────────────────────
OVER_90_BUCKETS = ['91-120', '121-150', '151-180', '181-210', '211-240',
                   '241-270', '271-300', '301-330', '331-365', '366+']
FEEDS_INTO_90  = ['61-90']   # bucket that rolls into 90+ territory


def aging_contributors(latest_df: pd.DataFrame, prior_df: pd.DataFrame, top_n: int = 15) -> dict:
    """
    90+ aging contributor analysis by Responsible Financial Class and Health Plan.
    Shows which payers are driving growth in the 90+ bucket,
    and how much volume rolled FROM 61-90 into the 90+ range.
    """
    def _dedup(df):
        return df.sort_values('Balance Amount', ascending=False).drop_duplicates('Encounter Number')

    latest_df, prior_df = _decat(latest_df), _decat(prior_df)

    curr_90  = latest_df[latest_df['Discharge Aging Category'].isin(OVER_90_BUCKETS)]
    prev_90  = prior_df[prior_df['Discharge Aging Category'].isin(OVER_90_BUCKETS)]
    prev_feed = prior_df[prior_df['Discharge Aging Category'].isin(FEEDS_INTO_90)]

    # ── summary KPIs ──────────────────────────────────────────
    c_bal  = float(curr_90['Balance Amount'].sum())
    p_bal  = float(prev_90['Balance Amount'].sum())
    c_cnt  = int(_dedup(curr_90)['Encounter Number'].nunique())
    p_cnt  = int(_dedup(prev_90)['Encounter Number'].nunique())

    # Encounters that rolled from 61-90 into 90+ (present in both, bucket changed)
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

    # ── by Responsible Financial Class ───────────────────────
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

    # Rolled-in breakdown by fin class
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

    # ── auto key points ───────────────────────────────────────
    key_points = []
    delta = round(c_bal - p_bal, 2)
    delta_pct = _safe_pct(delta, p_bal)
    key_points.append({
        'type': 'danger' if delta > 0 else 'success',
        'text': f'90+ balance {"increased" if delta > 0 else "decreased"} by '
                f'${abs(delta):,.0f} ({("+" if delta > 0 else "")}{delta_pct}%) this week.'
    })
    key_points.append({
        'type': 'warning',
        'text': f'{rolled_cnt:,} encounters (${rolled_bal:,.0f}) rolled from 61-90 days into 90+ territory.'
    })
    if by_rfc:
        top_rfc = by_rfc[0]
        key_points.append({
            'type': 'info',
            'text': f'Largest 90+ contributor by Fin. Class: '
                    f'{top_rfc["name"]} — ${top_rfc["curr_balance"]:,.0f} '
                    f'({round(top_rfc["curr_balance"] / c_bal * 100, 1) if c_bal else 0}% of total 90+).'
        })
    if by_rhp:
        # Biggest balance INCREASE
        biggest_increase = max(by_rhp, key=lambda r: r['delta_balance'], default=None)
        if biggest_increase and biggest_increase['delta_balance'] > 0:
            key_points.append({
                'type': 'danger',
                'text': f'Highest 90+ balance jump: {biggest_increase["name"]} '
                        f'+${biggest_increase["delta_balance"]:,.0f}'
                        f'{(" (" + str(biggest_increase["delta_pct"]) + "%)") if biggest_increase["delta_pct"] else ""}.'
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
