"""Cleaner seasonal correction with pre-IDA as explicit reference category."""
from __future__ import annotations
import duckdb
import pandas as pd
import statsmodels.api as sm
from pathlib import Path

LER = Path('data/derived/firm_lerner_hourly.parquet')

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

df = con.sql(f"""
    SELECT date, hour, firm, lerner_index, clearing_price_eur_mwh AS p
    FROM '{LER}'
    WHERE lerner_index BETWEEN 0 AND 1
""").df()
df['date'] = pd.to_datetime(df['date'])
df['month'] = df['date'].dt.month
df['year'] = df['date'].dt.year

def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-06-14'):
        return 'pre-IDA'
    if d < pd.Timestamp('2024-12-01'):
        return '3-sess'
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15 window'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15'
    return 'DA15/ID15'
df['regime'] = df['date'].apply(assign_regime)

# Force pre-IDA as the dropped reference
REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']
df['regime_cat'] = pd.Categorical(df['regime'], categories=REGIME_ORDER, ordered=False)

# Price bin
df['p_bin'] = pd.cut(df['p'], bins=[-1000, 0, 25, 50, 100, 200, 1e6],
                     labels=['neg', '0-25', '25-50', '50-100', '100-200', '200+'])

print('=' * 100)
print('Regime contrasts vs pre-IDA reference, three specs (month FE, m+y FE, p_bin FE)')
print('=' * 100)
print(f"\n{'firm':<5} {'regime':<14}  {'raw med':>8}  {'spec1: m':>15}  {'spec2: m+y':>15}  {'spec3: p_bin':>15}")

for firm in ['GE', 'IB', 'GN', 'HC']:
    sub = df[df['firm'] == firm].copy()

    def fit(extra_cols):
        # Build design with pre-IDA as reference
        X_parts = []
        # regime dummies (keep all 4 non-reference)
        rd = pd.get_dummies(sub['regime_cat'], prefix='regime', drop_first=False, dtype=float)
        rd = rd.drop(columns='regime_pre-IDA')
        X_parts.append(rd)
        # other FE columns
        for c in extra_cols:
            d = pd.get_dummies(sub[c], prefix=c, drop_first=True, dtype=float)
            X_parts.append(d)
        X = pd.concat(X_parts, axis=1).assign(const=1.0)
        y = sub['lerner_index'].astype(float)
        return sm.OLS(y, X).fit(cov_type='HC3')

    res1 = fit(['month'])
    res2 = fit(['month', 'year'])
    res3 = fit(['p_bin'])

    base_med = sub.loc[sub['regime'] == 'pre-IDA', 'lerner_index'].median()
    print(f"{firm:<5} {'pre-IDA (ref)':<14}  {base_med:>8.3f}  {'-':>15}  {'-':>15}  {'-':>15}")

    for r in ['3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']:
        col = f'regime_{r}'
        raw_val = sub.loc[sub['regime'] == r, 'lerner_index'].median()

        def fmt(res):
            if col in res.params.index:
                return f'{res.params[col]:+.3f} ({res.bse[col]:.3f})'
            return '       —       '

        print(f"{firm:<5} {r:<14}  {raw_val:>8.3f}  {fmt(res1):>15}  {fmt(res2):>15}  {fmt(res3):>15}")
    print()
