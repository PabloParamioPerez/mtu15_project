# STATUS: DEAD-KEPT-AS-RECORD
# LAST-AUDIT: 2026-04-26
# FEEDS: (superseded by pigouvian_clean_regression.py for S7)
# CLAIM: Raw per-segment marginal imbalance cost (no seasonal controls); replaced by clean version
"""Marginal imbalance cost regression — fix NaN handling.
NaN in segment files means 'no deviation that quarter' = 0.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import statsmodels.api as sm

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
LIQ = 'data/processed/esios/liquicomun_all.parquet'

wide = con.sql(f"""
    SELECT date, hour, quarter,
           MAX(CASE WHEN family='impdsvqh' THEN value END)  AS imp_eur,
           MAX(CASE WHEN family='endrozrqh' THEN value END) AS conv_rz,
           MAX(CASE WHEN family='endronzqh' THEN value END) AS conv_nrz,
           MAX(CASE WHEN family='endreeoqh' THEN value END) AS wind,
           MAX(CASE WHEN family='endrehiqh' THEN value END) AS hydro,
           MAX(CASE WHEN family='endretqh' THEN value END)  AS thermal_re,
           MAX(CASE WHEN family='endcurqh' THEN value END)  AS cor_ret,
           MAX(CASE WHEN family='endlibqh' THEN value END)  AS lib_ret,
           MAX(CASE WHEN family='endexpqh' THEN value END)  AS export_u,
           MAX(CASE WHEN family='endimpqh' THEN value END)  AS import_u,
           MAX(CASE WHEN family='imresecqh' THEN value END) AS sec_reserve_eur
    FROM '{LIQ}' GROUP BY 1, 2, 3
""").df()
wide['date'] = pd.to_datetime(wide['date'])
wide = wide.dropna(subset=['imp_eur'])

def regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-12-01'): return 'pre-ISP15'
    if d < pd.Timestamp('2025-03-19'): return 'ISP15 win'
    if d < pd.Timestamp('2025-10-01'): return 'DA60/ID15'
    return 'DA15/ID15'
wide['regime'] = wide['date'].apply(regime)

# Fill NaN with 0 (NaN = no deviation in that segment that ISP)
SEGS = ['conv_rz','conv_nrz','wind','hydro','thermal_re','cor_ret','lib_ret','export_u','import_u']
for s in SEGS:
    wide[s] = wide[s].fillna(0)
    wide[f'abs_{s}'] = wide[s].abs()
wide['abs_imp'] = wide['imp_eur'].abs()

REGIMES = ['ISP15 win', 'DA60/ID15', 'DA15/ID15']

print('=' * 80)
print('Marginal imbalance cost €/MWh per segment (OLS, abs vals, all regimes)')
print('=' * 80)
print()
print(f'{"":<13}  ' + '  '.join(f'{r:>13}' for r in REGIMES))
print(f'{"":<13}  ' + '  '.join(f'{"β (€/MWh)":>13}' for _ in REGIMES))

for s in SEGS:
    line = f'{s:<13}  '
    for r in REGIMES:
        sub = wide[wide['regime'] == r]
        if len(sub) < 100:
            continue
        X = sm.add_constant(sub[[f'abs_{s}']].astype(float))
        y = sub['abs_imp'].astype(float)
        res = sm.OLS(y, X).fit(cov_type='HC3')
        beta = res.params[f'abs_{s}']
        p = res.pvalues[f'abs_{s}']
        marker = '*' if p < 0.001 else ' '
        line += f'{beta:>+11.2f}{marker}  '
    print(line)

# Joint regression with all segments at once (multivariate)
print()
print('=' * 80)
print('Joint regression: |imp_eur| ~ all segments simultaneously')
print('=' * 80)
for r in REGIMES:
    sub = wide[wide['regime'] == r]
    if len(sub) < 100:
        continue
    X = sub[[f'abs_{s}' for s in SEGS]].astype(float)
    X = sm.add_constant(X)
    y = sub['abs_imp'].astype(float)
    res = sm.OLS(y, X).fit(cov_type='HC3')
    print(f'\n--- {r} (n={len(sub):,}, R²={res.rsquared:.3f}) ---')
    print(f'  {"":<14}  {"β (€/MWh)":>11}  {"se":>9}  {"p":>7}')
    for nm in ['const'] + [f'abs_{s}' for s in SEGS]:
        b = res.params[nm]
        se = res.bse[nm]
        p = res.pvalues[nm]
        s = '*' if p < 0.001 else (' ' if p > 0.05 else '.')
        label = nm.replace('abs_','')
        print(f'  {label:<14}  {b:>+11.2f}{s} {se:>9.2f}  {p:>7.3f}')
