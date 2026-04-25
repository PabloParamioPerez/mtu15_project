"""(1) Cross-check ESIOS impdsvqh aggregated vs ENTSO-E A87 net income.

ENTSO-E A87 publishes:
  expenses (TSO -> BSPs):  reserve procurement + activation costs
  net income (BRPs -> TSO): imbalance settlement receipts

ESIOS impdsvqh publishes:
  quarter-hourly EUR amount of system imbalance settlement.
  This corresponds (approximately) to A87 net income, signed by
  surplus/deficit direction.

If aggregating |impdsvqh| or signed impdsvqh to monthly gives a series
that tracks A87 net income, we have cross-source confirmation.

(2) Compare segment-level imbalance volumes (MWh) by regime:
    endrozrqh — conventional in regulation zone
    endronzqh — conventional outside regulation zone
    endreeoqh — RE wind
    endrehiqh — RE hydro
    endretqh  — RE thermal
    endcurqh  — COR retailers
    endlibqh  — free-market retailers

If a segment dominates the post-ISP15 |imbalance| MWh rise, that is
the segment that drives the cost.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

LIQ = 'data/processed/esios/liquicomun_all.parquet'
A87 = 'data/processed/entsoe/balancing/financial_balance_all.parquet'

# ----------------------------------------------------------------
# 1. Cross-check ESIOS impdsvqh vs ENTSO-E A87
# ----------------------------------------------------------------
print('=' * 70)
print('(1) ESIOS impdsvqh vs ENTSO-E A87 — monthly comparison')
print('=' * 70)
print()

esios_imp = con.sql(f"""
    SELECT DATE_TRUNC('month', date)::DATE AS month,
           SUM(value)        AS sum_signed_eur,
           SUM(ABS(value))   AS sum_abs_eur,
           COUNT(*)          AS n_isp,
           AVG(value)        AS avg_eur_per_isp
    FROM '{LIQ}'
    WHERE family = 'impdsvqh'
    GROUP BY 1 ORDER BY 1
""").df()
esios_imp['month'] = pd.to_datetime(esios_imp['month'])

a87 = con.sql(f"""
    SELECT month,
           MAX(CASE WHEN direction_code='A01' THEN amount_eur END) AS a87_expenses,
           MAX(CASE WHEN direction_code='A02' THEN amount_eur END) AS a87_net_income
    FROM '{A87}'
    GROUP BY 1 ORDER BY 1
""").df()
a87['month'] = pd.to_datetime(a87['month'])

cmp = esios_imp.merge(a87, on='month', how='inner')
cmp['esios_imp_M']    = cmp['sum_abs_eur']    / 1e6
cmp['esios_signed_M'] = cmp['sum_signed_eur'] / 1e6
cmp['a87_income_M']   = cmp['a87_net_income'] / 1e6
cmp['a87_expenses_M'] = cmp['a87_expenses']   / 1e6

print(f"{'month':<12}  {'ESIOS|imp|':>10}  {'ESIOS sgn':>10}  {'A87 income':>10}  {'A87 expns':>10}")
for _, r in cmp.iterrows():
    print(f"{r['month'].strftime('%Y-%m'):<12}  "
          f"{r['esios_imp_M']:>10.2f}  "
          f"{r['esios_signed_M']:>+10.2f}  "
          f"{r['a87_income_M']:>10.2f}  "
          f"{r['a87_expenses_M']:>10.2f}")

# Correlations
if len(cmp) > 5:
    print()
    print(f'corr(|ESIOS imp|, A87 net income): {cmp[["esios_imp_M","a87_income_M"]].corr().iloc[0,1]:.3f}')
    print(f'corr(ESIOS signed, A87 net income): {cmp[["esios_signed_M","a87_income_M"]].corr().iloc[0,1]:.3f}')

# ----------------------------------------------------------------
# 2. Segment decomposition of imbalance volumes by regime
# ----------------------------------------------------------------
print()
print('=' * 70)
print('(2) Segment decomposition of imbalance volumes |MWh| per ISP')
print('=' * 70)
print()

SEGMENTS = {
    'endrozrqh': 'Conv in regulation zone',
    'endronzqh': 'Conv outside reg zone',
    'endreeoqh': 'RE Wind',
    'endrehiqh': 'RE Hydro',
    'endretqh':  'RE Thermal',
    'endcurqh':  'COR retailers',
    'endlibqh':  'Free-market retailers',
    'endexpqh':  'Export units',
    'endimpqh':  'Import units',
}

REGIME_ORDER = ['ISP15 window', 'DA60/ID15', 'DA15/ID15']

def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-12-01'):
        return 'pre-ISP15'   # only cdsvbrp pre-ISP15 anyway
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15 window'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15'
    return 'DA15/ID15'

df = con.sql(f"""
    SELECT date, family, value
    FROM '{LIQ}'
    WHERE family IN ({','.join(repr(k) for k in SEGMENTS)})
""").df()
df['date'] = pd.to_datetime(df['date'])
df['regime'] = df['date'].apply(assign_regime)
df['abs_value'] = df['value'].abs()

# Per regime, sum of |imbalance MWh| per segment, plus daily mean
agg = df.groupby(['family', 'regime']).agg(
    n=('abs_value', 'count'),
    total_GWh=('abs_value', lambda x: x.sum() / 1e3),
    mean_MWh=('abs_value', 'mean'),
).reset_index()

# Pivot: regimes as columns, families as rows
piv_total = agg.pivot(index='family', columns='regime', values='total_GWh').reindex(columns=REGIME_ORDER)
piv_mean  = agg.pivot(index='family', columns='regime', values='mean_MWh').reindex(columns=REGIME_ORDER)

print('Total |imbalance MWh| (GWh) per regime × segment:')
print(piv_total.round(1).to_string())
print()
print('Mean |imbalance MWh| per ISP × segment:')
print(piv_mean.round(2).to_string())
print()

# Share of total |imbalance| per regime contributed by each segment
print('Share of total |imbalance| per regime (% by segment):')
piv_share = piv_total.div(piv_total.sum(axis=0), axis=1) * 100
print(piv_share.round(1).to_string())
print()
# Friendly labels
for k, v in SEGMENTS.items():
    print(f'  {k} = {v}')

# ----------------------------------------------------------------
# 3. Skepticism: Apr-Sep 2025 vs same-calendar pre-ISP15 baseline
#    Note: pre-ISP15 has NO quarter-hourly data, so we can only
#    compare ISP15+ vintages. The skepticism is whether DA60/ID15
#    segment shares differ from DA15/ID15 (which is the relevant
#    cross-reform contrast).
# ----------------------------------------------------------------
print()
print('=' * 70)
print('(3) Per-segment regime contrasts (DA60/ID15 vs DA15/ID15)')
print('=' * 70)
print()
print(f"{'segment':<25}  {'DA60/ID15 GWh':>13}  {'DA15/ID15 GWh':>13}  {'Δ vs DA60/ID15':>15}")
for fam, label in SEGMENTS.items():
    row = piv_total.loc[fam]
    val_60 = row.get('DA60/ID15', np.nan)
    val_15 = row.get('DA15/ID15', np.nan)
    if pd.isna(val_60) or pd.isna(val_15):
        continue
    delta_pct = (val_15 - val_60) / val_60 * 100 if val_60 > 0 else np.nan
    print(f"{label:<25}  {val_60:>13.1f}  {val_15:>13.1f}  {delta_pct:>+15.1f}%")
