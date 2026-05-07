# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: S1
# CLAIM: A87 net income decomposition into impdsvqh + reserve cost (segment-level)
"""Decompose A87 net income into:
  (a) direct imbalance settlement   = ESIOS sum_abs(impdsvqh)
  (b) reserve-cost allocation       = ESIOS cdvbrp × endvBRPqh

cdvbrp     = average quarter-hourly deviation cost per BRP (€/MWh)
endvBRPqh  = quarter-hourly energy of deviations for cost allocation (MWh)

Hypothesis: A87 net income ≈ |direct imbalance| + reserve-cost
allocation. If yes, the 2× → 1× shift at MTU15-DA is mechanically
explained.
"""
from __future__ import annotations
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")

LIQ = 'data/processed/esios/liquicomun_all.parquet'
A87 = 'data/processed/entsoe/balancing/financial_balance_all.parquet'

# Aggregate impdsvqh (direct imbalance € per ISP) monthly
imp = con.sql(f"""
    SELECT DATE_TRUNC('month', date)::DATE AS month,
           SUM(ABS(value))/1e6 AS direct_imp_M,
           SUM(value)/1e6      AS direct_imp_signed_M
    FROM '{LIQ}'
    WHERE family = 'impdsvqh'
    GROUP BY 1
""").df()

# cdvbrp is € per MWh per ISP (price). endvBRPqh is MWh per ISP (volume).
# Reserve-cost allocation: per-ISP cdvbrp × endvBRPqh, summed monthly.
# We need to JOIN on (date, hour, quarter).
res = con.sql(f"""
    WITH p AS (
        SELECT date, hour, quarter, value AS price
        FROM '{LIQ}' WHERE family = 'cdvbrp'
    ),
    q AS (
        SELECT date, hour, quarter, value AS qty
        FROM '{LIQ}' WHERE family = 'endvBRPqh'
    )
    SELECT DATE_TRUNC('month', p.date)::DATE AS month,
           SUM(p.price * q.qty) / 1e6 AS reserve_cost_M,
           SUM(ABS(p.price * q.qty)) / 1e6 AS reserve_cost_abs_M,
           COUNT(*) AS n
    FROM p JOIN q USING (date, hour, quarter)
    GROUP BY 1
""").df()

# A87 net income
a87 = con.sql(f"""
    SELECT month,
           MAX(CASE WHEN direction_code='A02' THEN amount_eur END)/1e6 AS a87_income_M,
           MAX(CASE WHEN direction_code='A01' THEN amount_eur END)/1e6 AS a87_expenses_M
    FROM '{A87}'
    GROUP BY 1
""").df()

for d in [imp, res, a87]:
    d['month'] = pd.to_datetime(d['month'])

cmp = imp.merge(res, on='month', how='outer').merge(a87, on='month', how='outer')
cmp = cmp.sort_values('month').reset_index(drop=True)
cmp = cmp[cmp['month'] >= '2024-12']
cmp['sum_check_M'] = cmp['direct_imp_M'].fillna(0) + cmp['reserve_cost_M'].fillna(0)
cmp['ratio_a87_to_check'] = cmp['a87_income_M'] / cmp['sum_check_M']

print('Monthly A87 decomposition (€M):')
print()
print(f"{'month':<10}  {'A87 inc':>8}  {'imp_dir':>8}  {'res_cost':>9}  "
      f"{'imp+res':>8}  {'A87/check':>10}  {'A87 expns':>10}")
for _, r in cmp.iterrows():
    print(f"{r['month'].strftime('%Y-%m'):<10}  "
          f"{r['a87_income_M']:>8.1f}  "
          f"{r['direct_imp_M']:>8.1f}  "
          f"{r['reserve_cost_M']:>9.1f}  "
          f"{r['sum_check_M']:>8.1f}  "
          f"{r['ratio_a87_to_check']:>10.3f}  "
          f"{r['a87_expenses_M']:>10.1f}")

# Look at when reserve-cost component changes
print()
print('reserve_cost_M by regime:')
def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp('2024-12-01'):
        return 'pre-ISP15'
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15 window'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15'
    return 'DA15/ID15'
cmp['regime'] = cmp['month'].apply(assign_regime)
agg = cmp.groupby('regime', sort=False).agg(
    n=('a87_income_M','count'),
    a87_avg=('a87_income_M','mean'),
    direct_avg=('direct_imp_M','mean'),
    reserve_avg=('reserve_cost_M','mean'),
).round(1)
print(agg.to_string())

# Test: is reserve_cost = A87 - direct_imp?
print()
print('Implied reserve cost (A87 income − direct impdsvqh) vs computed reserve cost:')
cmp['implied_reserve'] = cmp['a87_income_M'] - cmp['direct_imp_M']
print(f"{'month':<10}  {'implied':>10}  {'computed':>10}")
for _, r in cmp.iterrows():
    print(f"{r['month'].strftime('%Y-%m'):<10}  "
          f"{r['implied_reserve']:>10.2f}  "
          f"{r['reserve_cost_M']:>10.2f}")
