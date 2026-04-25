# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: S1, S5, B3
# CLAIM: Stress-test 'remaining robust findings' for pre-2024 secular trends
"""Stress-test the remaining 'robust' findings:
  1. HHI rise: was concentration trending UP pre-2022 too?
  2. XBID liquidity 15×: was it growing pre-reform secularly?
  3. A87 settlement swing: was anything similar in pre-reform winters?
"""
from __future__ import annotations
import duckdb
import pandas as pd

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=6")

# ---- (1) HHI long history ----
print('=' * 80)
print('(1) HHI long history — was concentration already trending up pre-2022?')
print('=' * 80)

con.execute("""
    CREATE TEMP TABLE firm_q AS
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           COALESCE(grupo_empresarial,'NA') AS firm,
           SUM(assigned_power_mw) /
              CASE WHEN mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
    WHERE offer_type = 1 AND assigned_power_mw > 0
    GROUP BY 1, 2, 3, mtu_minutes
""")
con.execute("""
    CREATE TEMP TABLE share AS
    SELECT date, hour, firm, q_mwh,
           q_mwh / SUM(q_mwh) OVER (PARTITION BY date, hour) AS s
    FROM firm_q
""")

df = con.sql("""
    SELECT DATE_TRUNC('year', CAST(date AS DATE))::DATE AS yr,
           AVG(hhi)        AS hhi_mean,
           AVG(big4_share) AS big4_share
    FROM (
        SELECT date, hour,
               SUM(s*s) AS hhi,
               SUM(CASE WHEN firm IN ('GE','IB','GN','HC') THEN s END) AS big4_share
        FROM share
        GROUP BY date, hour
    )
    GROUP BY 1 ORDER BY 1
""").df()
df['yr'] = pd.to_datetime(df['yr']).dt.year
print('\nAnnual HHI + Big-4 share:')
print(df.to_string(index=False))

# ---- (2) XBID liquidity long history ----
print()
print('=' * 80)
print('(2) XBID liquidity long history — was orders/h growing pre-reform?')
print('=' * 80)
xbid = con.sql("""
    SELECT DATE_TRUNC('year', CAST(date AS DATE))::DATE AS yr,
           AVG(n_orders) avg_orders,
           AVG(n_trades) avg_trades,
           AVG(fill_rate) avg_fill,
           AVG(sd_trade_price) avg_sd
    FROM 'data/derived/panels/xbid_liquidity_hourly.parquet'
    GROUP BY 1 ORDER BY 1
""").df()
xbid['yr'] = pd.to_datetime(xbid['yr']).dt.year
print()
print(xbid.round(2).to_string(index=False))

# ---- (3) A87 long history ----
print()
print('=' * 80)
print('(3) A87 net income — was there a similar swing in any pre-reform winter?')
print('=' * 80)
a87 = con.sql("""
    SELECT month,
           MAX(CASE WHEN direction_code='A01' THEN amount_eur END)/1e6 AS expenses_M,
           MAX(CASE WHEN direction_code='A02' THEN amount_eur END)/1e6 AS net_income_M
    FROM 'data/processed/entsoe/balancing/financial_balance_all.parquet'
    GROUP BY month ORDER BY month
""").df()
a87['month'] = pd.to_datetime(a87['month'])
a87['year'] = a87['month'].dt.year
a87['m'] = a87['month'].dt.month

# Mean per (calendar month, year-bucket)
print('\nA87 net income (€M/month) by year × month:\n')
piv = a87.pivot(index='year', columns='m', values='net_income_M').round(1)
piv.columns = [f'{m:02d}' for m in piv.columns]
print(piv.to_string())

# Annual mean
print('\nAnnual mean A87 net income (€M/month):')
yr = a87.groupby('year')['net_income_M'].mean().round(1)
print(yr.to_string())

# Year-over-year change
print('\nYear-over-year Δ in net income mean:')
print(yr.diff().round(1).to_string())
