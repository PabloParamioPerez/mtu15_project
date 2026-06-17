# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: (diagnostic)
# CLAIM: Tests if 3-sess reservation pricing jumps at ISP15 announcement vs implementation
"""3-sess anticipation test: does reservation pricing jump at the
ISP15 announcement (2024-10-03) within the 3-sess regime?

Setup:
  3-sess regime = 2024-06-14 to 2024-11-30 (settlement still 60-min)
  ISP15 announcement (CNMC resolution) = 2024-10-03
  ISP15 activation = 2024-12-01

Three sub-windows:
  pre-announcement 3-sess: 2024-06-14 to 2024-10-02 (3.5 months)
  post-announcement 3-sess: 2024-10-04 to 2024-11-30 (~2 months)
  ISP15 window: 2024-12-01 onwards

If the 3-sess reservation pricing (>€100 share = 84% for GE×CCGT) is
anticipation-driven, we should see a DISCRETE JUMP in reservation share
around 2024-10-03.

If the 3-sess pricing is IDA-reform-driven (independent of ISP15
anticipation), reservation share should be elevated throughout 3-sess
without a 2024-10-03 break.

We also do a finer monthly time series of reservation share across the
entire 2024-2025 period.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# Recreate the IDA-offers panel with firm + tech
ref = pd.read_csv('data/external/omie_reference/lista_unidades.csv', encoding='latin1')
def bucket(t):
    if pd.isna(t):
        return 'Other'
    t = str(t).lower()
    if 'ciclo combinado' in t:
        return 'CCGT'
    if 'nuclear' in t:
        return 'Nuclear'
    if 'bombeo' in t or 'bomba' in t:
        return 'PumpHydro'
    if 'hidr' in t:
        return 'Hydro'
    return 'Other'
ref['tech'] = ref['technology'].apply(bucket)
con.register('ref', ref[['unit_code', 'tech']])

con.execute("""
    CREATE TEMP TABLE unit_firm AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE offer_type=1 AND grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ), ranked AS (
        SELECT unit_code, grupo_empresarial AS firm,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, firm FROM ranked WHERE rk=1
""")

print('Build IDA bid panel for Big-4 × CCGT, 2024-2025')
df = con.sql("""
    SELECT i.date, i.session_number,
           uf.firm,
           COALESCE(r.tech, 'Other') AS tech,
           i.price_eur_mwh AS p,
           i.quantity_mw AS q
    FROM 'data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet' i
    JOIN 'data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet' c
       ON c.date = i.date
      AND c.session_number = i.session_number
      AND c.offer_code = i.offer_code
      AND c.version = i.version
      AND c.buy_sell = 'V'
    LEFT JOIN unit_firm uf ON uf.unit_code = i.unit_code
    LEFT JOIN ref r ON r.unit_code = i.unit_code
    WHERE i.quantity_mw > 0
      AND i.price_eur_mwh BETWEEN -500 AND 4000
      AND uf.firm IN ('GE','IB','GN','HC')
      AND COALESCE(r.tech,'Other') = 'CCGT'
      AND CAST(i.date AS DATE) BETWEEN DATE '2024-04-01' AND DATE '2025-12-31'
""").df()
df['date'] = pd.to_datetime(df['date'])
df['ym'] = df['date'].dt.to_period('M').dt.to_timestamp()

# --- Sub-window analysis around ISP15 announcement ---
print('\n=== Sub-window analysis around ISP15 announcement (2024-10-03) ===\n')
windows = {
    'pre-IDA tail (Apr-Jun 2024)':           ('2024-04-01', '2024-06-13'),
    '3-sess PRE-announcement':               ('2024-06-14', '2024-10-02'),
    '3-sess POST-announcement':              ('2024-10-04', '2024-11-30'),
    'ISP15 window':                          ('2024-12-01', '2025-03-18'),
    'DA60/ID15':                             ('2025-03-19', '2025-09-30'),
    'DA15/ID15':                             ('2025-10-01', '2025-12-31'),
}

print(f"{'firm':<5} {'window':<32}  {'n_sess':>7}  {'q_GWh':>7}  {'p_wmean':>8}  {'res>€100':>9}")
for firm in ['GE', 'IB', 'GN', 'HC']:
    for wname, (s, e) in windows.items():
        sub = df[(df['firm'] == firm) & (df['date'] >= s) & (df['date'] <= e)]
        if len(sub) < 100:
            continue
        q_total = sub['q'].sum()
        wmean = (sub['p'] * sub['q']).sum() / q_total if q_total > 0 else np.nan
        res = sub.loc[sub['p'] > 100, 'q'].sum() / q_total if q_total > 0 else np.nan
        n_sess = len(sub.groupby(['date', 'session_number']))
        print(f"{firm:<5} {wname:<32}  {n_sess:>7}  {q_total/1e3:>7.0f}  {wmean:>8.1f}  {res:>9.2%}")
    print()

# --- Monthly time series of reservation share ---
print('=== Monthly reservation share (>€100/MWh) per firm ===\n')
monthly_res = df.groupby(['firm', 'ym']).apply(
    lambda g: g.loc[g['p'] > 100, 'q'].sum() / g['q'].sum() if g['q'].sum() > 0 else np.nan
).reset_index()
monthly_res.columns = ['firm', 'ym', 'res_share']
piv = monthly_res.pivot(index='ym', columns='firm', values='res_share')
piv = piv.reindex(columns=['GE', 'IB', 'GN', 'HC'])
print(piv.round(3).to_string())

# Save the panel for nb14 figure if useful
piv.to_parquet('data/derived/panels/anticipation_test_panel.parquet')

# --- Summary: jump test ---
print('\n=== Jump test: average reservation share PRE vs POST 2024-10-03 within 3-sess ===\n')
for firm in ['GE', 'IB', 'GN', 'HC']:
    pre_w = df[(df['firm'] == firm)
                & (df['date'] >= '2024-06-14') & (df['date'] < '2024-10-04')]
    post_w = df[(df['firm'] == firm)
                & (df['date'] >= '2024-10-04') & (df['date'] <= '2024-11-30')]
    pre_res  = pre_w.loc[pre_w['p'] > 100, 'q'].sum() / pre_w['q'].sum() if pre_w['q'].sum() > 0 else np.nan
    post_res = post_w.loc[post_w['p'] > 100, 'q'].sum() / post_w['q'].sum() if post_w['q'].sum() > 0 else np.nan
    delta = post_res - pre_res
    print(f"  {firm}: pre-announce {pre_res:.2%}, post-announce {post_res:.2%}, "
          f"Δ = {delta:+.2%}")
