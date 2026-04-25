"""Why does IB hold reservation pricing longer than GE/GN/HC?

Hypotheses:
  (H1) Portfolio composition: IB has more hydro and storage; firms with
       more flexible technology benefit longer from reservation pricing
       because they can clear at high prices when scarce.
  (H2) Bilateral-contract structure: post-Rule 28.8 elimination
       (March 2025), firms with more bilateral coverage have less
       exposure to spot prices and continue strategic IDA bidding.
  (H3) Market share / dominance: a firm with higher CCGT market share
       has more market power per unit and benefits longer from
       reservation.

Test: per firm, time-varying:
  - share of own offered MW that's hydro vs CCGT
  - share of own cleared MW that's hydro vs CCGT  
  - market share in CCGT specifically
  - bid-function reservation share evolution
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

# Tech ref + unit→firm
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
    if 'almacen' in t:
        return 'Storage'
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

# 1. Firm portfolio composition over time (offered MW by tech)
print('=== Firm portfolio composition (DA cleared MW share by tech) ===')
print('       Per regime, what fraction of each firm\'s cleared MW is each tech?')
df = con.sql("""
    SELECT p.date,
           CASE WHEN p.mtu_minutes=15 THEN CEIL(p.period/4.0)::INTEGER ELSE p.period END AS hour,
           p.grupo_empresarial AS firm,
           COALESCE(r.tech, 'Other') AS tech,
           SUM(p.assigned_power_mw) /
              CASE WHEN p.mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet' p
    LEFT JOIN ref r ON r.unit_code = p.unit_code
    WHERE p.offer_type=1 AND p.assigned_power_mw > 0
      AND p.grupo_empresarial IN ('GE','IB','GN','HC')
    GROUP BY 1, 2, 3, 4, p.mtu_minutes
""").df()
df['date'] = pd.to_datetime(df['date'])

REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']

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

# Per-firm × regime: tech share of cleared MW
firm_total = df.groupby(['firm', 'regime'])['q_mwh'].sum()
firm_tech = df.groupby(['firm', 'regime', 'tech'])['q_mwh'].sum()
share = (firm_tech / firm_total).unstack('tech').fillna(0)
share = share.reindex(REGIME_ORDER, level='regime')
print(share.round(3).to_string())

# 2. CCGT market share per firm × regime
print('\n=== CCGT market share per firm × regime ===')
ccgt_total = df[df['tech']=='CCGT'].groupby('regime')['q_mwh'].sum()
firm_ccgt = df[df['tech']=='CCGT'].groupby(['firm','regime'])['q_mwh'].sum()
ccgt_share = firm_ccgt / ccgt_total
ccgt_share = ccgt_share.unstack('firm')
ccgt_share = ccgt_share.reindex(REGIME_ORDER)
print((ccgt_share * 100).round(2).to_string())

# 3. IB-specific deep dive: late-2025 reservation behavior
print('\n=== IB-specific: Apr 2025 onwards monthly reservation share by tech ===')
# IDA bid-tranche level
con.execute("""
    CREATE OR REPLACE TEMP TABLE ida_offers AS
    SELECT i.date,
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
      AND CAST(i.date AS DATE) BETWEEN DATE '2025-01-01' AND DATE '2026-01-31'
""")

ib_df = con.sql("""
    SELECT date, tech, p, q
    FROM ida_offers WHERE firm = 'IB'
""").df()
ib_df['date'] = pd.to_datetime(ib_df['date'])
ib_df['ym'] = ib_df['date'].dt.to_period('M').dt.to_timestamp()

# IB monthly reservation share by tech
print('\nIB reservation share (>€100/MWh), monthly × tech 2025:')
def res_share(g):
    return g.loc[g['p']>100, 'q'].sum() / g['q'].sum() if g['q'].sum() > 0 else np.nan
piv = ib_df.groupby(['ym','tech']).apply(res_share).unstack('tech')
print(piv.round(3).to_string())

# Same for GE
ge_df = con.sql("""
    SELECT date, tech, p, q
    FROM ida_offers WHERE firm = 'GE'
""").df()
ge_df['date'] = pd.to_datetime(ge_df['date'])
ge_df['ym'] = ge_df['date'].dt.to_period('M').dt.to_timestamp()

print('\nGE reservation share (>€100/MWh), monthly × tech 2025:')
piv2 = ge_df.groupby(['ym','tech']).apply(res_share).unstack('tech')
print(piv2.round(3).to_string())
