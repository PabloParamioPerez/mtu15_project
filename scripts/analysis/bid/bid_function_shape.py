# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B2
# CLAIM: Bid-function moments (p25-p95, reservation share >EUR100) per regime / firm
"""GE/IB IDA sell-side bid function shape analysis.

Economic model the patterns motivate:

  Hortaçsu-Puller (2008) optimal bid function under uncertainty:
    p*(q) = MC(q) + q · |residual demand slope|^{-1}
                 = MC(q) + markup(q)

  Strategic firms post bid SCHEDULES (offer curves), not point bids.
  The shape of the bid function (slope, dispersion of price tranches,
  highest-price tranche level) reveals the firm's strategic position.

  Test: per (regime, firm), compute moments of the price distribution
  in offer tranches:
    - p25, p50, p75, p95 of price tranches
    - quantity-weighted average price (already in nb13)
    - dispersion: p95 - p25 (range of bidding)
    - share of capacity offered above €100/MWh ("reservation tranches")

  Predictions:
    - Strategic withholding ↑ price-tranches above marginal cost
    - Asymmetric-granularity DA60/ID15 ↑ reservation-tranche use
    - MTU15-DA closure ↓ reservation-tranches
"""
from __future__ import annotations
import duckdb
import numpy as np
import pandas as pd


con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=4")

REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']

print('Build IDA sell-side offer panel with per-tranche prices')
con.execute("""
    CREATE TEMP TABLE unit_firm AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE offer_type=1 AND grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ), ranked AS (
        SELECT unit_code, grupo_empresarial,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, grupo_empresarial AS firm
    FROM ranked WHERE rk=1
""")

# Tech mapping
ref = pd.read_csv('data/external/omie_reference/lista_unidades.csv', encoding='latin1')
def bucket(t):
    if pd.isna(t):
        return 'Other'
    t = str(t).lower()
    if 'ciclo combinado' in t:
        return 'CCGT'
    if 'nuclear' in t:
        return 'Nuclear'
    if 'hidr' in t:
        return 'Hydro'
    return 'Other'
ref['tech'] = ref['technology'].apply(bucket)
con.register('ref', ref[['unit_code', 'tech']])

con.execute("""
    CREATE TEMP TABLE ida_offers AS
    SELECT i.date, i.session_number,
           i.unit_code,
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
""")

# Per-firm price-tranche distribution (each tranche weighted by its q)
print('\n1. IDA sell-side price-tranche distribution moments per firm × regime')
print('   (weighted by tranche quantity)\n')

df = con.sql("""
    SELECT date, firm, tech, p, q
    FROM ida_offers
    WHERE firm IN ('GE','IB','GN','HC')
""").df()
df['date'] = pd.to_datetime(df['date'])

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

def wmoments(group):
    p = group['p'].values
    q = group['q'].values
    if q.sum() == 0:
        return pd.Series({'q_total_GWh': 0, 'p_wmean': np.nan, 'p25': np.nan,
                          'p50': np.nan, 'p75': np.nan, 'p95': np.nan, 'p_max': np.nan,
                          'share_above_100': np.nan, 'share_above_300': np.nan})
    # Quantity-weighted percentiles
    order = np.argsort(p)
    p_sorted = p[order]
    q_sorted = q[order]
    cumq = np.cumsum(q_sorted) / q_sorted.sum()
    def percentile(pct):
        idx = np.searchsorted(cumq, pct)
        if idx >= len(p_sorted):
            return p_sorted[-1]
        return p_sorted[idx]
    return pd.Series({
        'q_total_GWh': q.sum() / 1e3,
        'p_wmean':     (p * q).sum() / q.sum(),
        'p25':         percentile(0.25),
        'p50':         percentile(0.50),
        'p75':         percentile(0.75),
        'p95':         percentile(0.95),
        'p_max':       p.max(),
        'share_above_100': q[p > 100].sum() / q.sum(),
        'share_above_300': q[p > 300].sum() / q.sum(),
    })

print(f"{'firm':<5} {'tech':<8} {'regime':<14}  {'q_GWh':>7}  {'p_wmean':>8}  {'p25':>5}  {'p50':>5}  {'p75':>5}  {'p95':>5}  {'>€100':>5}  {'>€300':>5}")
for firm in ['GE', 'IB']:
    for tech in ['CCGT', 'Hydro', 'Nuclear']:
        sub = df[(df['firm'] == firm) & (df['tech'] == tech)]
        if len(sub) == 0:
            continue
        for r in REGIME_ORDER:
            srm = sub[sub['regime'] == r]
            if len(srm) < 10:
                continue
            m = wmoments(srm)
            print(f"{firm:<5} {tech:<8} {r:<14}  "
                  f"{m['q_total_GWh']:>7.0f}  "
                  f"{m['p_wmean']:>8.1f}  "
                  f"{m['p25']:>5.0f}  "
                  f"{m['p50']:>5.0f}  "
                  f"{m['p75']:>5.0f}  "
                  f"{m['p95']:>5.0f}  "
                  f"{m['share_above_100']:>5.2f}  "
                  f"{m['share_above_300']:>5.2f}")
        print()
