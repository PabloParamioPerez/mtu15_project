"""Welfare-proxy analysis: share of cleared MW that came from
high-price (>€100) IDA bid tranches per regime.

Logic:
  For each (date, session, firm, tech) cell:
    - Sort the firm's offered tranches by price ascending.
    - Clearing volume Q_cleared (from pibcie if available, else
      derived from per-firm cleared in IDA — but that's not in our
      data, so we approximate).

  Approach: at each session's clearing price p*, a tranche is
  "infra-marginal cleared" if its price < p* and the firm's cumulative
  offered MW up to that tranche ≤ firm's total cleared in that session.

  Simpler approach we'll take: for each (date, session, firm),
  approximate cleared = sum of offered tranches priced ≤ p* (assumption:
  rationing-pro-rata at p* is small).

  Welfare proxy: fraction of cleared revenue that came from tranches
  with price > €100. If this fraction rises post-reform, firms are
  earning more revenue from high-bid tranches that DID clear (they
  weren't pure reservation; they were marginal).
"""
from __future__ import annotations
import duckdb
import pandas as pd
import numpy as np

con = duckdb.connect()
con.execute("SET memory_limit='10GB'")
con.execute("SET threads=6")

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

# Load tech ref + unit→firm
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

# Build IDA offered tranches with clearing price attached
print('1. Build IDA offered-tranche panel with clearing prices')
con.execute("""
    CREATE TEMP TABLE ida_tranches AS
    SELECT i.date, i.session_number,
           uf.firm,
           COALESCE(r.tech, 'Other') AS tech,
           i.unit_code,
           i.price_eur_mwh AS p,
           i.quantity_mw AS q,
           m.clearing_p
    FROM 'data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet' i
    JOIN 'data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet' c
       ON c.date = i.date
      AND c.session_number = i.session_number
      AND c.offer_code = i.offer_code
      AND c.version = i.version
      AND c.buy_sell = 'V'
    LEFT JOIN unit_firm uf ON uf.unit_code = i.unit_code
    LEFT JOIN ref r ON r.unit_code = i.unit_code
    LEFT JOIN (
        SELECT date, session_number,
               AVG(price_es_eur_mwh) AS clearing_p
        FROM 'data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    ) m ON m.date = i.date AND m.session_number = i.session_number
    WHERE i.quantity_mw > 0
      AND i.price_eur_mwh BETWEEN -500 AND 4000
      AND m.clearing_p IS NOT NULL
""")

# For each (firm, tech, regime), aggregate:
#   - cleared_mw_total: sum of q where p <= clearing_p (approximate cleared)
#   - cleared_mw_lo: sum of q where p <= clearing_p AND p <= 100
#   - cleared_mw_hi: sum of q where p <= clearing_p AND p > 100
#   - cleared_revenue_total: sum of q*clearing_p where p <= clearing_p
#   - cleared_revenue_hi: sum of q*clearing_p where p <= clearing_p AND p > 100

print('2. Aggregate cleared volumes by tranche-price bucket')
df = con.sql("""
    SELECT date, session_number, firm, tech,
           SUM(CASE WHEN p <= clearing_p THEN q ELSE 0 END) AS cleared_mw,
           SUM(CASE WHEN p <= clearing_p AND p <= 100 THEN q ELSE 0 END) AS cleared_mw_lo,
           SUM(CASE WHEN p <= clearing_p AND p > 100 THEN q ELSE 0 END) AS cleared_mw_hi,
           SUM(CASE WHEN p <= clearing_p AND p > 300 THEN q ELSE 0 END) AS cleared_mw_vh,
           AVG(clearing_p) AS clearing_p,
           SUM(CASE WHEN p <= clearing_p THEN q*clearing_p ELSE 0 END) AS rev_total_eur,
           SUM(CASE WHEN p <= clearing_p AND p > 100 THEN q*clearing_p ELSE 0 END) AS rev_hi_eur
    FROM ida_tranches
    WHERE firm IN ('GE','IB','GN','HC')
    GROUP BY date, session_number, firm, tech
""").df()

df['date'] = pd.to_datetime(df['date'])
df['regime'] = df['date'].apply(assign_regime)

print('3. Welfare-proxy summary: per (firm, tech, regime)')
print()
agg = df.groupby(['firm', 'tech', 'regime']).agg(
    n=('cleared_mw', 'count'),
    cleared_GWh=('cleared_mw', lambda x: x.sum() / 1e3),
    cleared_GWh_hi=('cleared_mw_hi', lambda x: x.sum() / 1e3),
    cleared_GWh_vh=('cleared_mw_vh', lambda x: x.sum() / 1e3),
    rev_total_M=('rev_total_eur', lambda x: x.sum() / 1e6),
    rev_hi_M=('rev_hi_eur', lambda x: x.sum() / 1e6),
).reset_index()

# Calculate ratios
agg['share_hi_mw'] = agg['cleared_GWh_hi'] / agg['cleared_GWh'].replace(0, np.nan)
agg['share_hi_rev'] = agg['rev_hi_M'] / agg['rev_total_M'].replace(0, np.nan)
agg['regime'] = pd.Categorical(agg['regime'], categories=REGIME_ORDER, ordered=True)
agg = agg.sort_values(['firm', 'tech', 'regime'])

# Filter to interesting cells
focus = agg[agg['tech'].isin(['CCGT', 'Hydro'])].copy()

print(f"{'firm':<4} {'tech':<6} {'regime':<14}  {'cleared_GWh':>12}  {'rev_M€':>8}  {'%MW>€100':>9}  {'%rev>€100':>9}  {'%MW>€300':>9}")
for firm in ['GE', 'IB', 'GN', 'HC']:
    for tech in ['CCGT', 'Hydro']:
        sub = focus[(focus['firm'] == firm) & (focus['tech'] == tech)]
        if len(sub) == 0:
            continue
        for _, row in sub.iterrows():
            print(f"{row['firm']:<4} {row['tech']:<6} {row['regime']:<14}  "
                  f"{row['cleared_GWh']:>12.1f}  "
                  f"{row['rev_total_M']:>8.1f}  "
                  f"{row['share_hi_mw']*100:>9.2f}  "
                  f"{row['share_hi_rev']*100:>9.2f}  "
                  f"{row['cleared_GWh_vh']/max(row['cleared_GWh'],0.001)*100:>9.2f}")
        print()

# Cross-firm aggregate for CCGT
print('=' * 80)
print('Big-4 CCGT aggregate by regime')
print('=' * 80)
ccgt_agg = focus[focus['tech'] == 'CCGT'].groupby('regime').agg(
    n=('n', 'sum'),
    cleared_GWh=('cleared_GWh', 'sum'),
    rev_total_M=('rev_total_M', 'sum'),
    cleared_GWh_hi=('cleared_GWh_hi', 'sum'),
    rev_hi_M=('rev_hi_M', 'sum'),
    cleared_GWh_vh=('cleared_GWh_vh', 'sum'),
).reindex(REGIME_ORDER)
ccgt_agg['share_hi_mw'] = ccgt_agg['cleared_GWh_hi'] / ccgt_agg['cleared_GWh']
ccgt_agg['share_hi_rev'] = ccgt_agg['rev_hi_M'] / ccgt_agg['rev_total_M']
ccgt_agg['share_vh_mw'] = ccgt_agg['cleared_GWh_vh'] / ccgt_agg['cleared_GWh']
print(f"{'regime':<14}  {'cleared GWh':>12}  {'rev M€':>8}  {'%MW>€100':>9}  {'%rev>€100':>9}  {'%MW>€300':>9}")
for r in REGIME_ORDER:
    if r in ccgt_agg.index and pd.notna(ccgt_agg.loc[r, 'cleared_GWh']):
        row = ccgt_agg.loc[r]
        print(f"{r:<14}  "
              f"{row['cleared_GWh']:>12.1f}  "
              f"{row['rev_total_M']:>8.1f}  "
              f"{row['share_hi_mw']*100:>9.2f}  "
              f"{row['share_hi_rev']*100:>9.2f}  "
              f"{row['share_vh_mw']*100:>9.2f}")

# A "shaded revenue" measure: revenue from cleared HIGH-PRICE tranches
# is the strategic income beyond what would have been earned if those
# tranches were priced at, say, €50 (a reasonable CCGT MC proxy).
# Shaded markup ≈ (clearing_p - 50) × cleared_mw_hi
print()
print('Strategic markup proxy (CCGT, Big-4): cleared HIGH-bid tranches × (p* - €50/MWh):')
print('Interpretation: revenue ABOVE a counterfactual MC-pricing baseline.')
print()
ccgt_markup_rows = []
for r in REGIME_ORDER:
    sub = focus[(focus['tech'] == 'CCGT') & (focus['regime'] == r)]
    if len(sub) == 0:
        continue
    # Per session-firm: cleared_hi_mw × (clearing_p - 50)
    raw_data = df[(df['regime'] == r) & (df['firm'].isin(['GE','IB','GN','HC']))
                   & (df['tech'] == 'CCGT')]
    raw_data = raw_data[raw_data['cleared_mw_hi'] > 0]
    raw_data['shaded_rev_M'] = (raw_data['cleared_mw_hi']
                                  * (raw_data['clearing_p'] - 50)) / 1e6
    raw_data['shaded_rev_M'] = raw_data['shaded_rev_M'].clip(lower=0)
    total_shaded = raw_data['shaded_rev_M'].sum()
    n_days = raw_data['date'].nunique()
    ccgt_markup_rows.append({
        'regime': r,
        'n_days': n_days,
        'shaded_rev_M_total': total_shaded,
        'shaded_rev_M_per_day': total_shaded / max(n_days, 1),
    })
mtbl = pd.DataFrame(ccgt_markup_rows)
print(mtbl.to_string(index=False))

# Save panel
df.to_parquet('data/derived/welfare_proxy_panel.parquet', index=False)
print(f'\nSaved {len(df):,}-row panel to data/derived/welfare_proxy_panel.parquet')
