"""HHI + capacity withholding + bid shading checks (P3.D1, P2.C2, P2.C1).

(A) HHI panel: Herfindahl–Hirschman Index of DA cleared sell-side
    market shares per (date, hour, regime). Three variants:
    (i)   all firms (full HHI)
    (ii)  CCGT-only (concentration in marginal tech)
    (iii) within Big-4 (excluding fringe)

(B) Capacity-withholding ratio: per (firm, regime), the ratio of
    cleared MW to offered MW, weighted by offered MW. If firms are
    strategically withholding, this should DECLINE in DA60/ID15.

(C) Bid-shading regression: offer_price - clearing_price as outcome,
    regressed on regime + price_bin FE per firm.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import statsmodels.api as sm
from pathlib import Path

PDBCE = 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
CURVA = 'data/processed/omie/mercado_diario/curvas/curva_pbc_all.parquet'
CAB   = 'data/processed/omie/mercado_diario/ofertas/cab_all.parquet'
DET   = 'data/processed/omie/mercado_diario/ofertas/det_all.parquet'
ICAB  = 'data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet'
IDET  = 'data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet'
MARG  = 'data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet'
MARGI = 'data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet'
REF   = Path('data/external/omie_reference/lista_unidades.csv')

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

con = duckdb.connect()
con.execute("SET memory_limit='10GB'")
con.execute("SET threads=6")

# Load tech ref
ref = pd.read_csv(REF, encoding='latin1')
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
    if 'eóli' in t or 'eoli' in t:

        return 'Wind'
    if 'solar' in t:

        return 'Solar'
    return 'Other'
ref['tech'] = ref['technology'].apply(bucket)
con.register('ref', ref[['unit_code', 'tech']])

# ============================================================================
# (A) HHI panel
# ============================================================================
print('=' * 70)
print('(A) HHI panel — Herfindahl-Hirschman Index of DA cleared sell shares')
print('=' * 70)

# Per (date, hour) compute firm shares of cleared MW; HHI = sum of shares^2.
# Then aggregate to monthly mean per regime.
con.execute(f"""
    CREATE TEMP TABLE firm_q AS
    SELECT date,
           CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INTEGER ELSE period END AS hour,
           COALESCE(grupo_empresarial, 'NA') AS firm,
           SUM(assigned_power_mw) /
              CASE WHEN mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM '{PDBCE}'
    WHERE offer_type = 1 AND assigned_power_mw > 0
    GROUP BY 1, 2, 3, mtu_minutes
""")
con.execute("""
    CREATE TEMP TABLE firm_share AS
    SELECT date, hour, firm, q_mwh,
           q_mwh / SUM(q_mwh) OVER (PARTITION BY date, hour) AS s
    FROM firm_q
""")
con.execute("""
    CREATE TEMP TABLE hhi AS
    SELECT date, hour,
           SUM(s*s) AS hhi_full,
           SUM(CASE WHEN firm IN ('GE','IB','GN','HC') THEN s*s END) AS hhi_big4_only,
           SUM(CASE WHEN firm IN ('GE','IB','GN','HC') THEN s END) AS big4_share
    FROM firm_share
    GROUP BY date, hour
""")

# CCGT-only HHI
con.execute(f"""
    CREATE TEMP TABLE ccgt_q AS
    SELECT p.date,
           CASE WHEN p.mtu_minutes=15 THEN CEIL(p.period/4.0)::INTEGER ELSE p.period END AS hour,
           COALESCE(p.grupo_empresarial, 'NA') AS firm,
           SUM(p.assigned_power_mw) /
              CASE WHEN p.mtu_minutes=15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM '{PDBCE}' p
    JOIN ref r ON r.unit_code = p.unit_code
    WHERE p.offer_type = 1 AND p.assigned_power_mw > 0 AND r.tech = 'CCGT'
    GROUP BY 1, 2, 3, p.mtu_minutes
""")
con.execute("""
    CREATE TEMP TABLE ccgt_hhi AS
    WITH s AS (
        SELECT date, hour, firm, q_mwh,
               q_mwh / SUM(q_mwh) OVER (PARTITION BY date, hour) AS s
        FROM ccgt_q
    )
    SELECT date, hour, SUM(s*s) AS hhi_ccgt
    FROM s GROUP BY date, hour
""")

hhi = con.sql("""
    SELECT a.date, a.hour, a.hhi_full, a.hhi_big4_only, a.big4_share, b.hhi_ccgt
    FROM hhi a LEFT JOIN ccgt_hhi b USING (date, hour)
""").df()
hhi['date'] = pd.to_datetime(hhi['date'])
hhi['regime'] = hhi['date'].apply(assign_regime)

print('\nMonthly mean HHI by regime:')
agg = hhi.groupby('regime').agg(
    n=('hhi_full', 'count'),
    hhi_full=('hhi_full', 'mean'),
    hhi_big4_only=('hhi_big4_only', 'mean'),
    big4_share=('big4_share', 'mean'),
    hhi_ccgt=('hhi_ccgt', 'mean'),
).round(3).reindex(REGIME_ORDER)
print(agg.to_string())

# ============================================================================
# (B) Capacity withholding ratio
# ============================================================================
print()
print('=' * 70)
print('(B) Capacity withholding: cleared / offered ratio per Big-4 firm × regime')
print('=' * 70)

# Need offered volume from cab+det (sell side) per firm.
# Map offer_code → unit_code via cab, then unit_code → firm via pdbce mode-firm mapping.
con.execute("""
    CREATE TEMP TABLE unit_firm_mode AS
    WITH cnts AS (
        SELECT unit_code, grupo_empresarial, COUNT(*) n
        FROM 'data/processed/omie/mercado_diario/programas/pdbce_all.parquet'
        WHERE offer_type=1 AND grupo_empresarial IS NOT NULL
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT unit_code, grupo_empresarial,
               ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY n DESC) rk
        FROM cnts
    )
    SELECT unit_code, grupo_empresarial AS firm
    FROM ranked WHERE rk=1
""")

con.execute(f"""
    CREATE TEMP TABLE da_offered AS
    SELECT d.date,
           uf.firm,
           SUM(d.quantity_mw) AS mw_offered
    FROM '{DET}' d
    JOIN '{CAB}' c USING (date, offer_code, version)
    LEFT JOIN unit_firm_mode uf ON uf.unit_code = c.unit_code
    WHERE c.buy_sell = 'V' AND d.quantity_mw > 0
    GROUP BY 1, 2
""")
con.execute(f"""
    CREATE TEMP TABLE da_cleared AS
    SELECT date, COALESCE(grupo_empresarial, 'NA') AS firm,
           SUM(assigned_power_mw) AS mw_cleared
    FROM '{PDBCE}'
    WHERE offer_type=1 AND assigned_power_mw > 0
    GROUP BY 1, 2
""")
ratio = con.sql("""
    SELECT o.date, o.firm,
           o.mw_offered,
           c.mw_cleared,
           c.mw_cleared / NULLIF(o.mw_offered, 0) AS ratio
    FROM da_offered o
    JOIN da_cleared c USING (date, firm)
    WHERE o.firm IN ('GE','IB','GN','HC')
""").df()
ratio['date'] = pd.to_datetime(ratio['date'])
ratio['regime'] = ratio['date'].apply(assign_regime)

print('\nDA daily-aggregate cleared/offered ratio (sell-side), per Big-4 firm × regime:')
print('Note: ratio < 1 means firm offered more than cleared; could be price-rejected')
print('or strategic withholding (offered at high price knowing it would not clear).\n')
ag = ratio.groupby(['firm','regime'])['ratio'].agg(['mean','median','count']).round(3).reset_index()
ag['regime'] = pd.Categorical(ag['regime'], categories=REGIME_ORDER, ordered=True)
ag = ag.sort_values(['firm','regime']).set_index(['firm','regime'])
print(ag.to_string())

# ============================================================================
# (C) Bid-shading regression
# ============================================================================
print()
print('=' * 70)
print('(C) Bid-shading: offer_price - clearing_price (IDA, sell-side)')
print('=' * 70)

# Use IDA (idet+icab) since DA det has 0-padded prices pre-MTU15-IDA.
# clearing price comparator: marginalpibc per (date, session)
con.execute(f"""
    CREATE TEMP TABLE ida_offers_with_clearing AS
    SELECT i.date, i.session_number,
           i.unit_code,
           uf.firm,
           i.price_eur_mwh AS offer_p,
           i.quantity_mw   AS q,
           m.price_es_eur_mwh AS clearing_p
    FROM '{IDET}' i
    JOIN '{ICAB}' c
       ON c.date = i.date
      AND c.session_number = i.session_number
      AND c.offer_code = i.offer_code
      AND c.version = i.version
      AND c.buy_sell = 'V'
    LEFT JOIN unit_firm_mode uf ON uf.unit_code = i.unit_code
    LEFT JOIN (
        SELECT date, session_number,
               AVG(price_es_eur_mwh) AS price_es_eur_mwh
        FROM '{MARGI}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    ) m ON m.date = i.date AND m.session_number = i.session_number
    WHERE i.quantity_mw > 0 AND i.price_eur_mwh BETWEEN -500 AND 4000
      AND m.price_es_eur_mwh IS NOT NULL
""")

# Quantity-weighted average shade per (date, session, firm)
con.execute("""
    CREATE TEMP TABLE shade_panel AS
    SELECT date, session_number,
           CASE WHEN firm IN ('GE','IB','GN','HC') THEN firm ELSE 'Fringe' END AS firm_group,
           AVG(clearing_p) AS clearing_p,
           SUM(q * (offer_p - clearing_p)) / SUM(q) AS wavg_shade
    FROM ida_offers_with_clearing
    WHERE firm IS NOT NULL
    GROUP BY 1, 2, 3
""")

shade = con.sql("SELECT * FROM shade_panel WHERE wavg_shade BETWEEN -1000 AND 4000").df()
shade['date'] = pd.to_datetime(shade['date'])
shade['regime'] = shade['date'].apply(assign_regime)
shade['p_bin'] = pd.cut(shade['clearing_p'], bins=[-1000, 0, 25, 50, 100, 200, 1e6],
                         labels=['neg','0-25','25-50','50-100','100-200','200+'])

print('\nMedian quantity-weighted shade (offer − clearing) per firm_group × regime (€/MWh):')
ag = shade.groupby(['firm_group','regime'])['wavg_shade'].agg(['mean','median','count']).round(2).reset_index()
ag['regime'] = pd.Categorical(ag['regime'], categories=REGIME_ORDER, ordered=True)
ag = ag.sort_values(['firm_group','regime']).set_index(['firm_group','regime'])
print(ag.to_string())

# Regression: shade ~ regime + p_bin FE per firm group
print('\nOLS regression — wavg_shade ~ regime + price_bin FE, per firm_group:')
print(f"\n{'firm':<8} {'regime':<14}  {'coef':>8}  {'se':>6}  {'p':>6}")
shade['regime_cat'] = pd.Categorical(shade['regime'], categories=REGIME_ORDER, ordered=False)
for fg in ['GE','IB','GN','HC','Fringe']:
    sub = shade[shade['firm_group']==fg].copy()
    if len(sub) < 100:
        continue
    rd = pd.get_dummies(sub['regime_cat'], prefix='regime', drop_first=False, dtype=float)
    rd = rd.drop(columns='regime_pre-IDA')
    pb = pd.get_dummies(sub['p_bin'], prefix='p_bin', drop_first=True, dtype=float)
    X = pd.concat([rd, pb], axis=1).assign(const=1.0)
    y = sub['wavg_shade'].astype(float)
    res = sm.OLS(y, X).fit(cov_type='HC3')
    base = sub.loc[sub['regime']=='pre-IDA','wavg_shade'].mean()
    print(f"{fg:<8} {'pre-IDA mean':<14}  {base:>8.1f}  {'-':>6}  {'-':>6}")
    for r in REGIME_ORDER[1:]:
        col = f'regime_{r}'
        if col in res.params.index:
            print(f"{fg:<8} {r:<14}  {res.params[col]:>+8.1f}  {res.bse[col]:>6.1f}  {res.pvalues[col]:>6.3f}")
