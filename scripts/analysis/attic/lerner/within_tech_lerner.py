# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: F1, W1, W2
# CLAIM: Tech-decomposition of firm Lerner (CCGT-only vs full); identifies CCGT-null caveat
"""Within-tech Lerner decomposition (P1.1) — v2 with CCGT-only Lerner.

Key finding from v1: GE/IB/HC's cleared volume post-IDA is dominated by
NUCLEAR (76-90% of GE; 60% of IB DA60/ID15+; 88% of HC DA60/ID15+).
The original nb12 Lerner aggregates infra-marginal nuclear/hydro with
marginal CCGT, conflating "settlement-agent share of cleared MW" with
"strategic market power".

A cleaner structural-markup measure: restrict q_i to firm i's CCGT-only
cleared MW. CCGT is the marginal technology in Spain's stack, so its
Lerner is what reform mechanics should affect.

Outputs:
  data/derived/panels/firm_tech_share_panel.parquet
  data/derived/panels/firm_ccgt_lerner_panel.parquet
"""
from __future__ import annotations
import duckdb
import pandas as pd
import statsmodels.api as sm
from pathlib import Path

REF = Path('data/external/omie_reference/lista_unidades.csv')
SLOPE = Path('data/derived/panels/supply_slope_hourly.parquet')
PDBCE = Path('data/processed/omie/mercado_diario/programas/pdbce_all.parquet')

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
    if 'tÃ©rmica' in t or 'termica' in t or 'térmica' in t:
        return 'Thermal'
    if 'almacen' in t:
        return 'Storage'
    if 'bombeo' in t or 'bomba' in t:
        return 'PumpHydro'
    return 'Other'
ref['tech'] = ref['technology'].apply(bucket)

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
con.execute("SET threads=6")
con.register('ref', ref[['unit_code', 'tech']])

# 1. Per (firm, tech, date, hour) cleared MWh
print('1. firm × tech × hour cleared MWh...')
con.execute(f"""
    CREATE TEMP TABLE firm_tech_q AS
    SELECT p.date,
           CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period/4.0)::INTEGER
                ELSE p.period END AS hour,
           p.grupo_empresarial AS firm,
           COALESCE(r.tech, 'Other') AS tech,
           SUM(p.assigned_power_mw) /
              CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
    FROM '{PDBCE}' p
    LEFT JOIN ref r USING (unit_code)
    WHERE p.offer_type = 1 AND p.assigned_power_mw > 0
      AND p.grupo_empresarial IN ('GE','IB','GN','HC')
    GROUP BY 1, 2, 3, 4, p.mtu_minutes
""")

# 2. Aggregate Q across all sellers (denominator for share)
print('2. total market sellers Q hourly...')
con.execute(f"""
    CREATE TEMP TABLE total_q AS
    SELECT p.date,
           CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period/4.0)::INTEGER
                ELSE p.period END AS hour,
           SUM(p.assigned_power_mw) /
              CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_total
    FROM '{PDBCE}' p
    WHERE p.offer_type = 1 AND p.assigned_power_mw > 0
    GROUP BY 1, 2, p.mtu_minutes
""")

# 3. CCGT-only firm Lerner: restrict q_i to CCGT cleared volume.
#    Use firm i's CCGT q as the strategic-marginal q. Share is firm's
#    CCGT q over total CCGT q (firm's share of marginal-tech cleared MW).
print('3. CCGT-only Lerner panel: q_i_CCGT / (p* × (1-s_CCGT) × slope)')
print('   where s_CCGT = q_i_CCGT / q_total_CCGT_market')

con.execute(f"""
    CREATE TEMP TABLE ccgt_market_q AS
    SELECT p.date,
           CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period/4.0)::INTEGER
                ELSE p.period END AS hour,
           SUM(p.assigned_power_mw) /
              CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_ccgt_total
    FROM '{PDBCE}' p
    JOIN ref r ON r.unit_code = p.unit_code
    WHERE p.offer_type = 1 AND p.assigned_power_mw > 0
      AND r.tech = 'CCGT'
    GROUP BY 1, 2, p.mtu_minutes
""")

con.execute(f"""
    COPY (
        SELECT
            ft.date, ft.hour, ft.firm,
            ft.q_mwh AS q_ccgt,
            ccgt.q_ccgt_total,
            ft.q_mwh / NULLIF(ccgt.q_ccgt_total, 0) AS s_ccgt,
            s.clearing_price_eur_mwh AS p_star,
            s.supply_slope_mw_per_eur AS slope,
            CASE WHEN s.clearing_price_eur_mwh > 0
                  AND s.supply_slope_mw_per_eur > 0
                  AND ccgt.q_ccgt_total > ft.q_mwh
                  AND ft.q_mwh > 0
            THEN ft.q_mwh /
                 (s.clearing_price_eur_mwh
                  * (1 - ft.q_mwh / NULLIF(ccgt.q_ccgt_total, 0))
                  * s.supply_slope_mw_per_eur)
            END AS lerner_ccgt
        FROM firm_tech_q ft
        JOIN ccgt_market_q ccgt USING (date, hour)
        JOIN '{SLOPE}' s USING (date, hour)
        WHERE ft.tech = 'CCGT'
        ORDER BY date, hour, firm
    ) TO 'data/derived/panels/firm_ccgt_lerner_panel.parquet' (FORMAT PARQUET)
""")

# 4. Regime medians of CCGT-only Lerner
print('\n4. CCGT-only Lerner: median per firm × regime (trimmed [0,1])')
df = con.sql("""
    SELECT date, firm, lerner_ccgt, q_ccgt, s_ccgt, p_star
    FROM 'data/derived/panels/firm_ccgt_lerner_panel.parquet'
    WHERE lerner_ccgt BETWEEN 0 AND 1
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
REGIME_ORDER = ['pre-IDA', '3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']

summary = df.groupby(['firm', 'regime']).agg(
    n=('lerner_ccgt', 'size'),
    med_L=('lerner_ccgt', 'median'),
    avg_share=('s_ccgt', 'mean'),
    avg_q=('q_ccgt', 'mean'),
    avg_p=('p_star', 'mean'),
).round(3).reset_index()
summary['regime'] = pd.Categorical(summary['regime'], categories=REGIME_ORDER, ordered=True)
summary = summary.sort_values(['firm', 'regime']).set_index(['firm', 'regime'])
print(summary.to_string())

# 5. Same with price-bin FE correction (Spec 3 from the seasonal analysis)
print('\n5. CCGT-only Lerner: regime contrasts with price-bin FE (vs pre-IDA)\n')
df['p_bin'] = pd.cut(df['p_star'], bins=[-1000, 0, 25, 50, 100, 200, 1e6],
                     labels=['neg', '0-25', '25-50', '50-100', '100-200', '200+'])
df['regime_cat'] = pd.Categorical(df['regime'], categories=REGIME_ORDER)

print(f"{'firm':<5} {'regime':<14}  {'raw med':>8}  {'p-bin contrast':>15}  {'se':>8}  {'p':>7}")
for firm in ['GE', 'IB', 'GN', 'HC']:
    sub = df[df['firm']==firm].copy()
    rd = pd.get_dummies(sub['regime_cat'], prefix='regime', drop_first=False, dtype=float).drop(columns='regime_pre-IDA')
    pb = pd.get_dummies(sub['p_bin'], prefix='p_bin', drop_first=True, dtype=float)
    X = pd.concat([rd, pb], axis=1).assign(const=1.0)
    y = sub['lerner_ccgt'].astype(float)
    res = sm.OLS(y, X).fit(cov_type='HC3')
    base_med = sub.loc[sub['regime']=='pre-IDA', 'lerner_ccgt'].median()
    print(f"{firm:<5} {'pre-IDA (ref)':<14}  {base_med:>8.3f}  {'-':>15}  {'-':>8}  {'-':>7}")
    for r in ['3-sess', 'ISP15 window', 'DA60/ID15', 'DA15/ID15']:
        col = f'regime_{r}'
        if col not in res.params.index:
            continue
        raw = sub.loc[sub['regime']==r, 'lerner_ccgt'].median()
        print(f"{firm:<5} {r:<14}  {raw:>8.3f}  {res.params[col]:>+15.3f}  {res.bse[col]:>8.3f}  {res.pvalues[col]:>7.3f}")
    print()
