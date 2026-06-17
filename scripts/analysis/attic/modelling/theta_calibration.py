# STATUS: ALIVE
# LAST-AUDIT: 2026-04-25
# FEEDS: B2, B6 (modelling-track input)
# CLAIM: Calibrate regime-dependent settlement-risk theta from reservation share + pass-through
"""Calibrate regime-dependent θ from bid-function reservation share +
forecast-error pass-through slope.

(2) Extend Pattern 2 bid-function shape to ALL Big-4 firms × all techs.
(1) Compute per-regime calibration: s_r (share > €100) vs β_r
    (pass-through slope on |wind+solar forecast error|).
    Test if these line up across regimes as a single-θ-parameter story
    would predict.

Theoretical map:
  σ = settlement clock, δ = DA clock, τ = IDA clock
  θ = 1[σ < δ]   "settlement-risk exposure"
  ρ = 1[τ ≤ σ]   "IDA responsiveness"
  Reservation share s_r = f(θ_r), high when settlement risk and DA can't
    fully match settlement clock.
  Pass-through β_r = g(θ_r, ρ_r), high when σ_settlement is fine AND
    intraday can respond.

Regime mapping:
  pre-IDA       σ=60 δ=60 τ=60 → θ=0, ρ=N/A.    Pred: low s, low β.
  3-sess        σ=60 δ=60 τ=60 → θ=0, ρ=N/A.    Pred: low s, low β.
  ISP15 window  σ=15 δ=60 τ=60 → θ=1, ρ=0.      Pred: high s, low β.
  DA60/ID15     σ=15 δ=60 τ=15 → θ=1, ρ=1.      Pred: med s, high β.
  DA15/ID15     σ=15 δ=15 τ=15 → θ=0, ρ=1.      Pred: low s, low β.
"""
from __future__ import annotations
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

con = duckdb.connect()
con.execute("SET memory_limit='8GB'")
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

# Theoretical regime tags
THEORY = {
    'pre-IDA':       (0, 0),  # (θ, ρ)
    '3-sess':        (0, 0),
    'ISP15 window':  (1, 0),
    'DA60/ID15':     (1, 1),
    'DA15/ID15':     (0, 1),
}

# ============================================================================
# (2) Extended bid-function shape: all Big-4 × all techs
# ============================================================================
print('=' * 80)
print('(2) Bid-function shape: ALL Big-4 firms × CCGT/Hydro/Nuclear/PumpHydro')
print('=' * 80)

# Load tech mapping
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

con.execute("""
    CREATE TEMP TABLE ida_offers AS
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
""")

df = con.sql("""
    SELECT date, firm, tech, p, q
    FROM ida_offers
    WHERE firm IN ('GE','IB','GN','HC')
""").df()
df['date'] = pd.to_datetime(df['date'])
df['regime'] = df['date'].apply(assign_regime)


def reservation_share(p, q):
    if q.sum() == 0:
        return np.nan
    return q[p > 100].sum() / q.sum()


def wmoments_brief(group):
    p = group['p'].values
    q = group['q'].values
    if q.sum() == 0:
        return pd.Series({'n_GWh': 0, 'p25': np.nan, 'p50': np.nan,
                          'p75': np.nan, 'p95': np.nan, 'res100': np.nan})
    order = np.argsort(p)
    p_s = p[order]
    q_s = q[order]
    cumq = np.cumsum(q_s) / q_s.sum()
    def pct(x):
        idx = np.searchsorted(cumq, x)
        return p_s[min(idx, len(p_s)-1)]
    return pd.Series({
        'n_GWh': q.sum() / 1e3,
        'p25':   pct(0.25),
        'p50':   pct(0.50),
        'p75':   pct(0.75),
        'p95':   pct(0.95),
        'res100': reservation_share(p, q),
    })


print(f"\n{'firm':<4} {'tech':<10} {'regime':<14}  {'n_GWh':>7}  {'p25':>5}  {'p50':>5}  {'p75':>5}  {'p95':>5}  {'res>€100':>9}")
records = []
for firm in ['GE', 'IB', 'GN', 'HC']:
    for tech in ['CCGT', 'Hydro', 'Nuclear', 'PumpHydro']:
        sub = df[(df['firm'] == firm) & (df['tech'] == tech)]
        if len(sub) < 100:
            continue
        for r in REGIME_ORDER:
            srm = sub[sub['regime'] == r]
            if len(srm) < 10:
                continue
            m = wmoments_brief(srm)
            records.append({'firm': firm, 'tech': tech, 'regime': r,
                            'p25': m['p25'], 'p50': m['p50'],
                            'p75': m['p75'], 'p95': m['p95'],
                            'res100': m['res100'], 'n_GWh': m['n_GWh']})
            print(f"{firm:<4} {tech:<10} {r:<14}  "
                  f"{m['n_GWh']:>7.0f}  "
                  f"{m['p25']:>5.0f}  "
                  f"{m['p50']:>5.0f}  "
                  f"{m['p75']:>5.0f}  "
                  f"{m['p95']:>5.0f}  "
                  f"{m['res100']:>9.2%}")
        print()

calib_df = pd.DataFrame(records)
calib_df.to_parquet('data/derived/panels/bid_function_shape_panel.parquet', index=False)

# ============================================================================
# (1) Calibration: pass-through β + reservation share s
# ============================================================================
print('=' * 80)
print('(1) Calibration: per-regime θ (settlement risk) + ρ (IDA responsiveness)')
print('=' * 80)

# Pass-through β by regime (from passthrough panel)
panel = pd.read_parquet('data/derived/panels/passthrough_panel.parquet')
panel['date'] = pd.to_datetime(panel['date'])
panel['regime'] = panel['date'].apply(assign_regime)
panel['imb_GWh'] = panel['abs_imb_mwh'] / 1e3
panel['wind_GWh'] = panel['abs_wind_err'] / 1e3
panel['solar_GWh'] = panel['abs_solar_err'] / 1e3

beta_table = {}
for r in REGIME_ORDER:
    sub = panel[panel['regime'] == r].copy()
    sub = sub.dropna(subset=['imb_GWh', 'wind_GWh', 'solar_GWh'])
    if len(sub) < 30:
        continue
    X_r = sm.add_constant(sub[['wind_GWh', 'solar_GWh']].astype(float))
    y_r = sub['imb_GWh'].astype(float)
    res = sm.OLS(y_r, X_r).fit(cov_type='HC3')
    beta_table[r] = {
        'n': len(sub),
        'β_wind':  res.params['wind_GWh'],
        'β_solar': res.params['solar_GWh'],
        'R²':      res.rsquared,
        # Combined pass-through: how much imbalance per unit total error
        'β_total': (res.params['wind_GWh'] + res.params['solar_GWh']) / 2,
    }

# Reservation share by regime: weighted across firm × CCGT
# (CCGT is the marginal tech, where the model applies cleanly)
ccgt_calib = calib_df[calib_df['tech'] == 'CCGT'].copy()
# Quantity-weighted res100 by regime across firms
res_table = {}
for r in REGIME_ORDER:
    sub = ccgt_calib[ccgt_calib['regime'] == r]
    if len(sub) == 0:
        continue
    w = sub['n_GWh'].values
    if w.sum() == 0:
        continue
    res_avg = (sub['res100'] * w).sum() / w.sum()
    res_table[r] = {
        'res100_GE':     sub.loc[sub['firm']=='GE', 'res100'].iloc[0]
                          if (sub['firm']=='GE').any() else np.nan,
        'res100_IB':     sub.loc[sub['firm']=='IB', 'res100'].iloc[0]
                          if (sub['firm']=='IB').any() else np.nan,
        'res100_GN':     sub.loc[sub['firm']=='GN', 'res100'].iloc[0]
                          if (sub['firm']=='GN').any() else np.nan,
        'res100_HC':     sub.loc[sub['firm']=='HC', 'res100'].iloc[0]
                          if (sub['firm']=='HC').any() else np.nan,
        'res100_Big4':   res_avg,
    }

print('\nCalibration table — reservation share s_r vs pass-through β_r per regime:\n')
print(f"{'regime':<14}  {'θ':>2}  {'ρ':>2}  {'res100_GE':>10}  {'res100_IB':>10}  "
      f"{'res100_Big4':>11}  {'β_wind':>7}  {'β_solar':>7}  {'R²':>5}")
for r in REGIME_ORDER:
    if r not in beta_table or r not in res_table:
        continue
    θ, ρ = THEORY[r]
    bt = beta_table[r]
    rt = res_table[r]
    print(f"{r:<14}  {θ:>2}  {ρ:>2}  "
          f"{rt['res100_GE']:>10.2%}  "
          f"{rt['res100_IB']:>10.2%}  "
          f"{rt['res100_Big4']:>11.2%}  "
          f"{bt['β_wind']:>+7.3f}  "
          f"{bt['β_solar']:>+7.3f}  "
          f"{bt['R²']:>5.3f}")

# Theory check: predicted regime ordering
print('\n=== Theory check ===\n')
print('Model predicts:')
print('  ISP15 window  (θ=1, ρ=0): high res100, low β_r')
print('  DA60/ID15     (θ=1, ρ=1): moderate res100, high β_r')
print('  DA15/ID15     (θ=0, ρ=1): low res100, low β_r')
print('  pre-IDA, 3-sess (θ=0, ρ=0): low res100, low β_r')
print()
print('Observed:')
for r in ['ISP15 window', 'DA60/ID15', 'DA15/ID15']:
    bt = beta_table[r]
    rt = res_table[r]
    print(f'  {r}: res100_Big4={rt["res100_Big4"]:.0%}, β_wind={bt["β_wind"]:+.3f}, R²={bt["R²"]:.3f}')

# Print correlation between res100 and β across regimes
print()
print('Cross-regime calibration ratios:')
res_v = [res_table[r]['res100_Big4'] for r in REGIME_ORDER if r in res_table]
beta_v = [beta_table[r]['β_wind'] + beta_table[r]['β_solar']
          for r in REGIME_ORDER if r in beta_table]
print(f'  Pearson corr(res100_Big4, β_wind+β_solar) across {len(res_v)} regimes: '
      f'{np.corrcoef(res_v, beta_v)[0,1]:+.3f}')
