"""Forecast-error → imbalance pass-through regression by regime.

Economic model the regression motivates:

  BRP utility:  E[π · q] − E[settlement] − ½ · γ · Var[settlement]

  Pre-ISP15:    settlement = π^imb · | Σ_q deviation_q |   (intra-hour net)
  Post-ISP15:   settlement = π^imb · Σ_q | deviation_q |   (no netting)

  When the ISP changes from 60-min to 15-min, the "exposure" to forecast
  error rises mechanically because intra-hour offsetting positions no
  longer cancel. The relationship between the SOURCE of forecast error
  (A75 wind + solar generation realised vs A69 forecast) and the
  RESULTING imbalance volume (A86) becomes:

      |V_imb_t| = α_r + β_r · |forecast_error_t| + u_t

  with β_r = pass-through coefficient. Theory predicts β_post-ISP15
  > β_pre-ISP15. The magnitude difference identifies the structural
  netting parameter ν_pre vs ν_post.

  Output: data/derived/passthrough_panel.parquet plus regression table.
"""
from __future__ import annotations
import duckdb
import pandas as pd
import statsmodels.api as sm
from pathlib import Path

WIND_F = Path('data/processed/entsoe/generation/wind_solar_forecast_all.parquet')
WIND_A = Path('data/processed/entsoe/generation/wind_solar_actual_all.parquet')
A86    = Path('data/processed/entsoe/balancing/imbalance_volumes_all.parquet')

con = duckdb.connect()
con.execute("SET memory_limit='6GB'")
con.execute("SET threads=4")

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

print('1. Build daily forecast-error and imbalance series')
con.execute(f"""
    CREATE TEMP TABLE wind_da AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           SUM(quantity_mw) / 4.0 AS wind_da_mwh
    FROM '{WIND_F}'
    WHERE psr_type = 'B19'
    GROUP BY 1
""")
con.execute(f"""
    CREATE TEMP TABLE wind_act AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           SUM(quantity_mw) / 4.0 AS wind_actual_mwh
    FROM '{WIND_A}'
    WHERE psr_type = 'B19'
    GROUP BY 1
""")
con.execute(f"""
    CREATE TEMP TABLE solar_da AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           SUM(quantity_mw) / 4.0 AS solar_da_mwh
    FROM '{WIND_F}'
    WHERE psr_type = 'B16'
    GROUP BY 1
""")
con.execute(f"""
    CREATE TEMP TABLE solar_act AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           SUM(quantity_mw) / 4.0 AS solar_actual_mwh
    FROM '{WIND_A}'
    WHERE psr_type = 'B16'
    GROUP BY 1
""")
con.execute(f"""
    CREATE TEMP TABLE imb AS
    SELECT CAST(isp_start_utc AS DATE) AS date,
           SUM(ABS(volume_mwh)) AS abs_imb_mwh,
           SUM(volume_mwh) AS net_imb_mwh,
           COUNT(*) AS n_isp,
           MIN(mtu_minutes) AS min_mtu
    FROM '{A86}'
    WHERE volume_mwh IS NOT NULL
    GROUP BY 1
""")

panel = con.sql("""
    SELECT i.date,
           i.abs_imb_mwh,
           i.net_imb_mwh,
           wd.wind_da_mwh,
           wa.wind_actual_mwh,
           wa.wind_actual_mwh - wd.wind_da_mwh AS wind_err_mwh,
           sd.solar_da_mwh,
           sa.solar_actual_mwh,
           sa.solar_actual_mwh - sd.solar_da_mwh AS solar_err_mwh,
           i.min_mtu, i.n_isp
    FROM imb i
    JOIN wind_da wd USING (date)
    JOIN wind_act wa USING (date)
    LEFT JOIN solar_da sd USING (date)
    LEFT JOIN solar_act sa USING (date)
    ORDER BY date
""").df()
panel['date'] = pd.to_datetime(panel['date'])
panel['regime'] = panel['date'].apply(assign_regime)
panel['abs_wind_err'] = panel['wind_err_mwh'].abs()
panel['abs_solar_err'] = panel['solar_err_mwh'].fillna(0).abs()
panel['abs_total_err'] = panel['abs_wind_err'] + panel['abs_solar_err']

# Filter incomplete days
expected = {60: 24, 30: 48, 15: 96}
panel = panel.dropna(subset=['abs_imb_mwh', 'wind_err_mwh'])
panel['exp_n'] = panel['min_mtu'].map(expected)
panel = panel[panel['n_isp'] >= 0.95 * panel['exp_n']].copy()

panel.to_parquet('data/derived/passthrough_panel.parquet', index=False)
print(f'  panel rows: {len(panel)}; date range {panel["date"].min().date()} → {panel["date"].max().date()}')

print('\n2. Pass-through regression: |V_imb| = α_r + β_r · |wind_err| + γ_r · |solar_err| + ε')
print('   With regime × forecast_error interaction.\n')

# All in GWh for readability
panel['imb_GWh']     = panel['abs_imb_mwh']     / 1e3
panel['wind_GWh']    = panel['abs_wind_err']    / 1e3
panel['solar_GWh']   = panel['abs_solar_err']   / 1e3

panel['regime_cat'] = pd.Categorical(panel['regime'], categories=REGIME_ORDER, ordered=False)
rd = pd.get_dummies(panel['regime_cat'], prefix='regime', drop_first=False, dtype=float)
rd = rd.drop(columns='regime_pre-IDA')

# Interaction: each regime × forecast error
X_parts = [rd]
for col in ['wind_GWh', 'solar_GWh']:
    X_parts.append(panel[[col]].rename(columns={col: f'{col}'}))
    for r in REGIME_ORDER[1:]:
        nm = f'{col}_x_{r}'
        X_parts.append(pd.DataFrame({nm: panel[col].values * (panel['regime'] == r).astype(float).values}))
X = pd.concat(X_parts, axis=1).assign(const=1.0)
y = panel['imb_GWh'].astype(float).reset_index(drop=True)
X = X.reset_index(drop=True).astype(float)
mask = y.notna() & X.notna().all(axis=1)
X = X[mask]
y = y[mask]
res = sm.OLS(y, X).fit(cov_type='HC3')

# Report only the slopes
print(f'{"Coef":<35}  {"value":>10}  {"se":>8}  {"p":>6}')
print('-'*70)
print(f'{"const (pre-IDA intercept)":<35}  {res.params["const"]:>10.3f}  {res.bse["const"]:>8.3f}  {res.pvalues["const"]:>6.3f}')
for c in ['wind_GWh', 'solar_GWh']:
    print(f'{c+" (pre-IDA slope)":<35}  {res.params[c]:>10.3f}  {res.bse[c]:>8.3f}  {res.pvalues[c]:>6.3f}')
for r in REGIME_ORDER[1:]:
    print()
    print(f'  Regime {r}:')
    rc = f'regime_{r}'
    if rc in res.params.index:
        print(f'  {"intercept Δ":<33}  {res.params[rc]:>10.3f}  {res.bse[rc]:>8.3f}  {res.pvalues[rc]:>6.3f}')
    for c in ['wind_GWh', 'solar_GWh']:
        ic = f'{c}_x_{r}'
        if ic in res.params.index:
            base = res.params[c]
            slope = base + res.params[ic]
            print(f'  {c+" slope":<33}  {slope:>10.3f}  ({"+":>1}{res.params[ic]:>+8.3f})    p={res.pvalues[ic]:.3f}')
print(f'\nR²: {res.rsquared:.3f}; n={len(y):,}')

# Simpler version: just fit by-regime independently
print('\n3. By-regime linear fit |V_imb| = α + β·|wind_err| + γ·|solar_err|:')
print(f'\n{"regime":<14}  {"n":>5}  {"α (GWh)":>9}  {"β_wind":>8}  {"γ_solar":>8}  {"R²":>6}')
for r in REGIME_ORDER:
    sub = panel[panel['regime'] == r]
    if len(sub) < 30:
        continue
    X_r = sm.add_constant(sub[['wind_GWh', 'solar_GWh']].astype(float))
    y_r = sub['imb_GWh'].astype(float)
    rfit = sm.OLS(y_r, X_r).fit(cov_type='HC3')
    print(f'{r:<14}  {len(sub):>5}  '
          f'{rfit.params["const"]:>9.2f}  '
          f'{rfit.params["wind_GWh"]:>8.3f}  '
          f'{rfit.params["solar_GWh"]:>8.3f}  '
          f'{rfit.rsquared:>6.3f}')
