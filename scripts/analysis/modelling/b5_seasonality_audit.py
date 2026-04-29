# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B5
# CLAIM: Forecast-error -> imbalance volume pass-through with month + hour FE
"""B5 seasonality re-audit (per CLAUDE.md mandatory standard).

The original B5 claim is "forecast-error → imbalance volume pass-through
R²=0.305 in DA60/ID15." This re-audit:

(1) Builds the |fe| → |V_imb| regression at native ISP resolution
(2) Splits DA60/ID15 into PRE-blackout (2025-03-19 → 2025-04-27,
    pre-operación-reforzada) vs POST-blackout (2025-04-28 → 2025-09-30)
(3) Adds month-of-year + hour-of-day FE to control for seasonality
(4) Reports both raw and FE-controlled coefficients per regime

Reasoning before running:
  Pass-through magnitude depends on (a) settlement granularity (post-ISP15
  intra-hour offsets don't net), (b) reserve scarcity (operación reforzada
  forces more conventional commitments → less flexibility to absorb
  forecast errors), (c) seasonality (winter has higher load AND higher
  forecast errors AND higher imbalance volumes — composition channel that
  the FE absorbs).

  Predicted: slope rises post-ISP15 (mechanical from no-netting); jumps
  further post-blackout (operational scarcity); compresses at MTU15-DA
  (granularity asymmetry closes).

Output: results/regressions/b5_seasonality_audit.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]


def assign_regime(d: pd.Timestamp) -> str:
    if d < pd.Timestamp('2024-06-14'):
        return 'pre-IDA'
    if d < pd.Timestamp('2024-12-01'):
        return '3-sess'
    if d < pd.Timestamp('2025-03-19'):
        return 'ISP15-win'
    if d < pd.Timestamp('2025-04-28'):
        return 'DA60/ID15 PRE-blackout'
    if d < pd.Timestamp('2025-10-01'):
        return 'DA60/ID15 POST-blackout'
    return 'DA15/ID15'


def main() -> None:
    fe = pd.read_parquet(PROJECT / 'data/processed/entsoe/load/load_forecast_error_panel.parquet')
    fe['ts'] = pd.to_datetime(fe['isp_start_utc'])

    imb = pd.read_parquet(PROJECT / 'data/processed/entsoe/balancing/imbalance_volumes_all.parquet')
    imb_dedup = imb.drop_duplicates('isp_start_utc', keep='last')[['isp_start_utc', 'volume_mwh']]

    mg = fe.merge(imb_dedup, on='isp_start_utc', how='inner')
    mg['abs_fe'] = mg['forecast_err_mw'].abs()
    mg['abs_imb'] = mg['volume_mwh'].abs()
    mg['hour'] = mg['ts'].dt.hour
    mg['month_num'] = mg['ts'].dt.month
    mg['regime'] = mg['ts'].apply(assign_regime)

    rows = []
    for reg in ['pre-IDA', '3-sess', 'ISP15-win',
                'DA60/ID15 PRE-blackout', 'DA60/ID15 POST-blackout', 'DA15/ID15']:
        sub = mg[mg.regime == reg]
        if len(sub) < 100:
            continue
        Xr = sm.add_constant(sub[['abs_fe']])
        mr = sm.OLS(sub['abs_imb'], Xr).fit()
        Xd = pd.concat([
            sub[['abs_fe']].astype(float).reset_index(drop=True),
            pd.get_dummies(sub['month_num'], prefix='m', drop_first=True).astype(int).reset_index(drop=True),
            pd.get_dummies(sub['hour'], prefix='h', drop_first=True).astype(int).reset_index(drop=True),
        ], axis=1)
        Xd = sm.add_constant(Xd)
        mFE = sm.OLS(sub['abs_imb'].reset_index(drop=True), Xd).fit()
        rows.append({
            'regime': reg,
            'n': len(sub),
            'slope_raw': float(mr.params['abs_fe']),
            'r2_raw': float(mr.rsquared),
            'slope_FE': float(mFE.params['abs_fe']),
            'r2_FE': float(mFE.rsquared),
        })

    df = pd.DataFrame(rows)
    print(df.to_string(index=False))
    out = PROJECT / 'results/regressions/b5_seasonality_audit.csv'
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f'\nwrote {out}')


if __name__ == '__main__':
    main()
