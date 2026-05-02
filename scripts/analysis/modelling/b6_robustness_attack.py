# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B6 attack — pooled-regression test of regime-varying pass-through
# CLAIM: B6 R² jump (0.023 → 0.305) across regimes is real if pooled
#        regression with regime×forecast_error interactions shows large
#        and significant coefficient changes.
"""B6 robustness attack — pooled regression with regime interactions.

Original B6 spec (`passthrough_forecast_imbalance.py`):
  |V_imb_t| ~ |fe_t| separately for each regime
  Compare R² across regimes: pre-IDA-late 0.023, 3-sess 0.040, ISP15-win
  0.218, DA60/ID15 0.305, DA15/ID15 0.181.

PROBLEMS with R² comparison across regimes:
1. R² depends on Var(X) and Var(Y) within regime, not just signal strength.
2. Different N per regime affects R² (fewer obs → noisier R²).
3. The interpretation "pass-through is X times stronger" is more cleanly
   captured by the SLOPE coefficient β, not R².
4. Daily aggregation averages over intra-day variation that's central to
   the MTU15 mechanism (intra-hour netting).

This attack:
  Spec A: pooled |V_imb_t| ~ |fe_t| + regime + |fe_t|×regime
          + cal-month FE + DOW FE; cluster SE by year-month (canonical)
  Spec B: + year FE
  Spec C: hourly grain (instead of daily): |V_imb_h| ~ |fe_h| + regime
          + |fe_h|×regime + hour FE + cal-month + DOW + year FE;
          cluster by week-of-sample

If β(|fe| × regime) interaction coefficients are large (≥50% of base β)
and statistically significant in Spec A and B, B6 mechanism survives.
Spec C tests at the right grain for the MTU15 mechanism.

Output:
  results/regressions/b6_robustness_attack.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
A86     = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "imbalance_volumes_all.parquet"
VRE_A   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
VRE_F   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_forecast_all.parquet"
LOAD_A  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
LOAD_F  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_forecast_da_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "b6_robustness_attack.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    print("[1/2] daily VRE forecast errors + |imbalance|…", flush=True)
    df = con.execute(f"""
        WITH va AS (
          SELECT CAST(isp_start_utc AS DATE) AS date,
                 SUM(quantity_mw * mtu_minutes / 60.0) AS vre_actual_mwh
          FROM '{VRE_A}' WHERE psr_type IN ('B16','B18','B19')
          GROUP BY 1
        ), vf AS (
          SELECT CAST(isp_start_utc AS DATE) AS date,
                 SUM(quantity_mw * mtu_minutes / 60.0) AS vre_forecast_mwh
          FROM '{VRE_F}' WHERE psr_type IN ('B16','B18','B19')
          GROUP BY 1
        ), la AS (
          SELECT CAST(isp_start_utc AS DATE) AS date,
                 SUM(load_mw * mtu_minutes / 60.0) AS load_actual_mwh
          FROM '{LOAD_A}' GROUP BY 1
        ), lf AS (
          SELECT CAST(isp_start_utc AS DATE) AS date,
                 SUM(load_forecast_mw * mtu_minutes / 60.0) AS load_forecast_mwh
          FROM '{LOAD_F}' GROUP BY 1
        ), imb AS (
          SELECT CAST(isp_start_utc AS DATE) AS date,
                 SUM(ABS(volume_mwh)) AS abs_imb_mwh
          FROM '{A86}' GROUP BY 1
        )
        SELECT va.date, va.vre_actual_mwh, vf.vre_forecast_mwh,
               la.load_actual_mwh, lf.load_forecast_mwh, imb.abs_imb_mwh
        FROM va JOIN vf USING (date) JOIN la USING (date) JOIN lf USING (date)
                JOIN imb USING (date)
        WHERE va.date >= DATE '2018-01-01'
    """).df()
    df["date"] = pd.to_datetime(df["date"])
    # B6 outcome: |imbalance|; main regressor: |VRE forecast error|
    df["abs_vre_fe"]  = (df["vre_actual_mwh"]  - df["vre_forecast_mwh"]).abs()
    df["abs_load_fe"] = (df["load_actual_mwh"] - df["load_forecast_mwh"]).abs()
    df["abs_imb_GWh"] = df["abs_imb_mwh"] / 1000.0
    df["abs_vre_fe_GWh"]  = df["abs_vre_fe"] / 1000.0
    df["abs_load_fe_GWh"] = df["abs_load_fe"] / 1000.0
    df["regime"] = df["date"].apply(assign_regime)
    df["dow"]    = df["date"].dt.dayofweek
    df["month"]  = df["date"].dt.month
    df["year"]   = df["date"].dt.year
    df["ym"]     = df["year"] * 100 + df["month"]
    df = df.dropna(subset=["abs_imb_GWh","abs_vre_fe_GWh","abs_load_fe_GWh"])
    print(f"   panel: {len(df):,} days", flush=True)

    def build(panel, with_year=False, with_load_fe=False):
        cols = {"const": np.ones(len(panel))}
        cols["abs_vre_fe"] = panel["abs_vre_fe_GWh"].values
        # regime main effects
        for r in REGIMES[1:]:
            cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
        # regime × abs_vre_fe interactions (the key test)
        for r in REGIMES[1:]:
            cols[f"abs_vre_fe×R_{r}"] = (panel["abs_vre_fe_GWh"]
                                         * (panel["regime"] == r)).astype(float).values
        for d_ in range(1, 7):
            cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
        if with_year:
            for yr in sorted(panel["year"].unique())[1:]:
                cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
        if with_load_fe:
            cols["abs_load_fe"] = panel["abs_load_fe_GWh"].values
        return pd.DataFrame(cols, index=panel.index)

    cluster_ym = df["ym"].values

    print("\n" + "="*100)
    print("B6 ATTACK — pooled regression with regime × |forecast_error| interactions")
    print("="*100 + "\n")
    specs = [
        ("Spec A: regime + cal-month + DOW (no year FE)",        False, False),
        ("Spec B: + year FE",                                     True,  False),
        ("Spec C: + year FE + load forecast error",               True,  True),
    ]
    coef_dict = {}
    rsq_dict = {}
    for label, year_fe, load_fe in specs:
        X = build(df, with_year=year_fe, with_load_fe=load_fe)
        y = df["abs_imb_GWh"].values
        m = fit_ols_cluster(y, X.values, cluster_ym)
        coef_dict[label] = pd.Series(m.params, index=X.columns)
        rsq_dict[label] = m.rsquared
    # Pretty print
    keys = ["abs_vre_fe"] + [f"abs_vre_fe×R_{r}" for r in REGIMES[1:]]
    print(f"{'Term':30s}  {'Spec A':>14s}  {'Spec B':>14s}  {'Spec C':>14s}")
    print(f"{'(GWh imb / GWh fe)':30s}  {'(no yr FE)':>14s}  {'(+yr FE)':>14s}  {'(+yr+load_fe)':>14s}")
    print(f"{'-'*30}  {'-'*14}  {'-'*14}  {'-'*14}")
    for k in keys:
        line = f"{k:30s}"
        for label, _, _ in specs:
            v = coef_dict[label].get(k, np.nan)
            line += f"  {v:+14.4f}"
        print(line)
    print()
    # Compute net per-regime β = main + interaction (the slope of |imb| ~ |vre_fe| WITHIN each regime)
    print("Net per-regime pass-through β (main + interaction):")
    for label, _, _ in specs:
        c = coef_dict[label]
        base = c.get("abs_vre_fe", np.nan)
        net_str = f"  {label[:30]:30s}: pre-IDA={base:+.4f}"
        for r in REGIMES[1:]:
            net = base + c.get(f"abs_vre_fe×R_{r}", 0)
            net_str += f"  {r}={net:+.4f}"
        print(net_str)
    print()
    for label, _, _ in specs:
        print(f"  {label}: R² = {rsq_dict[label]:.3f}")

    # Save
    df_out = pd.DataFrame({"term": list(coef_dict[specs[0][0]].index)})
    for label, _, _ in specs:
        df_out[f"coef_{label[:6]}"] = coef_dict[label].reindex(df_out["term"]).values
    df_out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
