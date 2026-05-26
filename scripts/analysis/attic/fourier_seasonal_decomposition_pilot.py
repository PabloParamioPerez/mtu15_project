# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Pilot — Fourier seasonal decomposition of Fase I cost per day,
#        with multi-year structural-state controls (CO2, RES installed
#        capacity, reservoir filling, gas price). Fit on pre-IDA only
#        (2018-01-01 through 2024-06-13). Compare three specs:
#          (A) Fourier-only baseline
#          (B) Fourier + CO2 + RES capacity + reservoir + linear trend (full window)
#          (C) Fourier + (B) + gas TNP (restricted to Nov 2022 - Jun 2024)
#        Year-fold cross-validation tests stability of Fourier coefficients.
#        Project pre-IDA-estimated seasonal to DA15/ID15 dates and compare
#        observed vs Fourier-expected.
# FEEDS: EXPLORATORY — not into descriptive_facts.tex yet. User approval
#        required before integration.
# OUT:
#   results/regressions/balancing/fourier_seasonal/{spec_compare,yr_fold_cv,da15id15_residual}.csv

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/balancing/fourier_seasonal"

PRE_IDA_END = pd.Timestamp("2024-06-13")
DA15_ID15_START = pd.Timestamp("2025-10-01")
DA15_ID15_END = pd.Timestamp("2026-05-15")

# Spanish RES installed capacity (wind + solar PV + solar thermal + RoR hydro + biomass + waste)
# in GW, year-end. 2021-2025 from ENTSO-E A65; 2018-2020 from REE annual statistics
# (https://www.ree.es/es/datos/publicaciones/series-estadisticas-nacionales).
# Pre-2021 values are approximate (no quarterly granularity).
RES_GW_ANNUAL = {
    2018: 28.0,
    2019: 34.4,
    2020: 39.0,
    2021: 42.1,  # will be overridden by A65 if available
    2022: 48.6,
    2023: 56.3,
    2024: 61.0,
    2025: 65.5,
    2026: 67.0,
}


def fourier_terms(doy: np.ndarray, K: int = 4) -> pd.DataFrame:
    """Annual Fourier basis with K harmonics."""
    out = {}
    for k in range(1, K + 1):
        out[f"cos_{k}"] = np.cos(2 * np.pi * k * doy / 365.25)
        out[f"sin_{k}"] = np.sin(2 * np.pi * k * doy / 365.25)
    return pd.DataFrame(out)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    # 1. Outcome: Fase I cost per day, summing 15-min EUR values per day
    print("Loading Fase I cost per day (ESIOS id 1373)...")
    y = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, SUM(value) AS fase1_cost_eur
    FROM '{IND}'
    WHERE indicator_id = 1373
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    y["d"] = pd.to_datetime(y["d"])
    print(f"  {len(y)} daily obs, range {y['d'].min().date()} -> {y['d'].max().date()}")
    print(f"  Fase I cost: mean={y['fase1_cost_eur'].mean()/1e6:.2f} M EUR/day, median={y['fase1_cost_eur'].median()/1e6:.2f}")

    # 2. CO2 monthly -> daily forward-fill
    co2 = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, value AS co2_eur_t
    FROM '{IND}'
    WHERE indicator_id = 1391
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    ORDER BY 1
    """).fetchdf()
    co2["d"] = pd.to_datetime(co2["d"])
    co2 = co2.set_index("d").asfreq("D").ffill().reset_index()

    # 3. Gas TNP daily (only from Nov 2022)
    gas = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, value AS gas_eur_ton
    FROM '{IND}'
    WHERE indicator_id = 1940
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    ORDER BY 1
    """).fetchdf()
    gas["d"] = pd.to_datetime(gas["d"])
    gas = gas.set_index("d").asfreq("D").ffill().reset_index()

    # 4. RES installed capacity from ENTSO-E A65 (annual, 2021+). Compose with
    # the project-level Spanish capacity series for 2018-2020.
    cap = pd.read_parquet(RES_CAP)
    # RES = wind + solar PV + solar thermal + RoR + biomass + waste
    res_psr = ["B01", "B11", "B15", "B16", "B17", "B19", "B25"]
    cap_res = cap[cap["psr_type"].isin(res_psr)].groupby("year")["capacity_mw"].sum() / 1000  # GW
    res_annual = {}
    for yr in range(2018, 2027):
        if yr in cap_res.index and cap_res[yr] > 0:
            res_annual[yr] = cap_res[yr]
        else:
            res_annual[yr] = RES_GW_ANNUAL.get(yr, np.nan)
    print(f"  RES capacity (GW) by year: {dict(sorted(res_annual.items()))}")
    # Build daily series by linear interpolation between annual values
    dates = pd.date_range("2018-01-01", "2026-05-15", freq="D")
    res_daily = pd.DataFrame({"d": dates})
    res_daily["res_gw"] = res_daily["d"].apply(
        lambda x: res_annual.get(x.year, np.nan)
        + (res_annual.get(x.year + 1, res_annual.get(x.year, 0)) - res_annual.get(x.year, 0))
        * (x.dayofyear - 1) / 365.25
    )

    # 5. Reservoir weekly -> daily
    res_w = pd.read_parquet(RESERVOIR)
    res_w["d"] = pd.to_datetime(res_w["week_start"]).dt.date.astype("datetime64[ns]")
    res_w = res_w[["d", "reservoir_twh"]].drop_duplicates(subset="d").set_index("d").sort_index()
    res_w = res_w.asfreq("D").ffill().reset_index()

    # Build the joint panel
    df = (
        y.merge(co2, on="d", how="left")
        .merge(gas, on="d", how="left")
        .merge(res_daily, on="d", how="left")
        .merge(res_w[["d", "reservoir_twh"]], on="d", how="left")
    )
    df["doy"] = df["d"].dt.dayofyear
    df["t_yr"] = (df["d"] - pd.Timestamp("2018-01-01")).dt.days / 365.25
    fk = fourier_terms(df["doy"].values, K=4)
    df = pd.concat([df.reset_index(drop=True), fk.reset_index(drop=True)], axis=1)
    df["log_cost"] = np.log(df["fase1_cost_eur"].clip(lower=1.0))
    print(f"  joined panel rows: {len(df):,}")
    print(f"  missing: co2={df['co2_eur_t'].isna().sum()}, gas={df['gas_eur_ton'].isna().sum()}, "
          f"res={df['res_gw'].isna().sum()}, reservoir={df['reservoir_twh'].isna().sum()}")

    df_pre = df[df["d"] < PRE_IDA_END].copy().reset_index(drop=True)
    df_da15 = df[(df["d"] >= DA15_ID15_START) & (df["d"] <= DA15_ID15_END)].copy().reset_index(drop=True)
    print(f"  pre-IDA n={len(df_pre)}, DA15/ID15 n={len(df_da15)}")

    fourier_cols = [c for c in df.columns if c.startswith(("cos_", "sin_"))]

    # =================================================================
    # Spec A: Fourier-only baseline (pre-IDA full window)
    # =================================================================
    print("\n=== Spec A: Fourier-only baseline (2018 - Jun 2024) ===")
    X_A = sm.add_constant(df_pre[fourier_cols].astype(float))
    y_A = df_pre["log_cost"].astype(float)
    fit_A = sm.OLS(y_A, X_A).fit(cov_type="HC3")
    print(f"R² = {fit_A.rsquared:.3f}, n = {fit_A.nobs}")

    # =================================================================
    # Spec B: + CO2 + RES + reservoir + linear trend (full window, no gas)
    # =================================================================
    print("\n=== Spec B: Fourier + CO2 + RES + reservoir + trend (2018 - Jun 2024) ===")
    cols_B = fourier_cols + ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    msk_B = df_pre[cols_B].notna().all(axis=1)
    X_B = sm.add_constant(df_pre.loc[msk_B, cols_B].astype(float))
    y_B = df_pre.loc[msk_B, "log_cost"].astype(float)
    fit_B = sm.OLS(y_B, X_B).fit(cov_type="HC3")
    print(f"R² = {fit_B.rsquared:.3f}, n = {fit_B.nobs}")
    print("Trend + structural coefs:")
    for c in ["t_yr", "co2_eur_t", "res_gw", "reservoir_twh"]:
        print(f"  {c:<20} = {fit_B.params[c]:+.4f}  (se={fit_B.bse[c]:.4f}, p={fit_B.pvalues[c]:.3f})")

    # =================================================================
    # Spec C: + gas (Nov 2022 - Jun 2024 only)
    # =================================================================
    print("\n=== Spec C: Fourier + gas + CO2 + RES + reservoir + trend (Nov 2022 - Jun 2024) ===")
    cols_C = fourier_cols + ["gas_eur_ton", "co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    msk_C = df_pre[cols_C].notna().all(axis=1)
    X_C = sm.add_constant(df_pre.loc[msk_C, cols_C].astype(float))
    y_C = df_pre.loc[msk_C, "log_cost"].astype(float)
    fit_C = sm.OLS(y_C, X_C).fit(cov_type="HC3")
    print(f"R² = {fit_C.rsquared:.3f}, n = {fit_C.nobs}")
    for c in ["gas_eur_ton", "co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]:
        if c in fit_C.params.index:
            print(f"  {c:<20} = {fit_C.params[c]:+.4f}  (se={fit_C.bse[c]:.4f}, p={fit_C.pvalues[c]:.3f})")

    # =================================================================
    # Compare Fourier coefficients across A, B, C
    # =================================================================
    print("\n=== Fourier coefficient comparison ===")
    rows = []
    for c in fourier_cols:
        rows.append({
            "term": c,
            "Spec A (Fourier-only)": fit_A.params.get(c, np.nan),
            "Spec B (+controls, full)": fit_B.params.get(c, np.nan),
            "Spec C (+gas, short)": fit_C.params.get(c, np.nan),
        })
    fcomp = pd.DataFrame(rows).set_index("term")
    print(fcomp.round(4).to_string())
    fcomp.to_csv(OUT_DIR / "spec_compare.csv")
    print(f"\nwrote {OUT_DIR / 'spec_compare.csv'}")

    # =================================================================
    # Year-fold CV: hold out each pre-IDA year, fit on rest, predict held-out
    # =================================================================
    print("\n=== Year-fold CV (Spec B) ===")
    cv_rows = []
    for yr_out in sorted(df_pre["d"].dt.year.unique()):
        is_test = df_pre["d"].dt.year == yr_out
        train = df_pre[~is_test & msk_B]
        test = df_pre[is_test & msk_B]
        if len(test) < 30 or len(train) < 200:
            continue
        Xtr = sm.add_constant(train[cols_B].astype(float))
        ytr = train["log_cost"].astype(float)
        fit = sm.OLS(ytr, Xtr).fit()
        Xte = sm.add_constant(test[cols_B].astype(float), has_constant="add")
        yhat = fit.predict(Xte)
        yte = test["log_cost"].astype(float)
        # Compute "Fourier-only" prediction by zeroing the non-Fourier params at test means
        params_seasonal = fit.params.copy()
        for c in ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr", "const"]:
            if c in params_seasonal.index:
                params_seasonal[c] = 0  # zero out, keep only Fourier
        params_seasonal["const"] = fit.params["const"]  # keep intercept
        yhat_seasonal = (Xte[params_seasonal.index] * params_seasonal).sum(axis=1)
        cv_rows.append({
            "year": yr_out,
            "n_test": len(test),
            "rmse_full": float(np.sqrt(((yte - yhat) ** 2).mean())),
            "mean_y": yte.mean(),
            "mean_yhat": yhat.mean(),
        })
        print(f"  year {yr_out}: n={len(test)}, RMSE(log) = {cv_rows[-1]['rmse_full']:.3f}, "
              f"mean(y)={yte.mean():.3f}, mean(ŷ)={yhat.mean():.3f}")
    pd.DataFrame(cv_rows).to_csv(OUT_DIR / "yr_fold_cv.csv", index=False)

    # =================================================================
    # Project pre-IDA seasonal to DA15/ID15: predict and compute residual.
    # We compare three predictions:
    #   1. Spec A (Fourier-only)
    #   2. Spec B (Fourier + controls) — uses DA15/ID15 covariate values
    #   3. Spec B_seasonal_only — uses Spec B parameters but with covariates SET TO PRE-IDA MEANS
    # =================================================================
    print("\n=== DA15/ID15 deseasonalization residuals ===")
    if len(df_da15) == 0:
        print("  no DA15/ID15 data, skipping")
        return

    msk_da_B = df_da15[cols_B].notna().all(axis=1)
    df_da_use = df_da15[msk_da_B].copy()

    Xda_A = sm.add_constant(df_da_use[fourier_cols].astype(float), has_constant="add")
    yhat_A = fit_A.predict(Xda_A)

    Xda_B = sm.add_constant(df_da_use[cols_B].astype(float), has_constant="add")
    yhat_B = fit_B.predict(Xda_B)

    # Spec B "seasonal-only" prediction: replace covariates with their pre-IDA means.
    pre_means = df_pre[["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]].mean()
    df_da_ref = df_da_use.copy()
    for c in pre_means.index:
        df_da_ref[c] = pre_means[c]
    Xda_B_ref = sm.add_constant(df_da_ref[cols_B].astype(float), has_constant="add")
    yhat_B_seasonal = fit_B.predict(Xda_B_ref)

    obs = df_da_use["log_cost"].astype(float).reset_index(drop=True)
    res = pd.DataFrame({
        "d": df_da_use["d"].reset_index(drop=True),
        "obs_log_cost": obs.values,
        "obs_cost_M": np.exp(obs.values) / 1e6,
        "yhat_A_Fourier_only": yhat_A.values,
        "yhat_B_full": yhat_B.values,
        "yhat_B_seasonal_only_preIDA_means": yhat_B_seasonal.values,
    })
    res["resid_A"] = res["obs_log_cost"] - res["yhat_A_Fourier_only"]
    res["resid_B"] = res["obs_log_cost"] - res["yhat_B_full"]
    res["resid_B_seasonal"] = res["obs_log_cost"] - res["yhat_B_seasonal_only_preIDA_means"]

    print("DA15/ID15 average log-cost vs each prediction:")
    print(f"  obs mean log     = {res['obs_log_cost'].mean():.3f}  (level = {np.exp(res['obs_log_cost'].mean())/1e6:.2f} M EUR/day)")
    print(f"  Spec A Fourier   = {res['yhat_A_Fourier_only'].mean():.3f}  (level = {np.exp(res['yhat_A_Fourier_only'].mean())/1e6:.2f} M EUR/day)")
    print(f"  Spec B full      = {res['yhat_B_full'].mean():.3f}  (level = {np.exp(res['yhat_B_full'].mean())/1e6:.2f} M EUR/day)")
    print(f"  Spec B seasonal  = {res['yhat_B_seasonal_only_preIDA_means'].mean():.3f}  (level = {np.exp(res['yhat_B_seasonal_only_preIDA_means'].mean())/1e6:.2f} M EUR/day)")
    print(f"  mean resid A     = {res['resid_A'].mean():+.3f}  -> obs is {(np.exp(res['resid_A'].mean())-1)*100:+.1f}% vs Fourier-only expectation")
    print(f"  mean resid B     = {res['resid_B'].mean():+.3f}  -> obs is {(np.exp(res['resid_B'].mean())-1)*100:+.1f}% vs Fourier+controls expectation")
    print(f"  mean resid B-sea = {res['resid_B_seasonal'].mean():+.3f}  -> obs is {(np.exp(res['resid_B_seasonal'].mean())-1)*100:+.1f}% vs pre-IDA-mean-covariates seasonal expectation")

    res.to_csv(OUT_DIR / "da15id15_residual.csv", index=False)
    print(f"\nwrote {OUT_DIR / 'da15id15_residual.csv'}")

    # Also a raw same-calendar comparison for benchmarking
    print("\n=== Calendar-match benchmark (no Fourier) ===")
    df_pre_oct_may = df_pre[df_pre["d"].dt.month.isin([10, 11, 12, 1, 2, 3, 4, 5])]
    print(f"  pre-IDA Oct-May mean log-cost = {df_pre_oct_may['log_cost'].mean():.3f}  (level = {np.exp(df_pre_oct_may['log_cost'].mean())/1e6:.2f} M EUR/day)")
    print(f"  DA15/ID15 mean log-cost       = {res['obs_log_cost'].mean():.3f}  (level = {np.exp(res['obs_log_cost'].mean())/1e6:.2f} M EUR/day)")
    diff = res["obs_log_cost"].mean() - df_pre_oct_may["log_cost"].mean()
    print(f"  diff (DA15 - pre-IDA Oct-May) = {diff:+.3f}  -> DA15 is {(np.exp(diff)-1)*100:+.1f}% higher than pre-IDA same-cal")


if __name__ == "__main__":
    main()
