# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Multi-outcome Fourier seasonal decomposition pilot. Runs the same
#        Fourier(K=4) + structural-controls + linear-trend spec across
#        four candidate outcomes with full pre-IDA history:
#          1. DA price daily mean (OMIE marginalpdbc)
#          2. Demand daily total (ESIOS id 10027 P48 total)
#          3. Fase I cost per day (ESIOS id 1373)
#          4. System imbalance abs volume per day (ESIOS id 762)
#        For each: fit on pre-IDA (2018-01-01 to 2024-06-13) with covariates
#        (CO2, RES installed capacity, reservoir filling, linear trend t/365),
#        project Fourier seasonal to DA15/ID15 dates, report:
#         - the *quarterly* Fourier coefficients (k=4: cos_4, sin_4)
#         - annual k=1 (should shrink under linear trend)
#         - year-fold CV RMSE
#         - DA15/ID15 mean deseasonalized residual
# FEEDS: EXPLORATORY — not in descriptive_facts.tex.
# OUT: results/regressions/balancing/fourier_seasonal/multi_outcome_summary.csv

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/balancing/fourier_seasonal"

PRE_IDA_END = pd.Timestamp("2024-06-13")
DA15_ID15_START = pd.Timestamp("2025-10-01")
DA15_ID15_END = pd.Timestamp("2026-05-15")
K_HARMONICS = 4

# Spanish RES (wind + solar + biomass + RoR) installed GW. Annual values.
RES_GW_ANNUAL = {
    2018: 28.0, 2019: 34.4, 2020: 39.0, 2021: 42.1, 2022: 48.6,
    2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0,
}


def fourier_terms(doy: np.ndarray, K: int) -> pd.DataFrame:
    out = {}
    for k in range(1, K + 1):
        out[f"cos_{k}"] = np.cos(2 * np.pi * k * doy / 365.25)
        out[f"sin_{k}"] = np.sin(2 * np.pi * k * doy / 365.25)
    return pd.DataFrame(out)


def load_outcomes(con) -> pd.DataFrame:
    print("Loading outcomes (daily aggregates, 2018-01 to 2026-05)...")
    # 1. DA price (OMIE marginalpdbc) — daily mean across the day's periods
    da_p = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, AVG(price_es_eur_mwh) AS da_price_eur_mwh
    FROM read_parquet('{MPDBC}')
    WHERE date BETWEEN '2018-01-01' AND '2026-05-15'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    # 2. Demand P48 total — daily sum of MW values (scaled to MWh given periodicity)
    dem = con.execute(f"""
    SELECT CAST(date AS DATE) AS d,
           AVG(value) AS demand_mw_mean,
           SUM(value)/96.0 AS demand_mwh_day_proxy
    FROM '{IND}' WHERE indicator_id = 10027
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    # 3. Fase I cost — daily SUM of 15-min EUR
    fase1 = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, SUM(value) AS fase1_cost_eur
    FROM '{IND}' WHERE indicator_id = 1373
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    # 4. System imbalance abs vol — daily SUM of |value|
    imb = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, SUM(ABS(value)) AS imb_abs_mwh
    FROM '{IND}' WHERE indicator_id = 762
      AND date BETWEEN '2018-01-01' AND '2026-05-15'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()

    df = da_p.merge(dem, on="d", how="outer").merge(fase1, on="d", how="outer").merge(imb, on="d", how="outer")
    df["d"] = pd.to_datetime(df["d"])
    df = df.sort_values("d").reset_index(drop=True)
    print(f"  panel rows: {len(df):,}, range {df['d'].min().date()} -> {df['d'].max().date()}")
    return df


def add_covariates(df: pd.DataFrame, con) -> pd.DataFrame:
    # CO2 monthly
    co2 = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, value AS co2_eur_t
    FROM '{IND}' WHERE indicator_id = 1391
      AND date BETWEEN '2018-01-01' AND '2026-05-15' ORDER BY 1
    """).fetchdf()
    co2["d"] = pd.to_datetime(co2["d"])
    co2 = co2.set_index("d").asfreq("D").ffill().reset_index()

    # RES annual -> daily linear interp
    cap = pd.read_parquet(RES_CAP)
    res_psr = ["B01", "B11", "B15", "B16", "B17", "B19", "B25"]
    cap_res = cap[cap["psr_type"].isin(res_psr)].groupby("year")["capacity_mw"].sum() / 1000
    res_annual = {}
    for yr in range(2018, 2027):
        res_annual[yr] = cap_res[yr] if yr in cap_res.index and cap_res[yr] > 0 else RES_GW_ANNUAL.get(yr, np.nan)
    df["res_gw"] = df["d"].apply(
        lambda x: res_annual.get(x.year, np.nan)
        + (res_annual.get(x.year + 1, res_annual.get(x.year, 0)) - res_annual.get(x.year, 0))
        * (x.dayofyear - 1) / 365.25
    )

    # Reservoir weekly
    res_w = pd.read_parquet(RESERVOIR)
    res_w["d"] = pd.to_datetime(res_w["week_start"]).dt.date.astype("datetime64[ns]")
    res_w = res_w[["d", "reservoir_twh"]].drop_duplicates(subset="d").set_index("d").sort_index()
    res_w = res_w.asfreq("D").ffill().reset_index()

    df = df.merge(co2, on="d", how="left").merge(res_w[["d", "reservoir_twh"]], on="d", how="left")
    df["doy"] = df["d"].dt.dayofyear
    df["t_yr"] = (df["d"] - pd.Timestamp("2018-01-01")).dt.days / 365.25
    fk = fourier_terms(df["doy"].values, K_HARMONICS)
    return pd.concat([df.reset_index(drop=True), fk.reset_index(drop=True)], axis=1)


def fit_and_project(
    df: pd.DataFrame, outcome: str, log_transform: bool = True
) -> dict:
    """Fit Fourier+controls+trend on pre-IDA; project to DA15/ID15."""
    fourier_cols = [c for c in df.columns if c.startswith(("cos_", "sin_"))]
    ctrl_cols = ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    cols = fourier_cols + ctrl_cols

    df = df.copy()
    df["y"] = np.log(df[outcome].clip(lower=1e-6)) if log_transform else df[outcome]

    pre = df[(df["d"] < PRE_IDA_END) & df[cols + ["y"]].notna().all(axis=1)].copy()
    da15 = df[(df["d"] >= DA15_ID15_START) & (df["d"] <= DA15_ID15_END) & df[cols + ["y"]].notna().all(axis=1)].copy()

    if len(pre) < 200 or len(da15) < 30:
        return {"outcome": outcome, "skip": True, "n_pre": len(pre), "n_da15": len(da15)}

    X = sm.add_constant(pre[cols].astype(float))
    y = pre["y"].astype(float)
    fit = sm.OLS(y, X).fit(cov_type="HC3")

    # Same-calendar baseline (Oct-May) for benchmarking
    pre_oct_may = pre[pre["d"].dt.month.isin([10, 11, 12, 1, 2, 3, 4, 5])]
    cal_match_mean = pre_oct_may["y"].mean()

    # Project Fourier-only seasonal (covariates at pre-IDA mean)
    pre_means = pre[ctrl_cols].mean()
    da15_ref = da15.copy()
    for c in ctrl_cols:
        da15_ref[c] = pre_means[c]
    Xda_ref = sm.add_constant(da15_ref[cols].astype(float), has_constant="add")
    yhat_seasonal = fit.predict(Xda_ref)

    # Project full (covariates at DA15/ID15 actual values)
    Xda_full = sm.add_constant(da15[cols].astype(float), has_constant="add")
    yhat_full = fit.predict(Xda_full)

    obs = da15["y"].astype(float)

    # Year-fold CV on the quarterly Fourier coefficient
    yr_coefs = []
    for yr_out in sorted(pre["d"].dt.year.unique()):
        is_test = pre["d"].dt.year == yr_out
        train = pre[~is_test]
        if len(train) < 200:
            continue
        Xtr = sm.add_constant(train[cols].astype(float))
        ytr = train["y"].astype(float)
        f = sm.OLS(ytr, Xtr).fit()
        yr_coefs.append({
            "outcome": outcome, "year_out": yr_out,
            "cos_1": f.params.get("cos_1", np.nan),
            "sin_1": f.params.get("sin_1", np.nan),
            "cos_4": f.params.get("cos_4", np.nan),
            "sin_4": f.params.get("sin_4", np.nan),
        })

    return {
        "outcome": outcome,
        "n_pre": len(pre),
        "n_da15": len(da15),
        "R2": fit.rsquared,
        "obs_mean": obs.mean(),
        "cal_match_pre_oct_may": cal_match_mean,
        "yhat_seasonal_mean": yhat_seasonal.mean(),
        "yhat_full_mean": yhat_full.mean(),
        "resid_seasonal_mean": (obs.values - yhat_seasonal.values).mean(),
        "resid_full_mean": (obs.values - yhat_full.values).mean(),
        "cos_1": fit.params.get("cos_1", np.nan),
        "sin_1": fit.params.get("sin_1", np.nan),
        "cos_2": fit.params.get("cos_2", np.nan),
        "sin_2": fit.params.get("sin_2", np.nan),
        "cos_3": fit.params.get("cos_3", np.nan),
        "sin_3": fit.params.get("sin_3", np.nan),
        "cos_4": fit.params.get("cos_4", np.nan),
        "sin_4": fit.params.get("sin_4", np.nan),
        "amp_4": np.sqrt(fit.params.get("cos_4", 0) ** 2 + fit.params.get("sin_4", 0) ** 2),
        "amp_1": np.sqrt(fit.params.get("cos_1", 0) ** 2 + fit.params.get("sin_1", 0) ** 2),
        "yr_coefs": pd.DataFrame(yr_coefs),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    df = load_outcomes(con)
    df = add_covariates(df, con)

    outcomes = [
        ("da_price_eur_mwh", "DA price (€/MWh)", True),
        ("demand_mw_mean", "Demand (MW mean)", True),
        ("fase1_cost_eur", "Fase I cost (€/day)", True),
        ("imb_abs_mwh", "System imbalance |MWh|/day", True),
    ]

    results = []
    yr_coef_long = []
    for col, label, logT in outcomes:
        if col not in df.columns:
            print(f"\n!! {col} missing, skip")
            continue
        print(f"\n=== Outcome: {label} ===")
        out = fit_and_project(df, col, log_transform=logT)
        if out.get("skip"):
            print(f"  skipped (n_pre={out['n_pre']}, n_da15={out['n_da15']})")
            continue

        # Print clean summary
        is_log = "log" if logT else "lvl"
        print(f"  n_pre={out['n_pre']}, n_da15={out['n_da15']}, R²={out['R2']:.3f}")
        print(f"  Fourier amplitudes:   annual k=1 amp={out['amp_1']:.4f}   QUARTERLY k=4 amp={out['amp_4']:.4f}")
        print(f"  k=1 cos/sin: ({out['cos_1']:+.4f}, {out['sin_1']:+.4f})")
        print(f"  k=4 cos/sin: ({out['cos_4']:+.4f}, {out['sin_4']:+.4f})")
        print(f"  DA15/ID15 (in {is_log} units):")
        print(f"    obs                  = {out['obs_mean']:+.3f}")
        print(f"    Fourier seasonal     = {out['yhat_seasonal_mean']:+.3f}  -> resid = {out['resid_seasonal_mean']:+.3f}")
        print(f"    Fourier+ctrls@DA15   = {out['yhat_full_mean']:+.3f}  -> resid = {out['resid_full_mean']:+.3f}")
        print(f"    cal-match (Oct-May)  = {out['cal_match_pre_oct_may']:+.3f}")
        if logT:
            print(f"    obs/Fourier ratio    = {np.exp(out['resid_seasonal_mean']):.2f}x   obs/full ratio = {np.exp(out['resid_full_mean']):.2f}x")
        results.append(out)
        yr_coef_long.append(out["yr_coefs"])

    # Save summary
    keep = ["outcome", "n_pre", "n_da15", "R2", "obs_mean", "cal_match_pre_oct_may",
            "yhat_seasonal_mean", "yhat_full_mean", "resid_seasonal_mean", "resid_full_mean",
            "amp_1", "amp_4", "cos_1", "sin_1", "cos_2", "sin_2", "cos_3", "sin_3", "cos_4", "sin_4"]
    summary = pd.DataFrame([{k: r.get(k) for k in keep} for r in results])
    summary.to_csv(OUT_DIR / "multi_outcome_summary.csv", index=False)
    print(f"\nwrote {OUT_DIR / 'multi_outcome_summary.csv'}")

    if yr_coef_long:
        yr_long = pd.concat(yr_coef_long, ignore_index=True)
        yr_long.to_csv(OUT_DIR / "multi_outcome_yr_cv.csv", index=False)
        print("\n=== Year-fold stability of quarterly (k=4) coefficient ===")
        wide = yr_long.pivot_table(index="year_out", columns="outcome", values="cos_4")
        print("cos_4 leave-one-year-out:")
        print(wide.round(4).to_string())
        wide_s = yr_long.pivot_table(index="year_out", columns="outcome", values="sin_4")
        print("\nsin_4 leave-one-year-out:")
        print(wide_s.round(4).to_string())


if __name__ == "__main__":
    main()
