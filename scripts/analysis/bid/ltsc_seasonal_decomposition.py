# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Weron-style Long-Term Seasonal Component (LTSC) decomposition,
#        following Weron (2008, 2014) and Janczura, Trück, Weron, Wolff (2013).
#        Fits log(P_t) = α + β·t + Σ_k Fourier_k(t) + γ·X_t + ε_t on pre-IDA,
#        then projects the LTSC to DA15/ID15 dates. Deseasonalized residual
#        = observed − LTSC.
#
#        For BOUNDED outcomes (in-band share ∈ [0,1]) the dependent variable
#        is logit-transformed first, so the linear-OLS spec lives in
#        unbounded space; predictions are back-transformed via sigmoid.
#
#        Diagnostics computed for each outcome:
#         (D1) overall R² in the transformed (log/logit) space
#         (D2) year-fold stability of Fourier coefficients (jack-knife)
#         (D3) residual autocorrelation (Ljung-Box up to lag 30)
#         (D4) visual seasonal-cycle fit: yearly-overlaid empirical vs fitted
#        Standard errors are Newey-West HAC (maxlags=14, ≈ 2 weeks).
#
#        Each outcome is reported separately. The point of this pilot is
#        also to check whether the LTSC methodology — originally developed
#        for electricity PRICES — transfers cleanly to BID-SHAPE outcomes
#        (which have a structurally different generating process).
#
# FEEDS: descriptive_facts.tex (Weron-style LTSC subsection — only outcomes
#        that PASS the diagnostics make it into the document).
#
# OUT:
#   results/regressions/bid/ltsc_decomposition/
#     {outcome}_summary.csv
#     {outcome}_yr_fold.csv
#     {outcome}_residual_acf.csv
#     {outcome}_seasonal_cycle.pdf

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/bid/ltsc_decomposition"

START = "2022-01-01"
END = "2026-05-15"
PRE_IDA_END = pd.Timestamp("2024-06-13")
DA15_ID15_START = pd.Timestamp("2025-10-01")
DA15_ID15_END = pd.Timestamp("2026-05-15")
K_HARMONICS = 4

RES_GW_ANNUAL = {
    2018: 28.0, 2019: 34.4, 2020: 39.0, 2021: 42.1, 2022: 48.6,
    2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0,
}


def tech_bucket(t: str | None) -> str:
    if t is None:
        return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t:
        return "CCGT"
    if "nuclear" in t:
        return "Nuclear"
    if "hidráulica generación" in t:
        return "Hydro"
    if "bombeo" in t:
        return "Hydro_pump"
    if "re mercado eólica" in t:
        return "Wind"
    if "re mercado solar fotovolt" in t:
        return "Solar_PV"
    if "re mercado solar térmica" in t:
        return "Solar_Thermal"
    return "Other"


def fourier_terms(doy: np.ndarray, K: int) -> pd.DataFrame:
    out = {}
    for k in range(1, K + 1):
        out[f"cos_{k}"] = np.cos(2 * np.pi * k * doy / 365.25)
        out[f"sin_{k}"] = np.sin(2 * np.pi * k * doy / 365.25)
    return pd.DataFrame(out)


def add_covariates(df: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    co2 = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, value AS co2_eur_t
    FROM '{IND}' WHERE indicator_id = 1391
      AND date BETWEEN '{START}' AND '{END}' ORDER BY 1
    """).fetchdf()
    co2["d"] = pd.to_datetime(co2["d"])
    co2 = co2.set_index("d").asfreq("D").ffill().reset_index()

    cap = pd.read_parquet(RES_CAP)
    res_psr = ["B01", "B11", "B15", "B16", "B17", "B19", "B25"]
    cap_res = cap[cap["psr_type"].isin(res_psr)].groupby("year")["capacity_mw"].sum() / 1000
    res_annual = {}
    for yr in range(2018, 2027):
        res_annual[yr] = cap_res[yr] if yr in cap_res.index and cap_res[yr] > 0 else RES_GW_ANNUAL.get(yr, np.nan)
    df = df.copy()
    df["res_gw"] = df["d"].apply(
        lambda x: res_annual.get(x.year, np.nan)
        + (res_annual.get(x.year + 1, res_annual.get(x.year, 0)) - res_annual.get(x.year, 0))
        * (x.dayofyear - 1) / 365.25
    )

    res_w = pd.read_parquet(RESERVOIR)
    res_w["d"] = pd.to_datetime(res_w["week_start"]).dt.date.astype("datetime64[ns]")
    res_w = res_w[["d", "reservoir_twh"]].drop_duplicates(subset="d").set_index("d").sort_index().asfreq("D").ffill().reset_index()

    df = df.merge(co2, on="d", how="left").merge(res_w[["d", "reservoir_twh"]], on="d", how="left")
    df["doy"] = df["d"].dt.dayofyear
    df["t_yr"] = (df["d"] - pd.Timestamp(START)).dt.days / 365.25
    fk = fourier_terms(df["doy"].values, K_HARMONICS)
    return pd.concat([df.reset_index(drop=True), fk.reset_index(drop=True)], axis=1)


def build_da_price() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    df = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, AVG(price_es_eur_mwh) AS da_price
    FROM read_parquet('{MPDBC}')
    WHERE date BETWEEN '{START}' AND '{END}'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_bidshape() -> pd.DataFrame:
    """Per-tech daily in-band share. Reuses logic from fourier_bidshape_pilot."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])
    H = 50.0
    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}')
      WHERE buy_sell = 'V' AND date >= '{START}' AND date <= '{END}'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period, price_eur_mwh AS p,
             quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu_minutes
      FROM read_parquet('{DET}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}')
      WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu_minutes) AS mtu_minutes
      FROM det dv
        JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    ),
    per_cell AS (
      SELECT d, period, unit_code,
             SUM(q * mtu_minutes/60.0) AS mw_total,
             SUM(q * mtu_minutes/60.0 * in_band) AS mw_in
      FROM joined GROUP BY 1, 2, 3
    ),
    per_tech AS (
      SELECT pc.d, u.tech,
             SUM(pc.mw_total) AS mw_total, SUM(pc.mw_in) AS mw_in
      FROM per_cell pc JOIN u ON pc.unit_code=u.unit_code
      GROUP BY 1, 2
    )
    SELECT d, tech, mw_in/NULLIF(mw_total,0) AS in_band_share
    FROM per_tech WHERE mw_total > 0
    ORDER BY d, tech
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def fit_ltsc(
    df: pd.DataFrame,
    y_col: str,
    transform: str,
    label: str,
) -> dict:
    """Fit Weron-style LTSC on pre-IDA, return full diagnostics."""
    assert transform in ("log", "logit", "none")
    fourier_cols = [f"cos_{k}" for k in range(1, K_HARMONICS + 1)] + [f"sin_{k}" for k in range(1, K_HARMONICS + 1)]
    ctrl_cols = ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    cols = fourier_cols + ctrl_cols

    df = df.copy()
    raw = df[y_col].astype(float)
    if transform == "log":
        df["y"] = np.log(raw.clip(lower=0.1))
    elif transform == "logit":
        p = raw.clip(0.001, 0.999)
        df["y"] = np.log(p / (1 - p))
    else:
        df["y"] = raw
    df = df.dropna(subset=cols + ["y"]).reset_index(drop=True)

    pre = df[df["d"] < PRE_IDA_END].copy()
    da15 = df[(df["d"] >= DA15_ID15_START) & (df["d"] <= DA15_ID15_END)].copy()
    if len(pre) < 200 or len(da15) < 30:
        return {"label": label, "skip": True, "n_pre": len(pre), "n_da15": len(da15)}

    X_pre = sm.add_constant(pre[cols].astype(float))
    y_pre = pre["y"].astype(float)
    # HAC standard errors for time-series autocorrelation.
    fit = sm.OLS(y_pre, X_pre).fit(cov_type="HAC", cov_kwds={"maxlags": 14})

    # D1: R²
    r2 = fit.rsquared

    # D3: residual autocorrelation
    resid = y_pre - fit.predict(X_pre)
    lb = acorr_ljungbox(resid, lags=[5, 10, 20, 30], return_df=True)

    # D2: year-fold stability of Fourier coefficients
    yr_stab = []
    for yr_out in sorted(pre["d"].dt.year.unique()):
        msk_train = pre["d"].dt.year != yr_out
        if msk_train.sum() < 200:
            continue
        X_tr = sm.add_constant(pre.loc[msk_train, cols].astype(float))
        y_tr = pre.loc[msk_train, "y"].astype(float)
        f = sm.OLS(y_tr, X_tr).fit()
        yr_stab.append({
            "year_out": yr_out,
            **{c: f.params.get(c, np.nan) for c in fourier_cols}
        })
    yr_stab_df = pd.DataFrame(yr_stab)

    # Projections
    pre_means = pre[ctrl_cols].mean()
    da15_ref = da15.copy()
    for c in ctrl_cols:
        da15_ref[c] = pre_means[c]
    Xda_ref = sm.add_constant(da15_ref[cols].astype(float), has_constant="add")
    Xda_full = sm.add_constant(da15[cols].astype(float), has_constant="add")
    yhat_seasonal = fit.predict(Xda_ref)  # LTSC only (covariates at pre-IDA mean)
    yhat_full = fit.predict(Xda_full)  # LTSC + DA15 covariates

    def back(z: np.ndarray) -> np.ndarray:
        if transform == "log":
            return np.exp(z)
        if transform == "logit":
            return 1 / (1 + np.exp(-z))
        return z

    yhat_seasonal_lvl = back(yhat_seasonal.values)
    yhat_full_lvl = back(yhat_full.values)
    obs_lvl = back(da15["y"].values)
    obs_lvl_mean = obs_lvl.mean()

    return {
        "label": label,
        "transform": transform,
        "n_pre": len(pre),
        "n_da15": len(da15),
        "R2": r2,
        "lb_p_lag5":  float(lb["lb_pvalue"].iloc[0]),
        "lb_p_lag10": float(lb["lb_pvalue"].iloc[1]),
        "lb_p_lag20": float(lb["lb_pvalue"].iloc[2]),
        "lb_p_lag30": float(lb["lb_pvalue"].iloc[3]),
        "yr_stab_df": yr_stab_df,
        "amp_1": float(np.sqrt(fit.params.get("cos_1", 0) ** 2 + fit.params.get("sin_1", 0) ** 2)),
        "amp_2": float(np.sqrt(fit.params.get("cos_2", 0) ** 2 + fit.params.get("sin_2", 0) ** 2)),
        "amp_3": float(np.sqrt(fit.params.get("cos_3", 0) ** 2 + fit.params.get("sin_3", 0) ** 2)),
        "amp_4": float(np.sqrt(fit.params.get("cos_4", 0) ** 2 + fit.params.get("sin_4", 0) ** 2)),
        "obs_lvl_mean":        float(obs_lvl_mean),
        "yhat_seasonal_lvl":   float(yhat_seasonal_lvl.mean()),
        "yhat_full_lvl":       float(yhat_full_lvl.mean()),
        "resid_seasonal_lvl":  float(obs_lvl_mean - yhat_seasonal_lvl.mean()),
        "resid_full_lvl":      float(obs_lvl_mean - yhat_full_lvl.mean()),
        "n_oob_full":          int(((yhat_full_lvl < 0) | (yhat_full_lvl > 1)).sum()) if transform == "logit" else 0,
        "pre_df":              pre[["d", "y", "doy"]].assign(yhat=fit.predict(X_pre).values),
        "da15_df":             da15[["d", "y", "doy"]].assign(yhat_seasonal=yhat_seasonal.values, yhat_full=yhat_full.values),
    }


def plot_seasonal_cycle(result: dict, out_path: Path, transform: str) -> None:
    """Yearly-overlaid empirical vs fitted seasonal cycle (Weron-style diagnostic)."""
    pre = result["pre_df"].copy()
    pre["year"] = pre["d"].dt.year
    pre["doy"] = pre["d"].dt.dayofyear

    fig, ax = plt.subplots(1, 1, figsize=(8.5, 4.5))
    years = sorted(pre["year"].unique())
    cmap = plt.colormaps["viridis"]
    for i, y in enumerate(years):
        sub = pre[pre["year"] == y].sort_values("doy")
        ax.scatter(sub["doy"], sub["y"], s=4, alpha=0.45, color=cmap(i / max(1, len(years) - 1)), label=str(y))
    # Fitted seasonal-only cycle: zero out covariates, vary doy
    grid_doy = np.linspace(1, 365, 365)
    fk = fourier_terms(grid_doy, K_HARMONICS)
    seasonal_fit = pre.groupby("doy")["yhat"].mean().reindex(grid_doy.astype(int))
    ax.plot(grid_doy, seasonal_fit.values, color="red", lw=1.6, label="LTSC fitted")
    ax.set_xlabel("Day of year")
    ax.set_ylabel(f"{result['label']} ({transform}-space)" if transform != "none" else result["label"])
    ax.set_title(f"{result['label']} — pre-IDA seasonal cycle (yearly overlay)")
    ax.legend(loc="best", fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # ============ DA price ============
    print("\n" + "=" * 60 + "\nDA PRICE — canonical Weron LTSC case (log-space)\n" + "=" * 60)
    df_p = build_da_price()
    df_p = add_covariates(df_p)
    res_p = fit_ltsc(df_p, y_col="da_price", transform="log", label="DA price (EUR/MWh)")
    if res_p.get("skip"):
        print(f"  skipped: {res_p}")
    else:
        print(f"  n_pre={res_p['n_pre']}, n_da15={res_p['n_da15']}, R²={res_p['R2']:.3f}")
        print(f"  Fourier amps: k=1 {res_p['amp_1']:.3f}, k=2 {res_p['amp_2']:.3f}, k=3 {res_p['amp_3']:.3f}, k=4 {res_p['amp_4']:.3f}")
        print(f"  Ljung-Box residual (p-values): lag5={res_p['lb_p_lag5']:.3f}, lag10={res_p['lb_p_lag10']:.3f}, lag20={res_p['lb_p_lag20']:.3f}, lag30={res_p['lb_p_lag30']:.3f}")
        print(f"  DA15/ID15 obs={res_p['obs_lvl_mean']:.1f} EUR/MWh, LTSC seasonal pred={res_p['yhat_seasonal_lvl']:.1f}, resid={res_p['resid_seasonal_lvl']:+.1f}")
        print(f"  LTSC+ctrls pred={res_p['yhat_full_lvl']:.1f}, resid={res_p['resid_full_lvl']:+.1f}")
        plot_seasonal_cycle(res_p, OUT_DIR / "da_price_cycle.pdf", "log")
        res_p["yr_stab_df"].to_csv(OUT_DIR / "da_price_yr_stab.csv", index=False)
        pd.DataFrame([{k: v for k, v in res_p.items() if not isinstance(v, pd.DataFrame)}]).to_csv(OUT_DIR / "da_price_summary.csv", index=False)

    # ============ Bid-shape per tech ============
    print("\n" + "=" * 60 + "\nBID-SHAPE per tech (in-band share, logit-space)\n" + "=" * 60)
    bs = build_bidshape()
    print(f"  raw daily-tech panel: {len(bs):,} rows, {bs['d'].nunique()} days")

    focus = ["CCGT", "Wind", "Solar_PV", "Hydro", "Nuclear"]
    bid_summary_rows = []
    for tech in focus:
        df_t = bs[bs["tech"] == tech].copy()
        df_t = add_covariates(df_t)
        res = fit_ltsc(df_t, y_col="in_band_share", transform="logit", label=f"{tech} in-band share")
        if res.get("skip"):
            print(f"\n  {tech}: skipped (n_pre={res['n_pre']}, n_da15={res['n_da15']})")
            continue
        print(f"\n  === {tech} ===")
        print(f"  n_pre={res['n_pre']}, n_da15={res['n_da15']}, R²(logit)={res['R2']:.3f}")
        print(f"  Fourier amps (logit): k=1 {res['amp_1']:.3f}, k=2 {res['amp_2']:.3f}, k=3 {res['amp_3']:.3f}, k=4 {res['amp_4']:.3f}")
        print(f"  Ljung-Box residual: lag5={res['lb_p_lag5']:.3f}, lag10={res['lb_p_lag10']:.3f}, lag20={res['lb_p_lag20']:.3f}, lag30={res['lb_p_lag30']:.3f}")
        print(f"  Year-fold k=4 stability:")
        ystab = res["yr_stab_df"]
        print(f"    {ystab[['year_out', 'cos_4', 'sin_4']].round(3).to_string(index=False)}")
        print(f"  DA15/ID15 obs share={res['obs_lvl_mean']:.3f}")
        print(f"  LTSC seasonal pred share={res['yhat_seasonal_lvl']:.3f}, resid={res['resid_seasonal_lvl']*100:+.1f}pp")
        print(f"  LTSC+ctrls pred share={res['yhat_full_lvl']:.3f}, resid={res['resid_full_lvl']*100:+.1f}pp")
        plot_seasonal_cycle(res, OUT_DIR / f"bidshape_{tech}_cycle.pdf", "logit")
        res["yr_stab_df"].to_csv(OUT_DIR / f"bidshape_{tech}_yr_stab.csv", index=False)
        bid_summary_rows.append({k: v for k, v in res.items() if not isinstance(v, pd.DataFrame)})

    pd.DataFrame(bid_summary_rows).to_csv(OUT_DIR / "bidshape_summary.csv", index=False)
    print(f"\nwrote results to {OUT_DIR}")


if __name__ == "__main__":
    main()
