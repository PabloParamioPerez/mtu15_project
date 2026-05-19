# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Seasonality-adjusted descriptive regime comparison of per-tech
#        bid-shape, using Frisch-Waugh-Lovell-style single regression on
#        the pooled 2022-2026 daily-tech panel:
#
#          y_t = α + Σ_r β_r·D_regime_r(t)
#                  + Σ_{k=1..K} [a_k cos(2π k·doy/365) + b_k sin(...)]
#                  + δ·t + γ'·X_t + ε_t
#
#        where:
#          - y_t = logit(in-band share) for the bounded-share outcome
#          - D_regime: 5 reform dummies (baseline = 2022/2023/early-2024 pre-IDA)
#          - Σ Fourier(K=4): annual + harmonics (smooth seasonal cycle that
#            extrapolates to any calendar date — solves the non-overlapping
#            regime-windows issue that defeats month FE)
#          - δ·t: residual linear trend
#          - X_t: structural covariates (CO2, RES installed GW, reservoir)
#          - HAC standard errors (Newey-West, maxlags=14)
#
#        Idea borrowed from the Weron-Nowotarski LTSC literature (Fourier as
#        the seasonal basis); functional form is the standard FWL regression
#        used across applied econometrics. The point of this script is
#        DESCRIPTIVE adjusted comparison, not causal estimation.
#
# FEEDS: descriptive_facts.tex (only if diagnostics pass).
# OUT: results/regressions/bid/bidshape_seasonality_adjusted/
#        per_tech_coefs.csv
#        per_tech_diagnostics.csv

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm
from statsmodels.stats.diagnostic import acorr_ljungbox

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/bid/bidshape_seasonality_adjusted"

START = "2022-01-01"
END = "2026-05-15"
K_HARMONICS = 4

REGIME_DATES = [
    ("3sess",      pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",   pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",  pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]
# Baseline = pre-IDA (anything before 2024-06-14 in the 2022+ window).

RES_GW_ANNUAL = {2022: 48.6, 2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0}


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
    return pd.DataFrame({
        **{f"cos_{k}": np.cos(2 * np.pi * k * doy / 365.25) for k in range(1, K + 1)},
        **{f"sin_{k}": np.sin(2 * np.pi * k * doy / 365.25) for k in range(1, K + 1)},
    })


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
    for yr in range(2022, 2027):
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
    for label, lo, hi in REGIME_DATES:
        df[f"D_{label}"] = ((df["d"] >= lo) & (df["d"] <= hi)).astype(float)
    fk = fourier_terms(df["doy"].values, K_HARMONICS)
    return pd.concat([df.reset_index(drop=True), fk.reset_index(drop=True)], axis=1)


def build_bidshape() -> pd.DataFrame:
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


def estimate_per_tech(df_t: pd.DataFrame, tech: str) -> dict:
    fourier_cols = [f"{p}_{k}" for k in range(1, K_HARMONICS + 1) for p in ("cos", "sin")]
    regime_cols = [f"D_{r[0]}" for r in REGIME_DATES]
    ctrl_cols = ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    cols = regime_cols + fourier_cols + ctrl_cols

    df_t = df_t.copy()
    p = df_t["in_band_share"].clip(0.001, 0.999).astype(float)
    df_t["y_logit"] = np.log(p / (1 - p))
    df_t = df_t.dropna(subset=cols + ["y_logit"]).reset_index(drop=True)
    if len(df_t) < 300:
        return {"tech": tech, "skip": True, "n": len(df_t)}

    X = sm.add_constant(df_t[cols].astype(float))
    y = df_t["y_logit"].astype(float)
    fit = sm.OLS(y, X).fit(cov_type="HAC", cov_kwds={"maxlags": 14})

    # Diagnostics
    resid = y - fit.predict(X)
    lb = acorr_ljungbox(resid, lags=[5, 10, 20, 30], return_df=True)

    # Convert regime coefficients to "average pp share differential at mean
    # covariates + mean doy" (back-transformed).
    def sig(z): return 1 / (1 + np.exp(-z))

    means = df_t[fourier_cols + ctrl_cols].mean()
    base_logit = fit.params["const"] + sum(fit.params[c] * means[c] for c in fourier_cols + ctrl_cols)
    base_share = sig(base_logit)
    # The regime's expected share = sig(base_logit + β_r)
    regime_effects = {}
    for r_col in regime_cols:
        beta = fit.params.get(r_col, np.nan)
        se = fit.bse.get(r_col, np.nan)
        pval = fit.pvalues.get(r_col, np.nan)
        share_regime = sig(base_logit + beta)
        regime_effects[r_col] = {
            "beta_logit": float(beta),
            "se_logit": float(se),
            "pval": float(pval),
            "base_share": float(base_share),
            "regime_share_at_avg_cov_doy": float(share_regime),
            "share_diff_pp": float((share_regime - base_share) * 100),
        }

    return {
        "tech": tech,
        "n": len(df_t),
        "R2": float(fit.rsquared),
        "lb_p_lag10": float(lb["lb_pvalue"].iloc[1]),
        "lb_p_lag30": float(lb["lb_pvalue"].iloc[3]),
        "base_share_at_avg": float(base_share),
        "regime_effects": regime_effects,
        "fourier_amp_4": float(np.sqrt(fit.params["cos_4"] ** 2 + fit.params["sin_4"] ** 2)),
        "fourier_amp_1": float(np.sqrt(fit.params["cos_1"] ** 2 + fit.params["sin_1"] ** 2)),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Building daily-tech bid-shape, {START} to {END}...")
    bs = build_bidshape()
    bs = add_covariates(bs)
    print(f"  rows: {len(bs):,}, days: {bs['d'].nunique()}, techs: {bs['tech'].nunique()}")

    focus = ["CCGT", "Wind", "Solar_PV", "Hydro", "Hydro_pump", "Nuclear"]
    coef_rows = []
    diag_rows = []
    for tech in focus:
        df_t = bs[bs["tech"] == tech]
        res = estimate_per_tech(df_t, tech)
        if res.get("skip"):
            print(f"\n{tech}: skipped (n={res['n']})")
            continue
        print(f"\n=== {tech} (n={res['n']}, R²={res['R2']:.3f}, LB p@10={res['lb_p_lag10']:.3f}) ===")
        print(f"  Fourier amps logit: k=1 {res['fourier_amp_1']:.3f}, k=4 {res['fourier_amp_4']:.3f}")
        print(f"  Base share (at avg cov + avg doy): {res['base_share_at_avg']:.3f}")
        for r_col, eff in res["regime_effects"].items():
            sig_flag = "***" if eff["pval"] < 0.01 else ("**" if eff["pval"] < 0.05 else ("*" if eff["pval"] < 0.10 else ""))
            print(f"    {r_col:<22} β_logit = {eff['beta_logit']:+.3f} (HAC SE {eff['se_logit']:.3f}, p={eff['pval']:.3f}) {sig_flag}"
                  f"   →  share = {eff['regime_share_at_avg_cov_doy']:.3f}  (Δ vs base = {eff['share_diff_pp']:+.1f} pp)")
            coef_rows.append({
                "tech": tech, "regime": r_col[2:],  # drop D_
                "beta_logit": eff["beta_logit"], "se_logit": eff["se_logit"], "pval": eff["pval"],
                "base_share": eff["base_share"],
                "regime_share": eff["regime_share_at_avg_cov_doy"],
                "share_diff_pp": eff["share_diff_pp"],
            })
        diag_rows.append({
            "tech": tech, "n": res["n"], "R2": res["R2"],
            "lb_p_lag10": res["lb_p_lag10"], "lb_p_lag30": res["lb_p_lag30"],
            "amp_1_logit": res["fourier_amp_1"], "amp_4_logit": res["fourier_amp_4"],
        })

    pd.DataFrame(coef_rows).to_csv(OUT_DIR / "per_tech_coefs.csv", index=False)
    pd.DataFrame(diag_rows).to_csv(OUT_DIR / "per_tech_diagnostics.csv", index=False)
    print(f"\nwrote {OUT_DIR}/")


if __name__ == "__main__":
    main()
