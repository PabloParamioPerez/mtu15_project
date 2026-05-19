# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Pilot — Fourier seasonal decomposition of per-tech daily bid-shape
#        metrics. Backfills the bid-shape computation directly from
#        cab+det+marginalpdbc parquets for 2022-01-01 onward (3.5+ years
#        pre-IDA available; uses Jun 2024 as the IDA-reform cutoff). For
#        each (date, tech) computes the in-band MW share around MCP, then
#        fits Fourier(K=4) on pre-IDA only with structural-state controls,
#        projects to DA15/ID15.
# FEEDS: EXPLORATORY — not in descriptive_facts.tex.
# OUT: results/regressions/bid/fourier_bidshape/
#        per_tech_daily.parquet       daily-tech bid-shape series
#        per_tech_summary.csv         Fourier coefficients + DA15 residual
#
# Approach: daily-tech in-band share = SUM(mw_in_band * weight) / SUM(mw * weight)
# where weight = mtu_minutes/60 (handles MTU15 vs MTU60 in the bid stack).

from __future__ import annotations
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/bid/fourier_bidshape"

START = "2022-01-01"
END = "2026-05-15"
PRE_IDA_END = pd.Timestamp("2024-06-13")
DA15_ID15_START = pd.Timestamp("2025-10-01")
DA15_ID15_END = pd.Timestamp("2026-05-15")
H = 50.0  # band half-width EUR/MWh
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


def build_daily_tech() -> pd.DataFrame:
    print(f"Building daily-tech bid-shape series, {START} to {END}...")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")

    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])

    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}')
      WHERE buy_sell = 'V'
        AND date >= '{START}' AND date <= '{END}'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn = 1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period, price_eur_mwh AS p,
             quantity_mw AS q,
             COALESCE(mtu_minutes, 60) AS mtu_minutes
      FROM read_parquet('{DET}')
      WHERE date >= '{START}' AND date <= '{END}'
        AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period,
             price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}')
      WHERE date >= '{START}' AND date <= '{END}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.p, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu_minutes) AS mtu_minutes
      FROM det dv
        JOIN cab_l c ON dv.d = c.d AND dv.offer_code = c.offer_code
        JOIN mp ON mp.d = dv.d AND mp.period = dv.period
    ),
    per_cell AS (
      SELECT d, period, unit_code,
             SUM(q * mtu_minutes/60.0)                    AS mw_total_mwh,
             SUM(q * mtu_minutes/60.0 * in_band)          AS mw_in_band_mwh,
             ANY_VALUE(mtu_minutes) AS mtu_minutes
      FROM joined
      GROUP BY 1, 2, 3
    ),
    per_tech AS (
      SELECT pc.d, u.tech,
             SUM(pc.mw_total_mwh)    AS mw_total_mwh,
             SUM(pc.mw_in_band_mwh)  AS mw_in_band_mwh
      FROM per_cell pc JOIN u ON pc.unit_code = u.unit_code
      GROUP BY 1, 2
    )
    SELECT d, tech,
           mw_in_band_mwh / NULLIF(mw_total_mwh, 0) AS in_band_share,
           mw_total_mwh,
           mw_in_band_mwh
    FROM per_tech
    WHERE mw_total_mwh > 0
    ORDER BY d, tech
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    print(f"  daily-tech panel: {len(df):,} rows, {df['d'].nunique()} days, {df['tech'].nunique()} techs")
    print(df.groupby("tech")["in_band_share"].describe()[["count", "mean", "std"]].round(3).to_string())
    return df


def add_covariates(df: pd.DataFrame) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    # CO2 monthly
    co2 = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, value AS co2_eur_t
    FROM '{IND}' WHERE indicator_id = 1391
      AND date BETWEEN '{START}' AND '{END}' ORDER BY 1
    """).fetchdf()
    co2["d"] = pd.to_datetime(co2["d"])
    co2 = co2.set_index("d").asfreq("D").ffill().reset_index()

    # RES capacity annual -> daily linear interp
    cap = pd.read_parquet(RES_CAP)
    res_psr = ["B01", "B11", "B15", "B16", "B17", "B19", "B25"]
    cap_res = cap[cap["psr_type"].isin(res_psr)].groupby("year")["capacity_mw"].sum() / 1000
    res_annual = {}
    for yr in range(2018, 2027):
        res_annual[yr] = cap_res[yr] if yr in cap_res.index and cap_res[yr] > 0 else RES_GW_ANNUAL.get(yr, np.nan)

    # Reservoir
    res_w = pd.read_parquet(RESERVOIR)
    res_w["d"] = pd.to_datetime(res_w["week_start"]).dt.date.astype("datetime64[ns]")
    res_w = res_w[["d", "reservoir_twh"]].drop_duplicates(subset="d").set_index("d").sort_index().asfreq("D").ffill().reset_index()

    df = df.copy()
    df["res_gw"] = df["d"].apply(
        lambda x: res_annual.get(x.year, np.nan)
        + (res_annual.get(x.year + 1, res_annual.get(x.year, 0)) - res_annual.get(x.year, 0))
        * (x.dayofyear - 1) / 365.25
    )
    df = df.merge(co2, on="d", how="left").merge(res_w[["d", "reservoir_twh"]], on="d", how="left")
    df["doy"] = df["d"].dt.dayofyear
    df["t_yr"] = (df["d"] - pd.Timestamp(START)).dt.days / 365.25
    fk = fourier_terms(df["doy"].values, K_HARMONICS)
    return pd.concat([df.reset_index(drop=True), fk.reset_index(drop=True)], axis=1)


def fit_one_tech(df_t: pd.DataFrame, tech: str) -> dict:
    """Fit BOTH linear-OLS and logit-linear; report side-by-side."""
    fourier_cols = [c for c in df_t.columns if c.startswith(("cos_", "sin_"))]
    ctrl_cols = ["co2_eur_t", "res_gw", "reservoir_twh", "t_yr"]
    cols = fourier_cols + ctrl_cols

    df_t = df_t.copy()
    df_t["y_lin"] = df_t["in_band_share"].astype(float)
    p = df_t["in_band_share"].clip(0.001, 0.999).astype(float)
    df_t["y_logit"] = np.log(p / (1 - p))
    df_t = df_t.dropna(subset=cols + ["y_lin", "y_logit"])

    pre = df_t[df_t["d"] < PRE_IDA_END]
    da15 = df_t[(df_t["d"] >= DA15_ID15_START) & (df_t["d"] <= DA15_ID15_END)]
    if len(pre) < 200 or len(da15) < 30:
        return {"tech": tech, "skip": True, "n_pre": len(pre), "n_da15": len(da15)}

    X = sm.add_constant(pre[cols].astype(float))
    fit_lin = sm.OLS(pre["y_lin"].astype(float), X).fit(cov_type="HAC", cov_kwds={"maxlags": 14})
    fit_log = sm.OLS(pre["y_logit"].astype(float), X).fit(cov_type="HAC", cov_kwds={"maxlags": 14})

    pre_oct_may = pre[pre["d"].dt.month.isin([10, 11, 12, 1, 2, 3, 4, 5])]
    cal_match = pre_oct_may["in_band_share"].mean()

    pre_means = pre[ctrl_cols].mean()
    da15_ref = da15.copy()
    for c in ctrl_cols:
        da15_ref[c] = pre_means[c]
    Xda_ref = sm.add_constant(da15_ref[cols].astype(float), has_constant="add")
    Xda_full = sm.add_constant(da15[cols].astype(float), has_constant="add")

    def sig(x): return 1 / (1 + np.exp(-x))

    yhat_seasonal_lin = fit_lin.predict(Xda_ref)
    yhat_full_lin = fit_lin.predict(Xda_full)
    yhat_seasonal_log = sig(fit_log.predict(Xda_ref))
    yhat_full_log = sig(fit_log.predict(Xda_full))

    # Track LPM boundary violations
    n_below_lin = int((yhat_full_lin < 0).sum())
    n_above_lin = int((yhat_full_lin > 1).sum())
    obs_mean = da15["y_lin"].mean()

    return {
        "tech": tech,
        "n_pre": len(pre),
        "n_da15": len(da15),
        "R2_lin": fit_lin.rsquared,
        "R2_logit": fit_log.rsquared,
        "obs_share": obs_mean,
        "cal_match_share": cal_match,
        "lin_seasonal_share": yhat_seasonal_lin.mean(),
        "lin_full_share":     yhat_full_lin.mean(),
        "lin_n_oob":          n_below_lin + n_above_lin,
        "logit_seasonal_share": yhat_seasonal_log.mean(),
        "logit_full_share":     yhat_full_log.mean(),
        "resid_lin_seasonal_pp":   (obs_mean - yhat_seasonal_lin.mean()) * 100,
        "resid_lin_full_pp":       (obs_mean - yhat_full_lin.mean()) * 100,
        "resid_logit_seasonal_pp": (obs_mean - yhat_seasonal_log.mean()) * 100,
        "resid_logit_full_pp":     (obs_mean - yhat_full_log.mean()) * 100,
        "lin_amp_4":     np.sqrt(fit_lin.params.get("cos_4", 0) ** 2 + fit_lin.params.get("sin_4", 0) ** 2),
        "logit_amp_4":   np.sqrt(fit_log.params.get("cos_4", 0) ** 2 + fit_log.params.get("sin_4", 0) ** 2),
        "lin_amp_1":     np.sqrt(fit_lin.params.get("cos_1", 0) ** 2 + fit_lin.params.get("sin_1", 0) ** 2),
        "logit_amp_1":   np.sqrt(fit_log.params.get("cos_1", 0) ** 2 + fit_log.params.get("sin_1", 0) ** 2),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df = build_daily_tech()
    df.to_parquet(OUT_DIR / "per_tech_daily.parquet", index=False)
    print(f"\nwrote {OUT_DIR / 'per_tech_daily.parquet'}")

    df = add_covariates(df)

    focus = ["CCGT", "Wind", "Solar_PV", "Hydro", "Nuclear"]
    rows = []
    for tech in focus:
        df_t = df[df["tech"] == tech]
        if len(df_t) == 0:
            continue
        out = fit_one_tech(df_t, tech)
        if out.get("skip"):
            print(f"\n=== {tech}: skip (n_pre={out['n_pre']}, n_da15={out['n_da15']}) ===")
            continue
        print(f"\n=== {tech}  (n_pre={out['n_pre']}, n_da15={out['n_da15']}) ===")
        print(f"  R² linear={out['R2_lin']:.3f}  R² logit={out['R2_logit']:.3f}")
        print(f"  k=1 amp:  linear={out['lin_amp_1']:.4f}  logit={out['logit_amp_1']:.4f}")
        print(f"  k=4 amp (QUARTERLY):  linear={out['lin_amp_4']:.4f}  logit={out['logit_amp_4']:.4f}")
        print(f"  Observed DA15/ID15 share        = {out['obs_share']:.3f}")
        print(f"  Cal-match Oct-May (pre-IDA)     = {out['cal_match_share']:.3f}  -> resid = {(out['obs_share']-out['cal_match_share'])*100:+.1f}pp")
        print(f"  LINEAR OLS Fourier-seasonal     = {out['lin_seasonal_share']:.3f}  -> resid = {out['resid_lin_seasonal_pp']:+.1f}pp")
        print(f"  LINEAR OLS Fourier+ctrls(DA15)  = {out['lin_full_share']:.3f}  -> resid = {out['resid_lin_full_pp']:+.1f}pp  [oob days: {out['lin_n_oob']}/{out['n_da15']}]")
        print(f"  LOGIT      Fourier-seasonal     = {out['logit_seasonal_share']:.3f}  -> resid = {out['resid_logit_seasonal_pp']:+.1f}pp")
        print(f"  LOGIT      Fourier+ctrls(DA15)  = {out['logit_full_share']:.3f}  -> resid = {out['resid_logit_full_pp']:+.1f}pp")
        rows.append(out)

    summary = pd.DataFrame(rows)
    summary.to_csv(OUT_DIR / "per_tech_summary.csv", index=False)
    print(f"\nwrote {OUT_DIR / 'per_tech_summary.csv'}")


if __name__ == "__main__":
    main()
