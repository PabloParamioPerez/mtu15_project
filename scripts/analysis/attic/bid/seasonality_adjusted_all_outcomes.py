# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Seasonality-adjusted descriptive regime comparison APPLIED PER
#        OUTCOME VARIABLE. Uses the shared SA helper (src/mtu/analysis/sa_fwl.py):
#
#          g(y_t) = α + Σ_r β_r·D_regime_r(t)
#                     + Σ_{k=1..K} [a_k cos(2π k·doy/365) + b_k sin(...)]
#                     + Σ_{j=1..6} δ_j·1{dow(t)=j}
#                     + γ'·X_t + ε_t
#
#        with the link g(·) chosen by outcome (log / logit / identity).
#        SA value = Duan-smeared inverse link of (α + β_r) at within-week DOW
#        mean, annual-mean Fourier, panel-mean controls. No HAC: the spec is
#        a point-estimation device for the SA values, not for inference on
#        regime contrasts.
#
# OUTCOMES COVERED (one regression per (outcome, tech-if-applicable)):
#   1. Bid-shape: in-band share, per tech (CCGT, Wind, Solar_PV, Hydro,
#      Hydro_pump, Nuclear) [logit]
#   2. DA price (€/MWh) [log]
#   3. Fase I cost (€/day, ESIOS 1373) [log]
#   4. TR up cost (€/day, ESIOS 723 × volume) [log]
#   5. aFRR up energy price (€/MWh) [log]
#   6. System imbalance |MWh|/day (ESIOS 762) [log]
#   7. CCGT post-DA gap (PHF − PDBC, GWh/day) [identity, can be negative]
#   8. CCGT DA-auction-cleared volume (GWh/day) [log]
#
# Diagnostics per outcome: R², Ljung-Box residual ACF, n.
# OUTPUT:
#   results/regressions/bid/seasonality_adjusted/
#     all_outcomes_coefs.csv      — long format (outcome, tech, regime, β, SE, p, Δ vs base)
#     all_outcomes_diagnostics.csv

from __future__ import annotations
from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
REPO_FOR_IMPORT = Path(__file__).resolve().parents[3]
if str(REPO_FOR_IMPORT / "src") not in sys.path:
    sys.path.insert(0, str(REPO_FOR_IMPORT / "src"))
from mtu.analysis.sa_fwl import fit_sa, attach_design_columns  # noqa: E402

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PHF = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/phf_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/bid/seasonality_adjusted"

START = "2022-01-01"
END = "2026-05-15"
K_HARMONICS = 4

REGIME_DATES = [
    ("3sess",          pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",       pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",   pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post",  pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",      pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]
RES_GW_ANNUAL = {2022: 48.6, 2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0}


def tech_bucket(t):
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


def covariate_panel():
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
    res_annual = {yr: (cap_res[yr] if yr in cap_res.index and cap_res[yr] > 0 else RES_GW_ANNUAL.get(yr, np.nan)) for yr in range(2022, 2027)}
    dates = pd.date_range(START, END, freq="D")
    base = pd.DataFrame({"d": dates})
    base["res_gw"] = base["d"].apply(
        lambda x: res_annual.get(x.year, np.nan)
        + (res_annual.get(x.year + 1, res_annual.get(x.year, 0)) - res_annual.get(x.year, 0))
        * (x.dayofyear - 1) / 365.0
    )
    res_w = pd.read_parquet(RESERVOIR)
    res_w["d"] = pd.to_datetime(res_w["week_start"]).dt.date.astype("datetime64[ns]")
    res_w = res_w[["d", "reservoir_twh"]].drop_duplicates(subset="d").set_index("d").sort_index().asfreq("D").ffill().reset_index()
    base = base.merge(co2, on="d", how="left").merge(res_w[["d", "reservoir_twh"]], on="d", how="left")
    return attach_design_columns(base, REGIME_DATES, K=K_HARMONICS)


# ===========================================================================
# Outcome builders — each returns a long DataFrame with columns
#   (d, [tech], value), and a transform tag.
# ===========================================================================

def build_bidshape_per_tech():
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
      FROM read_parquet('{CAB}') WHERE buy_sell = 'V' AND date >= '{START}' AND date <= '{END}'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period, price_eur_mwh AS p,
             quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu_minutes
      FROM read_parquet('{DET}') WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}') WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    joined AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, mp.p_clear,
             (dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H})::INT AS in_band,
             COALESCE(mp.mtu_p, dv.mtu_minutes) AS mtu_minutes
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
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
    SELECT d, tech, mw_in/NULLIF(mw_total,0) AS value
    FROM per_tech WHERE mw_total > 0 ORDER BY d, tech
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_da_price():
    con = duckdb.connect()
    df = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, AVG(price_es_eur_mwh) AS value
    FROM read_parquet('{MPDBC}')
    WHERE date BETWEEN '{START}' AND '{END}'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_esios_daily(indicator_id: int, agg: str = "sum"):
    con = duckdb.connect()
    if agg == "sum":
        aggf = "SUM(value)"
    elif agg == "mean":
        aggf = "AVG(value)"
    elif agg == "abs_sum":
        aggf = "SUM(ABS(value))"
    else:
        raise ValueError(agg)
    df = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, {aggf} AS value
    FROM '{IND}' WHERE indicator_id = {indicator_id}
      AND date BETWEEN '{START}' AND '{END}'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_ccgt_post_da_gap():
    """PHF (last session) − PDBC per (unit, period) summed to CCGT daily total GWh."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])
    sql = f"""
    WITH pdbc AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             assigned_power_mw AS pdbc_mw, COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{PDBC}')
      WHERE date >= '{START}' AND date <= '{END}'
    ),
    phf_last AS (
      SELECT CAST(date AS DATE) AS d, period, unit_code,
             MAX(session_number) AS last_session,
             ANY_VALUE(mtu_minutes) AS mtu
      FROM read_parquet('{PHF}')
      WHERE date >= '{START}' AND date <= '{END}'
      GROUP BY 1, 2, 3
    ),
    phf AS (
      SELECT CAST(p.date AS DATE) AS d, p.period, p.unit_code,
             p.assigned_power_mw AS phf_mw
      FROM read_parquet('{PHF}') p
        JOIN phf_last pl ON CAST(p.date AS DATE)=pl.d AND p.period=pl.period
                          AND p.unit_code=pl.unit_code AND p.session_number=pl.last_session
      WHERE p.date >= '{START}' AND p.date <= '{END}'
    ),
    j AS (
      SELECT COALESCE(pdbc.d, phf.d) AS d,
             COALESCE(pdbc.period, phf.period) AS period,
             COALESCE(pdbc.unit_code, phf.unit_code) AS unit_code,
             COALESCE(pdbc.pdbc_mw, 0) AS pdbc_mw,
             COALESCE(phf.phf_mw, 0) AS phf_mw,
             COALESCE(pdbc.mtu, 60) AS mtu
      FROM pdbc FULL OUTER JOIN phf
        ON pdbc.d=phf.d AND pdbc.period=phf.period AND pdbc.unit_code=phf.unit_code
    )
    SELECT j.d,
           SUM((j.phf_mw - j.pdbc_mw) * j.mtu/60.0) / 1000.0 AS value
    FROM j JOIN u ON j.unit_code = u.unit_code
    WHERE u.tech = 'CCGT'
    GROUP BY 1 ORDER BY 1
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_ccgt_da_cleared():
    """CCGT total auction-cleared MWh per day."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)[["unit_code", "technology"]]
    units["tech"] = units["technology"].apply(tech_bucket)
    con.register("u", units[["unit_code", "tech"]])
    sql = f"""
    SELECT CAST(date AS DATE) AS d,
           SUM(assigned_power_mw * COALESCE(mtu_minutes, 60)/60.0) / 1000.0 AS value
    FROM read_parquet('{PDBC}') p
      JOIN u ON p.unit_code = u.unit_code
    WHERE date >= '{START}' AND date <= '{END}'
      AND u.tech = 'CCGT'
      AND assigned_power_mw > 0
    GROUP BY 1 ORDER BY 1
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


# ===========================================================================
# Estimator: FWL with regime dummies + Fourier + trend + covariates
# ===========================================================================

def fit_outcome(df_y, cov, outcome_name, tech_label, transform):
    """Run TWO specs side-by-side via the shared SA helper:
       (A) Regime + Fourier + DOW (seasonality-only)
       (B) (A) + RES_GW + reservoir_twh (slow-moving structural controls)

    SA value = Duan-smeared inverse link of (alpha + beta_r) at within-week
    DOW mean, annual-mean Fourier, panel-mean extras. Ljung-Box is reported as
    a residual-autocorrelation diagnostic, not used to set SEs.
    """
    df = df_y.merge(cov, on="d", how="inner").reset_index(drop=True)
    rows = []
    for spec_label, extras in [
        ("A_seasonality_only", []),
        ("B_with_controls",    ["res_gw", "reservoir_twh"]),
    ]:
        res = fit_sa(df, "value", REGIME_DATES, transform=transform, K=K_HARMONICS,
                     extra_cols=extras, min_obs=200)
        if res is None:
            return None
        base_level = res["baseline_sa"]
        for label, _, _ in REGIME_DATES:
            sa = res[f"{label}_sa"]
            b = res[f"{label}_beta"]
            pv = res[f"{label}_p"]
            if transform == "logit":
                diff_val = (sa - base_level) * 100
                diff_label = "pp"
            elif transform == "log":
                diff_val = (sa / base_level - 1.0) * 100 if base_level else np.nan
                diff_label = "%"
            else:
                diff_val = sa - base_level
                diff_label = "abs"
            rows.append({
                "outcome": outcome_name, "tech": tech_label, "regime": label,
                "spec": spec_label, "transform": transform,
                "n": res["n"], "R2": res["R2"],
                "beta": b, "pval": pv,
                "base_level": base_level, "regime_level": sa,
                "diff": diff_val, "diff_label": diff_label,
            })
    return rows


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Building covariate panel...")
    cov = covariate_panel()
    print(f"  {len(cov):,} days")

    print("\n--- Building outcomes ---")
    print("1/8 bid-shape per tech...")
    bs = build_bidshape_per_tech()
    print(f"   {len(bs):,} tech-days")
    print("2/8 DA price...")
    da_p = build_da_price()
    print("3/8 Fase I cost (ESIOS 1373, daily sum)...")
    fase1 = build_esios_daily(1373, "sum")
    print("4/8 TR up cost ESIOS 1723...")
    tr_cost = build_esios_daily(1723, "sum")
    print("5/8 aFRR up price (id 634)...")
    afrr_price = build_esios_daily(634, "mean")
    print("6/8 System imbalance |MWh|/day (id 762)...")
    imb = build_esios_daily(762, "abs_sum")
    print("7/8 CCGT post-DA gap GWh/day (PHF-PDBC)...")
    gap = build_ccgt_post_da_gap()
    print("8/8 CCGT auction-cleared GWh/day (PDBC)...")
    ccgt_clear = build_ccgt_da_cleared()

    all_rows = []

    # 1. Bid-shape per tech
    focus_techs = ["CCGT", "Wind", "Solar_PV", "Hydro", "Hydro_pump", "Nuclear"]
    for tech in focus_techs:
        df_t = bs[bs["tech"] == tech][["d", "value"]]
        out = fit_outcome(df_t, cov, "bidshape_in_band_share", tech, "logit")
        if out: all_rows.extend(out)

    # 2-8: system-wide outcomes
    for name, df_o, transform in [
        ("DA_price",         da_p,        "log"),
        ("Fase1_cost",       fase1,       "log"),
        ("TR_up_cost",       tr_cost,     "log"),
        ("aFRR_up_price",    afrr_price,  "log"),
        ("sys_imbalance",    imb,         "log"),
        ("CCGT_post_DA_gap", gap,         "identity"),
        ("CCGT_DA_cleared",  ccgt_clear,  "log"),
    ]:
        out = fit_outcome(df_o, cov, name, "system", transform)
        if out: all_rows.extend(out)

    out_df = pd.DataFrame(all_rows)
    out_df.to_csv(OUT_DIR / "all_outcomes_coefs.csv", index=False)

    # Compact diagnostic per (outcome, tech)
    diag = (
        out_df.groupby(["outcome", "tech"], observed=True)
        .agg(n=("n", "first"), R2=("R2", "first"),
             transform=("transform", "first"))
        .reset_index()
    )
    diag.to_csv(OUT_DIR / "all_outcomes_diagnostics.csv", index=False)

    # Print compact summary — one row per (outcome, tech, spec)
    print("\n" + "=" * 110)
    print(f"{'OUTCOME':<24} {'TECH':<10} {'SPEC':<22} {'R²':>5}  REGIME EFFECTS (Δ vs pre-IDA baseline)")
    print("=" * 110)
    for (out_name, tech, spec), grp in out_df.groupby(["outcome", "tech", "spec"]):
        r2 = grp["R2"].iloc[0]
        diff_label = grp["diff_label"].iloc[0]
        parts = []
        for _, r in grp.iterrows():
            star = "***" if r["pval"] < 0.01 else "**" if r["pval"] < 0.05 else "*" if r["pval"] < 0.10 else ""
            parts.append(f"{r['regime'][:9]}={r['diff']:+6.2f}{diff_label}{star}")
        print(f"{out_name:<24} {tech:<10} {spec:<22} {r2:>5.3f}  {'  '.join(parts)}")

    print(f"\nwrote {OUT_DIR}/")


if __name__ == "__main__":
    main()
