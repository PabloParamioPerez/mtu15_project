# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Continuous-intraday (XBID) volume analysis with per-quarter
#        resolution, hour-class split, and Fourier-deseasonalization.
#
#        Three outcomes per regime:
#         (1) Total daily CI volume (MWh): gross MWh transacted in CI per day
#         (2) Critical-hour CI volume vs flat-hour CI volume:
#              tests whether CI activity has shifted toward
#              high-within-hour-variance hours (where MTU15 has economic content)
#         (3) Within-hour quarter heterogeneity (post-MTU15-IDA only):
#              std of per-quarter MWh within an hour, capturing offsetting
#              adjustments that AGGREGATE to zero hourly but show real quarter
#              activity. The point is: do NOT aggregate quarters — if firms
#              make mean-zero rebalancing across quarters within an hour, you
#              miss it entirely when summing.
#
# Each outcome → shared SA helper (regime + Fourier K=4 + DOW dummies),
# Duan-smeared inverse-link prediction at (alpha + beta_r). Report raw
# daily mean vs adjusted regime mean.
#
# OUT: results/regressions/bid/ci_volume_per_quarter/{outcomes_summary,by_tech,by_firm}.csv

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
TRADES = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
RES_CAP = REPO / "data/processed/entsoe/generation/installed_capacity_all.parquet"
RESERVOIR = REPO / "data/processed/entsoe/generation/reservoir_filling_es_weekly.parquet"
OUT_DIR = REPO / "results/regressions/bid/ci_volume_per_quarter"

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
HOUR_CLASS = {
    "Critical": [5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22],
    "Flat":     [1, 2, 3],
    "Midday":   [11, 12, 13, 14],
}
RES_GW_ANNUAL = {2022: 48.6, 2023: 56.3, 2024: 61.0, 2025: 65.5, 2026: 67.0}


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    if "re mercado eólica" in t: return "Wind"
    if "re mercado solar fotovolt" in t: return "Solar_PV"
    return "Other"


def firm_bucket(agent):
    if not isinstance(agent, str): return "Other"
    o = agent.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o or "hidrocan" in o: return "HC"
    return "Other"


def hour_class_of(clock_hour):
    for hc, hs in HOUR_CLASS.items():
        if int(clock_hour) in hs:
            return hc
    return None


def build_daily_panel():
    """Per (date, hour_class) and per (date, tech, hour_class) CI volume."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    con.register("u_full", units[["unit_code", "tech", "firm"]])

    # Per-trade: gross MWh = quantity_mw * mtu_minutes/60; classify by clock_hour.
    # DuckDB '/' is real division — use FLOOR((period-1)/4.0)::INT for the MTU15
    # bin, otherwise float clock_hour breaks downstream GROUP BY.
    sql = f"""
    WITH t AS (
      SELECT
        CAST(delivery_date AS DATE) AS d,
        CASE WHEN mtu_minutes = 60 THEN period - 1
             ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
        period, mtu_minutes,
        seller_unit, buyer_unit,
        quantity_mw * mtu_minutes/60.0 AS mwh
      FROM read_parquet('{TRADES}')
      WHERE delivery_date >= '{START}' AND delivery_date <= '{END}'
    )
    SELECT
      d, clock_hour, period, mtu_minutes,
      SUM(mwh) AS gross_mwh,
      SUM(CASE WHEN us.tech = 'CCGT' THEN mwh ELSE 0 END) AS ccgt_seller_mwh,
      SUM(CASE WHEN us.tech = 'Wind' THEN mwh ELSE 0 END) AS wind_seller_mwh,
      SUM(CASE WHEN us.tech = 'Solar_PV' THEN mwh ELSE 0 END) AS solar_seller_mwh,
      SUM(CASE WHEN us.tech = 'Hydro' OR us.tech = 'Hydro_pump' THEN mwh ELSE 0 END) AS hydro_seller_mwh,
      SUM(CASE WHEN us.tech = 'Nuclear' THEN mwh ELSE 0 END) AS nuclear_seller_mwh
    FROM t
      LEFT JOIN u_full us ON t.seller_unit = us.unit_code
    GROUP BY 1, 2, 3, 4
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class_of)

    # === Outcome 1: Total daily gross CI volume ===
    daily_total = df.groupby("d")["gross_mwh"].sum().reset_index()
    daily_total.columns = ["d", "ci_gwh_day"]
    daily_total["ci_gwh_day"] = daily_total["ci_gwh_day"] / 1000

    # === Outcome 2: Per-hour-class daily CI volume ===
    df_hc = df[df["hour_class"].notna()]
    daily_hc = df_hc.groupby(["d", "hour_class"])["gross_mwh"].sum().reset_index()
    daily_hc["ci_gwh_day"] = daily_hc["gross_mwh"] / 1000
    daily_hc = daily_hc[["d", "hour_class", "ci_gwh_day"]]

    # === Outcome 3: Within-hour quarter heterogeneity (post-MTU15-IDA only) ===
    # For each (date, clock_hour) where mtu=15, compute std of mwh across the 4 quarters in the hour
    df15 = df[(df["mtu_minutes"] == 15) & (df["hour_class"].notna())].copy()
    quarter_std = (
        df15.groupby(["d", "clock_hour", "hour_class"])
        .agg(mwh_mean_q=("gross_mwh", "mean"),
             mwh_std_q=("gross_mwh", "std"),
             n_quarters=("gross_mwh", "count"))
        .reset_index()
    )
    # CV-like measure: std/mean per hour (if at least 3 quarters traded)
    quarter_std = quarter_std[quarter_std["n_quarters"] >= 3].copy()
    quarter_std["cv"] = quarter_std["mwh_std_q"] / quarter_std["mwh_mean_q"].clip(lower=0.1)
    # Aggregate to (date, hour_class): mean within-hour quarter std
    daily_qstd = (
        quarter_std.groupby(["d", "hour_class"])
        .agg(within_hour_std_mwh=("mwh_std_q", "mean"),
             within_hour_cv=("cv", "mean"),
             n_hours=("clock_hour", "count"))
        .reset_index()
    )

    # === Outcome 4: Per-tech sells daily volume ===
    daily_tech = df.groupby("d").agg(
        ccgt_gwh=("ccgt_seller_mwh", lambda x: x.sum() / 1000),
        wind_gwh=("wind_seller_mwh", lambda x: x.sum() / 1000),
        solar_gwh=("solar_seller_mwh", lambda x: x.sum() / 1000),
        hydro_gwh=("hydro_seller_mwh", lambda x: x.sum() / 1000),
        nuclear_gwh=("nuclear_seller_mwh", lambda x: x.sum() / 1000),
    ).reset_index()

    return daily_total, daily_hc, daily_qstd, daily_tech


def add_covariates(df):
    df = df.copy()
    df["d"] = pd.to_datetime(df["d"])
    return attach_design_columns(df, REGIME_DATES, K=K_HARMONICS)


def fit_outcome(df, y_col, transform="log"):
    """Spec A SA on a daily series using the shared helper."""
    res = fit_sa(df, y_col, REGIME_DATES, transform=transform, K=K_HARMONICS, min_obs=200)
    if res is None:
        return None
    out = {"R2": res["R2"], "n": res["n"]}
    for r_lab, _, _ in REGIME_DATES:
        out[f"{r_lab}_raw"] = res[f"{r_lab}_raw"]
        out[f"{r_lab}_adj"] = res[f"{r_lab}_sa"]
        out[f"{r_lab}_p"] = res[f"{r_lab}_p"]
    return out


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("Building daily CI panel (per period, then aggregated)...")
    daily_total, daily_hc, daily_qstd, daily_tech = build_daily_panel()
    print(f"  daily_total: {len(daily_total):,} rows")
    print(f"  daily_hc: {len(daily_hc):,} rows ({daily_hc['hour_class'].nunique()} hour-classes)")
    print(f"  daily_qstd: {len(daily_qstd):,} rows (within-hour quarter std)")
    print(f"  daily_tech: {len(daily_tech):,} rows")

    cov_dummy = add_covariates(pd.DataFrame({"d": pd.date_range(START, END, freq="D")}))
    keep = (["d"]
            + [f"cos_{k}" for k in range(1, K_HARMONICS + 1)]
            + [f"sin_{k}" for k in range(1, K_HARMONICS + 1)]
            + [f"dow_{i}" for i in range(1, 7)]
            + [f"D_{r[0]}" for r in REGIME_DATES])
    cov_dummy = cov_dummy[keep]

    out_rows = []

    # Outcome 1: Total daily CI volume
    print("\n=== Outcome 1: Total daily CI volume (GWh/day) ===")
    df1 = daily_total.merge(cov_dummy, on="d", how="left")
    r = fit_outcome(df1, "ci_gwh_day", "log")
    if r:
        print(f"  R² = {r['R2']:.3f}")
        for r_lab, _, _ in REGIME_DATES:
            print(f"    {r_lab:<18} raw {r[f'{r_lab}_raw']:6.1f}   adjusted {r[f'{r_lab}_adj']:6.1f}  (p={r[f'{r_lab}_p']:.3f})")
        out_rows.append({"outcome": "ci_total", "scope": "system", **r})

    # Outcome 2: Critical vs Flat daily CI volume
    print("\n=== Outcome 2: CI volume per hour-class (GWh/day) ===")
    for hc in ["Critical", "Flat"]:
        sub = daily_hc[daily_hc["hour_class"] == hc].merge(cov_dummy, on="d", how="left")
        r = fit_outcome(sub, "ci_gwh_day", "log")
        if r:
            print(f"  -- {hc} --  R² = {r['R2']:.3f}")
            for r_lab, _, _ in REGIME_DATES:
                print(f"    {r_lab:<18} raw {r[f'{r_lab}_raw']:6.2f}   adjusted {r[f'{r_lab}_adj']:6.2f}  (p={r[f'{r_lab}_p']:.3f})")
            out_rows.append({"outcome": f"ci_{hc.lower()}", "scope": "system", **r})

    # Outcome 3: Within-hour quarter STD (post-MTU15-IDA only — only MTU15 has quarter data)
    print("\n=== Outcome 3: Within-hour quarter std (MWh, post-MTU15-IDA only) ===")
    print("  How much CI activity varies across the 4 quarters within an hour")
    print("  — captures offsetting Q1-Q4 adjustments that aggregate to ~0 hourly")
    # Direct per-regime means (no Fourier — only 3 post-MTU15-IDA regimes, sample too short for Fourier)
    daily_qstd["regime"] = "other"
    for r_lab, lo, hi in REGIME_DATES:
        m = (daily_qstd["d"] >= lo) & (daily_qstd["d"] <= hi)
        daily_qstd.loc[m, "regime"] = r_lab
    for hc in ["Critical", "Flat"]:
        sub = daily_qstd[daily_qstd["hour_class"] == hc]
        print(f"  -- {hc} --")
        for r_lab in ["MTU15IDA_pre", "MTU15IDA_post", "DA15_ID15"]:
            rsub = sub[sub["regime"] == r_lab]
            if len(rsub) > 30:
                m_std = rsub["within_hour_std_mwh"].mean()
                m_cv = rsub["within_hour_cv"].mean()
                print(f"    {r_lab:<18} mean within-hour std = {m_std:6.2f} MWh, CV = {m_cv:.2f}, n_days={len(rsub)}")
                out_rows.append({"outcome": f"within_hour_qstd_{hc.lower()}", "scope": "system",
                                 "regime": r_lab, "value_mwh": float(m_std), "cv": float(m_cv), "n_days": len(rsub)})

    # Outcome 4: Per-tech CI volume
    print("\n=== Outcome 4: CI volume per tech (sell-side, GWh/day) ===")
    for tech_col, tech_label in [("ccgt_gwh", "CCGT"), ("wind_gwh", "Wind"),
                                   ("solar_gwh", "Solar PV"), ("hydro_gwh", "Hydro"),
                                   ("nuclear_gwh", "Nuclear")]:
        sub = daily_tech.merge(cov_dummy, on="d", how="left")
        r = fit_outcome(sub, tech_col, "log")
        if r:
            print(f"  -- {tech_label} sell GWh/day --  R² = {r['R2']:.3f}")
            for r_lab, _, _ in REGIME_DATES:
                print(f"    {r_lab:<18} raw {r[f'{r_lab}_raw']:6.2f}   adjusted {r[f'{r_lab}_adj']:6.2f}  (p={r[f'{r_lab}_p']:.3f})")
            out_rows.append({"outcome": f"ci_sell_{tech_label}", "scope": "tech", **r})

    # === Outcome 5: per-firm within-hour quarter heterogeneity (post-MTU15-IDA only) ===
    # For each (firm, date, clock_hour, hour_class) compute std of MWh across the 4 quarters
    # the firm participated in. This is the within-hour-rebalancing signature the user flagged.
    print("\n=== Outcome 5: PER-FIRM within-hour quarter heterogeneity (post-MTU15-IDA) ===")
    print("  Per (firm, date, hour) std of MWh across the 4 quarters of that hour, mean over regime.")
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    units = pd.read_csv(UNITS)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    con.register("u_firm", units[["unit_code", "firm"]])
    sql_firm = f"""
    WITH t AS (
      SELECT
        CAST(delivery_date AS DATE) AS d,
        CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
        period, mtu_minutes,
        seller_unit, quantity_mw * mtu_minutes/60.0 AS mwh
      FROM read_parquet('{TRADES}')
      WHERE delivery_date >= '2025-03-19' AND delivery_date <= '{END}'
        AND mtu_minutes = 15
    ),
    per_q AS (
      SELECT t.d, t.clock_hour, t.period, u.firm,
             SUM(t.mwh) AS mwh
      FROM t JOIN u_firm u ON t.seller_unit = u.unit_code
      WHERE u.firm IN ('GN','IB','GE','HC')
      GROUP BY 1, 2, 3, 4
    ),
    per_hour AS (
      SELECT d, clock_hour, firm,
             STDDEV_POP(mwh) AS std_q_mwh, AVG(mwh) AS mean_q_mwh, COUNT(*) AS n_q
      FROM per_q GROUP BY 1, 2, 3
    )
    SELECT d, clock_hour, firm, std_q_mwh, mean_q_mwh, n_q
    FROM per_hour WHERE n_q >= 3
    """
    df_firm = con.execute(sql_firm).fetchdf()
    df_firm["d"] = pd.to_datetime(df_firm["d"])
    df_firm["hour_class"] = df_firm["clock_hour"].apply(hour_class_of)
    df_firm = df_firm[df_firm["hour_class"].notna()].copy()
    df_firm["regime"] = "other"
    for r_lab, lo, hi in REGIME_DATES:
        m = (df_firm["d"] >= lo) & (df_firm["d"] <= hi)
        df_firm.loc[m, "regime"] = r_lab
    # Mean per (firm, regime, hour_class)
    summary = (df_firm.groupby(["firm", "regime", "hour_class"])
               .agg(std_mwh_per_hour=("std_q_mwh", "mean"),
                    mean_mwh_per_q=("mean_q_mwh", "mean"),
                    n_hours=("d", "count")).reset_index())
    summary = summary[summary["n_hours"] >= 50]
    print("\n  Firm × Hour-class × Regime: mean within-hour std of CI MWh across the 4 quarters")
    for hc in ["Critical", "Flat"]:
        print(f"  -- {hc} --")
        sub = summary[summary["hour_class"] == hc].pivot_table(index="firm", columns="regime", values="std_mwh_per_hour")
        for r_lab in ["MTU15IDA_pre", "MTU15IDA_post", "DA15_ID15"]:
            if r_lab in sub.columns:
                pass
        if not sub.empty:
            cols = [c for c in ["MTU15IDA_pre", "MTU15IDA_post", "DA15_ID15"] if c in sub.columns]
            print(sub[cols].round(2).to_string())
    summary.to_csv(OUT_DIR / "per_firm_within_hour_std.csv", index=False)
    print(f"\nwrote {OUT_DIR}/per_firm_within_hour_std.csv")

    pd.DataFrame(out_rows).to_csv(OUT_DIR / "outcomes_summary.csv", index=False)
    print(f"\nwrote {OUT_DIR}/outcomes_summary.csv")


if __name__ == "__main__":
    main()
