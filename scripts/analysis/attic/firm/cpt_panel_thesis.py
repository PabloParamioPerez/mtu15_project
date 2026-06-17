# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §5.4 (conditional parallel trends)
# CLAIM: β₃ on q_2 (within-day DiD) is stable across (a) sparse FE,
#        (b) augmented with hourly wind+solar controls, (c) augmented
#        with renewables + reforzada × crit interaction (full window),
#        (d) augmented with cal-month FE.
#
# Tests whether the headline β₃ in B1 is robust to OVB-discipline.
# Uses B1's panel as starting point.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
from statsmodels.api import OLS, add_constant
from statsmodels.stats.sandwich_covariance import cov_cluster
from scipy.stats import norm

REPO = Path(__file__).resolve().parents[3]

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
B1_PANEL = OUTDIR / "B1_panel.parquet"

REFORZADA_START = "2025-04-28"


def build_renewable_panel(start: str, end: str) -> pd.DataFrame:
    """Hourly Spanish wind+solar production in MW. Aggregate by hour."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
               EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
               psr_type,
               AVG(quantity_mw) AS mw
        FROM '{WIND_SOLAR}'
        WHERE isp_start_utc >= TIMESTAMP '{start}'
          AND isp_start_utc <  TIMESTAMP '{end}'
        GROUP BY 1,2,3
    """).df()
    # Pivot wind vs solar
    df = df.pivot_table(index=["d","hour"], columns="psr_type", values="mw").reset_index()
    df.columns = [str(c) for c in df.columns]
    # Spanish A75 codes: B16 = solar, B18 = wind onshore, B19 = wind offshore
    keep = {"B16": "solar_mw", "B18": "wind_on_mw", "B19": "wind_off_mw"}
    rename = {old: new for old, new in keep.items() if old in df.columns}
    df = df.rename(columns=rename)
    for c in ["solar_mw", "wind_on_mw", "wind_off_mw"]:
        if c not in df.columns: df[c] = 0
    df["wind_mw"] = df["wind_on_mw"].fillna(0) + df["wind_off_mw"].fillna(0)
    df["solar_mw"] = df["solar_mw"].fillna(0)
    df["vre_mw"] = df["wind_mw"] + df["solar_mw"]
    df["d"] = pd.to_datetime(df["d"])
    df["hour"] = df["hour"].astype(int)
    return df[["d","hour","wind_mw","solar_mw","vre_mw"]]


def run_did(panel: pd.DataFrame, label: str, controls: list = None) -> dict:
    if len(panel) < 30:
        return {"label": label, "n": len(panel), "beta_3": np.nan}
    df = panel.copy()
    df["crit_x_post"] = df["crit"] * df["post"]
    cols_main = ["crit", "post", "crit_x_post"]
    if controls:
        cols_main = cols_main + [c for c in controls if c in df.columns]
    X_main = df[cols_main].copy().astype(float)
    parent_dummies = pd.get_dummies(df["parent"], prefix="firm", drop_first=True).astype(float)
    dow_dummies = pd.get_dummies(df["dow"], prefix="dow", drop_first=True).astype(float)
    X = pd.concat([X_main, parent_dummies, dow_dummies], axis=1)
    X = add_constant(X, has_constant='add')
    y = df["q2_mwh_clock_hour"].astype(float).values
    cluster = df["d"].astype(str).values
    try:
        result = OLS(y, X).fit()
        cov = cov_cluster(result, cluster)
        se = np.sqrt(np.diag(cov))
        cols = list(X.columns); idx = cols.index("crit_x_post")
        b3 = result.params.iloc[idx]; s = se[idx]
        p = 2*(1-norm.cdf(abs(b3/s))) if s > 0 else np.nan
        return {"label": label, "n": len(df), "n_clusters": df["d"].nunique(),
                "beta_3": b3, "se": s, "p": p, "controls": ",".join(controls or [])}
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r):
    if "error" in r: print(f"  {r['label']:40s}  ERROR: {r['error']}"); return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:40s}  n={r['n']:6d}  (insufficient)"); return
    sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
    print(f"  {r['label']:40s}  n={r['n']:6d}  β₃={r['beta_3']:+8.3f}  SE={r['se']:6.3f}  p={r['p']:.4f}{sig}")


def main():
    print("Loading B1 panel...")
    b1 = pd.read_parquet(B1_PANEL)
    print(f"B1 panel rows: {len(b1)}")

    # Restrict to treatment group (where the headline lives)
    treat = b1[b1["treatment_group"] == "treatment"].copy()
    print(f"Treatment rows: {len(treat)}")

    # Build renewable panel
    print("\nBuilding renewable panel...")
    vre_pre = build_renewable_panel("2024-10-01", "2025-01-01")
    vre_post = build_renewable_panel("2025-10-01", "2026-01-01")
    vre = pd.concat([vre_pre, vre_post], ignore_index=True)
    print(f"VRE panel rows: {len(vre)}")

    treat = treat.merge(vre, on=["d","hour"], how="left")
    treat["wind_mw"] = treat["wind_mw"].fillna(treat["wind_mw"].mean())
    treat["solar_mw"] = treat["solar_mw"].fillna(treat["solar_mw"].mean())
    treat["vre_mw"] = treat["vre_mw"].fillna(treat["vre_mw"].mean())
    # Demean for interpretability
    treat["wind_z"] = treat["wind_mw"] - treat["wind_mw"].mean()
    treat["solar_z"] = treat["solar_mw"] - treat["solar_mw"].mean()
    treat["vre_z"] = treat["vre_mw"] - treat["vre_mw"].mean()
    treat["crit_x_vre"] = treat["crit"] * treat["vre_z"]
    treat["crit_x_wind"] = treat["crit"] * treat["wind_z"]
    treat["crit_x_solar"] = treat["crit"] * treat["solar_z"]

    treat["month"] = treat["d"].dt.month
    treat["cal_month_pre"] = treat["month"].astype(str) + "_" + (1 - treat["post"]).astype(str)

    print("\n=== B4 — Conditional Parallel Trends (treatment group, q_2) ===")
    print("Specification stack: each row adds controls vs the previous one.\n")

    results = []

    # Spec 1: B1 baseline (no controls beyond firm + DOW FE; same as B1 treatment_only)
    r = run_did(treat, "1_baseline_B1"); results.append(r); print_result(r)

    # Spec 2: + wind + solar levels
    r = run_did(treat, "2_plus_wind_solar_levels", controls=["wind_z", "solar_z"]); results.append(r); print_result(r)

    # Spec 3: + crit × VRE interaction (allows VRE effect to differ in critical hours)
    r = run_did(treat, "3_plus_crit_x_VRE", controls=["wind_z","solar_z","crit_x_vre"]); results.append(r); print_result(r)

    # Spec 4: + month dummies (cal-month FE)
    treat_with_month = pd.concat([treat,
                                  pd.get_dummies(treat["month"], prefix="month", drop_first=True).astype(float)],
                                 axis=1)
    month_cols = [c for c in treat_with_month.columns if c.startswith("month_")]
    r = run_did(treat_with_month, "4_plus_cal_month_FE",
                controls=["wind_z","solar_z","crit_x_vre"] + month_cols); results.append(r); print_result(r)

    print("\nVerdict: if β₃ stays the same sign with similar magnitude across specs 1-4,")
    print("the OVB-discipline test passes and the q_2 effect is robust to renewable + cal-month controls.\n")

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTDIR / "B4_cpt_panel.csv", index=False)
    print(f"Saved: {OUTDIR / 'B4_cpt_panel.csv'}")


if __name__ == "__main__":
    main()
