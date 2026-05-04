# STATUS: ALIVE
# LAST-AUDIT: 2026-05-05
# FEEDS: Pre-IDA baseline-window sensitivity for q_1 and q_2 critical-hours DiD
# CLAIM: The +83 q_2 critical × DA15/ID15 coefficient is robust to narrowing
#        the pre-IDA baseline to recent observations.
"""Pre-IDA baseline-window sensitivity test for q_1 and q_2 critical-hours DiD.

The default DiD spec uses 6.5 years of pre-IDA data (2018-01 → 2024-06-13)
as the baseline. That window includes the 2022-2023 energy crisis, the
COVID period, and pre-COVID conditions — quite different counterfactual
environments. This test rerunns the DiD with progressively narrower pre-IDA
windows to test whether the coefficient survives.

Outcomes (each in MWh per firm-day-hour):
  q_1 = SUM(assigned_power_mw × mtu_minutes/60) per Big-4 firm-day-hour
        from PDBCE rows where offer_type=1 (auction-cleared sell)
  q_2 = SUM(assigned_power_mw × mtu_minutes/60) per Big-4 firm-day-hour
        from PIBCIE rows (signed IDA repositioning)

Specification (each baseline window):
  outcome ~ critical + post + crit×post + firm FE + DOW FE,
           cluster-robust SEs by date.

Baseline windows tested:
  full        : 2018-01-01 → 2024-06-13  (6.5 years, default)
  excl_crisis : 2018-01 → 2021-12 ∪ 2024-01 → 2024-06  (excludes 2022-23 crisis)
  recent_2y   : 2022-06-14 → 2024-06-13  (2 years immediately pre-IDA)
  recent_1y   : 2023-06-14 → 2024-06-13  (1 year immediately pre-IDA)
  recent_6m   : 2023-12-14 → 2024-06-13  (6 months immediately pre-IDA)

If the +83 q_2 coefficient drops substantially under recent-only baselines,
then deep pre-IDA history is providing more "lift" than is appropriate for
a clean reform-attributable interpretation. If it survives, the +83 is robust
to the choice of pre-window.

Output:
  results/regressions/critical_hours_baseline_sensitivity.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"

OUT_DIR_R = PROJECT / "results" / "regressions"

CRITICAL_HOURS = [7, 8, 16, 17, 18]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")
PRE_IDA_END    = pd.Timestamp("2024-06-14")
DA15_START     = pd.Timestamp("2025-10-01")

# Baseline windows: list of (label, list of (start, end_exclusive) tuples)
BASELINES = [
    ("full",        [(pd.Timestamp("2018-01-01"), PRE_IDA_END)]),
    ("excl_crisis", [(pd.Timestamp("2018-01-01"), pd.Timestamp("2022-01-01")),
                     (pd.Timestamp("2024-01-01"), PRE_IDA_END)]),
    ("recent_2y",   [(pd.Timestamp("2022-06-14"), PRE_IDA_END)]),
    ("recent_1y",   [(pd.Timestamp("2023-06-14"), PRE_IDA_END)]),
    ("recent_6m",   [(pd.Timestamp("2023-12-14"), PRE_IDA_END)]),
]


def assign_hour(date_series, period_series, mtu15_cutoff):
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def fit_did(panel, outcome="q"):
    cols = {"const": np.ones(len(panel))}
    cols["critical"] = panel["critical"].values.astype(float)
    cols["post"] = panel["post"].values.astype(float)
    cols["crit×post"] = (panel["critical"] * panel["post"]).values.astype(float)
    for f in sorted(panel["firm"].unique())[1:]:
        cols[f"firm_{f}"] = (panel["firm"] == f).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    return dict(coef=float(m.params[X.columns.get_loc("crit×post")]),
                se=float(m.bse[X.columns.get_loc("crit×post")]),
                pval=float(m.pvalues[X.columns.get_loc("crit×post")]),
                n=len(panel),
                n_clusters=int(np.unique(cluster).size))


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # q_1 panel
    q1 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(CASE WHEN offer_type = 1 AND assigned_power_mw > 0
                        THEN assigned_power_mw * mtu_minutes / 60.0 ELSE 0 END) AS q
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q1["date"] = pd.to_datetime(q1["date"])
    q1["hour"] = assign_hour(q1["date"], q1["period"], MTU15_DA_DATE)
    q1_h = q1.groupby(["date","firm","hour"], as_index=False)["q"].sum()

    # q_2 panel
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])
    q2["hour"] = assign_hour(q2["date"], q2["period"], MTU15_IDA_DATE)
    q2_h = q2.groupby(["date","firm","hour"], as_index=False)["q"].sum()

    rows = []
    for label, panel in [("q_1 (PDBCE auction)", q1_h), ("q_2 (PIBCIE IDA)", q2_h)]:
        panel = panel.copy()
        panel["critical"] = panel["hour"].isin(CRITICAL_HOURS).astype(int)
        panel["dow"] = panel["date"].dt.dayofweek

        post = panel[panel["date"] >= DA15_START].assign(post=1)
        n_post = len(post)
        print()
        print("=" * 110)
        print(f"Baseline-window sensitivity for {label}")
        print(f"DA15/ID15 post sample (Oct 2025+): N = {n_post:,} firm-day-hour rows")
        print("=" * 110)
        print(f"  {'baseline':14s} {'months':>9s} {'N_pre':>10s} {'δ_crit×post':>14s} {'SE':>7s}  {'p':>9s}")

        for bl_label, intervals in BASELINES:
            pre = pd.concat([
                panel[(panel["date"] >= s) & (panel["date"] < e)]
                for s, e in intervals
            ]).assign(post=0)
            if len(pre) < 100:
                print(f"  {bl_label:14s} (too few pre rows: {len(pre)})")
                continue
            full = pd.concat([pre, post], ignore_index=True)
            res = fit_did(full)
            n_months = sum((e - s).days / 30 for s, e in intervals)
            print(f"  {bl_label:14s} {n_months:>9.1f} {len(pre):>10,} {res['coef']:+14.2f} {res['se']:>7.2f}  {res['pval']:>9.2e}")
            rows.append({"outcome": label, "baseline": bl_label,
                         "n_months": n_months,
                         "delta": res["coef"], "se": res["se"], "pval": res["pval"],
                         "n_pre": len(pre), "n_post": n_post})

    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_baseline_sensitivity.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_baseline_sensitivity.csv'}")


if __name__ == "__main__":
    main()
