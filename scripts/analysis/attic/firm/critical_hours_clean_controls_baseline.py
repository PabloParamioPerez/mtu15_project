# STATUS: ALIVE
# LAST-AUDIT: 2026-05-05
# FEEDS: Parallel-trends robustness for q_1 and q_2 with FLAT-hour controls
# CLAIM: Under clean flat-hour controls (h{3,4,5}), the q_2 V-shape is
#        identification-clean: the pre-IDA crit-flat gap is stable across
#        sub-windows, so the +59 DiD δ is reform-attributable.
"""Baseline-window sensitivity with FLAT-hour controls (q_1 + q_2).

Combines two earlier diagnostics:
  - critical_hours_baseline_sensitivity.py (varying pre-IDA windows; but used
    contaminated all-non-critical-hours control)
  - critical_hours_clean_controls.py (flat-hour controls h{3,4,5}; but used
    only full pre-IDA window)

Spec: q ~ critical + post + crit×post + firm FE + DOW FE, cluster by date.
Treated = top-5 critical hours h{7, 8, 16, 17, 18}.
Control = h{3, 4, 5} (flat pre-dawn hours).
Sample = treated ∪ control hours, observations from selected pre-IDA window
         + DA15/ID15 post window.

Pre-IDA sub-windows tested:
  full        : 2018-01 → 2024-06-13 (78.5 mo)
  excl_crisis : 2018-01-2021-12 ∪ 2024-01 → 2024-06 (54.2 mo)
  recent_2y   : 2022-06 → 2024-06 (24.4 mo)
  recent_1y   : 2023-06 → 2024-06 (12.2 mo)
  recent_6m   : 2023-12 → 2024-06 (6.1 mo)

If crit_pre (parallel-trends gap) and DiD δ are stable across sub-windows,
the identification is clean. If they drift, parallel trends fails.

Outcomes:
  q_1 = Big-4 PDBCE auction-cleared sell per firm-day-hour, MWh
  q_2 = Big-4 PIBCIE signed IDA repositioning per firm-day-hour, MWh
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
FLAT_CONTROL   = [3, 4, 5]
MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
MTU15_DA_DATE  = pd.Timestamp("2025-10-01")
PRE_IDA_END    = pd.Timestamp("2024-06-14")
DA15_START     = pd.Timestamp("2025-10-01")

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
    return dict(
        crit_pre=float(m.params[X.columns.get_loc("critical")]),
        crit_pre_se=float(m.bse[X.columns.get_loc("critical")]),
        crit_pre_p=float(m.pvalues[X.columns.get_loc("critical")]),
        did=float(m.params[X.columns.get_loc("crit×post")]),
        did_se=float(m.bse[X.columns.get_loc("crit×post")]),
        did_p=float(m.pvalues[X.columns.get_loc("crit×post")]),
        crit_post=float(m.params[X.columns.get_loc("critical")]
                         + m.params[X.columns.get_loc("crit×post")]),
        n=len(panel))


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    print("[panel] Building Big-4 q_1 (PDBCE auction-cleared sell)…", flush=True)
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
    q1_h["dow"] = q1_h["date"].dt.dayofweek

    print("[panel] Building Big-4 q_2 (PIBCIE signed IDA)…", flush=True)
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
    q2_h["dow"] = q2_h["date"].dt.dayofweek

    rows = []
    for label, panel_full in [("q_1 (PDBCE auction)", q1_h), ("q_2 (PIBCIE IDA)", q2_h)]:
        # Subsample to treated ∪ control hours
        panel = panel_full[panel_full["hour"].isin(CRITICAL_HOURS + FLAT_CONTROL)].copy()
        panel["critical"] = panel["hour"].isin(CRITICAL_HOURS).astype(int)

        post = panel[panel["date"] >= DA15_START].assign(post=1)
        n_post = len(post)
        print()
        print("=" * 130)
        print(f"Baseline-window sensitivity for {label}, control = h{{3,4,5}}, treated = h{{7,8,16,17,18}}")
        print(f"DA15/ID15 post sample (Oct 2025+): N = {n_post:,} firm-day-hour rows")
        print("=" * 130)
        print(f"  {'pre-window':14s} {'mo':>4s} {'N_pre':>9s} | {'crit_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'crit_post':>10s}")

        for bl_label, intervals in BASELINES:
            pre = pd.concat([
                panel[(panel["date"] >= s) & (panel["date"] < e)]
                for s, e in intervals
            ]).assign(post=0)
            if len(pre) < 100:
                continue
            sub = pd.concat([pre, post], ignore_index=True)
            res = fit_did(sub)
            n_months = sum((e - s).days / 30 for s, e in intervals)
            print(f"  {bl_label:14s} {n_months:>4.1f} {len(pre):>9,} | {res['crit_pre']:>+9.2f} ({res['crit_pre_se']:>5.2f}, p={res['crit_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['crit_post']:>+10.2f}")
            rows.append({"outcome": label, "baseline": bl_label,
                         "n_months": n_months,
                         "crit_pre": res["crit_pre"], "crit_pre_se": res["crit_pre_se"], "crit_pre_p": res["crit_pre_p"],
                         "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                         "crit_post": res["crit_post"],
                         "n_pre": len(pre), "n_post": n_post})

    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_clean_controls_baseline.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_clean_controls_baseline.csv'}")


if __name__ == "__main__":
    main()
