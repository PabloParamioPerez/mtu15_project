# STATUS: ALIVE
# LAST-AUDIT: 2026-05-05
# FEEDS: Critical-hour set sensitivity for q_2 V-shape DiD
# CLAIM: The +83 q_2 critical × DA15/ID15 coefficient is robust to the choice
#        of critical-hour set; per-hour DiDs reveal which hours carry the signal.
"""Critical-hour set sensitivity for q_2 V-shape.

Two diagnostics:

  (A) Per-hour DiD: for each hour h ∈ {0..23}, run DiD treating that hour as
      the only treated unit, with all other 23 hours as control. The coefficient
      tells us the DA15/ID15 reform effect at that hour specifically.

      Spec: q_2 ~ I[hour=h] + post + I[hour=h]×post + firm FE + DOW FE,
            cluster by date.

  (B) Cumulative critical-set sensitivity: rank hours by σ²_within (from
      critical_hours_ranking.csv), then expand the critical set from top-1
      to top-12. Re-run the DiD at each step. Watch how the coefficient evolves.

      Spec: q_2 ~ critical_topK + post + critical_topK×post + firm FE + DOW FE.

This answers two questions:
  1. Is the +83 driven by a few hours or distributed across the 5-hour set?
  2. Are we over/under-including by stopping at top 5?
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
RANKING  = PROJECT / "results" / "regressions" / "critical_hours_ranking.csv"

OUT_DIR_R = PROJECT / "results" / "regressions"

MTU15_IDA_DATE = pd.Timestamp("2025-03-19")
PRE_IDA_END    = pd.Timestamp("2024-06-14")
DA15_START     = pd.Timestamp("2025-10-01")


def assign_hour(date_series, period_series, mtu15_cutoff):
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def fit_did(panel, treat_col, outcome="q"):
    """DiD with treat_col as treatment dummy, post=DA15/ID15, controls FE."""
    cols = {"const": np.ones(len(panel))}
    cols["treated"] = panel[treat_col].values.astype(float)
    cols["post"] = panel["post"].values.astype(float)
    cols["treat×post"] = (panel[treat_col] * panel["post"]).values.astype(float)
    for f in sorted(panel["firm"].unique())[1:]:
        cols[f"firm_{f}"] = (panel["firm"] == f).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
    X = pd.DataFrame(cols, index=panel.index)
    y = panel[outcome].values
    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    return dict(
        treat_pre=float(m.params[X.columns.get_loc("treated")]),
        treat_pre_se=float(m.bse[X.columns.get_loc("treated")]),
        treat_pre_p=float(m.pvalues[X.columns.get_loc("treated")]),
        did=float(m.params[X.columns.get_loc("treat×post")]),
        did_se=float(m.bse[X.columns.get_loc("treat×post")]),
        did_p=float(m.pvalues[X.columns.get_loc("treat×post")]),
        treat_post_implied=float(m.params[X.columns.get_loc("treated")]
                                  + m.params[X.columns.get_loc("treat×post")]),
        n=len(panel))


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")

    print("[panel] Building Big-4 firm-day-hour q_2 panel from PIBCIE…", flush=True)
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

    pre = q2_h[q2_h["date"] < PRE_IDA_END].assign(post=0)
    post_ = q2_h[q2_h["date"] >= DA15_START].assign(post=1)
    panel = pd.concat([pre, post_], ignore_index=True)
    print(f"   panel rows: {len(panel):,} (pre {len(pre):,}, post {len(post_):,})")

    # ---------------------------------------------------------------
    # (A) Per-hour DiD
    # ---------------------------------------------------------------
    print()
    print("=" * 130)
    print("(A) Per-hour DiD: for each hour h, treat that hour vs all others (q_2 outcome)")
    print(f"   pre = pre-IDA full window, post = DA15/ID15")
    print("=" * 130)
    print(f"  {'hour':>4s} | {'treat_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'treat_post':>10s}")

    rows_A = []
    for h in range(24):
        panel["is_h"] = (panel["hour"] == h).astype(int)
        res = fit_did(panel, treat_col="is_h", outcome="q")
        print(f"  {h:>4d} | {res['treat_pre']:>+9.2f} ({res['treat_pre_se']:>5.2f}, p={res['treat_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['treat_post_implied']:>+10.2f}")
        rows_A.append({"diagnostic":"per_hour", "hour": h,
                       "treat_pre": res["treat_pre"], "treat_pre_se": res["treat_pre_se"], "treat_pre_p": res["treat_pre_p"],
                       "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                       "treat_post": res["treat_post_implied"]})

    # ---------------------------------------------------------------
    # (B) Cumulative critical-set sensitivity
    # ---------------------------------------------------------------
    print()
    print("=" * 130)
    print("(B) Cumulative critical-set sensitivity: top-K hours by σ²_within (q_2 outcome)")
    print("=" * 130)
    rank = pd.read_csv(RANKING)
    ranked_hours = rank.sort_values("rank_within")["hour"].astype(int).tolist()
    print(f"   ranked top-12 by σ²_within: {ranked_hours[:12]}")
    print()
    print(f"  {'top_K':>5s} {'hours_added':>30s} | {'crit_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'crit_post':>10s}")

    rows_B = []
    for K in range(1, 13):
        crit_set = set(ranked_hours[:K])
        panel["critical_K"] = panel["hour"].isin(crit_set).astype(int)
        res = fit_did(panel, treat_col="critical_K", outcome="q")
        added = ranked_hours[K-1]
        print(f"  {K:>5d} {sorted(crit_set)!s:>30s} | {res['treat_pre']:>+9.2f} ({res['treat_pre_se']:>5.2f}, p={res['treat_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['treat_post_implied']:>+10.2f}")
        rows_B.append({"diagnostic":"cumulative", "top_K": K, "hours_added_last": added,
                       "critical_set": sorted(crit_set),
                       "crit_pre": res["treat_pre"], "crit_pre_se": res["treat_pre_se"], "crit_pre_p": res["treat_pre_p"],
                       "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                       "crit_post": res["treat_post_implied"]})

    pd.DataFrame(rows_A + rows_B).to_csv(OUT_DIR_R / "critical_hours_set_sensitivity.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_set_sensitivity.csv'}")


if __name__ == "__main__":
    main()
