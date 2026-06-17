# STATUS: ALIVE
# LAST-AUDIT: 2026-05-05
# FEEDS: Per-hour and cumulative DiD with FLAT-HOUR controls (uncontaminated)
# CLAIM: With genuinely flat hours as control (bottom-N by σ²_within, pre-dawn),
#        the per-hour and cumulative DiDs identify the granularity mechanism
#        cleanly without contaminating the control group with opposite-sign hours.
"""Critical-hour DiD with FLAT-hour controls (not "all-other-hours" controls).

The previous design used "all other 23 hours" as control, which contaminates
the control with midday hours that have OPPOSITE-sign mechanism (negative
DiD δ from solar-abundance crowding out Big-4). Cleaner: use the bottom-N
hours by σ²_within (the genuinely flat pre-dawn hours h{3, 4, 5}) as control.
These are uncontroversial: low within-hour residual demand variation, low
solar, low industrial activity, no granularity-mechanism response expected.

Two diagnostics:

  (A) Per-hour DiD: for each hour h, run DiD with TREATED = h and
      CONTROL = flat-hour set. Subsample panel to {h} ∪ flat_control.
      Spec: q_2 ~ treated + post + treated×post + firm FE + DOW FE.

  (B) Cumulative critical-set DiD: for each top-K critical set, run DiD
      with TREATED = top-K hours and CONTROL = flat-hour set. Subsample
      panel to top-K ∪ flat_control.

Robustness across three flat-control choices:
  C3  = h{3, 4, 5}        (bottom 3 by σ²_within range — pre-dawn)
  C4  = h{2, 3, 4, 5}     (bottom 4)
  C5  = h{1, 2, 3, 4, 5}  (bottom 5)

If the coefficient is stable across these three control sets, the design is
robust to control-hour choice. If it's sensitive, we should worry.

Outcome: q_2 = signed Big-4 IDA repositioning per firm-day-hour, MWh.
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

FLAT_CONTROL_SETS = {
    "C3 (h{3,4,5})":     [3, 4, 5],
    "C4 (h{2,3,4,5})":   [2, 3, 4, 5],
    "C5 (h{1,2,3,4,5})": [1, 2, 3, 4, 5],
}


def assign_hour(date_series, period_series, mtu15_cutoff):
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def fit_did(panel, outcome="q"):
    cols = {"const": np.ones(len(panel))}
    cols["treated"] = panel["treated"].values.astype(float)
    cols["post"] = panel["post"].values.astype(float)
    cols["treat×post"] = (panel["treated"] * panel["post"]).values.astype(float)
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
        treat_post=float(m.params[X.columns.get_loc("treated")]
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
    panel_full = pd.concat([pre, post_], ignore_index=True)
    print(f"   panel rows: {len(panel_full):,} (pre {len(pre):,}, post {len(post_):,})")

    rank = pd.read_csv(RANKING)
    ranked_hours = rank.sort_values("rank_within")["hour"].astype(int).tolist()
    print(f"   ranked top-12 by σ²_within: {ranked_hours[:12]}")

    rows = []

    # ---------------------------------------------------------------
    # (A) Per-hour DiD with FLAT controls
    # ---------------------------------------------------------------
    for cset_label, flat_hours in FLAT_CONTROL_SETS.items():
        print()
        print("=" * 130)
        print(f"(A) Per-hour DiD with control = {cset_label}")
        print("=" * 130)
        print(f"  {'hour':>4s} | {'treat_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'treat_post':>10s}")

        for h in range(24):
            if h in flat_hours:
                continue  # don't treat a control hour as treated
            sub = panel_full[panel_full["hour"].isin([h] + flat_hours)].copy()
            sub["treated"] = (sub["hour"] == h).astype(int)
            res = fit_did(sub)
            print(f"  {h:>4d} | {res['treat_pre']:>+9.2f} ({res['treat_pre_se']:>5.2f}, p={res['treat_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['treat_post']:>+10.2f}")
            rows.append({"diagnostic":"per_hour", "control_set": cset_label, "hour": h,
                         "treat_pre": res["treat_pre"], "treat_pre_se": res["treat_pre_se"], "treat_pre_p": res["treat_pre_p"],
                         "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                         "treat_post": res["treat_post"], "n": res["n"]})

    # ---------------------------------------------------------------
    # (B) Cumulative critical-set DiD with FLAT controls
    # ---------------------------------------------------------------
    for cset_label, flat_hours in FLAT_CONTROL_SETS.items():
        print()
        print("=" * 130)
        print(f"(B) Cumulative critical-set DiD with control = {cset_label}")
        print("=" * 130)
        print(f"  {'top_K':>5s} {'treated':>32s} | {'crit_pre':>9s} ({'SE':>5s}, p={'p':>7s}) | {'DiD δ':>9s} ({'SE':>5s}, p={'p':>7s}) | {'crit_post':>10s}")

        for K in range(1, 13):
            crit_set = [h for h in ranked_hours[:K] if h not in flat_hours]
            if len(crit_set) == 0:
                continue
            sub = panel_full[panel_full["hour"].isin(crit_set + flat_hours)].copy()
            sub["treated"] = sub["hour"].isin(crit_set).astype(int)
            res = fit_did(sub)
            print(f"  {K:>5d} {sorted(crit_set)!s:>32s} | {res['treat_pre']:>+9.2f} ({res['treat_pre_se']:>5.2f}, p={res['treat_pre_p']:>7.1e}) | {res['did']:>+9.2f} ({res['did_se']:>5.2f}, p={res['did_p']:>7.1e}) | {res['treat_post']:>+10.2f}")
            rows.append({"diagnostic":"cumulative", "control_set": cset_label, "top_K": K,
                         "critical_set": sorted(crit_set),
                         "crit_pre": res["treat_pre"], "crit_pre_se": res["treat_pre_se"], "crit_pre_p": res["treat_pre_p"],
                         "did": res["did"], "did_se": res["did_se"], "did_p": res["did_p"],
                         "crit_post": res["treat_post"], "n": res["n"]})

    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_clean_controls.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_clean_controls.csv'}")


if __name__ == "__main__":
    main()
