# STATUS: ALIVE
# LAST-AUDIT: 2026-05-04
# FEEDS: Same-cal-month robustness on q_1 (PDBCE) and q_2 (PIBCIE) V-shape
# CLAIM: The critical-hours DiD V-shape on q_1 and q_2 survives restricting
#        pre-IDA observations to the same calendar months as each post-reform
#        regime, ruling out calendar-mix as the driver.
"""Same-cal-month robustness for q_1 and q_2 critical-hours DiD.

For each post-reform regime, identify its calendar months. Restrict the pre-IDA
baseline to the same calendar months. Recompute the DiD δ on that matched-month
sample. If the δ keeps the same sign and >50% of the original magnitude, the
claim is robust to calendar mix.

Outcomes:
  q_1 = Big-4 PDBCE auction-cleared sell, MWh per firm-day-hour
  q_2 = Big-4 PIBCIE signed IDA repositioning, MWh per firm-day-hour

Specification mirrors supply_decomp.py / critical_hours_did_v2 framework:
  outcome ~ critical + crit×regime + firm FE + cal-month FE + year FE + DOW FE,
  cluster-robust SEs by date.

The DiD δ on `crit×regime` is the headline coefficient. For same-cal-month, we
drop the cal-month FE (since the within-regime cal-month range is restricted)
and re-estimate within the matched window.
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

# Each regime → its calendar-month set + its actual date window
REGIMES = {
    "3-sess":           ({6, 7, 8, 9, 10, 11},      "2024-06-14", "2024-12-01"),
    "ISP15-win":        ({12, 1, 2, 3},             "2024-12-01", "2025-03-19"),
    "DA60/ID15-prebo":  ({3, 4},                    "2025-03-19", "2025-04-28"),
    "DA60/ID15-reforz": ({4, 5, 6, 7, 8, 9},        "2025-04-28", "2025-10-01"),
    "DA15/ID15":        ({10, 11, 12, 1, 2, 3, 4, 5}, "2025-10-01", "2026-06-01"),
}
PRE_IDA_END = "2024-06-14"


def assign_hour(date_series, period_series, mtu15_cutoff):
    is_post = date_series >= mtu15_cutoff
    h = np.where(is_post,
                 ((period_series - 1) // 4).astype(int),
                 (period_series - 1).astype(int))
    return np.clip(h, 0, 23)


def fit_did(panel, outcome="q"):
    """DiD spec for a TWO-regime panel: pre-IDA-same-cal vs one post-reform regime.

    Spec: q ~ critical + post + crit×post + firm FE + DOW FE, cluster by date.
    No cal-month FE (the sample is restricted to matched cal months by design).
    No year FE (within regime, year is collinear with post).
    """
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

    # ---------------------------------------------------------------
    # Build q_1 panel (PDBCE auction-cleared sell, hourly periods 1-24)
    # ---------------------------------------------------------------
    print("[q_1] from PDBCE…", flush=True)
    q1 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, period, grupo_empresarial AS firm,
               SUM(CASE WHEN offer_type = 1 AND assigned_power_mw > 0
                        THEN assigned_power_mw * mtu_minutes / 60.0 ELSE 0 END) AS q
        FROM '{PDBCE}'
        WHERE grupo_empresarial IN ('IB','GE','GN','HC')
        GROUP BY 1, 2, 3
    """).df()
    q1["date"] = pd.to_datetime(q1["date"])
    # PDBCE stays hourly until 2025-10-01; period=hour for all pre-MTU15-DA
    q1["hour"] = assign_hour(q1["date"], q1["period"], pd.Timestamp("2025-10-01"))
    # Aggregate to firm-day-hour (sums quarter-rows post-MTU15-DA)
    q1_h = q1.groupby(["date","firm","hour"], as_index=False)["q"].sum()
    print(f"   {len(q1_h):,} firm-day-hour rows")

    # ---------------------------------------------------------------
    # Build q_2 panel (PIBCIE signed IDA, periods 1-24 pre-MTU15-IDA, 1-96 post)
    # ---------------------------------------------------------------
    print("[q_2] from PIBCIE…", flush=True)
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
    print(f"   {len(q2_h):,} firm-day-hour rows")

    # ---------------------------------------------------------------
    # For each (outcome, regime) pair, build matched-cal-month panel
    # and fit two-regime DiD
    # ---------------------------------------------------------------
    rows = []
    for label, panel in [("q_1 (PDBCE auction)", q1_h), ("q_2 (PIBCIE IDA)", q2_h)]:
        panel = panel.copy()
        panel["critical"] = panel["hour"].isin(CRITICAL_HOURS).astype(int)
        panel["dow"] = panel["date"].dt.dayofweek
        panel["month"] = panel["date"].dt.month

        print()
        print("=" * 100)
        print(f"Same-cal-month DiD on {label}")
        print("=" * 100)
        print(f"  {'regime':25s} {'months':14s} {'δ_full':>10s} {'δ_same-cal':>14s} {'SE':>7s}  {'p':>9s}  {'N_pre':>8s} {'N_post':>8s}")

        for r, (months, d0, d1) in REGIMES.items():
            # FULL-SAMPLE DiD δ for reference: pre-IDA all + this regime
            pre = panel[panel["date"] < pd.Timestamp(PRE_IDA_END)].copy()
            post = panel[(panel["date"] >= pd.Timestamp(d0)) & (panel["date"] < pd.Timestamp(d1))].copy()
            full = pd.concat([pre.assign(post=0), post.assign(post=1)], ignore_index=True)
            res_full = fit_did(full)
            # SAME-CAL-MONTH DiD δ
            pre_sc = pre[pre["month"].isin(months)].copy()
            sc = pd.concat([pre_sc.assign(post=0), post.assign(post=1)], ignore_index=True)
            if len(pre_sc) < 100 or len(post) < 100:
                print(f"  {r:25s} {sorted(months)!s:14.14s}: too few rows (N_pre={len(pre_sc)}, N_post={len(post)})")
                continue
            res_sc = fit_did(sc)
            mag_ratio = (res_sc["coef"] / res_full["coef"] * 100) if abs(res_full["coef"]) > 1e-6 else float("nan")
            print(f"  {r:25s} {sorted(months)!s:14.14s} {res_full['coef']:+10.1f} {res_sc['coef']:+14.1f} {res_sc['se']:7.2f}  {res_sc['pval']:9.2e}  {len(pre_sc):>8,} {len(post):>8,}  (ratio {mag_ratio:.0f}%)")
            rows.append({
                "outcome": label, "regime": r, "months": sorted(months),
                "delta_full": res_full["coef"], "delta_same_cal": res_sc["coef"],
                "se_same_cal": res_sc["se"], "pval_same_cal": res_sc["pval"],
                "magnitude_ratio_pct": mag_ratio,
                "n_pre_sc": len(pre_sc), "n_post": len(post)})

    pd.DataFrame(rows).to_csv(OUT_DIR_R / "critical_hours_same_cal_month.csv", index=False)
    print()
    print(f"wrote {OUT_DIR_R / 'critical_hours_same_cal_month.csv'}")
    print("Done.")


if __name__ == "__main__":
    main()
