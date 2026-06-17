# STATUS: ALIVE
# LAST-AUDIT: 2026-05-23
# FEEDS: thesis/provisional/advisor_memo.tex sec 5 OVB (b) cross-border;
#        thesis/provisional/descriptive_facts.tex cross-border section.
# CLAIM: Cross-border balancing flows do NOT exhibit a differential
#        critical-flat shock across the MTU15 cutovers (ID15 2025-03-19,
#        DA15 2025-10-01). Empirical check of the priors-side argument in
#        the memo that the (b) cross-border OVB channel is not biting:
#        we run the same critical-vs-flat DiD on aggregate net cross-border
#        imports as outcome and report a null theta.
#
# Data: ESIOS indicators 535 (FR imp), 536 (FR exp), 539 (PT imp),
#       540 (PT exp), all 15-min balancing flows in MW. Net flow per
#       (date, clock-hour) is (imp_FR + imp_PT - exp_FR - exp_PT) summed
#       across the 4 quarter-hours of the clock hour.
#
# OUT: results/regressions/bid/mtu15_critical_flat/cross_border_flow_did.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
IND = REPO / "data/processed/esios/indicators"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/cross_border_flow_did.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

WINDOWS = {
    "ID15": {"pre_lo": "2024-12-19", "pre_hi": "2025-03-18",
             "post_lo": "2025-03-19", "post_hi": "2025-04-27"},
    "DA15": {"pre_lo": "2025-07-01", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2025-12-31"},
}


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
    return "Other"


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G = len(np.unique(cluster))
    n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def load_net_flow(lo, hi):
    con = duckdb.connect()
    sql = f"""
    WITH imp_fr AS (SELECT date, hour, value AS v FROM '{IND}/535.parquet'
                    WHERE date BETWEEN '{lo}' AND '{hi}'),
         exp_fr AS (SELECT date, hour, value AS v FROM '{IND}/536.parquet'
                    WHERE date BETWEEN '{lo}' AND '{hi}'),
         imp_pt AS (SELECT date, hour, value AS v FROM '{IND}/539.parquet'
                    WHERE date BETWEEN '{lo}' AND '{hi}'),
         exp_pt AS (SELECT date, hour, value AS v FROM '{IND}/540.parquet'
                    WHERE date BETWEEN '{lo}' AND '{hi}')
    SELECT date, hour,
           SUM(COALESCE(imp_fr.v, 0) + COALESCE(imp_pt.v, 0)
             - COALESCE(exp_fr.v, 0) - COALESCE(exp_pt.v, 0)) AS net_mw,
           SUM(COALESCE(imp_fr.v, 0) + COALESCE(imp_pt.v, 0)
             + COALESCE(exp_fr.v, 0) + COALESCE(exp_pt.v, 0)) AS gross_mw
    FROM imp_fr
    FULL JOIN exp_fr USING (date, hour)
    FULL JOIN imp_pt USING (date, hour)
    FULL JOIN exp_pt USING (date, hour)
    GROUP BY date, hour
    """
    df = con.execute(sql).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    df["hour_class"] = df["hour"].map(hour_class)
    return df


def run_did(panel, reform, outcome):
    w = WINDOWS[reform]
    pre_lo, pre_hi = pd.Timestamp(w["pre_lo"]), pd.Timestamp(w["pre_hi"])
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    in_pre = (p["date"] >= pre_lo) & (p["date"] <= pre_hi)
    in_post = (p["date"] >= post_lo) & (p["date"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p["post"] = (p["date"] >= post_lo).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    cell = p.groupby(["post", "crit"])[outcome].mean().unstack()
    y = p[outcome].values.astype(float)
    X = np.column_stack([np.ones(len(p)), p["post"].values.astype(float),
                         p["crit"].values.astype(float),
                         p["post_crit"].values.astype(float)])
    beta, se = clustered_ols(y, X, p["date"].astype(str).values)
    return {"reform": reform, "outcome": outcome, "n": len(p),
            "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
            "pre_crit": cell.loc[0, 1], "post_crit": cell.loc[1, 1],
            "pre_flat": cell.loc[0, 0], "post_flat": cell.loc[1, 0]}


def main():
    rows = []
    for reform in ["ID15", "DA15"]:
        w = WINDOWS[reform]
        print(f"\n=== {reform}: loading {w['pre_lo']} to {w['post_hi']} ===")
        panel = load_net_flow(w["pre_lo"], w["post_hi"])
        print(f"  {len(panel):,} (date,hour) cells; "
              f"net_mw mean {panel['net_mw'].mean():+.1f}, "
              f"gross_mw mean {panel['gross_mw'].mean():.1f}")
        for outcome in ["net_mw", "gross_mw"]:
            r = run_did(panel, reform, outcome)
            print(f"  {outcome:10s} DiD={r['DiD']:+8.2f}  se={r['se']:6.2f}  "
                  f"t={r['t']:+6.2f}  n={r['n']:,}")
            print(f"             pre_crit={r['pre_crit']:+7.1f}  "
                  f"post_crit={r['post_crit']:+7.1f}  "
                  f"pre_flat={r['pre_flat']:+7.1f}  "
                  f"post_flat={r['post_flat']:+7.1f}")
            rows.append(r)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
