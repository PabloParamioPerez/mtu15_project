# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: thesis/provisional/advisor_memo.tex sec 5(b) -- pre-only midpoint
#        placebo at the window-and-market-specific p90 bandwidth.
#
# Splits each reform's pre-window at the midpoint and runs a fake critical-flat
# DiD comparing first-half-pre to second-half-pre. A surviving theta flags a
# pre-existing critical-flat trend in the outcome and wounds the headline.
#
# OUT: results/regressions/bid/mtu15_critical_flat/pre_only_placebo_p90.csv

from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL_DIR = REPO / "data/derived/panels/per_curve_windowed"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/pre_only_placebo_p90.csv"

# (reform, market, panel, T_real, T_midpoint_placebo)
CONFIG = [
    ("ID15", "da",  "per_curve_ID15_real_DA_h50.parquet",  "2025-03-19", "2024-10-30"),
    ("ID15", "ida", "per_curve_ID15_real_IDA_h62.parquet", "2025-03-19", "2024-10-30"),
    ("DA15", "da",  "per_curve_DA15_real_DA_h50.parquet",  "2025-10-01", "2025-07-15"),
    ("DA15", "ida", "per_curve_DA15_real_IDA_h58.parquet", "2025-10-01", "2025-07-15"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind"]


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    clusters = np.unique(cluster)
    meat = np.zeros((X.shape[1], X.shape[1]))
    for c in clusters:
        m = cluster == c
        u = X[m].T @ resid[m]
        meat += np.outer(u, u)
    G = len(clusters); n, k = X.shape
    if G > 1:
        meat *= G / (G - 1) * (n - 1) / (n - k)
    cov = XtX_inv @ meat @ XtX_inv
    return beta, np.sqrt(np.diag(cov))


def did_placebo(panel, tech, T_real, T_mid):
    p = panel[(panel["tech"] == tech)
              & panel["hour_class"].isin(["Critical", "Flat"])].copy()
    p["d"] = pd.to_datetime(p["d"])
    # Restrict to true pre-window only
    p = p[p["d"] < pd.Timestamp(T_real)]
    if len(p) < 100:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_mid)).astype(float)
    p["crit"] = (p["hour_class"] == "Critical").astype(float)
    p["did"]  = p["post"] * p["crit"]
    rows = []
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 100:
            continue
        for col in ["post", "crit", "did", outcome]:
            mu = d.groupby("unit_code")[col].transform("mean")
            d[col] = d[col] - mu
        X = d[["post", "crit", "did"]].values.astype(float)
        y = d[outcome].values.astype(float)
        beta, se = clustered_ols(y, X, d["d"].astype(str).values)
        theta = beta[2]; se_t = se[2]
        rows.append(dict(tech=tech, outcome=outcome,
                          theta=theta, se=se_t,
                          t=theta / se_t if se_t else 0,
                          n=len(d)))
    return rows


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    all_rows = []
    for reform, market, fname, T_real, T_mid in CONFIG:
        print(f"\n=== {reform} {market} placebo (midpoint {T_mid}) ===")
        panel = pd.read_parquet(PANEL_DIR / fname)
        for tech in TECHS:
            res = did_placebo(panel, tech, T_real, T_mid)
            if not res:
                continue
            for r in res:
                r["reform"] = reform; r["market"] = market
                all_rows.append(r)
                star = ("***" if abs(r["t"]) > 2.576 else
                        "**"  if abs(r["t"]) > 1.96  else
                        "*"   if abs(r["t"]) > 1.645 else "")
                print(f"  {tech:11s} {r['outcome']:8s}  "
                      f"theta={r['theta']:+7.3f}{star:<3s}  "
                      f"se={r['se']:.3f}  t={r['t']:+5.2f}  n={r['n']:6d}")
    df = pd.DataFrame(all_rows)
    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT} ({len(df)} rows)")


if __name__ == "__main__":
    main()
