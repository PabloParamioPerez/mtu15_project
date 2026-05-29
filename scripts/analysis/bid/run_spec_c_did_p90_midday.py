# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: thesis/provisional/advisor_memo.tex sec 5(a) -- midday-vs-flat
#        falsification at the window-and-market-specific p90 bandwidth.
#        Parallel to run_spec_c_did_p90.py but with hour_class restricted
#        to Midday + Flat (instead of Critical + Flat); theta = (post * midday)
#        interaction.
#
# Reads the per_curve_windowed panels:
#   ID15 DA  h=50; ID15 IDA h=62;
#   DA15 DA  h=50; DA15 IDA h=58.
#
# OUT: results/regressions/bid/mtu15_critical_flat/spec_c_did_p90_midday.csv

from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL_DIR = REPO / "data/derived/panels/per_curve_windowed"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/spec_c_did_p90_midday.csv"

CONFIG = [
    ("ID15", "da",  "per_curve_ID15_real_DA_h50.parquet",  "2025-03-19"),
    ("ID15", "ida", "per_curve_ID15_real_IDA_h62.parquet", "2025-03-19"),
    ("DA15", "da",  "per_curve_DA15_real_DA_h50.parquet",  "2025-10-01"),
    ("DA15", "ida", "per_curve_DA15_real_IDA_h58.parquet", "2025-10-01"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind"]


def clustered_ols(y, X, cluster):
    n, k = X.shape
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    clusters = np.unique(cluster)
    meat = np.zeros((k, k))
    for c in clusters:
        m = cluster == c
        u = (X[m].T @ resid[m]).reshape(-1, 1)
        meat += u @ u.T
    G = len(clusters)
    if G > 1:
        meat *= G / (G - 1) * (n - 1) / (n - k)
    cov = XtX_inv @ meat @ XtX_inv
    se = np.sqrt(np.diag(cov))
    return beta, se


def did_one(panel, tech, T_reform):
    p = panel[panel["tech"] == tech].copy()
    p["d"] = pd.to_datetime(p["d"])
    p = p[p["hour_class"].isin(["Midday", "Flat"])]
    if len(p) < 100:
        return None
    p["post"]   = (p["d"] >= pd.Timestamp(T_reform)).astype(float)
    p["midday"] = (p["hour_class"] == "Midday").astype(float)
    p["did"]    = p["post"] * p["midday"]
    rows = []
    for outcome in ["sigma_p", "n_eff"]:
        d = p.dropna(subset=[outcome]).copy()
        if len(d) < 100:
            continue
        for col in ["post", "midday", "did", outcome]:
            mu = d.groupby("unit_code")[col].transform("mean")
            d[col] = d[col] - mu
        X = d[["post", "midday", "did"]].values.astype(float)
        y = d[outcome].values.astype(float)
        try:
            beta, se = clustered_ols(y, X, d["d"].astype(str).values)
        except Exception as e:
            print(f"  [{tech} {outcome}] failed: {e}")
            continue
        theta = beta[2]; se_theta = se[2]
        t_theta = theta / se_theta if se_theta else 0
        rows.append(dict(tech=tech, outcome=outcome,
                          theta=theta, se=se_theta, t=t_theta,
                          n=len(d)))
    return rows


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    all_rows = []
    for reform, market, fname, T_reform in CONFIG:
        path = PANEL_DIR / fname
        print(f"\n=== {reform} {market} ({fname}) ===")
        panel = pd.read_parquet(path)
        for tech in TECHS:
            res = did_one(panel, tech, T_reform)
            if not res:
                print(f"  {tech}: skipped (low sample)")
                continue
            for r in res:
                r["reform"] = reform; r["market"] = market
                all_rows.append(r)
                star = ("***" if abs(r["t"]) > 2.576 else
                        "**"  if abs(r["t"]) > 1.96  else
                        "*"   if abs(r["t"]) > 1.645 else "")
                print(f"  {tech:11s} {r['outcome']:8s}  "
                      f"theta={r['theta']:+7.3f}{star:<3s}  se={r['se']:.3f}  "
                      f"t={r['t']:+5.2f}  n={r['n']:6d}")
    df = pd.DataFrame(all_rows)
    df.to_csv(OUT, index=False)
    print(f"\nWrote {OUT} ({len(df)} rows)")


if __name__ == "__main__":
    main()
