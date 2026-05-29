# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: thesis/provisional/advisor_memo.tex sec 5(d) cross-border row,
#        at the window-and-market-specific p90 bandwidth (ID15 IDA h=62).
#
# Test: re-run the ID15 IDA critical-flat sigma_p / n_eff DiD with hourly
# cross-border net flow as a linear control, on top of within-unit demeaning
# and date-clustered SEs. Reports the attenuation (theta with vs. without xb
# control) -- diagnostic for whether SIDC/XBID 15-min cross-border timing
# is a sibling treatment OVB or a non-binding channel.
#
# OUT: results/regressions/bid/mtu15_critical_flat/xb_control_p90.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANEL = REPO / "data/derived/panels/per_curve_windowed/per_curve_ID15_real_IDA_h62.parquet"
IND = REPO / "data/processed/esios/indicators"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/xb_control_p90.csv"
T_REFORM = "2025-03-19"
LO, HI = "2024-06-14", "2025-04-27"
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind"]


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ X.T @ y
    resid = y - X @ beta
    clusters = np.unique(cluster)
    meat = np.zeros((X.shape[1], X.shape[1]))
    for c in clusters:
        m = cluster == c
        u = (X[m].T @ resid[m])
        meat += np.outer(u, u)
    G = len(clusters)
    n, k = X.shape
    if G > 1:
        meat *= G / (G - 1) * (n - 1) / (n - k)
    cov = XtX_inv @ meat @ XtX_inv
    return beta, np.sqrt(np.diag(cov))


def load_xb(lo, hi):
    sql = f"""
    SELECT date, hour,
           SUM(COALESCE(f1.value,0) + COALESCE(f3.value,0)
             - COALESCE(f2.value,0) - COALESCE(f4.value,0)) AS xb_net_mw
    FROM '{IND}/535.parquet' f1
    FULL JOIN '{IND}/536.parquet' f2 USING (date, hour)
    FULL JOIN '{IND}/539.parquet' f3 USING (date, hour)
    FULL JOIN '{IND}/540.parquet' f4 USING (date, hour)
    WHERE date BETWEEN '{lo}' AND '{hi}' GROUP BY date, hour
    """
    xb = duckdb.connect().execute(sql).fetchdf()
    xb["d"] = pd.to_datetime(xb["date"])
    return xb[["d", "hour", "xb_net_mw"]].rename(columns={"hour": "clock_hour"})


def run_did(panel, tech, outcome, with_xb):
    p = panel[(panel["tech"] == tech)
              & panel["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=[outcome, "xb_net_mw"]) if with_xb else p.dropna(subset=[outcome])
    if len(p) < 100:
        return None
    p["d"] = pd.to_datetime(p["d"])
    p["post"] = (p["d"] >= pd.Timestamp(T_REFORM)).astype(float)
    p["crit"] = (p["hour_class"] == "Critical").astype(float)
    p["did"]  = p["post"] * p["crit"]
    cols = ["post", "crit", "did", outcome]
    if with_xb:
        cols.append("xb_net_mw")
    d = p.copy()
    for col in cols:
        mu = d.groupby("unit_code")[col].transform("mean")
        d[col] = d[col] - mu
    Xcols = ["post", "crit", "did"] + (["xb_net_mw"] if with_xb else [])
    X = d[Xcols].values.astype(float)
    y = d[outcome].values.astype(float)
    beta, se = clustered_ols(y, X, d["d"].astype(str).values)
    theta = beta[2]
    se_theta = se[2]
    return dict(tech=tech, outcome=outcome,
                spec="with_xb" if with_xb else "no_xb",
                theta=theta, se=se_theta,
                t=theta / se_theta if se_theta else 0,
                n=len(d))


def main():
    OUT.parent.mkdir(parents=True, exist_ok=True)
    panel = pd.read_parquet(PANEL)
    panel["d"] = pd.to_datetime(panel["d"])
    xb = load_xb(LO, HI)
    panel = panel.merge(xb, on=["d", "clock_hour"], how="left")
    panel["xb_net_mw"] = panel["xb_net_mw"].fillna(0)

    rows = []
    for tech in TECHS:
        for outcome in ["sigma_p", "n_eff"]:
            for with_xb in [False, True]:
                r = run_did(panel, tech, outcome, with_xb)
                if r:
                    rows.append(r)
    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)

    # Compute attenuation per (tech, outcome)
    print("\n=== Attenuation summary: theta_no_xb -> theta_with_xb ===")
    for tech in TECHS:
        for outcome in ["sigma_p", "n_eff"]:
            sub = df[(df.tech == tech) & (df.outcome == outcome)]
            if len(sub) != 2:
                continue
            t_no  = sub[sub.spec == "no_xb"].iloc[0]["theta"]
            t_xb  = sub[sub.spec == "with_xb"].iloc[0]["theta"]
            pct = 100.0 * (1 - t_xb / t_no) if t_no else float("nan")
            print(f"  {tech:11s} {outcome:8s}  no_xb={t_no:+7.3f}  "
                  f"with_xb={t_xb:+7.3f}  atten={pct:+5.1f}%")
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
