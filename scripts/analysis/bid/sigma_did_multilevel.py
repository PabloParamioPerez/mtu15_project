# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: cross-method robustness of bid-shape DiD at three levels of
#        aggregation (per-tech, per-firm-within-tech, per-unit). Per-curve
#        observations throughout; no within-day aggregation.
#
# Companion to sigma_bsts_multilevel.R (BSTS at the same three levels).
#
# OUT:
#   results/regressions/bid/mtu15_critical_flat/sigma_did_per_tech.csv
#   results/regressions/bid/mtu15_critical_flat/sigma_did_per_firm.csv
#   results/regressions/bid/mtu15_critical_flat/sigma_did_per_unit.csv

from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT_DIR = REPO / "results/regressions/bid/mtu15_critical_flat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

WINDOWS = {
    "DA15": {"pre_lo": "2024-06-14", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2025-12-31"},
    "ID15": {"pre_lo": "2024-12-19", "pre_hi": "2025-03-18",
             "post_lo": "2025-03-19", "post_hi": "2025-04-27"},
}


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g
        s = X[m].T @ e[m]
        meat += np.outer(s, s)
    G = len(np.unique(cluster)); n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def filter_window(p, reform):
    w = WINDOWS[reform]
    p = p.copy()
    p["d"] = pd.to_datetime(p["d"])
    p = p[((p["d"] >= w["pre_lo"]) & (p["d"] <= w["pre_hi"])) |
          ((p["d"] >= w["post_lo"]) & (p["d"] <= w["post_hi"]))]
    p["post"] = (p["d"] >= w["post_lo"]).astype(int)
    p["crit"] = p["clock_hour"].isin(CRITICAL).astype(int)
    p = p[p["clock_hour"].isin(CRITICAL | FLAT)]
    p["post_crit"] = p["post"] * p["crit"]
    return p


def did_unit_FE(p, outcome):
    """DiD with unit FE: outcome ~ alpha_u + beta*post + xi*crit + theta*post*crit"""
    d = p.dropna(subset=[outcome]).copy()
    if len(d) < 100:
        return None
    for c in [outcome, "post", "crit", "post_crit"]:
        m = d.groupby("unit_code")[c].transform("mean")
        d[c + "_w"] = d[c] - m
    X = np.column_stack([np.ones(len(d)),
                         d["post_w"].values, d["crit_w"].values,
                         d["post_crit_w"].values])
    y = d[outcome + "_w"].values
    beta, se = clustered_ols(y, X, d["d"].astype(str).values)
    return {"theta": beta[3], "se": se[3], "t": beta[3]/se[3], "n": len(d),
            "n_units": d["unit_code"].nunique()}


def did_no_FE(p, outcome):
    """DiD without unit FE (for per-unit regressions where unit dim is trivial).
    outcome ~ a + beta*post + xi*crit + theta*post*crit, clustered by date.
    Returns None if any of (post, crit, post_crit) has no variation."""
    d = p.dropna(subset=[outcome]).copy()
    if len(d) < 50:
        return None
    # Need variation in all three indicators
    for c in ["post", "crit", "post_crit"]:
        if d[c].nunique() < 2:
            return None
    if d["d"].nunique() < 4:
        return None
    X = np.column_stack([np.ones(len(d)),
                         d["post"].values, d["crit"].values,
                         d["post_crit"].values])
    y = d[outcome].values
    try:
        beta, se = clustered_ols(y, X, d["d"].astype(str).values)
    except np.linalg.LinAlgError:
        return None
    return {"theta": beta[3], "se": se[3], "t": beta[3]/se[3], "n": len(d)}


def main():
    da = pd.read_parquet(DA_PANEL)
    ida = pd.read_parquet(IDA_PANEL)

    per_tech, per_firm, per_unit = [], [], []

    for market_label, panel in [("DA", da), ("IDA", ida)]:
        reform = "DA15" if market_label == "DA" else "ID15"
        for tech in ["CCGT", "Hydro"]:
            p_tech = panel[panel["tech"] == tech]
            p_tech_win = filter_window(p_tech, reform)
            if p_tech_win.empty:
                continue

            # Level 3: per-tech (unit FE)
            r = did_unit_FE(p_tech_win, "sigma_p")
            if r:
                per_tech.append({"market": market_label, "reform": reform,
                                  "tech": tech, **r})

            # Level 2: per (tech, firm)
            for firm, p_tf in p_tech_win.groupby("firm"):
                r = did_unit_FE(p_tf, "sigma_p")
                if r is None: continue
                per_firm.append({"market": market_label, "reform": reform,
                                  "tech": tech, "firm": firm, **r})

            # Level 1: per-unit
            for unit, p_u in p_tech_win.groupby("unit_code"):
                r = did_no_FE(p_u, "sigma_p")
                if r is None: continue
                firm = p_u["firm"].iloc[0] if "firm" in p_u.columns else "?"
                per_unit.append({"market": market_label, "reform": reform,
                                  "tech": tech, "firm": firm,
                                  "unit_code": unit, **r})

    pd.DataFrame(per_tech).to_csv(OUT_DIR / "sigma_did_per_tech.csv", index=False)
    pd.DataFrame(per_firm).to_csv(OUT_DIR / "sigma_did_per_firm.csv", index=False)
    pd.DataFrame(per_unit).to_csv(OUT_DIR / "sigma_did_per_unit.csv", index=False)

    print("=== Per-tech ===")
    df = pd.DataFrame(per_tech)
    print(df[["market","reform","tech","theta","se","t","n","n_units"]].to_string(index=False))

    print("\n=== Per-firm ===")
    df = pd.DataFrame(per_firm)
    if len(df):
        df["sig"] = df["t"].abs() > 1.96
        print(df[["market","reform","tech","firm","theta","se","t","n","sig"]].to_string(index=False))

    print("\n=== Per-unit summary (distribution by tech×reform) ===")
    df = pd.DataFrame(per_unit)
    if len(df):
        agg = (df.groupby(["market","reform","tech"])
                 .agg(n_units=("unit_code", "nunique"),
                      median_theta=("theta", "median"),
                      mean_theta=("theta", "mean"),
                      pct_positive=("theta", lambda x: (x > 0).mean() * 100),
                      pct_sig_pos=("t", lambda x: (x > 1.96).mean() * 100),
                      pct_sig_neg=("t", lambda x: (x < -1.96).mean() * 100))
                 .reset_index())
        print(agg.to_string(index=False))


if __name__ == "__main__":
    main()
