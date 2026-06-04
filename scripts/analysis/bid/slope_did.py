# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: Spec C robustness on the alternative bid-shape outcome `slope`
#        (unweighted OLS of price on cumulative in-band MW, EUR/MWh per MW).
#        Mirrors sigma_did_multilevel.py but reads the slope panels written
#        by build_per_curve_slope.py. Per-tech DiD with unit FE; per-firm
#        DiD with unit FE; per-unit DiD without FE.
#
# OUT:
#   results/regressions/bid/mtu15_critical_flat/slope_did_per_tech.csv
#   results/regressions/bid/mtu15_critical_flat/slope_did_per_firm.csv
#   results/regressions/bid/mtu15_critical_flat/slope_did_per_unit.csv

from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SLOPE_DIR = REPO / "data/derived/panels/per_curve_slope_windowed"
OUT_DIR = REPO / "results/regressions/bid/mtu15_critical_flat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

PANELS = {
    ("DA15", "DA"):  SLOPE_DIR / "slope_DA15_real_DA_h50.parquet",
    ("DA15", "IDA"): SLOPE_DIR / "slope_DA15_real_IDA_h58.parquet",
    ("ID15", "DA"):  SLOPE_DIR / "slope_ID15_real_DA_h50.parquet",
    ("ID15", "IDA"): SLOPE_DIR / "slope_ID15_real_IDA_h62.parquet",
}

WINDOWS = {
    "DA15": {"pre_lo": "2025-04-28", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2025-11-09"},
    "ID15": {"pre_lo": "2024-06-14", "pre_hi": "2025-03-18",
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


def did_unit_FE(p, outcome="slope"):
    d = p.dropna(subset=[outcome]).copy()
    if len(d) < 100:
        return None
    for c in [outcome, "post", "crit", "post_crit"]:
        m = d.groupby("unit_code")[c].transform("mean")
        d[c + "_w"] = d[c] - m
    # Singular when within-demeaned regressors collinear (single unit, etc.)
    for c in ["post_w", "crit_w", "post_crit_w"]:
        if d[c].abs().max() < 1e-10:
            return None
    X = np.column_stack([np.ones(len(d)),
                         d["post_w"].values, d["crit_w"].values,
                         d["post_crit_w"].values])
    y = d[outcome + "_w"].values
    try:
        beta, se = clustered_ols(y, X, d["d"].astype(str).values)
    except np.linalg.LinAlgError:
        return None
    return {"theta": beta[3], "se": se[3], "t": beta[3]/se[3], "n": len(d),
            "n_units": d["unit_code"].nunique()}


def did_no_FE(p, outcome="slope"):
    d = p.dropna(subset=[outcome]).copy()
    if len(d) < 50:
        return None
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
    per_tech, per_firm, per_unit = [], [], []
    for (reform, market_label), path in PANELS.items():
        if not path.exists():
            print(f"missing {path}; run build_per_curve_slope.py first")
            continue
        panel = pd.read_parquet(path)
        for tech in ["CCGT", "Hydro", "Hydro_pump"]:
            p_tech = panel[panel["tech"] == tech]
            p_tech_win = filter_window(p_tech, reform)
            if p_tech_win.empty:
                continue
            for outcome in ["slope", "intercept", "gamma"]:
                if outcome not in p_tech_win.columns: continue
                r = did_unit_FE(p_tech_win, outcome)
                if r:
                    per_tech.append({"market": market_label, "reform": reform,
                                      "tech": tech, "outcome": outcome, **r})
                for firm, p_tf in p_tech_win.groupby("firm"):
                    r = did_unit_FE(p_tf, outcome)
                    if r is None: continue
                    per_firm.append({"market": market_label, "reform": reform,
                                      "tech": tech, "firm": firm,
                                      "outcome": outcome, **r})
                for unit, p_u in p_tech_win.groupby("unit_code"):
                    r = did_no_FE(p_u, outcome)
                    if r is None: continue
                    firm = p_u["firm"].iloc[0] if "firm" in p_u.columns else "?"
                    per_unit.append({"market": market_label, "reform": reform,
                                      "tech": tech, "firm": firm,
                                      "unit_code": unit, "outcome": outcome,
                                      **r})
    pd.DataFrame(per_tech).to_csv(OUT_DIR / "slope_did_per_tech.csv", index=False)
    pd.DataFrame(per_firm).to_csv(OUT_DIR / "slope_did_per_firm.csv", index=False)
    pd.DataFrame(per_unit).to_csv(OUT_DIR / "slope_did_per_unit.csv", index=False)
    df = pd.DataFrame(per_tech)
    for outcome in ["slope", "intercept", "gamma"]:
        sub = df[df["outcome"] == outcome]
        if len(sub):
            print(f"=== Per-tech ({outcome} DiD) ===")
            print(sub[["market","reform","tech","theta","se","t","n","n_units"]].to_string(index=False))
            print()


if __name__ == "__main__":
    main()
