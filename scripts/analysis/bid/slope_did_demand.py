# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Spec C DiD on the DEMAND-side bid-shape outcomes (alpha, beta, gamma).
#        Critical/flat partition with unit FE, mirroring slope_did.py but on the
#        per-curve demand panels under
#        data/derived/panels/per_curve_slope_demand_windowed/.
#        Demand-side units are grouped by `demand_class`
#        (Retailer / DirectCons / CUR / Distrib / Portfolio / PumpBuy).
#
# OUT: results/regressions/bid/mtu15_critical_flat/
#        slope_did_demand_per_class.csv
#        slope_did_demand_per_firm.csv

from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PANELS_DIR = REPO / "data/derived/panels/per_curve_slope_demand_windowed"
OUT_DIR = REPO / "results/regressions/bid/mtu15_critical_flat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT     = {1, 2, 3}

PANELS = {
    ("DA15", "DA",  50): PANELS_DIR / "demand_DA15_real_DA_h50.parquet",
    ("DA15", "IDA", 58): PANELS_DIR / "demand_DA15_real_IDA_h58.parquet",
    ("ID15", "DA",  50): PANELS_DIR / "demand_ID15_real_DA_h50.parquet",
    ("ID15", "IDA", 62): PANELS_DIR / "demand_ID15_real_IDA_h62.parquet",
}
WINDOWS = {
    "DA15": {"pre_lo": "2025-04-28", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2025-11-09"},
    "ID15": {"pre_lo": "2024-06-14", "pre_hi": "2025-03-18",
             "post_lo": "2025-03-19", "post_hi": "2025-04-27"},
}
DEMAND_CLASSES = ["Retailer", "DirectCons", "CUR", "Distrib", "Portfolio", "PumpBuy"]
FIRM_BUCKETS = ["IB", "GE", "GN", "HC", "REP", "OTH"]


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


def did_unit_FE(p, outcome, post_lo):
    d = p.dropna(subset=[outcome]).copy()
    d["post"] = (d["d"] >= post_lo).astype(int)
    d["crit"] = d["clock_hour"].isin(CRITICAL).astype(int)
    d = d[d["clock_hour"].isin(CRITICAL | FLAT)]
    if len(d) < 100:
        return None
    d["post_crit"] = d["post"] * d["crit"]
    for c in [outcome, "post", "crit", "post_crit"]:
        if d[c].nunique() < 2:
            return None
        m = d.groupby("unit_code")[c].transform("mean")
        d[c + "_w"] = d[c] - m
    for c in ["post_w", "crit_w", "post_crit_w"]:
        if d[c].abs().max() < 1e-10:
            return None
    X = np.column_stack([np.ones(len(d)), d["post_w"].values,
                         d["crit_w"].values, d["post_crit_w"].values])
    y = d[outcome + "_w"].values
    try:
        b, se = clustered_ols(y, X, d["d"].astype(str).values)
    except np.linalg.LinAlgError:
        return None
    return {"theta": b[3], "se": se[3], "t": b[3]/se[3], "n": len(d),
            "n_units": d["unit_code"].nunique()}


def filter_window(p, reform):
    w = WINDOWS[reform]
    return p[((p["d"] >= w["pre_lo"]) & (p["d"] <= w["pre_hi"])) |
             ((p["d"] >= w["post_lo"]) & (p["d"] <= w["post_hi"]))].copy()


def run_per_class():
    rows = []
    for (reform, market_label, h), path in PANELS.items():
        if not path.exists():
            print(f"  missing {path}; skipping")
            continue
        p = pd.read_parquet(path); p["d"] = pd.to_datetime(p["d"])
        p_win = filter_window(p, reform)
        for dc in DEMAND_CLASSES:
            p_t = p_win[p_win["demand_class"] == dc]
            if p_t.empty: continue
            for outcome in ["slope", "intercept", "gamma"]:
                if outcome not in p_t.columns: continue
                r = did_unit_FE(p_t, outcome, WINDOWS[reform]["post_lo"])
                if r is None: continue
                r.update({"reform": reform, "market": market_label, "h": h,
                          "demand_class": dc, "outcome": outcome})
                rows.append(r)
    pd.DataFrame(rows).to_csv(OUT_DIR / "slope_did_demand_per_class.csv",
                              index=False)
    print(f"per_class: {len(rows)} rows -> slope_did_demand_per_class.csv")


def run_per_firm():
    """Big-4 retailers (IB, GE, GN, HC) — slope/intercept/gamma DiD within
    Retailer demand class."""
    rows = []
    for (reform, market_label, h), path in PANELS.items():
        if not path.exists(): continue
        p = pd.read_parquet(path); p["d"] = pd.to_datetime(p["d"])
        p_win = filter_window(p, reform)
        # focus on Retailer class (most volume + Big-4 concentration)
        p_ret = p_win[p_win["demand_class"] == "Retailer"]
        for f in FIRM_BUCKETS:
            p_f = p_ret[p_ret["firm"] == f]
            if p_f.empty: continue
            for outcome in ["slope", "intercept", "gamma"]:
                if outcome not in p_f.columns: continue
                r = did_unit_FE(p_f, outcome, WINDOWS[reform]["post_lo"])
                if r is None: continue
                r.update({"reform": reform, "market": market_label, "h": h,
                          "firm": f, "outcome": outcome})
                rows.append(r)
    pd.DataFrame(rows).to_csv(OUT_DIR / "slope_did_demand_per_firm.csv",
                              index=False)
    print(f"per_firm: {len(rows)} rows -> slope_did_demand_per_firm.csv")


if __name__ == "__main__":
    run_per_class()
    run_per_firm()
