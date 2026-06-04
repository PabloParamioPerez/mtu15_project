# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Spec C robustness battery on the four bid-shape outcomes
#        (alpha, beta, gamma, N_eff). Re-runs the appendix checks that
#        were originally specified on sigma_p:
#          (1) Pre-trend midpoint placebo: split each reform's pre-window
#              at its midpoint, run a fake critical/flat DiD on the pre
#              halves. Genuine reform effects should be near-zero on the
#              placebo.
#          (2) Midday vs flat falsification: substitute midday hours for
#              critical hours in the DiD. Should agree on DA15 DA cells
#              (broad-based widening) and diverge on ID15 IDA cells.
#          (3) Bandwidth robustness: re-run main DiD at wider deltas =
#              {100, 140, 200} EUR/MWh using the slope_*_h{H} panels.
#
# OUT: results/regressions/bid/mtu15_critical_flat/
#        slope_did_pretrend_placebo.csv
#        slope_did_midday_falsification.csv
#        slope_did_bandwidth.csv

from pathlib import Path
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SLOPE_DIR = REPO / "data/derived/panels/per_curve_slope_windowed"
N_EFF_DA  = REPO / "data/derived/panels/per_curve_metrics_da.parquet"
N_EFF_IDA = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT_DIR   = REPO / "results/regressions/bid/mtu15_critical_flat"
OUT_DIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT     = {1, 2, 3}
MIDDAY   = {11, 12, 13, 14}

PANELS = {
    ("DA15", "DA",  50): SLOPE_DIR / "slope_DA15_real_DA_h50.parquet",
    ("DA15", "IDA", 58): SLOPE_DIR / "slope_DA15_real_IDA_h58.parquet",
    ("ID15", "DA",  50): SLOPE_DIR / "slope_ID15_real_DA_h50.parquet",
    ("ID15", "IDA", 62): SLOPE_DIR / "slope_ID15_real_IDA_h62.parquet",
}
WINDOWS = {
    "DA15": {"pre_lo": "2025-04-28", "pre_hi": "2025-09-30",
             "post_lo": "2025-10-01", "post_hi": "2026-02-26"},
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


def did_unit_FE(p, outcome, critical_set, post_lo):
    """Critical/flat DiD with unit FE on a subset already filtered to a
    (tech, market, reform) cell, with critical defined by critical_set."""
    d = p.dropna(subset=[outcome]).copy()
    if len(d) < 100:
        return None
    d["post"] = (d["d"] >= post_lo).astype(int)
    d["crit"] = d["clock_hour"].isin(critical_set).astype(int)
    d = d[d["clock_hour"].isin(critical_set | FLAT)]
    if len(d) < 100: return None
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


def load_panel(reform, market_label, h):
    path = PANELS.get((reform, market_label, h))
    if path is None or not path.exists():
        return None
    df = pd.read_parquet(path)
    df["d"] = pd.to_datetime(df["d"])
    return df


def filter_window(p, reform):
    w = WINDOWS[reform]
    return p[((p["d"] >= w["pre_lo"]) & (p["d"] <= w["pre_hi"])) |
             ((p["d"] >= w["post_lo"]) & (p["d"] <= w["post_hi"]))].copy()


def _run_did(p_win, outcomes, critical_set, post_lo, tag_extra):
    out = []
    for tech in ["CCGT", "Hydro", "Hydro_pump"]:
        p_t = p_win[p_win["tech"] == tech]
        if p_t.empty: continue
        for outcome in outcomes:
            if outcome not in p_t.columns: continue
            r = did_unit_FE(p_t, outcome, critical_set, post_lo)
            if r is None: continue
            r.update({"tech": tech, "outcome": outcome, **tag_extra})
            out.append(r)
    return out


# ---------------------------------------------------------------------------
# (1) Pre-trend midpoint placebo
# ---------------------------------------------------------------------------
def pretrend_placebo():
    rows = []
    for (reform, market_label, h), path in PANELS.items():
        p = load_panel(reform, market_label, h)
        if p is None: continue
        w = WINDOWS[reform]
        pre = p[(p["d"] >= w["pre_lo"]) & (p["d"] <= w["pre_hi"])].copy()
        if len(pre) < 100: continue
        mid = pd.to_datetime(w["pre_lo"]) + (pd.to_datetime(w["pre_hi"]) -
                                              pd.to_datetime(w["pre_lo"])) / 2
        # Use the same did_unit_FE machinery: treat pre-mid as "pre",
        # post-mid as "post", flat as control, critical as treated.
        rows.extend(_run_did(pre, ["slope", "intercept", "gamma"],
                             CRITICAL, mid.strftime("%Y-%m-%d"),
                             {"market": market_label, "reform": reform, "h": h}))
    pd.DataFrame(rows).to_csv(OUT_DIR / "slope_did_pretrend_placebo.csv",
                              index=False)
    print(f"pretrend placebo: {len(rows)} rows -> slope_did_pretrend_placebo.csv")


# ---------------------------------------------------------------------------
# (2) Midday vs flat falsification
# ---------------------------------------------------------------------------
def midday_falsification():
    rows = []
    for (reform, market_label, h), path in PANELS.items():
        p = load_panel(reform, market_label, h)
        if p is None: continue
        p_win = filter_window(p, reform)
        rows.extend(_run_did(p_win, ["slope", "intercept", "gamma"],
                             MIDDAY, WINDOWS[reform]["post_lo"],
                             {"market": market_label, "reform": reform, "h": h}))
    pd.DataFrame(rows).to_csv(OUT_DIR / "slope_did_midday_falsification.csv",
                              index=False)
    print(f"midday falsification: {len(rows)} rows -> "
          f"slope_did_midday_falsification.csv")


# ---------------------------------------------------------------------------
# (3) Bandwidth robustness
# ---------------------------------------------------------------------------
def bandwidth_robustness():
    rows = []
    for reform, market_label in [("DA15", "DA"), ("DA15", "IDA"),
                                  ("ID15", "DA"), ("ID15", "IDA")]:
        for h in [100, 140, 200]:
            cell = (reform, market_label, h)
            # construct panel filename
            path = SLOPE_DIR / f"slope_{reform}_real_{market_label}_h{h}.parquet"
            if not path.exists():
                print(f"  missing {path}; skipping")
                continue
            p = pd.read_parquet(path); p["d"] = pd.to_datetime(p["d"])
            p_win = filter_window(p, reform)
            r = _run_did(p_win, ["slope", "intercept", "gamma"],
                         CRITICAL, WINDOWS[reform]["post_lo"],
                         {"market": market_label, "reform": reform, "h": h})
            rows.extend(r)
    pd.DataFrame(rows).to_csv(OUT_DIR / "slope_did_bandwidth.csv", index=False)
    print(f"bandwidth: {len(rows)} rows -> slope_did_bandwidth.csv")


if __name__ == "__main__":
    pretrend_placebo()
    midday_falsification()
    bandwidth_robustness()
