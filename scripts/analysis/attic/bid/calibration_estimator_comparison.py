# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 2 -- compare the FE-regression theta_crit
#        (with date FE + critical indicator) against the simple two-step
#        cross-day mean of (mean critical - mean flat). These coincide for
#        the system-aggregate spec (balanced) and may differ for per-unit
#        specs where each day's unit coverage varies.
#
# OUT: results/regressions/bid/mtu15_critical_flat/calibration_estimator_compare.csv

from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import (  # noqa: E402
    clustered_ols, WINDOWS, hour_class_label,
)

DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/calibration_estimator_compare.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)


def build_dispersion(per_curve):
    """Per (unit, date, clock_hour[, session]) cell with 4 quarters: D_sigma, D_N."""
    pc = per_curve.copy()
    pc["quarter"] = pc["period"].mod(4)
    group_cols = ["unit_code", "d", "clock_hour"]
    if "session_number" in pc.columns and pc["session_number"].notna().any():
        group_cols.append("session_number")
    nq = pc.groupby(group_cols)["quarter"].nunique().rename("nq").reset_index()
    pc = pc.merge(nq, on=group_cols)
    pc = pc[pc["nq"] == 4].copy()
    if pc.empty:
        return pd.DataFrame()
    agg = (pc.groupby(group_cols + ["hour_class"])
             .agg(D_sigma=("sigma_p", lambda x: np.std(x.values, ddof=1)),
                  D_N=("n_eff", lambda x: np.std(x.values, ddof=1)))
             .reset_index())
    return agg


def fe_estimator(d, outcome):
    """FE regression: outcome = date_FE + theta * crit_indicator. SE clustered by date."""
    sub = d.dropna(subset=[outcome]).copy()
    if len(sub) < 30:
        return None
    sub["y_d"] = sub[outcome] - sub.groupby("d")[outcome].transform("mean")
    sub["crit_d"] = sub["crit"] - sub.groupby("d")["crit"].transform("mean")
    X = np.column_stack([np.ones(len(sub)), sub["crit_d"].values])
    beta, se = clustered_ols(sub["y_d"].values, X, sub["d"].astype(str).values)
    return {"theta": beta[1], "se": se[1], "t": beta[1] / se[1], "n": len(sub)}


def twostep_estimator(d, outcome):
    """Two-step: for each day, mean(critical) - mean(flat); then average across days. SE = sd / sqrt(N_days)."""
    sub = d.dropna(subset=[outcome]).copy()
    if len(sub) < 30:
        return None
    daily = (sub.groupby(["d", "crit"])[outcome].mean().unstack("crit")
                .rename(columns={0: "mean_flat", 1: "mean_crit"})
                .dropna())
    if daily.empty:
        return None
    daily["diff"] = daily["mean_crit"] - daily["mean_flat"]
    diffs = daily["diff"].values
    return {
        "theta": float(np.mean(diffs)),
        "se": float(np.std(diffs, ddof=1) / np.sqrt(len(diffs))),
        "t": float(np.mean(diffs) / (np.std(diffs, ddof=1) / np.sqrt(len(diffs)))),
        "n_days": len(diffs),
    }


def run_outcome(panel, reform, outcome, market_label):
    w = WINDOWS[reform]
    post_lo, post_hi = pd.Timestamp(w["post_lo"]), pd.Timestamp(w["post_hi"])
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    p = p[(p["d"] >= post_lo) & (p["d"] <= post_hi)
          & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    fe = fe_estimator(p, outcome)
    ts = twostep_estimator(p, outcome)
    if fe is None or ts is None:
        return None
    return {
        "reform": reform, "market": market_label, "outcome": outcome,
        "fe_theta": fe["theta"], "fe_se": fe["se"], "fe_t": fe["t"], "fe_n": fe["n"],
        "ts_theta": ts["theta"], "ts_se": ts["se"], "ts_t": ts["t"], "ts_n_days": ts["n_days"],
    }


def main():
    rows = []
    da = pd.read_parquet(DA_PANEL)
    da["d"] = pd.to_datetime(da["d"])
    da_disp = build_dispersion(da)
    print(f"DA dispersion panel: {len(da_disp):,} (unit, date, hour) cells with 4 quarters")
    for outcome in ["D_sigma", "D_N"]:
        r = run_outcome(da_disp, "DA15", outcome, "DA")
        if r is not None:
            rows.append(r)
            print(f"  DA15 {outcome:8s}  FE={r['fe_theta']:+.4f} ({r['fe_se']:.4f})  "
                  f"TwoStep={r['ts_theta']:+.4f} ({r['ts_se']:.4f})  "
                  f"diff={(r['ts_theta']-r['fe_theta']):+.4f}")

    if IDA_PANEL.exists():
        ida = pd.read_parquet(IDA_PANEL)
        ida["d"] = pd.to_datetime(ida["d"])
        ida_disp = build_dispersion(ida)
        print(f"\nIDA dispersion panel: {len(ida_disp):,} cells with 4 quarters")
        for outcome in ["D_sigma", "D_N"]:
            r = run_outcome(ida_disp, "ID15", outcome, "IDA")
            if r is not None:
                rows.append(r)
                print(f"  ID15 {outcome:8s}  FE={r['fe_theta']:+.4f} ({r['fe_se']:.4f})  "
                      f"TwoStep={r['ts_theta']:+.4f} ({r['ts_se']:.4f})  "
                      f"diff={(r['ts_theta']-r['fe_theta']):+.4f}")
    if rows:
        pd.DataFrame(rows).to_csv(OUT, index=False)
        print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
