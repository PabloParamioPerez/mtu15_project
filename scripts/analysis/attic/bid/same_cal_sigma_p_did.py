# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 5 robustness -- same-calendar-month version of
#        the per-curve bid-shape DiD (Oct-Dec 24 vs Oct-Dec 25 for DA15).
#        Cleaner-than-Fourier robustness on sigma_p / N_eff for the
#        surviving Hydro and CCGT N_eff effects.
#
# Reads data/derived/panels/per_curve_metrics_da.parquet (953k DA curves
# 2024-06-14 -> 2026-01-09; coverage Oct-Dec 2024 is full so same-cal works).
#
# OUT: results/regressions/bid/mtu15_critical_flat/same_cal_sigma_p_did.csv

from pathlib import Path
import sys

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols, CRITICAL, FLAT  # noqa: E402

PANEL = REPO / "data/derived/panels/per_curve_metrics_da.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/same_cal_sigma_p_did.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

SAMECAL_PRE_LO = pd.Timestamp("2024-10-01"); SAMECAL_PRE_HI = pd.Timestamp("2024-12-31")
SAMECAL_POST_LO = pd.Timestamp("2025-10-01"); SAMECAL_POST_HI = pd.Timestamp("2025-12-31")
SAMECAL_T_REF = pd.Timestamp("2025-01-01")


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def run_did(panel, T_ref, outcome, tech_filter=None):
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    in_pre = (p["d"] >= SAMECAL_PRE_LO) & (p["d"] <= SAMECAL_PRE_HI)
    in_post = (p["d"] >= SAMECAL_POST_LO) & (p["d"] <= SAMECAL_POST_HI)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    if tech_filter is not None:
        p = p[p["tech"] == tech_filter].copy()
    p = p.dropna(subset=[outcome])
    if len(p) < 50: return None
    p["post"] = (p["d"] >= T_ref).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    cell = p.groupby(["post", "crit"])[outcome].mean().unstack()
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"].values,
                         p["crit_w"].values, p["post_crit_w"].values])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"outcome": outcome, "tech": tech_filter or "All", "n": len(p),
            "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
            "pre_crit": cell.loc[(0,1)] if (0,1) in cell.stack().index else np.nan,
            "post_crit": cell.loc[(1,1)] if (1,1) in cell.stack().index else np.nan,
            "pre_flat": cell.loc[(0,0)] if (0,0) in cell.stack().index else np.nan,
            "post_flat": cell.loc[(1,0)] if (1,0) in cell.stack().index else np.nan}


def main():
    print(f"Loading per-curve panel from {PANEL}...")
    import duckdb
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT d, period, clock_hour, unit_code, firm, tech, sigma_p, n_eff
        FROM '{PANEL}'
        WHERE d BETWEEN '{SAMECAL_PRE_LO.date()}' AND '{SAMECAL_POST_HI.date()}'
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    print(f"  {len(df):,} per-curve cells in same-cal window")
    print(f"  Tech distribution:")
    print(df["tech"].value_counts().to_string())

    rows = []
    print("\n=== DA15 same-cal Spec A DiD by tech ===")
    for tech in [None, "CCGT", "Hydro", "Hydro_pump"]:
        for outcome in ["sigma_p", "n_eff"]:
            r = run_did(df, SAMECAL_T_REF, outcome, tech_filter=tech)
            if r is None: continue
            rows.append({"spec": "DA15_samecal", **r})
            tlab = tech if tech else "All"
            print(f"  {tlab:12s} {outcome:10s}  DiD={r['DiD']:+8.3f}  se={r['se']:6.3f}  t={r['t']:+6.2f}  n={r['n']:,}")

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
