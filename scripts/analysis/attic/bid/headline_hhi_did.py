# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex Table 4 (tab:spec-c) -- recompute the
#        headline Spec~C DiD for the bid-stack concentration outcome HHI
#        (= 1 / N_eff), replacing the inverse-Herfindahl reading with
#        the IO-native Herfindahl. Uses the exact same per-curve panel
#        and windows as the headline N_eff DiD, with HHI = 1/n_eff per
#        curve, unit FE + clustered SE.
#
# WINDOWS (exactly match mtu15_critical_flat_did.py):
#   ID15 pre = 2024-12-19 to 2025-03-18 ; post = 2025-03-19 to 2025-04-27
#   DA15 pre = 2025-07-01 to 2025-09-30 ; post = 2025-10-01 to 2025-12-31
#
# OUT: results/regressions/bid/mtu15_critical_flat/headline_hhi_did.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import sys

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols, CRITICAL, FLAT  # noqa: E402

DA_PANEL  = REPO / "data/derived/panels/per_curve_metrics_da_full.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT       = REPO / "results/regressions/bid/mtu15_critical_flat/headline_hhi_did.csv"

WINDOWS = {
    "ID15": ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "DA15": ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),
}

TECHS = ["CCGT", "Hydro", "Hydro_pump"]


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def fetch_window(panel_fp, lo, hi, has_session=False):
    con = duckdb.connect()
    cols = "d, period, clock_hour, unit_code, firm, tech, n_tranche, n_eff"
    if has_session:
        cols += ", session_number"
    df = con.execute(f"""
        SELECT {cols}
        FROM '{panel_fp}'
        WHERE d BETWEEN '{lo}' AND '{hi}'
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    df["hhi"] = 1.0 / df["n_eff"]
    return df


def run_did(panel, pre_lo, pre_hi, post_lo, post_hi, T_ref, tech, outcome="hhi"):
    p = panel.copy()
    p = p[p["tech"] == tech].copy()
    p = p[p["hour_class"].isin(["Critical", "Flat"])]
    p = p.dropna(subset=[outcome])
    if len(p) < 50:
        return None
    in_pre  = (p["d"] >= pd.Timestamp(pre_lo))  & (p["d"] <= pd.Timestamp(pre_hi))
    in_post = (p["d"] >= pd.Timestamp(post_lo)) & (p["d"] <= pd.Timestamp(post_hi))
    p = p[in_pre | in_post].copy()
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    # Unit FE within transform
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"].values,
                         p["crit_w"].values, p["post_crit_w"].values])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    cell_means = p.groupby(["post", "crit"])[outcome].mean().to_dict()
    return {"tech": tech, "n": len(p),
            "DiD": beta[3], "se": se[3], "t": beta[3] / se[3],
            "pre_crit_mean": cell_means.get((0, 1), np.nan),
            "post_crit_mean": cell_means.get((1, 1), np.nan),
            "pre_flat_mean": cell_means.get((0, 0), np.nan),
            "post_flat_mean": cell_means.get((1, 0), np.nan)}


def main():
    rows = []
    for reform, (pre_lo, pre_hi, post_lo, post_hi) in WINDOWS.items():
        T_ref = post_lo
        # DA panel
        print(f"\n=== {reform} DA HHI ===", flush=True)
        df_da = fetch_window(DA_PANEL, pre_lo, post_hi)
        print(f"  rows: {len(df_da):,}")
        for tech in TECHS:
            r = run_did(df_da, pre_lo, pre_hi, post_lo, post_hi, T_ref, tech, "hhi")
            if r is None:
                print(f"  {tech:<12s} (insufficient data)")
                continue
            stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
            rows.append({"reform": reform, "market": "DA", **r})
            print(f"  {tech:<12s} HHI DiD = {r['DiD']:+.4f} (SE {r['se']:.4f}) t={r['t']:+.2f}{stars}  "
                  f"crit pre→post {r['pre_crit_mean']:.3f}→{r['post_crit_mean']:.3f}  "
                  f"flat pre→post {r['pre_flat_mean']:.3f}→{r['post_flat_mean']:.3f}  n={r['n']:,}")

        # IDA panel (pooled across sessions like the headline)
        print(f"\n=== {reform} IDA HHI ===", flush=True)
        df_ida = fetch_window(IDA_PANEL, pre_lo, post_hi, has_session=True)
        print(f"  rows: {len(df_ida):,}")
        for tech in TECHS:
            r = run_did(df_ida, pre_lo, pre_hi, post_lo, post_hi, T_ref, tech, "hhi")
            if r is None:
                print(f"  {tech:<12s} (insufficient data)")
                continue
            stars = "***" if abs(r["t"]) >= 2.58 else "**" if abs(r["t"]) >= 1.96 else "*" if abs(r["t"]) >= 1.645 else ""
            rows.append({"reform": reform, "market": "IDA", **r})
            print(f"  {tech:<12s} HHI DiD = {r['DiD']:+.4f} (SE {r['se']:.4f}) t={r['t']:+.2f}{stars}  "
                  f"crit pre→post {r['pre_crit_mean']:.3f}→{r['post_crit_mean']:.3f}  "
                  f"flat pre→post {r['pre_flat_mean']:.3f}→{r['post_flat_mean']:.3f}  n={r['n']:,}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
