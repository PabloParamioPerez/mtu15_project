# STATUS: ALIVE
# LAST-AUDIT: 2026-06-14
# FEEDS: thesis sec:empirical:bidshape -- answers the advisor's question on the
#        rationale for DiD vs a simple before-after comparison. DiD is most
#        compelling when an obvious confound threatens before-after and the
#        control purges it. Here the bid-shape outcomes are built relative to the
#        contemporaneous clearing price (bandwidth delta), so seasonality in the
#        LEVEL is already differenced out, and there is no single dominant
#        before-after threat -- the case for DiD rests on the existence of a
#        clean within-day control (flat hours), not on rescuing a biased
#        before-after. The right diagnostic is therefore whether the simple
#        before-after in the treated (critical) hours points the SAME way as the
#        DiD. If flat hours barely move, the two coincide; if they move with
#        critical, the DiD is the more conservative read.
#
# For each headline cell and outcome (sigma_p, HHI) over the tight window:
#   ba_crit = post coefficient, critical-hour subsample only (unit FE)
#   ba_flat = post coefficient, flat-hour subsample only    (unit FE)
#   did     = post*crit coefficient, both classes            (unit FE)
# By construction did = ba_crit - ba_flat. We report all three and flag whether
# sign(ba_crit) == sign(did). SEs clustered by date.
#
# OUT: results/regressions/bid/mtu15_critical_flat/before_after_vs_did.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import clustered_ols, CRITICAL, FLAT  # noqa: E402

DA_PANEL = REPO / "data/derived/panels/per_curve_metrics_da_full.parquet"
IDA_PANEL = REPO / "data/derived/panels/per_curve_metrics_ida.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/before_after_vs_did.csv"

WINDOWS = {
    "ID15_DA":  ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "ID15_IDA": ("2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    "DA15_DA":  ("2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),
}
CELLS = [
    ("DA15_DA",  "CCGT",       DA_PANEL),
    ("DA15_DA",  "Hydro_pump", DA_PANEL),
    ("ID15_DA",  "CCGT",       DA_PANEL),
    ("ID15_IDA", "Hydro_pump", IDA_PANEL),
]


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT:     return "Flat"
    return "Other"


def load_panel(panel_fp, lo, hi, tech, has_session=False):
    con = duckdb.connect()
    sess = "session_number," if has_session else ""
    df = con.execute(f"""
        SELECT d, {sess} period, clock_hour, unit_code, tech, sigma_p, n_eff
        FROM '{panel_fp}'
        WHERE d BETWEEN '{lo}' AND '{hi}' AND tech = '{tech}'
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    df = df[df["hour_class"].isin(["Critical", "Flat"])].copy()
    df["hhi"] = 1.0 / df["n_eff"]
    return df


def before_after(p, outcome, T_ref):
    """post coefficient on a single hour-class subsample, unit FE, clustered SE."""
    p = p.dropna(subset=[outcome]).copy()
    if p["unit_code"].nunique() < 2 or len(p) < 50:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    gmc = p.groupby("unit_code")["post"].transform("mean")
    p["post_w"] = p["post"] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"]])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"b": beta[1], "se": se[1], "t": beta[1] / se[1], "n": len(p)}


def did_basic(p, outcome, T_ref):
    p = p.dropna(subset=[outcome]).copy()
    if len(p) < 100:
        return None
    p["post"] = (p["d"] >= pd.Timestamp(T_ref)).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    gm = p.groupby("unit_code")[outcome].transform("mean")
    p["y_w"] = p[outcome] - gm
    for c in ["post", "crit", "post_crit"]:
        gmc = p.groupby("unit_code")[c].transform("mean")
        p[c + "_w"] = p[c] - gmc
    X = np.column_stack([np.ones(len(p)), p["post_w"], p["crit_w"], p["post_crit_w"]])
    beta, se = clustered_ols(p["y_w"].values, X, p["d"].astype(str).values)
    return {"b": beta[3], "se": se[3], "t": beta[3] / se[3], "n": len(p)}


def star(t):
    a = abs(t)
    return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def main():
    rows = []
    for reform_market, tech, panel_fp in CELLS:
        pre_lo, pre_hi, post_lo, post_hi = WINDOWS[reform_market]
        T_ref = post_lo
        p = load_panel(panel_fp, pre_lo, post_hi, tech, has_session=("IDA" in reform_market))
        print(f"\n=== {reform_market} {tech} ===")
        for outcome in ("sigma_p", "hhi"):
            crit = before_after(p[p["hour_class"] == "Critical"], outcome, T_ref)
            flat = before_after(p[p["hour_class"] == "Flat"], outcome, T_ref)
            did = did_basic(p, outcome, T_ref)
            if crit is None or did is None:
                continue
            agree = (np.sign(crit["b"]) == np.sign(did["b"]))
            rows.append({
                "cell": f"{reform_market} {tech}", "outcome": outcome,
                "ba_crit": round(crit["b"], 4), "ba_crit_t": round(crit["t"], 2),
                "ba_flat": round(flat["b"], 4) if flat else np.nan,
                "ba_flat_t": round(flat["t"], 2) if flat else np.nan,
                "did": round(did["b"], 4), "did_t": round(did["t"], 2),
                "sign_agree": agree,
            })
            fb = f"{flat['b']:+.4f}{star(flat['t'])}" if flat else "   n/a"
            print(f"  {outcome:>8s}: BA_crit={crit['b']:+.4f}{star(crit['t'])}  "
                  f"BA_flat={fb}  DiD={did['b']:+.4f}{star(did['t'])}  "
                  f"sign(BA_crit)==sign(DiD): {agree}")
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    n_agree = int(out["sign_agree"].sum())
    print(f"\nSign agreement (BA_crit vs DiD): {n_agree}/{len(out)} cells")
    print(f"Wrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
