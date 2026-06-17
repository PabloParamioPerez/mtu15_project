# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis S6.4 identifying-assumption check. The bid-shape critical-flat
#        DiD differences out the residual-demand-slope CHANGE on the assumption
#        that the reform shifts the slope b_{mt} by a common amount across hour
#        classes ("shifts every hour alike", S3 corollary). This script TESTS
#        that assumption directly: it runs the SAME critical-flat DiD with the
#        per-(date,period,firm) slope b as the outcome. If the post x crit
#        coefficient on b is small/insignificant relative to the b level, the
#        slope change is common across hour classes and the differencing is
#        valid; a large coefficient would threaten it.
#
# OUT: results/regressions/bid/mtu15_critical_flat/slope_change_critical_flat.csv

from pathlib import Path
import sys
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import hour_class_label, clustered_ols  # noqa: E402

PANEL = REPO / "data/derived/panels/per_firm_residual_demand_slope.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/slope_change_critical_flat.csv"

# (reform, market, pre_lo, pre_hi, post_lo, post_hi) -- EXACT bid-shape DiD windows
# (tab:spec-c): tight, reforzada-constant, pre-blackout for ID15.
CELLS = [
    ("ID15", "DA",  "2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),  # CCGT DA: anticipation
    ("ID15", "IDA", "2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),  # own market
    ("DA15", "DA",  "2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),  # CCGT DA: own reform
    ("DA15", "IDA", "2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31"),  # cross-market
]


def star(t):
    a = abs(t)
    return "***" if a >= 2.58 else "**" if a >= 1.96 else "*" if a >= 1.645 else ""


def load():
    con = duckdb.connect()
    df = con.execute(
        f'select d, period, market, focal_firm, b_residual_mw_per_eur as b, "window" as win '
        f"from '{PANEL}'"
    ).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    # MTU per (d, market): >30 periods => 15-min day
    mxp = df.groupby(["d", "market"])["period"].transform("max")
    df["mtu15"] = (mxp > 30).astype(int)
    # clock hour 0..23 (matches the DiD scripts' convention)
    df["clock_hour"] = np.where(df["mtu15"] == 1,
                                (df["period"] - 1) // 4,
                                df["period"] - 1)
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df


def did(panel, pre_lo, pre_hi, post_lo, post_hi):
    pre_lo, pre_hi, post_lo, post_hi = map(pd.Timestamp, (pre_lo, pre_hi, post_lo, post_hi))
    in_pre = (panel["d"] >= pre_lo) & (panel["d"] <= pre_hi)
    in_post = (panel["d"] >= post_lo) & (panel["d"] <= post_hi)
    q = panel[in_pre | in_post].copy()
    q = q[q["hour_class"].isin(["Critical", "Flat"])].dropna(subset=["b"])
    q["post"] = (q["d"] >= post_lo).astype(int)
    q["crit"] = (q["hour_class"] == "Critical").astype(int)
    q["pc"] = q["post"] * q["crit"]
    # within-firm demeaning (firm FE), cluster by date
    q["y_w"] = q["b"] - q.groupby("focal_firm")["b"].transform("mean")
    for c in ["post", "crit", "pc"]:
        q[c + "_w"] = q[c] - q.groupby("focal_firm")[c].transform("mean")
    X = np.column_stack([np.ones(len(q)), q["post_w"], q["crit_w"], q["pc_w"]])
    beta, se = clustered_ols(q["y_w"].values, X, q["d"].astype(str).values)
    # cell means for context
    m = q.groupby(["post", "crit"])["b"].mean()
    pre_f, pre_c = m.get((0, 0), np.nan), m.get((0, 1), np.nan)
    post_f, post_c = m.get((1, 0), np.nan), m.get((1, 1), np.nan)
    return dict(
        n=len(q),
        b_pre_flat=pre_f, b_pre_crit=pre_c, b_post_flat=post_f, b_post_crit=post_c,
        d_b_flat=post_f - pre_f, d_b_crit=post_c - pre_c,
        did_b=beta[3], se=se[3], t=beta[3] / se[3] if se[3] else np.nan,
    )


def main():
    df = load()
    rows = []
    for reform, market, pre_lo, pre_hi, post_lo, post_hi in CELLS:
        r = did(df[df["market"] == market], pre_lo, pre_hi, post_lo, post_hi)
        r = {"reform": reform, "market": market, **r}
        rows.append(r)
        print(f"\n=== {reform}  ({market}: {pre_lo}..{pre_hi} -> {post_lo}..{post_hi}) ===  n={r['n']:,}")
        print(f"  b  (MW per EUR/MWh)   flat            critical")
        print(f"    pre   {r['b_pre_flat']:8.2f}        {r['b_pre_crit']:8.2f}")
        print(f"    post  {r['b_post_flat']:8.2f}        {r['b_post_crit']:8.2f}")
        print(f"    Db    {r['d_b_flat']:+8.2f}        {r['d_b_crit']:+8.2f}")
        print(f"  DiD (post x crit) on b = {r['did_b']:+.3f}{star(r['t'])} "
              f"(SE {r['se']:.3f}, t={r['t']:+.2f})")
        # express the differential change relative to the flat-hour change and the level
        denom = abs(r['d_b_flat']) if r['d_b_flat'] else np.nan
        print(f"    |DiD| / |Db_flat| = {abs(r['did_b'])/denom:.2f}   "
              f"DiD / mean pre-b = {r['did_b']/np.nanmean([r['b_pre_flat'], r['b_pre_crit']]):+.1%}")
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nWrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
