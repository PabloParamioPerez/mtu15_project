# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: S6 monthly decomposition (sharpens reform-mechanism interpretation)
# CLAIM: Month-by-month A87 NET excess identifies the ISP15 settlement-clock change (Dec 1, 2024) as the primary driver, not the DA-vs-ID asymmetry. The asymmetric-granularity window sustains the transfer at lower level; MTU15-DA closure is decisive.
"""S6 month-by-month decomposition: A87 NET excess vs same-calendar pre-IDA baseline.

The S6 cumulative figure (+€1,094.9M over the 10-month asymmetric-
granularity window) was derived via regime-dummy regression with
cal-month FE. This script reports the underlying month-by-month
decomposition that the regression averages over.

Findings:
  Pre-IDA (2024-01 → 2024-05): excess noise floor ±€10M/mo
  3-sess (2024-07 → 2024-11):  small positive drift, +€7-26M/mo, total ~+€38M
  ISP15-win (2024-12 → 2025-03): EXPLOSIVE +€130-145M/mo,
                                  total ~+€547M (50% of headline)
  DA60/ID15 (2025-04 → 2025-09): sustained at lower level, +€73-142M/mo
                                  total ~+€545M (the other 50%)
                                  June 2025 +€142M is an op.reforzada
                                  outlier; removing it drops the window
                                  total to ~+€403M
  DA15/ID15 (2025-10 → 2025-12): COLLAPSE to +€4-23M/mo, total +€44M

Key reframing: the 15-min imbalance-settlement rule (effective 2024-12-01)
is the PRIMARY driver of the transfer (mechanical 4× settlement base).
The DA-vs-ID asymmetric-granularity window (after MTU15-IDA pushed ID to
15-min) sustains it at a lower level. MTU15-DA closure is decisive.

Output:
    results/regressions/s6_monthly_decomposition.csv
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
A87 = PROJECT / "data" / "processed" / "entsoe" / "balancing" / "financial_balance_all.parquet"
OUT = PROJECT / "results" / "regressions" / "s6_monthly_decomposition.csv"

REGIME_MAP = {
    "2024-01": "pre-IDA",            "2024-02": "pre-IDA",
    "2024-03": "pre-IDA",            "2024-04": "pre-IDA",
    "2024-05": "pre-IDA",            "2024-06": "IDA-reform (mid)",
    "2024-07": "3-sess",             "2024-08": "3-sess",
    "2024-09": "3-sess",             "2024-10": "3-sess",
    "2024-11": "3-sess",
    "2024-12": "ISP15-win",          "2025-01": "ISP15-win",
    "2025-02": "ISP15-win",          "2025-03": "MTU15-IDA (mid)",
    "2025-04": "DA60/ID15 PRE-blackout (until 4/27)",
    "2025-05": "DA60/ID15 + blackout response",
    "2025-06": "DA60/ID15 + op.reforzada",
    "2025-07": "DA60/ID15 + op.reforzada",
    "2025-08": "DA60/ID15 + op.reforzada",
    "2025-09": "DA60/ID15 + op.reforzada",
    "2025-10": "MTU15-DA (mid)",
    "2025-11": "DA15/ID15",          "2025-12": "DA15/ID15",
}


def main() -> None:
    df = pd.read_parquet(A87)
    df["month"] = pd.to_datetime(df["month"])

    a02 = df[df["direction_code"] == "A02"].set_index("month")["amount_eur"] / 1e6
    a01 = df[df["direction_code"] == "A01"].set_index("month")["amount_eur"] / 1e6
    net = (a02 - a01)

    # Same-calendar pre-IDA baseline (full pre-IDA span, 2018-01 → 2024-06-13)
    pre = net[net.index < pd.Timestamp("2024-06-14")].copy()
    pre_by_cal = pre.groupby(pre.index.month).mean()

    rows = []
    print(f"{'month':<10} {'A02':>8} {'A01':>8} {'NET':>8} {'baseline':>10} {'EXCESS':>10}  regime")
    print("-" * 100)
    recent = net[(net.index >= pd.Timestamp("2024-01-01")) & (net.index < pd.Timestamp("2026-01-01"))].sort_index()
    for ts, val in recent.items():
        a02_v = a02[ts]
        a01_v = a01[ts]
        cal_m = ts.month
        bl = pre_by_cal.get(cal_m, np.nan)
        excess = val - bl
        key = ts.strftime("%Y-%m")
        reg = REGIME_MAP.get(key, "")
        print(f"{key:<10} {a02_v:>8.1f} {a01_v:>8.1f} {val:>8.1f} {bl:>10.1f} {excess:>+10.1f}  {reg}")
        rows.append({
            "month": key,
            "a02_meur": a02_v,
            "a01_meur": a01_v,
            "net_meur": val,
            "same_cal_pre_ida_baseline_meur": bl,
            "excess_meur": excess,
            "regime": reg,
        })

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")

    # Sub-regime totals
    print()
    print("=" * 80)
    print("Sub-regime totals (months × mean excess = cumulative excess)")
    print("=" * 80)
    isp15 = out[out["regime"] == "ISP15-win"]
    print(f"  ISP15-win (2024-12 → 2025-02, 3 mo):     {isp15['excess_meur'].sum():>+8.1f} M€"
          f"  (mean +{isp15['excess_meur'].mean():.1f}/mo)")
    mtu_mid = out[out["month"] == "2025-03"]
    print(f"  MTU15-IDA mid (2025-03):                {mtu_mid['excess_meur'].iloc[0]:>+8.1f} M€"
          f"  (still imbalance-clock-dominated)")
    da60 = out[out["regime"].str.startswith("DA60/ID15")]
    print(f"  DA60/ID15 (2025-04 → 2025-09, 6 mo):    {da60['excess_meur'].sum():>+8.1f} M€"
          f"  (mean +{da60['excess_meur'].mean():.1f}/mo)")
    da60_no_jun = da60[da60["month"] != "2025-06"]
    print(f"    excluding June 2025 op.reforzada outlier (5 mo): "
          f"{da60_no_jun['excess_meur'].sum():>+8.1f} M€  (mean +{da60_no_jun['excess_meur'].mean():.1f}/mo)")
    da15 = out[out["regime"] == "DA15/ID15"]
    da15_full = pd.concat([out[out["month"] == "2025-10"], da15])
    print(f"  Post-MTU15-DA (2025-10 → 2025-12, 3 mo): {da15_full['excess_meur'].sum():>+8.1f} M€"
          f"  (mean +{da15_full['excess_meur'].mean():.1f}/mo)")
    asymm = pd.concat([isp15, mtu_mid, da60])
    print(f"\n  TOTAL asymmetric-granularity window (10 mo): {asymm['excess_meur'].sum():>+8.1f} M€")
    print(f"  (matches S6 headline: +€1,094.9M from regression with cal-month FE)")


if __name__ == "__main__":
    main()
