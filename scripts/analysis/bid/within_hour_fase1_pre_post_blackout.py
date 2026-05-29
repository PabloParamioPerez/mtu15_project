# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo discussion -- did REE's reforzada reduce within-hour
#        volatility of Fase I redispatch after the blackout?
#
# Source: ESIOS Total RP48 PRE-cierre (15-min granularity, system-wide
# redispatch volumes by tipo_redespacho). qty_up_mwh is the upward
# redispatch needed to cover the BUP constraint (what reforzada inflates).
#
# Method: both pre- and post-blackout sub-windows are within the MTU15-IDA
# regime (post-2025-03-19, pre-2025-10-01). The only thing that differs
# is the blackout / reforzada stance.
#
#   Pre-blackout MTU15-IDA:  2025-03-19 -> 2025-04-27 (40 days, no reforzada)
#   Post-blackout MTU15-IDA: 2025-04-28 -> 2025-09-30 (156 days, reforzada)
#
# For each (date, clock-hour) sum qty_up_mwh over all tipo_redespacho to
# get total Fase I up-redispatch per 15-min slot. Then compute SD across
# the 4 quarters within the clock-hour. Compare pre/post.
#
# OUT: results/regressions/bid/mtu15_critical_flat/within_hour_fase1_blackout.csv

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from scipy.stats import ttest_ind

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "data/processed/esios/restricciones/totalrp48preccierre_*.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/within_hour_fase1_blackout.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def main():
    con = duckdb.connect()
    sql = f"""
    WITH base AS (
      SELECT
        CAST(period_start_utc AS DATE) d,
        period_start_utc,
        EXTRACT(HOUR FROM (period_start_utc AT TIME ZONE 'Europe/Madrid')) AS clock_hour,
        EXTRACT(MINUTE FROM (period_start_utc AT TIME ZONE 'Europe/Madrid')) AS minute_within_hour,
        COALESCE(qty_up_mwh, 0) AS qty_up,
        COALESCE(qty_down_mwh, 0) AS qty_down
      FROM '{SRC}'
      WHERE CAST(period_start_utc AS DATE) BETWEEN '2025-03-19' AND '2025-09-30'
    ),
    per_qtr AS (
      SELECT d, clock_hour, minute_within_hour,
             SUM(qty_up) AS qty_up_total,
             SUM(qty_down) AS qty_down_total
      FROM base
      GROUP BY 1, 2, 3
    )
    SELECT d, clock_hour,
           STDDEV_POP(qty_up_total) AS sd_up_within_hour,
           STDDEV_POP(qty_down_total) AS sd_down_within_hour,
           AVG(qty_up_total) AS mean_up,
           AVG(qty_down_total) AS mean_down,
           COUNT(*) AS n_quarters
    FROM per_qtr
    GROUP BY 1, 2
    HAVING COUNT(*) = 4
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    df["period"] = np.where(df["d"] <= pd.Timestamp("2025-04-27"), "pre-blackout",
                  np.where(df["d"] >= pd.Timestamp("2025-04-28"), "post-blackout", "drop"))
    df = df[df["period"] != "drop"]

    print(f"\n=== {len(df):,} (date, clock-hour) cells with all 4 quarters ===")
    print(f"date range: {df['d'].min().date()} -> {df['d'].max().date()}")

    print("\n--- LEVEL: mean Fase I up-redispatch per quarter (MWh) ---")
    for hc in ["Critical", "Flat", "Other", "ALL"]:
        sub = df if hc == "ALL" else df[df["hour_class"] == hc]
        pre = sub[sub["period"] == "pre-blackout"]["mean_up"]
        post = sub[sub["period"] == "post-blackout"]["mean_up"]
        if len(pre) == 0 or len(post) == 0: continue
        t, p = ttest_ind(post, pre, equal_var=False, nan_policy="omit")
        print(f"  {hc:9s}  pre={pre.mean():7.1f} (n={len(pre):,})  "
              f"post={post.mean():7.1f} (n={len(post):,})  "
              f"diff={post.mean()-pre.mean():+7.1f}  t={t:+5.2f}  p={p:.3f}")

    print("\n--- WITHIN-HOUR SD of Fase I up-redispatch (MWh, across 4 quarters of clock-hour) ---")
    rows = []
    for hc in ["Critical", "Flat", "Other", "ALL"]:
        sub = df if hc == "ALL" else df[df["hour_class"] == hc]
        pre = sub[sub["period"] == "pre-blackout"]["sd_up_within_hour"]
        post = sub[sub["period"] == "post-blackout"]["sd_up_within_hour"]
        if len(pre) == 0 or len(post) == 0: continue
        t, p = ttest_ind(post, pre, equal_var=False, nan_policy="omit")
        pre_m, post_m = pre.mean(), post.mean()
        print(f"  {hc:9s}  pre={pre_m:7.2f} (n={len(pre):,})  "
              f"post={post_m:7.2f} (n={len(post):,})  "
              f"diff={post_m-pre_m:+7.2f}  t={t:+5.2f}  p={p:.3f}")
        rows.append({"hour_class": hc, "metric": "fase1_up_within_hour_sd",
                     "pre_mean": pre_m, "pre_n": len(pre),
                     "post_mean": post_m, "post_n": len(post),
                     "diff": post_m - pre_m, "t": t, "p": p})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
