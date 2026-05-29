# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo discussion -- did REE's reforzada reduce within-hour
#        volatility after the blackout (2025-04-28)?
#
# Method: both pre- and post-blackout sub-windows are within the MTU15-IDA
# regime (post-2025-03-19, pre-2025-10-01), so granularity is held constant.
# What differs is the reforzada stance (active post-2025-04-28).
#
#   Pre-blackout MTU15-IDA:  2025-03-19 -> 2025-04-27 (40 days, no reforzada)
#   Post-blackout MTU15-IDA: 2025-04-28 -> 2025-09-30 (156 days, reforzada)
#
# For each (date, IDA session, clock-hour) compute the SD across the four
# 15-min IDA quarter prices in that clock-hour, then take the daily mean
# (across IDA sessions and clock-hours). Compare the daily-mean series
# pre vs post blackout.
#
# Three views:
#  (a) System IDA quarter-price SD (across-quarter SD of MCP_q within a clock-hour)
#  (b) Residual demand SD (sd_q[load - wind - solar] from ENTSO-E A65+A75)
#  (c) Same-calendar 2024 placebo if data available (not central; main test
#      is direct pre/post-blackout within MTU15-IDA)
#
# OUT: results/regressions/bid/mtu15_critical_flat/within_hour_blackout.csv
#      (one row per metric x sub-window with mean/sd/n stats)

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/within_hour_blackout.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

PRE_LO = "2025-03-19"; PRE_HI = "2025-04-27"   # MTU15-IDA, no reforzada
POST_LO = "2025-04-28"; POST_HI = "2025-09-30"  # MTU15-IDA + reforzada


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def daily_within_hour_sd():
    """For each (date, session, clock-hour) compute SD across the 4 IDA
    quarter prices in that clock-hour. Filter to MTU15-IDA window where the
    file has 4 quarter prices per session-hour."""
    con = duckdb.connect()
    sql = f"""
    WITH p AS (
      SELECT CAST(date AS DATE) d, session_number, period,
             CAST(price_es_eur_mwh AS DOUBLE) p_es,
             COALESCE(mtu_minutes, 60) mtu
      FROM '{MPIBC}'
      WHERE date BETWEEN '{PRE_LO}' AND '{POST_HI}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    pq AS (
      SELECT d, session_number, period, p_es,
             CASE WHEN mtu = 60 THEN period - 1
                  ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
             mtu
      FROM p
    )
    SELECT d, session_number, clock_hour,
           STDDEV_POP(p_es) AS sd_p_within_hour,
           COUNT(*) AS n_quarters,
           MAX(mtu) AS mtu
    FROM pq
    GROUP BY 1, 2, 3
    HAVING COUNT(*) = 4 AND MAX(mtu) = 15
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["hour_class"] = df["clock_hour"].apply(hour_class)
    return df


def main():
    df = daily_within_hour_sd()
    print(f"\n=== {len(df):,} (date, session, clock-hour) cells with all 4 quarter prices ===")
    print(f"date range: {df['d'].min().date()} -> {df['d'].max().date()}")

    df["period"] = np.where(df["d"] <= pd.Timestamp(PRE_HI), "pre-blackout",
                  np.where(df["d"] >= pd.Timestamp(POST_LO), "post-blackout",
                           "drop"))
    df = df[df["period"] != "drop"]

    rows = []
    print("\n--- System within-hour IDA quarter-price SD (EUR/MWh) ---")
    for hc in ["Critical", "Flat", "Other", "ALL"]:
        sub = df if hc == "ALL" else df[df["hour_class"] == hc]
        pre = sub[sub["period"] == "pre-blackout"]["sd_p_within_hour"]
        post = sub[sub["period"] == "post-blackout"]["sd_p_within_hour"]
        if len(pre) == 0 or len(post) == 0:
            continue
        # Welch test
        from scipy.stats import ttest_ind
        t_stat, p_val = ttest_ind(post, pre, equal_var=False, nan_policy="omit")
        pre_m, pre_sd, pre_n = pre.mean(), pre.std(), len(pre)
        post_m, post_sd, post_n = post.mean(), post.std(), len(post)
        diff = post_m - pre_m
        print(f"  {hc:9s}  pre={pre_m:6.2f} (n={pre_n:,})  "
              f"post={post_m:6.2f} (n={post_n:,})  "
              f"diff={diff:+6.2f}  t={t_stat:+5.2f}  p={p_val:.3f}")
        rows.append({"hour_class": hc, "metric": "ida_within_hour_sd",
                     "pre_mean": pre_m, "pre_sd": pre_sd, "pre_n": pre_n,
                     "post_mean": post_m, "post_sd": post_sd, "post_n": post_n,
                     "diff": diff, "t": t_stat, "p": p_val})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
