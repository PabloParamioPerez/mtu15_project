# STATUS: ALIVE
# LAST-AUDIT: 2026-06-14
# FEEDS: thesis sec:data:partition (4.4) -- the critical/flat/midday partition
#        of clock-hours. Reports, per hour and per hour-class, BOTH the level
#        scale of within-hour residual-demand variation (sigma_within, MW) and
#        the scale-free coefficient of variation (CV), under the two competing
#        CV definitions, to settle which formula to report and to confirm they
#        agree where it matters (the critical-vs-flat ordering).
#
# Residual demand at 15-min: RD(d,h,q) = Load - Wind - Solar.
#   Load:  ENTSO-E A65 actual total load (MW, 15-min from 2022-05).
#   Wind:  ENTSO-E A75 B18 (river/offshore) + B19 (onshore).
#   Solar: ENTSO-E A75 B16.
#
# Per (day d, hour h) across the 4 quarters q:
#   mean_q(d,h) = mean over quarters,  sd_q(d,h) = within-hour SD (ddof=1).
# Two CV definitions:
#   CV_ratioE  (slide formula)      = E_d[sd_q] / E_d[mean_q]   -- ratio of means
#   CV_Eratio  (advisor's request)  = E_d[ sd_q / mean_q ]      -- mean of per-day CVs
# sigma_within reported as the per-hour MEDIAN of sd_q over days, matching the
# partition definition in the thesis text.
#
# Window: 2024-06-14 -> 2025-03-18 (post-European-IDA, pre-MTU15: the
# regime-constant pre-reform window the partition is defined on).
#
# OUT: results/regressions/balancing/partition_cv_sigma.csv

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
LOAD_A65 = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
ENTSOE_GEN = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
OUT = REPO / "results/regressions/balancing/partition_cv_sigma.csv"

START = "2024-06-14"
END = "2025-03-18"

HCLASS = {
    "morning_ramp": {5, 6, 7, 8},
    "evening_ramp": {16, 17, 18, 19, 20, 21, 22},
    "midday":       {11, 12, 13, 14},
    "flat":         {1, 2, 3},
}
CLASS_OF = {h: cls for cls, hs in HCLASS.items() for h in hs}


def load_a65_qh() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    WITH l AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local, load_mw
        FROM '{LOAD_A65}'
        WHERE mtu_minutes = 15
          AND isp_start_utc >= TIMESTAMP '{START} 00:00:00' - INTERVAL '2 hours'
          AND isp_start_utc <  TIMESTAMP '{END} 23:59:59'   + INTERVAL '2 hours'
    )
    SELECT CAST(ts_local AS DATE) AS d,
           CAST(EXTRACT(hour FROM ts_local) AS INT) AS hour,
           CAST(EXTRACT(minute FROM ts_local) / 15 AS INT) AS quarter,
           AVG(load_mw) AS load_mw
    FROM l GROUP BY 1, 2, 3
    """
    df = con.execute(q).df()
    df["d"] = pd.to_datetime(df["d"])
    return df[(df["d"] >= START) & (df["d"] <= END)]


def load_vre_qh() -> pd.DataFrame:
    con = duckdb.connect()
    q = f"""
    WITH g AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local,
               psr_type, quantity_mw
        FROM '{ENTSOE_GEN}'
        WHERE psr_type IN ('B16','B18','B19')
          AND isp_start_utc >= TIMESTAMP '{START} 00:00:00' - INTERVAL '2 hours'
          AND isp_start_utc <  TIMESTAMP '{END} 23:59:59'   + INTERVAL '2 hours'
    )
    SELECT CAST(ts_local AS DATE) AS d,
           CAST(EXTRACT(hour FROM ts_local) AS INT) AS hour,
           CAST(EXTRACT(minute FROM ts_local) / 15 AS INT) AS quarter,
           SUM(CASE WHEN psr_type = 'B16' THEN quantity_mw ELSE 0 END) AS solar_mw,
           SUM(CASE WHEN psr_type IN ('B18','B19') THEN quantity_mw ELSE 0 END) AS wind_mw
    FROM g GROUP BY 1, 2, 3
    """
    df = con.execute(q).df()
    df["d"] = pd.to_datetime(df["d"])
    return df[(df["d"] >= START) & (df["d"] <= END)]


def main():
    print(f"Window: {START} -> {END}")
    load = load_a65_qh()
    vre = load_vre_qh()
    df = load.merge(vre, on=["d", "hour", "quarter"], how="inner")
    df["rd_mw"] = df["load_mw"] - df["wind_mw"] - df["solar_mw"]
    print(f"  merged {len(df):,} (d,h,q) cells")

    # Per (day, hour): within-hour mean and SD across the 4 quarters
    per_dh = (df.groupby(["d", "hour"], as_index=False)["rd_mw"]
                .agg(mean_q="mean", sd_q=lambda s: s.std(ddof=1), n="count"))
    per_dh = per_dh[per_dh["n"] == 4].copy()
    per_dh["cv_dh"] = per_dh["sd_q"] / per_dh["mean_q"]
    per_dh["hclass"] = per_dh["hour"].map(CLASS_OF)

    # Per hour: median sigma_within + both CV definitions
    summary = (per_dh.groupby("hour", as_index=False)
                     .agg(sigma_within_med=("sd_q", "median"),
                          mean_SD=("sd_q", "mean"),
                          mean_RD=("mean_q", "mean"),
                          cv_Eratio=("cv_dh", "mean"),
                          n_days=("d", "nunique")))
    summary["cv_ratioE"] = summary["mean_SD"] / summary["mean_RD"]
    summary["hclass"] = summary["hour"].map(CLASS_OF)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT, index=False)

    pd.set_option("display.width", 140)
    print("\n=== Per hour ===")
    print(summary[["hour", "hclass", "sigma_within_med", "cv_ratioE", "cv_Eratio", "n_days"]]
          .round({"sigma_within_med": 0, "cv_ratioE": 4, "cv_Eratio": 4}).to_string(index=False))

    # Per class: pooled median sigma_within over all hour-day obs in the class,
    # and the class CV under both definitions.
    def cls_stats(mask):
        pd_sub = per_dh[mask]
        sig_med = pd_sub["sd_q"].median()
        sig_lo, sig_hi = (summary.loc[summary["hour"].isin(pd_sub["hour"].unique()), "sigma_within_med"].min(),
                          summary.loc[summary["hour"].isin(pd_sub["hour"].unique()), "sigma_within_med"].max())
        cv_re = pd_sub["sd_q"].mean() / pd_sub["mean_q"].mean()
        cv_er = pd_sub["cv_dh"].mean()
        return sig_med, sig_lo, sig_hi, cv_re, cv_er

    print("\n=== Per hour-class: pooled median sigma_within (MW); per-hour-median range; CV both defs ===")
    for cls in ["morning_ramp", "evening_ramp", "midday", "flat"]:
        sm, lo, hi, cre, cer = cls_stats(per_dh["hclass"] == cls)
        print(f"{cls:14s}  med {sm:5.0f}  range [{lo:.0f}, {hi:.0f}]   CV_ratioE {100*cre:.1f}%   CV_Eratio {100*cer:.1f}%")

    for lab, mask in [("CRITICAL (M+E)", per_dh["hclass"].isin(["morning_ramp", "evening_ramp"])),
                      ("FLAT", per_dh["hclass"] == "flat")]:
        sm, lo, hi, cre, cer = cls_stats(mask)
        print(f"{lab:14s}  med {sm:5.0f}  range [{lo:.0f}, {hi:.0f}]   CV_ratioE {100*cre:.1f}%   CV_Eratio {100*cer:.1f}%")
    print(f"\nWrote {OUT.relative_to(REPO)}")


if __name__ == "__main__":
    main()
