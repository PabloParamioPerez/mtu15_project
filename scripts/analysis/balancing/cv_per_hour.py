# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: slide narration around "critical-vs-flat hour-class differences" --
#        the coefficient of variation argument.
#
# We measure WITHIN-HOUR variability of residual demand:
#   RD(t) = Load(t) - Wind(t) - Solar(t), all at 15-min granularity.
#
# Then per HOUR h \in {0..23}:
#   mean_RD(h)   = mean_{day d} (mean_{quarter q in h} RD(d, h, q))   -- the
#                  average level of RD at that hour, across days.
#   mean_SD(h)   = mean_{day d} ( SD_{quarter q in h} RD(d, h, q) )   -- the
#                  average within-hour quarter-SD, across days. Captures
#                  intra-hour ramp volatility (the thing MTU15 lets you
#                  trade against).
#   CV(h)        = mean_SD(h) / mean_RD(h)
#
# Window: 2022-01-01 -> 2025-03-18 (pre-MTU15-IDA baseline).
#
# Data sources (15-min):
#   * Load:  ESIOS P48 demand (10027, MW, Peninsula).
#   * Wind:  ENTSO-E A75 B19 onshore + B18 offshore, Madrid local, MW.
#   * Solar: ENTSO-E A75 B16, Madrid local, MW.
#
# OUT: results/regressions/balancing/cv_residual_demand_per_hour.csv

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
LOAD_P48 = REPO / "data/processed/esios/indicators/10027.parquet"
ENTSOE_GEN = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
OUT = REPO / "results/regressions/balancing/cv_residual_demand_per_hour.csv"

START = "2023-01-01"
END = "2025-03-18"

HCLASS = {
    "morning_ramp": {5, 6, 7, 8},
    "midday":       {11, 12, 13, 14},
    "evening_ramp": {16, 17, 18, 19, 20, 21, 22},
    "flat":         {1, 2, 3},
}


def load_p48_qh() -> pd.DataFrame:
    df = pd.read_parquet(LOAD_P48, columns=["ts_local", "value"])
    df["ts_local"] = pd.to_datetime(df["ts_local"]).dt.tz_localize(None)
    df["d"] = df["ts_local"].dt.normalize()
    df["hour"] = df["ts_local"].dt.hour
    df["quarter"] = (df["ts_local"].dt.minute // 15).astype(int)
    df = df[(df["d"] >= START) & (df["d"] <= END)]
    # Indicator 10027 reports MWh per 15-min ISP from 2023 onward. Convert
    # to instantaneous MW (energy / 0.25 h = MW).
    out = (df.groupby(["d", "hour", "quarter"], as_index=False)["value"]
              .mean()
              .rename(columns={"value": "load_mwh_qh"}))
    out["load_mw"] = out["load_mwh_qh"] * 4.0
    return out[["d", "hour", "quarter", "load_mw"]]


def load_vre_qh() -> pd.DataFrame:
    """Wind + solar realised generation at 15-min in Madrid local, in MW."""
    con = duckdb.connect()
    q = f"""
    WITH g AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local,
               psr_type, quantity_mw, mtu_minutes
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
    FROM g
    GROUP BY 1, 2, 3
    """
    df = con.execute(q).df()
    df["d"] = pd.to_datetime(df["d"])
    df = df[(df["d"] >= START) & (df["d"] <= END)]
    return df


def main():
    print(f"Window: {START} -> {END}")
    print("Load (P48 demand, 15-min)..."); load = load_p48_qh()
    print(f"  {len(load):,} (d, hour, quarter) cells")
    print("Wind+Solar (ENTSO-E A75 realised, 15-min)..."); vre = load_vre_qh()
    print(f"  {len(vre):,} cells")

    df = load.merge(vre, on=["d", "hour", "quarter"], how="inner")
    df["rd_mw"] = df["load_mw"] - df["wind_mw"] - df["solar_mw"]
    print(f"  merged: {len(df):,} cells; RD range "
          f"{df['rd_mw'].min():.0f} -> {df['rd_mw'].max():.0f} MW")

    # Per (day, hour): within-hour stats across the 4 quarters
    g = df.groupby(["d", "hour"])["rd_mw"]
    per_dh = pd.DataFrame({
        "d": g.groups.keys(),                # multi-index iterable
        "mean_q": g.mean().values,
        "sd_q":   g.std(ddof=1).values,
        "n":      g.count().values,
    })
    # Restore the multi-index columns properly
    per_dh = (df.groupby(["d", "hour"], as_index=False)["rd_mw"]
                .agg(mean_q="mean", sd_q=lambda s: s.std(ddof=1), n="count"))
    # Drop hours with <4 quarters (DST edge cases)
    per_dh = per_dh[per_dh["n"] == 4]

    # Per hour: average over days
    summary = (per_dh.groupby("hour", as_index=False)
                      .agg(mean_RD=("mean_q", "mean"),
                            mean_SD_within=("sd_q", "mean"),
                            n_days=("d", "nunique")))
    summary["CV"] = summary["mean_SD_within"] / summary["mean_RD"]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT, index=False)

    print("\n=== CV of residual demand by HOUR ===")
    print("(mean_RD = avg level MW; mean_SD_within = avg within-hour quarter SD; CV = SD/mean)")
    print(summary.round({"mean_RD": 0, "mean_SD_within": 2, "CV": 4}).to_string(index=False))

    summary["hclass"] = summary["hour"].map(
        {h: cls for cls, hs in HCLASS.items() for h in hs})
    classy = (summary.dropna(subset=["hclass"])
                      .groupby("hclass", as_index=False)
                      .agg(mean_RD=("mean_RD", "mean"),
                            mean_SD_within=("mean_SD_within", "mean"),
                            CV=("CV", "mean")))
    classy = classy.round({"mean_RD": 0, "mean_SD_within": 2, "CV": 4})
    print("\n=== Mean of per-hour CV, by hour CLASS ===")
    print(classy.to_string(index=False))

    flat_cv = classy.loc[classy["hclass"] == "flat", "CV"].iloc[0]
    classy["ratio_vs_flat"] = (classy["CV"] / flat_cv).round(2)
    print("\n=== Ratio CV(class) / CV(flat) ===")
    print(classy[["hclass", "CV", "ratio_vs_flat"]].to_string(index=False))


if __name__ == "__main__":
    main()
