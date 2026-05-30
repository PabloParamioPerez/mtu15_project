# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# FEEDS: scripts/analysis/bid/ffr_supply_curves.R via
#        data/derived/panels/hourly_weather_panel.parquet.
#
# Per-date hourly wind and solar generation for Spain. Source: ENTSO-E A75
# actual generation per type (B19 wind onshore, B16 solar). Aggregates the
# native 15-min observations to clock-hour means.
#
# Wide format: one row per date, with 24 wind columns and 24 solar columns
# plus daily gas (joined from bsts_daily_panel.parquet).

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
A75 = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
DAILY = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT = REPO / "data/derived/panels/hourly_weather_panel.parquet"

DATE_MIN = "2023-06-01"


def main():
    con = duckdb.connect()
    q = f"""
    WITH hourly AS (
      SELECT CAST(isp_start_utc AS DATE) AS d,
             EXTRACT(HOUR FROM isp_start_utc)::INT AS clock_hour,
             psr_type,
             AVG(quantity_mw) AS mw
      FROM read_parquet('{A75}')
      WHERE psr_type IN ('B16', 'B19')
        AND isp_start_utc >= '{DATE_MIN}'
        AND quantity_mw IS NOT NULL
      GROUP BY 1, 2, 3
    )
    SELECT d, clock_hour,
           MAX(CASE WHEN psr_type = 'B19' THEN mw END) AS wind_mw,
           MAX(CASE WHEN psr_type = 'B16' THEN mw END) AS solar_mw
    FROM hourly GROUP BY 1, 2 ORDER BY 1, 2
    """
    long = con.execute(q).fetchdf()
    print(f"Hourly wind/solar: {len(long):,} (date, hour) rows")
    print(f"  date range: {long['d'].min()} -> {long['d'].max()}")

    # Pivot to wide: one row per date, 24 wind + 24 solar cols
    long["d"] = pd.to_datetime(long["d"])
    wind = long.pivot_table(index="d", columns="clock_hour", values="wind_mw", aggfunc="first")
    wind.columns = [f"wind_h{h:02d}_mw" for h in wind.columns]
    solar = long.pivot_table(index="d", columns="clock_hour", values="solar_mw", aggfunc="first")
    solar.columns = [f"solar_h{h:02d}_mw" for h in solar.columns]
    weather = wind.join(solar, how="outer").reset_index()

    # Join daily gas
    daily = pd.read_parquet(DAILY)[["d", "gas_eur"]]
    daily["d"] = pd.to_datetime(daily["d"])
    out = weather.merge(daily, on="d", how="left")
    out = out.sort_values("d").reset_index(drop=True)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")
    print(f"  shape: {out.shape}")
    print(f"  cols (first 5): {out.columns[:5].tolist()}")
    print(f"  cols (last 5): {out.columns[-5:].tolist()}")
    # NA check
    na_count = out.isna().sum()
    print(f"  cols with NA: {(na_count > 0).sum()}")


if __name__ == "__main__":
    main()
