# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: CNMC Article 64.37 hypothesis — chronic unauthorized capacity reduction by Spanish nuclear (system-level evidence)
# CLAIM: Spanish nuclear capacity factor collapsed from ~70% (2018-2021) to ~30% (2023-2025) per ENTSO-E A75 actual generation. The decline matches CNMC's 2023+ chronic-violations framing.

"""Spanish nuclear availability — ENTSO-E A75 system-level audit.

The pdbce DA-spot panel only captures ~5-30% of nuclear output (most
clears via bilateral contracts), so it's not a clean measure of plant
availability. ENTSO-E A75 'actual generation' (PSR=B14) is a cleaner
system-aggregate measure.

Reasoning before running:
  Spanish nuclear nameplate ~7.3 GW (Cofrentes 1064 + Almaraz I 1011 +
  Almaraz II 1006 + Ascó I 1032 + Ascó II 1027 + Vandellós II 1087 +
  Trillo 1066). Annual capacity = 64.1 TWh.

  Historical CF: 80-90%. Refueling allowance: ~5-10%. So CF below 75%
  starts looking like under-availability.

  Predicted patterns:
    - Strategic-reduction hypothesis: CF declines 2022-2025 vs
      2018-2021 in a way not explained by smooth age trend.
    - Age-only hypothesis: smooth monotonic decline.

  Caveats: ENTSO-E A75 may systematically undercount Spanish nuclear by
  ~20% based on cross-check with Foro Nuclear's annual ~52 TWh figure
  for 2018-2021. We report the RELATIVE change across years; the
  absolute level may be biased low.

Output: data/derived/results/nuclear_availability_v2.csv
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
A75 = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "nuclear_availability_v2.csv"

NUCLEAR_NAMEPLATE_MW = 7293  # Cofrentes 1064 + Almaraz I 1011 + Almaraz II 1006 + Ascó I 1032 + Ascó II 1027 + Vandellós II 1087 + Trillo 1066


def main() -> None:
    print("[1/4] Pull Spanish nuclear (B14) hourly generation from ENTSO-E A75...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    df = con.sql(f"""
        SELECT isp_start_utc, mtu_minutes,
               quantity_mw,
               quantity_mw * mtu_minutes / 60.0 AS mwh
        FROM '{A75}'
        WHERE psr_type = 'B14'
    """).df()
    df["isp_start"] = pd.to_datetime(df["isp_start_utc"]).dt.tz_localize(None)
    df["date"] = df["isp_start"].dt.date
    df["year"] = df["isp_start"].dt.year
    df["month"] = df["isp_start"].dt.month
    df["hour"] = df["isp_start"].dt.hour + 1
    df = df[df["isp_start"] >= pd.Timestamp("2018-01-01")]
    print(f"   nuclear panel: {len(df):,} ISPs; range {df.isp_start.min().date()} → {df.isp_start.max().date()}")

    print()
    print("[2/4] Annual capacity factor (% of nameplate × 8784h):")
    annual = df.groupby("year").agg(
        twh=("mwh", lambda x: x.sum() / 1e6),
        n_isps=("mwh", "size"),
    ).reset_index()
    annual["hours"] = annual["n_isps"]  # rough
    # nameplate = 7293 MW × 8784 hours = 64,066 GWh = 64.07 TWh per year
    annual["cf_pct"] = annual["twh"] / 64.07 * 100
    annual["delta_vs_2019"] = annual["cf_pct"] - annual.loc[annual["year"] == 2019, "cf_pct"].values[0]
    print(annual[["year", "twh", "cf_pct", "delta_vs_2019"]].round(2).to_string(index=False))

    print()
    print("[3/4] Monthly CF — within-year + across years:")
    monthly = df.groupby(["year", "month"]).agg(twh=("mwh", lambda x: x.sum() / 1e6)).reset_index()
    monthly["days"] = monthly["month"].map(lambda m: pd.Period(f"2024-{m:02d}").days_in_month)
    # nameplate per month = 7293 MW × 24h × days_in_month / 1000 (TWh)
    monthly["cf_pct"] = monthly["twh"] / (NUCLEAR_NAMEPLATE_MW * 24 * monthly["days"] / 1e6) * 100
    pivot = monthly.pivot(index="month", columns="year", values="cf_pct").round(0)
    print("Monthly CF by year (rows=month, cols=year):")
    print(pivot.to_string())

    print()
    print("[4/4] Same-calendar-month comparisons + low-price-hour cross-check:")
    print()
    print("(a) Era comparison (CF, %, mean across months):")
    df_era = df.copy()
    df_era["era"] = np.where(df_era["year"] <= 2021, "1.pre-2022", np.where(df_era["year"] <= 2024, "2.2022-2024", "3.2025+"))
    era = df_era.groupby("era").agg(twh=("mwh", lambda x: x.sum() / 1e6),
                                     n_isps=("mwh", "size")).reset_index()
    era["years"] = [4, 3, 1]
    era["cf_pct"] = era["twh"] / (64.07 * era["years"]) * 100
    print(era[["era", "twh", "years", "cf_pct"]].round(1).to_string(index=False))

    print()
    print("(b) Low-price-hour test: nuclear CF in below-€20/MWh hours vs above-€50/MWh hours, by year:")
    px = con.sql(f"""
        WITH hp AS (
            SELECT date, period,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE}'
            WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_da FROM hp GROUP BY 1, 2
    """).df()
    px["date"] = pd.to_datetime(px["date"])
    # Build hourly nuclear MWh
    df_h = df.groupby([pd.Grouper(key="isp_start", freq="h"), "year"]).agg(mwh=("mwh", "sum")).reset_index()
    df_h["date"] = df_h["isp_start"].dt.normalize()
    df_h["hour"] = df_h["isp_start"].dt.hour + 1
    df_h = df_h.merge(px[["date", "hour", "p_da"]], on=["date", "hour"], how="inner")
    df_h["price_bin"] = np.where(df_h["p_da"] < 20, "low (<€20)",
                                  np.where(df_h["p_da"] < 50, "mid", "high (>€50)"))
    cf_by_pricebin = df_h.groupby(["year", "price_bin"]).agg(
        mwh=("mwh", "sum"),
        n_hours=("p_da", "size"),
    ).reset_index()
    # CF: nuclear MWh / (nameplate × n_hours)
    cf_by_pricebin["cf_pct"] = cf_by_pricebin["mwh"] / (NUCLEAR_NAMEPLATE_MW * cf_by_pricebin["n_hours"]) * 100
    pivot_pb = cf_by_pricebin.pivot(index="year", columns="price_bin", values="cf_pct").round(0)
    print("CF by price-bin × year:")
    print(pivot_pb.to_string())

    print()
    print("Reading: if low-price hours show LOW CF, the reduction is economic (firms reduce output when DA price < MC).")
    print("         if all price bins show similar CF reduction, it's not economic — points to chronic availability issue.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    annual.to_csv(OUT, index=False)
    pivot_pb.to_csv(OUT.with_suffix(".pricebin.csv"))
    print(f"\nwrote {OUT} (and .pricebin.csv)")


if __name__ == "__main__":
    main()
