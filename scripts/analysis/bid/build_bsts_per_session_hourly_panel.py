# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: scripts/analysis/bid/ols_wedge_per_session_hourly.R via the panel at
#        data/derived/panels/bsts_per_session_hourly_panel.parquet.
#
# Builds an HOURLY per-IDA-session panel: one row per (date, clock_hour) with
# per-session IDA prices (s1, s2, s3), DA price, and renewables + gas controls.
#
# Aggregation rule mirrors the headline hourly panel: pre-MTU15 (mtu_minutes=60)
# the period IS the hour; post-MTU15 the 4 quarter-hour prices within an hour
# are averaged.
#
# Per-session pre-window is restricted to 2024-06-14+ (post-European-IDA reform
# 3-session regime). Earlier data had 6 MIBEL sessions; pooling regimes mixes
# very different products.
#
# OUT: data/derived/panels/bsts_per_session_hourly_panel.parquet

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
ENTSOE_GEN = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
GAS = REPO / "data/processed/esios/indicators/1940.parquet"
OUT = REPO / "data/derived/panels/bsts_per_session_hourly_panel.parquet"
START_DATE = "2024-06-14"


def hourly_prices_da():
    con = duckdb.connect()
    df = con.execute(f"""
    SELECT CAST(date AS DATE) AS d,
           CASE WHEN mtu_minutes = 60 THEN period - 1
                ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS hour,
           AVG(price_es_eur_mwh) AS da_price_eur
    FROM '{MPDBC}'
    WHERE price_es_eur_mwh IS NOT NULL AND date >= '{START_DATE}'
    GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def hourly_prices_ida_per_session():
    con = duckdb.connect()
    df = con.execute(f"""
    SELECT CAST(date AS DATE) AS d,
           session_number,
           CASE WHEN mtu_minutes = 60 THEN period - 1
                ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS hour,
           AVG(price_es_eur_mwh) AS p
    FROM '{MPIBC}'
    WHERE price_es_eur_mwh IS NOT NULL AND date >= '{START_DATE}'
      AND session_number IN (1, 2, 3)
    GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    wide = df.pivot_table(index=["d", "hour"], columns="session_number",
                          values="p").reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={1: "ida_price_eur_s1",
                                2: "ida_price_eur_s2",
                                3: "ida_price_eur_s3"})
    return wide


def hourly_entsoe_gen():
    con = duckdb.connect()
    df = con.execute(f"""
    WITH g AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local,
               mtu_minutes, psr_type, quantity_mw
        FROM '{ENTSOE_GEN}'
        WHERE psr_type IN ('B16','B18','B19')
          AND isp_start_utc >= TIMESTAMP '{START_DATE} 00:00:00' - INTERVAL '2 hours'
    )
    SELECT CAST(ts_local AS DATE) AS d,
           CAST(EXTRACT(hour FROM ts_local) AS INT) AS hour,
           psr_type,
           SUM(quantity_mw * (mtu_minutes / 60.0)) AS mwh
    FROM g GROUP BY 1, 2, 3
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    w = df.pivot_table(index=["d", "hour"], columns="psr_type",
                       values="mwh", fill_value=0).reset_index()
    w.columns.name = None
    w["wind_mwh"] = w.get("B19", 0).fillna(0) + w.get("B18", 0).fillna(0)
    w["solar_mwh"] = w.get("B16", 0).fillna(0)
    w = w[["d", "hour", "wind_mwh", "solar_mwh"]]
    w = w[w["d"] >= pd.Timestamp(START_DATE)]
    return w


def daily_gas():
    df = pd.read_parquet(GAS)
    df["d"] = pd.to_datetime(df["ts_local"]).dt.tz_localize(None).dt.normalize()
    df = (df.sort_values("d").drop_duplicates("d", keep="last")
            [["d", "value"]].rename(columns={"value": "gas_eur"}))
    df = df[df["d"] >= pd.Timestamp(START_DATE)]
    return df


def build_hour_grid(d_min, d_max):
    dates = pd.date_range(d_min, d_max, freq="D")
    grid = pd.MultiIndex.from_product([dates, range(24)],
                                       names=["d", "hour"]).to_frame(index=False)
    return grid


def main():
    print("Building hourly DA prices...")
    da = hourly_prices_da()
    print(f"  DA: {len(da):,} rows; range {da['d'].min().date()} -> {da['d'].max().date()}")
    print("Building hourly IDA prices per session...")
    ida = hourly_prices_ida_per_session()
    print(f"  IDA per session: {len(ida):,} rows")
    print("Building hourly wind+solar...")
    gen = hourly_entsoe_gen()
    print("Building daily gas...")
    gas = daily_gas()

    d_min = max(da["d"].min(), pd.Timestamp(START_DATE))
    d_max = max(da["d"].max(), ida["d"].max())
    grid = build_hour_grid(d_min, d_max)

    panel = (grid
             .merge(da, on=["d", "hour"], how="left")
             .merge(ida, on=["d", "hour"], how="left")
             .merge(gen, on=["d", "hour"], how="left")
             .merge(gas, on=["d"], how="left"))
    panel = panel.sort_values(["d", "hour"]).reset_index(drop=True)
    panel["gas_eur"] = panel["gas_eur"].ffill()
    # Per-session wedges (NaN-safe; downstream OLS will drop NaN rows)
    for s in (1, 2, 3):
        panel[f"wedge_s{s}_h"] = panel["da_price_eur"] - panel[f"ida_price_eur_s{s}"]

    print(f"\nPanel: {len(panel):,} rows, {len(panel.columns)} cols")
    print(f"  range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    for s in (1, 2, 3):
        n = panel[f"ida_price_eur_s{s}"].notna().sum()
        print(f"  ida_price_eur_s{s} non-NaN: {n:,}")
    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
