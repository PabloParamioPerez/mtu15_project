# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: scripts/analysis/bid/bsts_{da15,id15}_{price,quantity}_hourly.R and
#        scripts/analysis/bid/bsts_placebo_2024.R via the hourly panel at
#        data/derived/panels/bsts_hourly_panel.parquet.
#
# Builds an HOURLY panel of Spanish electricity prices + fundamentals + per-tech
# auction-cleared MWh, in local Madrid time. One row per (date, clock_hour).
# Pre-MTU15 OMIE periods are hourly (period 1-24, mtu=60 -> hour = period-1).
# Post-MTU15 OMIE periods are quarter-hourly (period 1-96, mtu=15 -> hour =
# floor((period-1)/4)); we average the 4 quarter-prices into the hourly mean
# and sum the 4 quarter-MW * 0.25 h into hourly MWh.
#
# ENTSO-E A75 (generation) and A65 (load) are 15-min in UTC; converted to
# Europe/Madrid local time, then aggregated to hour-MWh.
#
# ESIOS gas (1940) is daily, repeated hourly. Fase I/II (10051/10270) are
# 15-min absolute MW, summed * 0.25 -> hourly MWh.
#
# OUT columns:
#   d            -- date (Madrid local)
#   hour         -- clock hour 0..23
#   da_price_eur -- hourly mean DA clearing price (EUR/MWh)
#   ida_price_eur-- hourly mean IDA clearing price across sessions
#   wind_mwh, solar_mwh, demand_mwh    -- hourly sums (MWh)
#   gas_eur      -- daily gas price, repeated hourly
#   fase1_mwh, fase2_mwh               -- hourly Fase I/II MWh
#   q_{ccgt,hydro,wind,solar,nuclear}_mwh_{da,ida} -- per-tech hourly cleared MWh
#
# OUT: data/derived/panels/bsts_hourly_panel.parquet

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
ENTSOE_GEN = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
ENTSOE_LOAD = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
GAS = REPO / "data/processed/esios/indicators/1940.parquet"
FASE1 = REPO / "data/processed/esios/indicators/10051.parquet"
FASE2 = REPO / "data/processed/esios/indicators/10270.parquet"
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "data/derived/panels/bsts_hourly_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

TECHS = ["CCGT", "Hydro", "Wind", "Solar PV", "Nuclear"]
START_DATE = "2022-01-01"


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


def hourly_prices_ida():
    # Average across IDA sessions (so each clock hour has a single number)
    con = duckdb.connect()
    df = con.execute(f"""
    WITH per_session AS (
        SELECT CAST(date AS DATE) AS d,
               session_number,
               CASE WHEN mtu_minutes = 60 THEN period - 1
                    ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS hour,
               AVG(price_es_eur_mwh) AS p
        FROM '{MPIBC}'
        WHERE price_es_eur_mwh IS NOT NULL AND date >= '{START_DATE}'
        GROUP BY 1, 2, 3
    )
    SELECT d, hour, AVG(p) AS ida_price_eur
    FROM per_session GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def hourly_entsoe_gen():
    """ENTSO-E A75 wind+solar -> hourly MWh in Madrid local time."""
    con = duckdb.connect()
    df = con.execute(f"""
    WITH g AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local,
               mtu_minutes,
               psr_type,
               quantity_mw
        FROM '{ENTSOE_GEN}'
        WHERE psr_type IN ('B16','B18','B19')
          AND isp_start_utc >= TIMESTAMP '{START_DATE} 00:00:00' - INTERVAL '2 hours'
    )
    SELECT CAST(ts_local AS DATE) AS d,
           CAST(EXTRACT(hour FROM ts_local) AS INT) AS hour,
           psr_type,
           SUM(quantity_mw * (mtu_minutes / 60.0)) AS mwh
    FROM g
    GROUP BY 1, 2, 3
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    w = df.pivot_table(index=["d", "hour"], columns="psr_type", values="mwh", fill_value=0).reset_index()
    w.columns.name = None
    # B19 = wind onshore, B18 = wind offshore (negligible in Spain), B16 = solar PV
    if "B18" in w.columns:
        w["wind_mwh"] = w.get("B19", 0).fillna(0) + w.get("B18", 0).fillna(0)
        w = w.drop(columns=[c for c in ["B18"] if c in w.columns])
    else:
        w["wind_mwh"] = w.get("B19", 0).fillna(0)
    w["solar_mwh"] = w.get("B16", 0).fillna(0)
    w = w[["d", "hour", "wind_mwh", "solar_mwh"]]
    w = w[w["d"] >= pd.Timestamp(START_DATE)]
    return w


def hourly_entsoe_load():
    con = duckdb.connect()
    df = con.execute(f"""
    WITH l AS (
        SELECT TIMEZONE('Europe/Madrid', isp_start_utc) AS ts_local,
               mtu_minutes, load_mw
        FROM '{ENTSOE_LOAD}'
        WHERE isp_start_utc >= TIMESTAMP '{START_DATE} 00:00:00' - INTERVAL '2 hours'
    )
    SELECT CAST(ts_local AS DATE) AS d,
           CAST(EXTRACT(hour FROM ts_local) AS INT) AS hour,
           SUM(load_mw * (mtu_minutes / 60.0)) AS demand_mwh
    FROM l GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df = df[df["d"] >= pd.Timestamp(START_DATE)]
    return df


def daily_gas():
    df = pd.read_parquet(GAS)
    df["d"] = pd.to_datetime(df["ts_local"]).dt.tz_localize(None).dt.normalize()
    df = (df.sort_values("d").drop_duplicates("d", keep="last")
            [["d", "value"]].rename(columns={"value": "gas_eur"}))
    df = df[df["d"] >= pd.Timestamp(START_DATE)]
    return df


def hourly_fase(path, name):
    """Aggregate 15-min absolute-MW Fase I/II into hourly MWh in local time."""
    df = pd.read_parquet(path)
    df["ts_local"] = pd.to_datetime(df["ts_local"]).dt.tz_localize(None)
    df["d"] = df["ts_local"].dt.normalize()
    df["hour"] = df["ts_local"].dt.hour
    df = df[df["d"] >= pd.Timestamp(START_DATE)]
    out = (df.assign(mwh=lambda x: x["value"].abs() * 0.25)
             .groupby(["d", "hour"], as_index=False)["mwh"].sum()
             .rename(columns={"mwh": f"{name}_mwh"}))
    return out


def hourly_per_tech(prog_path, market_tag, has_session):
    con = duckdb.connect()
    session_col = ", session_number" if has_session else ""
    sql = f"""
    WITH u AS (SELECT unit_code, tech_group FROM '{UNIT_MAP}'),
         p AS (
            SELECT CAST(date AS DATE) AS d,
                   CASE WHEN mtu_minutes = 60 THEN period - 1
                        ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS hour,
                   mtu_minutes, unit_code{session_col},
                   assigned_power_mw
            FROM '{prog_path}'
            WHERE assigned_power_mw IS NOT NULL
              AND assigned_power_mw > 0
              AND date >= '{START_DATE}'
         )
    SELECT p.d, p.hour,
           CASE WHEN u.tech_group = 'Hydro_pump' THEN 'Hydro'
                ELSE u.tech_group END AS tech,
           SUM(p.assigned_power_mw * (p.mtu_minutes / 60.0)) AS mwh
    FROM p LEFT JOIN u ON p.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2, 3
    """
    df = con.execute(sql).fetchdf()
    df = df[df["tech"].isin(TECHS)].copy()
    df["col"] = ("q_" + df["tech"].str.lower().str.replace(" pv", "")
                                .str.replace(" ", "_") + f"_mwh_{market_tag}")
    wide = (df.pivot_table(index=["d", "hour"], columns="col", values="mwh", aggfunc="sum")
              .reset_index())
    wide.columns.name = None
    wide["d"] = pd.to_datetime(wide["d"])
    return wide


def build_hour_grid(d_min, d_max):
    """Full (date, hour) grid; absorbs DST 23-h fall and 25-h spring asymmetry
    by including every clock hour 0..23 every day. Spring missing hour will
    naturally be NaN across covariates; downstream BSTS handles via row drop."""
    dates = pd.date_range(d_min, d_max, freq="D")
    grid = pd.MultiIndex.from_product([dates, range(24)], names=["d", "hour"]).to_frame(index=False)
    return grid


def main():
    print("Building hourly DA prices...")
    da = hourly_prices_da()
    print(f"  DA: {len(da):,} hourly rows; range {da['d'].min().date()} -> {da['d'].max().date()}")

    print("Building hourly IDA prices (avg across sessions)...")
    ida = hourly_prices_ida()
    print(f"  IDA: {len(ida):,} rows; range {ida['d'].min().date()} -> {ida['d'].max().date()}")

    print("Building hourly wind+solar (ENTSO-E A75 -> Europe/Madrid)...")
    gen = hourly_entsoe_gen()
    print(f"  gen: {len(gen):,} rows")

    print("Building hourly demand (ENTSO-E A65 -> Europe/Madrid)...")
    load = hourly_entsoe_load()
    print(f"  load: {len(load):,} rows")

    print("Building daily gas (ESIOS 1940) -- repeated hourly downstream...")
    gas = daily_gas()
    print(f"  gas: {len(gas):,} days")

    print("Building hourly Fase I + Fase II (ESIOS 10051+10270)...")
    f1 = hourly_fase(FASE1, "fase1")
    f2 = hourly_fase(FASE2, "fase2")
    print(f"  fase1: {len(f1):,} rows; fase2: {len(f2):,} rows")

    print("Building hourly per-tech cleared MWh, DA (pdbc)...")
    q_da = hourly_per_tech(PDBC, "da", has_session=False)
    print(f"  q_da: {len(q_da):,} rows; tech cols: {[c for c in q_da.columns if c.startswith('q_')]}")

    print("Building hourly per-tech cleared MWh, IDA (pibci)...")
    q_ida = hourly_per_tech(PIBCI, "ida", has_session=True)
    print(f"  q_ida: {len(q_ida):,} rows; tech cols: {[c for c in q_ida.columns if c.startswith('q_')]}")

    d_min = max(da["d"].min(), pd.Timestamp(START_DATE))
    d_max = max(da["d"].max(), ida["d"].max())
    grid = build_hour_grid(d_min, d_max)
    print(f"\nFull (date,hour) grid: {len(grid):,} rows ({d_min.date()} -> {d_max.date()})")

    panel = (grid
             .merge(da, on=["d", "hour"], how="left")
             .merge(ida, on=["d", "hour"], how="left")
             .merge(gen, on=["d", "hour"], how="left")
             .merge(load, on=["d", "hour"], how="left")
             .merge(gas, on=["d"], how="left")
             .merge(f1, on=["d", "hour"], how="left")
             .merge(f2, on=["d", "hour"], how="left")
             .merge(q_da, on=["d", "hour"], how="left")
             .merge(q_ida, on=["d", "hour"], how="left"))

    panel = panel.sort_values(["d", "hour"]).reset_index(drop=True)
    # forward-fill gas across weekends/holidays
    panel["gas_eur"] = panel["gas_eur"].ffill()
    # Fase I/II should be 0 if no entry; default to 0 only when day has data elsewhere
    for c in ["fase1_mwh", "fase2_mwh"]:
        if c in panel.columns:
            panel[c] = panel[c].fillna(0.0)
    # Per-tech: NaN means no clearing for that tech that hour -> 0
    for c in panel.columns:
        if c.startswith("q_") and c.endswith(("_da", "_ida")):
            panel[c] = panel[c].fillna(0.0)

    print(f"\nHourly panel: {len(panel):,} rows, {len(panel.columns)} cols")
    print(f"  date range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    print(f"  columns: {list(panel.columns)}")

    # Cheap sanity check: hourly DA mean per day should match the daily panel mean.
    try:
        daily = pd.read_parquet(REPO / "data/derived/panels/bsts_daily_panel.parquet")
        daily["d"] = pd.to_datetime(daily["d"])
        h_d = panel.dropna(subset=["da_price_eur"]).groupby("d")["da_price_eur"].mean().reset_index()
        merged = h_d.merge(daily[["d", "da_price_eur"]].rename(columns={"da_price_eur": "da_daily"}), on="d")
        merged["abs_diff"] = (merged["da_price_eur"] - merged["da_daily"]).abs()
        n_check = len(merged)
        n_close = (merged["abs_diff"] < 0.01).sum()
        print(f"\n  Sanity: hourly->daily DA price reproduces daily panel in {n_close}/{n_check} days (<0.01 EUR)")
        if n_check - n_close > 0:
            print(f"    max abs diff: {merged['abs_diff'].max():.4f} EUR/MWh")
    except Exception as e:
        print(f"\n  (daily-panel sanity skipped: {e})")

    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")
    print(f"  size: {OUT.stat().st_size / 1e6:.1f} MB")


if __name__ == "__main__":
    main()
