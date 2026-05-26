# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: scripts/analysis/bid/bsts_{da15,id15}_price.R via the daily panel
#        at data/derived/panels/bsts_daily_panel.parquet.
#
# Builds a daily panel of Spanish electricity prices + contemporaneous
# fundamentals (wind, solar, demand, gas) for BSTS causal-impact analysis
# of the MTU15-IDA and MTU15-DA cutovers (per Markle-Huss et al. 2017
# Energy Economics, applied to EPEX 15-min). Two response series:
#   da_price_eur   -- daily mean OMIE DA clearing price (EUR/MWh)
#   ida_price_eur  -- daily mean OMIE IDA clearing price across sessions
# Covariates (all daily aggregates from the 15-min source data):
#   wind_gwh       -- ENTSO-E A75 B19 (wind), daily sum, GWh
#   solar_gwh      -- ENTSO-E A75 B16+B18, daily sum, GWh
#   demand_gwh     -- ENTSO-E A65 actual load, daily sum, GWh
#   gas_eur        -- ESIOS indicator 1940 (Gas Natural TNP), daily price
#
# OUT: data/derived/panels/bsts_daily_panel.parquet

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
ENTSOE_GEN = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
ENTSOE_LOAD = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
GAS = REPO / "data/processed/esios/indicators/1940.parquet"
FASE1_VOL = REPO / "data/processed/esios/indicators/10051.parquet"  # Daily Fase I volume (abs)
FASE2_VOL = REPO / "data/processed/esios/indicators/10270.parquet"  # Daily Fase II volume (abs)
OUT = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)


def build_daily_prices():
    con = duckdb.connect()
    da = con.execute(f"""
    SELECT CAST(date AS DATE) d, AVG(price_es_eur_mwh) AS da_price_eur
    FROM '{MPDBC}'
    WHERE price_es_eur_mwh IS NOT NULL AND date >= '2022-01-01'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    ida = con.execute(f"""
    SELECT CAST(date AS DATE) d, AVG(price_es_eur_mwh) AS ida_price_eur
    FROM '{MPIBC}'
    WHERE price_es_eur_mwh IS NOT NULL AND date >= '2022-01-01'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    da["d"] = pd.to_datetime(da["d"])
    ida["d"] = pd.to_datetime(ida["d"])
    return da, ida


def build_daily_gen():
    con = duckdb.connect()
    df = con.execute(f"""
    SELECT CAST(isp_start_utc AS DATE) d, psr_type,
           SUM(quantity_mw * (mtu_minutes / 60.0)) / 1000.0 AS gwh
    FROM '{ENTSOE_GEN}'
    WHERE psr_type IN ('B16','B18','B19') AND isp_start_utc >= '2022-01-01'
    GROUP BY 1, 2
    """).fetchdf()
    w = df.pivot_table(index="d", columns="psr_type", values="gwh", fill_value=0).reset_index()
    w.columns.name = None
    w["d"] = pd.to_datetime(w["d"])
    w = w.rename(columns={"B19": "wind_gwh", "B16": "solar_pv_gwh", "B18": "wind_offshore_gwh"})
    if "wind_offshore_gwh" in w.columns:
        w["wind_gwh"] = w["wind_gwh"].fillna(0) + w["wind_offshore_gwh"].fillna(0)
        w = w.drop(columns=["wind_offshore_gwh"])
    w["solar_gwh"] = w["solar_pv_gwh"]  # solar PV only (B16); B18 was wind offshore in our coding
    w = w[["d", "wind_gwh", "solar_gwh"]]
    return w


def build_daily_load():
    con = duckdb.connect()
    df = con.execute(f"""
    SELECT CAST(isp_start_utc AS DATE) d,
           SUM(load_mw * (mtu_minutes / 60.0)) / 1000.0 AS demand_gwh
    FROM '{ENTSOE_LOAD}'
    WHERE isp_start_utc >= '2022-01-01'
    GROUP BY 1 ORDER BY 1
    """).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    return df


def build_daily_gas():
    df = pd.read_parquet(GAS)
    df["d"] = pd.to_datetime(df["ts_utc"]).dt.tz_localize(None).dt.normalize()
    df = (df.sort_values("d").drop_duplicates("d", keep="last")
            [["d", "value"]].rename(columns={"value": "gas_eur"}))
    df = df[df["d"] >= pd.Timestamp("2022-01-01")]
    return df


def build_daily_restrictions():
    """Daily Fase I and Fase II restrictions volumes (absolute MW summed to GWh)."""
    f1 = pd.read_parquet(FASE1_VOL)
    f1["d"] = pd.to_datetime(f1["ts_utc"]).dt.tz_localize(None).dt.normalize()
    f1 = (f1[f1["d"] >= pd.Timestamp("2022-01-01")]
          .groupby("d", as_index=False)["value"].apply(lambda x: x.abs().sum() * 0.25 / 1000.0)
          .rename(columns={"value": "fase1_gwh"}))
    f2 = pd.read_parquet(FASE2_VOL)
    f2["d"] = pd.to_datetime(f2["ts_utc"]).dt.tz_localize(None).dt.normalize()
    f2 = (f2[f2["d"] >= pd.Timestamp("2022-01-01")]
          .groupby("d", as_index=False)["value"].apply(lambda x: x.abs().sum() * 0.25 / 1000.0)
          .rename(columns={"value": "fase2_gwh"}))
    return f1.merge(f2, on="d", how="outer")


def main():
    print("Building daily prices (DA + IDA)...")
    da, ida = build_daily_prices()
    print(f"  DA: {len(da):,} days, IDA: {len(ida):,} days")
    print("Building daily wind + solar (ENTSO-E A75)...")
    gen = build_daily_gen()
    print(f"  gen: {len(gen):,} days")
    print("Building daily demand (ENTSO-E A65)...")
    load = build_daily_load()
    print(f"  load: {len(load):,} days")
    print("Building daily gas (ESIOS 1940)...")
    gas = build_daily_gas()
    print(f"  gas: {len(gas):,} days (forward-filled below)")
    print("Building daily Fase I + Fase II restrictions volumes (ESIOS 10051+10270)...")
    restr = build_daily_restrictions()
    print(f"  restrictions: {len(restr):,} days")

    panel = (da.merge(ida, on="d", how="outer")
               .merge(gen, on="d", how="left")
               .merge(load, on="d", how="left")
               .merge(gas, on="d", how="left")
               .merge(restr, on="d", how="left"))
    panel = panel.sort_values("d").reset_index(drop=True)
    # forward-fill gas (daily series with weekend/holiday gaps)
    panel["gas_eur"] = panel["gas_eur"].ffill()

    # Drop rows with missing essentials
    before = len(panel)
    panel = panel.dropna(subset=["da_price_eur", "wind_gwh", "solar_gwh", "demand_gwh"])
    print(f"  panel: {len(panel):,} days after dropna (was {before:,})")
    print(f"  date range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    print(f"  columns: {list(panel.columns)}")

    panel.to_parquet(OUT, index=False)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
