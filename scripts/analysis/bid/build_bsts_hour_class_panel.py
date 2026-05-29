# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: scripts/analysis/bid/bsts_hour_class.R -- per-hour-class BSTS on
#        bid-level outcomes. For each (tech, market, hour_class, d), aggregates
#        from raw tranches (det / idet) filtered to the in-band region
#        |p_bid - MCP| <= H = 140 EUR/MWh:
#          mean_p_inband  -- MW-weighted mean of in-band tranche prices.
#          sum_q_inband   -- total in-band quantity (MW, summed across units,
#                            periods, and -- for IDA -- sessions).
#        Then merges with the wind / solar / gas covariates from the existing
#        bsts_daily_panel.parquet.
#
#        Hour-classes match the bid-shape DiD partition:
#          Critical = {5,6,7,8, 16,17,18,19,20,21,22}
#          Midday   = {11,12,13,14}
#          Flat     = {1,2,3}
#
#        Techs: CCGT, Hydro, Hydro_pump. (Wind / Solar / Nuclear are
#        price-takers and excluded here.)
#
# OUT: data/derived/panels/bsts_hour_class_panel.parquet

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
BSTS_BASE = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT = REPO / "data/derived/panels/bsts_hour_class_panel.parquet"
OUT.parent.mkdir(parents=True, exist_ok=True)

H = 140.0
START = "2023-06-01"  # covers ID15 + DA15 long pre-windows + 2024 placebo pres
END = "2026-05-15"

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
MIDDAY = {11, 12, 13, 14}
FLAT = {1, 2, 3}

TECHS = ["CCGT", "Hydro", "Hydro_pump"]
HOUR_CLASSES = ["critical", "midday", "flat"]


def da_query():
    """Per (date, clock_hour) tranche-level in-band aggregates for DA market."""
    crit_set = ",".join(str(h) for h in CRITICAL)
    mid_set  = ",".join(str(h) for h in MIDDAY)
    flat_set = ",".join(str(h) for h in FLAT)
    return f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}')
      WHERE date BETWEEN '{START}' AND '{END}' AND buy_sell='V'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period,
             price_eur_mwh AS p, quantity_mw AS q,
             COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{DET}')
      WHERE date BETWEEN '{START}' AND '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}')
      WHERE date BETWEEN '{START}' AND '{END}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv
        JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d,
           CASE WHEN i.clock_hour IN ({crit_set}) THEN 'critical'
                WHEN i.clock_hour IN ({mid_set})  THEN 'midday'
                WHEN i.clock_hour IN ({flat_set}) THEN 'flat'
                ELSE 'other' END AS hour_class,
           CASE WHEN u.tech_group = 'Hydro_pump' THEN 'Hydro_pump'
                ELSE u.tech_group END AS tech,
           SUM(i.q)       AS sum_q,
           SUM(i.q * i.p) AS sum_qp
    FROM inband i
      JOIN read_parquet('{UNIT_MAP}') u ON i.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2, 3
    """


def ida_query():
    """Per (date, clock_hour) tranche-level in-band aggregates for IDA market.
    Pools across sessions for the same delivery hour."""
    crit_set = ",".join(str(h) for h in CRITICAL)
    mid_set  = ",".join(str(h) for h in MIDDAY)
    flat_set = ",".join(str(h) for h in FLAT)
    return f"""
    WITH icab_last AS (
      SELECT CAST(date AS DATE) AS d, session_number, offer_code, version, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), session_number,
                                              offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{ICAB}')
      WHERE date BETWEEN '{START}' AND '{END}' AND buy_sell='V'
    ),
    icab_l AS (SELECT d, session_number, offer_code, version, unit_code
               FROM icab_last WHERE rn=1),
    idet AS (
      SELECT CAST(date AS DATE) AS d, session_number, offer_code, version,
             unit_code, period,
             price_eur_mwh AS p, quantity_mw AS q,
             COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{IDET}')
      WHERE date BETWEEN '{START}' AND '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, session_number, period,
             price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPIBC}')
      WHERE date BETWEEN '{START}' AND '{END}'
        AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM idet dv
        JOIN icab_l c
          ON dv.d=c.d AND dv.session_number=c.session_number
         AND dv.offer_code=c.offer_code AND dv.version=c.version
         AND dv.unit_code=c.unit_code
        JOIN mp ON mp.d=dv.d AND mp.session_number=dv.session_number
                AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    SELECT i.d,
           CASE WHEN i.clock_hour IN ({crit_set}) THEN 'critical'
                WHEN i.clock_hour IN ({mid_set})  THEN 'midday'
                WHEN i.clock_hour IN ({flat_set}) THEN 'flat'
                ELSE 'other' END AS hour_class,
           CASE WHEN u.tech_group = 'Hydro_pump' THEN 'Hydro_pump'
                ELSE u.tech_group END AS tech,
           SUM(i.q)       AS sum_q,
           SUM(i.q * i.p) AS sum_qp
    FROM inband i
      JOIN read_parquet('{UNIT_MAP}') u ON i.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2, 3
    """


def aggregate(con, market_tag, query):
    df = con.execute(query).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df = df[df["hour_class"].isin(HOUR_CLASSES)]
    df = df[df["tech"].isin(TECHS)]
    df["mean_p"] = df["sum_qp"] / df["sum_q"]
    df["market"] = market_tag
    return df


def pivot_wide(df):
    """Reshape long (d, tech, market, hour_class) → wide.
    Columns: p_{tech}_{market}_{hour_class}, q_{tech}_{market}_{hour_class}."""
    df = df.copy()
    df["tech_lower"] = df["tech"].str.lower()
    p = df.pivot_table(index="d",
                       columns=["tech_lower", "market", "hour_class"],
                       values="mean_p", aggfunc="first").reset_index()
    q = df.pivot_table(index="d",
                       columns=["tech_lower", "market", "hour_class"],
                       values="sum_q", aggfunc="first").reset_index()
    p.columns = ["d"] + [f"p_{t}_{m}_{hc}"
                          for (t, m, hc) in p.columns[1:]]
    q.columns = ["d"] + [f"q_{t}_{m}_{hc}"
                          for (t, m, hc) in q.columns[1:]]
    out = p.merge(q, on="d", how="outer").sort_values("d").reset_index(drop=True)
    return out


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")

    print("Aggregating DA tranche-level in-band data...")
    da = aggregate(con, "da", da_query())
    print(f"  DA rows: {len(da):,}, dates {da['d'].min().date()} -> {da['d'].max().date()}")

    print("Aggregating IDA tranche-level in-band data...")
    ida = aggregate(con, "ida", ida_query())
    print(f"  IDA rows: {len(ida):,}, dates {ida['d'].min().date()} -> {ida['d'].max().date()}")

    print("Pivoting to wide format...")
    long = pd.concat([da, ida], ignore_index=True)
    wide = pivot_wide(long)
    print(f"  Wide: {len(wide):,} dates x {len(wide.columns)-1} outcome cols")

    print("Merging with covariates from bsts_daily_panel.parquet...")
    base = pd.read_parquet(BSTS_BASE)
    base["d"] = pd.to_datetime(base["d"])
    covars = base[["d", "wind_gwh", "solar_gwh", "gas_eur"]]
    out = wide.merge(covars, on="d", how="left")
    out = out.sort_values("d").reset_index(drop=True)
    print(f"  Final panel: {len(out):,} dates x {len(out.columns)} cols")

    out.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
