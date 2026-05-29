# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: scripts/analysis/bid/bsts_wedge_hour_class.R -- BSTS on the DA - IDA
#        clearing-price wedge separately by hour-class (critical / midday /
#        flat), to check whether the wedge mechanism concentrates in hours
#        where within-hour residual demand actually varies.
#
# Builds a daily panel with three wedge series (one per hour-class) and the
# wind+solar+gas covariates from bsts_daily_panel.parquet.
#
# OUT: data/derived/panels/wedge_hour_class_panel.parquet

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
IDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
BASE = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT  = REPO / "data/derived/panels/wedge_hour_class_panel.parquet"

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
MIDDAY   = {11, 12, 13, 14}
FLAT     = {1, 2, 3}


def hour_class_sql(col):
    crit = ",".join(str(k) for k in CRITICAL)
    mid  = ",".join(str(k) for k in MIDDAY)
    flat = ",".join(str(k) for k in FLAT)
    return f"""
        CASE WHEN {col} IN ({crit}) THEN 'critical'
             WHEN {col} IN ({mid})  THEN 'midday'
             WHEN {col} IN ({flat}) THEN 'flat' END
    """


def main():
    con = duckdb.connect()
    # DA: hourly clearing price by clock hour (period-1 when MTU60, or
    # floor((period-1)/4) when MTU15).
    da_sql = f"""
        SELECT CAST(date AS DATE) d,
               CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                    ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
               AVG(price_es_eur_mwh) AS da_p
        FROM '{DA}' WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """
    # IDA: average across sessions at each delivery clock_hour.
    ida_sql = f"""
        SELECT CAST(date AS DATE) d,
               CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                    ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
               AVG(price_es_eur_mwh) AS ida_p
        FROM '{IDA}' WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """
    print("Computing per-(date, clock_hour) DA and IDA prices...")
    joined = con.execute(f"""
        WITH da AS ({da_sql}), ida AS ({ida_sql})
        SELECT da.d, da.clock_hour,
               da.da_p, ida.ida_p,
               da.da_p - ida.ida_p AS wedge,
               {hour_class_sql("da.clock_hour")} AS hour_class
        FROM da JOIN ida USING (d, clock_hour)
    """).fetchdf()
    print(f"  {len(joined):,} (date, clock_hour) cells")

    # Daily wedge per hour-class
    print("Aggregating to (date, hour_class) daily means...")
    daily = (joined[joined["hour_class"].notna()]
              .groupby(["d", "hour_class"])["wedge"].mean()
              .unstack("hour_class")
              .rename(columns={"critical": "wedge_critical",
                                "midday":   "wedge_midday",
                                "flat":     "wedge_flat"}))
    daily.index = pd.to_datetime(daily.index)
    daily = daily.reset_index().rename(columns={"d": "d"})

    # Merge with covariates from bsts_daily_panel
    base = pd.read_parquet(BASE)[["d", "wind_gwh", "solar_gwh", "gas_eur"]]
    base["d"] = pd.to_datetime(base["d"])
    out = daily.merge(base, on="d", how="left").sort_values("d").reset_index(drop=True)
    print(f"  {len(out):,} days; "
          f"wedge_critical range "
          f"{out['wedge_critical'].min():+.1f} to {out['wedge_critical'].max():+.1f}, "
          f"wedge_flat range "
          f"{out['wedge_flat'].min():+.1f} to {out['wedge_flat'].max():+.1f}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUT, index=False)
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
