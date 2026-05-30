# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: scripts/analysis/bid/bsts_hour_class_cleared.R via
#        data/derived/panels/bsts_hour_class_q_panel.parquet.
#
# Per-(date, tech, market, hour-class) auction-cleared MWh panel.
# Same energy convention as build_bsts_quantities_panel.py:
#   mwh = SUM(assigned_power_mw * mtu_minutes / 60).
# Clock-hour partition matches build_wedge_volatility_panel.py:
#   CRITICAL = {5..8, 16..22}, MIDDAY = {11..14}, FLAT = {1,2,3}.
#
# OUT: data/derived/panels/bsts_hour_class_q_panel.parquet
#      (wide: one column per tech x market x hour_class, plus base covariates)

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
BASE_PANEL = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT = REPO / "data/derived/panels/bsts_hour_class_q_panel.parquet"

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind", "Solar PV"]
CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
MIDDAY = {11, 12, 13, 14}
FLAT = {1, 2, 3}


def per_tech_hour_class_daily(prog_path, market_tag):
    con = duckdb.connect()
    crit_list = ",".join(str(h) for h in sorted(CRITICAL))
    mid_list = ",".join(str(h) for h in sorted(MIDDAY))
    flat_list = ",".join(str(h) for h in sorted(FLAT))
    q = f"""
    WITH u AS (SELECT unit_code, tech_group FROM '{UNIT_MAP}'),
         p AS (
           SELECT CAST(date AS DATE) d,
                  CASE WHEN COALESCE(mtu_minutes, 60) = 60 THEN period - 1
                       ELSE CAST(FLOOR((period - 1) / 4.0) AS INT) END AS clock_hour,
                  unit_code,
                  assigned_power_mw,
                  mtu_minutes
           FROM '{prog_path}'
           WHERE assigned_power_mw IS NOT NULL
             AND assigned_power_mw > 0
             AND date >= '2022-01-01'
         ),
         pclass AS (
           SELECT d, clock_hour, unit_code,
                  assigned_power_mw, mtu_minutes,
                  CASE
                    WHEN clock_hour IN ({crit_list}) THEN 'critical'
                    WHEN clock_hour IN ({mid_list})  THEN 'midday'
                    WHEN clock_hour IN ({flat_list}) THEN 'flat'
                  END AS hour_class
           FROM p
         )
    SELECT pclass.d, u.tech_group AS tech, pclass.hour_class,
           SUM(pclass.assigned_power_mw * pclass.mtu_minutes / 60.0) AS mwh
    FROM pclass LEFT JOIN u ON pclass.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL AND pclass.hour_class IS NOT NULL
    GROUP BY 1, 2, 3
    """
    df = con.execute(q).fetchdf()
    df = df[df["tech"].isin(TECHS)].copy()
    df["col"] = ("q_" + df["tech"].str.lower().str.replace(" pv", "")
                                .str.replace(" ", "_")
                 + f"_mwh_{market_tag}_" + df["hour_class"])
    wide = (df.pivot_table(index="d", columns="col", values="mwh", aggfunc="first")
              .reset_index())
    wide.columns.name = None
    wide["d"] = pd.to_datetime(wide["d"])
    return wide


def main():
    print("Per-tech x hour-class daily cleared MWh, DA (pdbc)...")
    da = per_tech_hour_class_daily(PDBC, "da")
    print(f"  {len(da):,} days; {len(da.columns)-1} per-tech-class columns")
    print("Per-tech x hour-class daily cleared MWh, IDA (pibci)...")
    ida = per_tech_hour_class_daily(PIBCI, "ida")
    print(f"  {len(ida):,} days; {len(ida.columns)-1} per-tech-class columns")

    base = pd.read_parquet(BASE_PANEL)[
        ["d", "da_price_eur", "ida_price_eur", "wind_gwh", "solar_gwh", "gas_eur"]
    ]
    base["d"] = pd.to_datetime(base["d"])

    panel = base.merge(da, on="d", how="left").merge(ida, on="d", how="left")
    panel = panel.sort_values("d").reset_index(drop=True)

    print(f"\nPanel: {len(panel):,} days, {len(panel.columns)} cols")
    print(f"  date range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
