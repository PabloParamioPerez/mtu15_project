# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: ols_quantity_migration.R + bsts_quantity_migration.R, both of which
#        feed the appendix "Cleared quantities" slide of the June 2026 deck.
#
# Adds two ingredients on top of the existing hourly/daily quantity panels:
#   (i)  Cogen (CHP) per-tech cleared MWh / GWh, which the original build
#        scripts skipped.
#   (ii) The signed migration outcome Q_ida - Q_da (the right sign for the
#        ID15 question "did volume move from DA to IDA?"); for DA15 we just
#        flip the sign downstream.
#
# OUT:
#   data/derived/panels/bsts_hourly_panel_w_cogen_mig.parquet
#   data/derived/panels/bsts_daily_panel_w_cogen_mig.parquet

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
HOURLY_IN = REPO / "data/derived/panels/bsts_hourly_panel.parquet"
DAILY_IN  = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
HOURLY_OUT = REPO / "data/derived/panels/bsts_hourly_panel_w_cogen_mig.parquet"
DAILY_OUT  = REPO / "data/derived/panels/bsts_daily_panel_w_cogen_mig.parquet"

START_DATE = "2022-01-01"
TECHS = {  # tech_group in unit_map → column slug
    "CCGT":      "ccgt",
    "Hydro":     "hydro",      # the original build merges Hydro_pump into Hydro
    "Wind":      "wind",
    "Solar PV":  "solar",
    "Nuclear":   "nuclear",
    "Cogen":     "cogen",
}


def tech_hourly(prog_path: Path, market_tag: str, has_session: bool,
                  tech_group: str, slug: str) -> pd.DataFrame:
    """Hourly MWh per (d, hour) for a single OMIE tech_group label."""
    con = duckdb.connect()
    session_col = ", session_number" if has_session else ""
    sql = f"""
    WITH u AS (SELECT unit_code, tech_group FROM '{UNIT_MAP}'
               WHERE tech_group = '{tech_group}'),
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
           SUM(p.assigned_power_mw * (p.mtu_minutes / 60.0)) AS mwh
    FROM p JOIN u ON p.unit_code = u.unit_code
    GROUP BY 1, 2
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df = df.rename(columns={"mwh": f"q_{slug}_mwh_{market_tag}"})
    return df


def main():
    print("Reading existing hourly panel...")
    hourly = pd.read_parquet(HOURLY_IN)
    hourly["d"] = pd.to_datetime(hourly["d"])

    print("Computing Cogen + Hydro_pump hourly per market...")
    cogen_da  = tech_hourly(PDBC,  "da",  has_session=False, tech_group="Cogen",      slug="cogen")
    cogen_ida = tech_hourly(PIBCI, "ida", has_session=True,  tech_group="Cogen",      slug="cogen")
    pump_da   = tech_hourly(PDBC,  "da",  has_session=False, tech_group="Hydro_pump", slug="hydro_pump")
    pump_ida  = tech_hourly(PIBCI, "ida", has_session=True,  tech_group="Hydro_pump", slug="hydro_pump")
    print(f"  cogen DA: {len(cogen_da):,} / IDA: {len(cogen_ida):,};  "
          f"hydro_pump DA: {len(pump_da):,} / IDA: {len(pump_ida):,}")

    for add in [cogen_da, cogen_ida, pump_da, pump_ida]:
        hourly = hourly.merge(add, on=["d", "hour"], how="left")
    for c in ["q_cogen_mwh_da", "q_cogen_mwh_ida",
              "q_hydro_pump_mwh_da", "q_hydro_pump_mwh_ida"]:
        hourly[c] = hourly[c].fillna(0)

    # The existing hourly Hydro column from build_bsts_hourly_panel.py merges
    # Hydro + Hydro_pump (see its SQL: CASE WHEN tech_group = 'Hydro_pump' THEN 'Hydro').
    # Subtract Hydro_pump to recover pure Hydro, matching the daily-panel convention.
    hourly["q_hydro_mwh_da"]  = hourly["q_hydro_mwh_da"]  - hourly["q_hydro_pump_mwh_da"]
    hourly["q_hydro_mwh_ida"] = hourly["q_hydro_mwh_ida"] - hourly["q_hydro_pump_mwh_ida"]

    # Migration outcomes: q_<tech>_mig = Q_ida - Q_da (positive ⇒ migration toward IDA;
    # for DA15 we just flip sign downstream).
    for slug in ["ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind", "cogen"]:
        hourly[f"q_{slug}_mig_mwh"] = (
            hourly[f"q_{slug}_mwh_ida"] - hourly[f"q_{slug}_mwh_da"]
        )

    HOURLY_OUT.parent.mkdir(parents=True, exist_ok=True)
    hourly.to_parquet(HOURLY_OUT, index=False)
    print(f"  hourly w/ cogen + mig -> {HOURLY_OUT}")

    # Daily: aggregate hourly cogen to daily GWh and merge into daily panel
    print("\nReading existing daily panel...")
    daily = pd.read_parquet(DAILY_IN)
    daily["d"] = pd.to_datetime(daily["d"])
    cogen_daily = (hourly.groupby("d", as_index=False)[["q_cogen_mwh_da", "q_cogen_mwh_ida"]]
                          .sum())
    cogen_daily["q_cogen_gwh_da"]  = cogen_daily["q_cogen_mwh_da"]  / 1000
    cogen_daily["q_cogen_gwh_ida"] = cogen_daily["q_cogen_mwh_ida"] / 1000
    daily = daily.merge(cogen_daily[["d", "q_cogen_gwh_da", "q_cogen_gwh_ida"]],
                          on="d", how="left")
    for slug in ["ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind", "cogen"]:
        da_col = f"q_{slug}_gwh_da"; ida_col = f"q_{slug}_gwh_ida"
        if da_col in daily.columns and ida_col in daily.columns:
            daily[f"q_{slug}_mig_gwh"] = daily[ida_col] - daily[da_col]

    daily.to_parquet(DAILY_OUT, index=False)
    print(f"  daily w/ cogen + mig -> {DAILY_OUT}")

    # Sanity print
    print("\nCogen sanity (mean MWh/h, mean GWh/d):")
    print(f"  hourly DA mean:  {hourly['q_cogen_mwh_da'].mean():7.1f} MWh/h")
    print(f"  hourly IDA mean: {hourly['q_cogen_mwh_ida'].mean():7.1f} MWh/h")
    print(f"  daily DA mean:   {daily['q_cogen_gwh_da'].mean():7.2f} GWh/d")
    print(f"  daily IDA mean:  {daily['q_cogen_gwh_ida'].mean():7.2f} GWh/d")


if __name__ == "__main__":
    main()
