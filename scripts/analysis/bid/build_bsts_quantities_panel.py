# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: scripts/analysis/bid/bsts_{da15,id15}_quantity.R and the placebo
#        scripts via data/derived/panels/bsts_quantities_panel.parquet.
#
# Extends bsts_daily_panel.parquet with daily per-tech auction-cleared energy
# (GWh) for the DA market (pdbc) and IDA market (pibci). Auction-cleared MW
# is pre-restriction by construction in OMIE (Fase I/II redispatch lives in
# phf, not pdbc/pibci).
#
# Tech join via _unit_map (tech_group). Daily aggregation:
#   energy_gwh = SUM(assigned_power_mw * mtu_minutes / 60) / 1000
#
# OUT: data/derived/panels/bsts_quantities_panel.parquet

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
PIBCI = REPO / "data/processed/omie/mercado_intradiario_subastas/programas/pibci_all.parquet"
UNIT_MAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
BASE_PANEL = REPO / "data/derived/panels/bsts_daily_panel.parquet"
OUT = REPO / "data/derived/panels/bsts_quantities_panel.parquet"

TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Nuclear"]


def per_tech_daily(prog_path, market_tag):
    con = duckdb.connect()
    q = f"""
    WITH u AS (SELECT unit_code, tech_group FROM '{UNIT_MAP}'),
         p AS (
           SELECT CAST(date AS DATE) d,
                  unit_code,
                  assigned_power_mw,
                  mtu_minutes
           FROM '{prog_path}'
           WHERE assigned_power_mw IS NOT NULL
             AND assigned_power_mw > 0
             AND date >= '2022-01-01'
         )
    SELECT p.d,
           u.tech_group AS tech,
           SUM(p.assigned_power_mw * p.mtu_minutes / 60.0) / 1000.0 AS gwh
    FROM p LEFT JOIN u ON p.unit_code = u.unit_code
    WHERE u.tech_group IS NOT NULL
    GROUP BY 1, 2
    """
    df = con.execute(q).fetchdf()
    df = df[df["tech"].isin(TECHS)].copy()
    df["col"] = ("q_" + df["tech"].str.lower().str.replace(" pv", "")
                                .str.replace(" ", "_") + f"_gwh_{market_tag}")
    wide = (df.pivot_table(index="d", columns="col", values="gwh", aggfunc="first")
              .reset_index())
    wide.columns.name = None
    wide["d"] = pd.to_datetime(wide["d"])
    return wide


def main():
    print("Per-tech daily cleared GWh, DA (pdbc)...")
    da = per_tech_daily(PDBC, "da")
    print(f"  {len(da):,} days; columns: {[c for c in da.columns if c != 'd']}")
    print("Per-tech daily cleared GWh, IDA (pibci)...")
    ida = per_tech_daily(PIBCI, "ida")
    print(f"  {len(ida):,} days; columns: {[c for c in ida.columns if c != 'd']}")

    base = pd.read_parquet(BASE_PANEL)
    base["d"] = pd.to_datetime(base["d"])

    panel = base.merge(da, on="d", how="left").merge(ida, on="d", how="left")
    panel = panel.sort_values("d").reset_index(drop=True)

    print(f"\nPanel: {len(panel):,} days, {len(panel.columns)} cols")
    print(f"  date range: {panel['d'].min().date()} -> {panel['d'].max().date()}")
    print(f"  columns: {list(panel.columns)}")

    panel.to_parquet(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
