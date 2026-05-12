# STATUS: ALIVE
# LAST-AUDIT: 2026-05-12
# FEEDS: scripts/stata/B4_cpt.do (data prep for the Stata CPT spec stack)
# CLAIM: Build a Stata-friendly .dta panel for the §5.2 conditional parallel
#        trends spec, restricted to pivotal firms and merged with Spanish
#        hourly wind and solar production (ENTSO-E A75).

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as TREATMENT_PARENTS,
)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "stata_panels"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"


def hour_class(h):
    if h in CRITICAL_HOURS: return "critical"
    if h in FLAT_HOURS:     return "flat"
    return "other"


def build_q2_panel(units):
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code", "parent", "tech_group", "zone"]])
    rows = []
    for label, start, end in [("PRE", PRE_START, PRE_END), ("POST", POST_START, POST_END)]:
        df = con.execute(f"""
            WITH pibci_summed AS (
                SELECT date::DATE AS d, period,
                       ANY_VALUE(mtu_minutes) AS mtu,
                       unit_code, SUM(assigned_power_mw) AS q2_mw
                FROM '{PIBCI}'
                WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
                GROUP BY 1,2,4
            ),
            with_hour AS (
                SELECT p.d, p.unit_code, p.period, p.mtu,
                       u.parent, u.tech_group, u.zone,
                       CASE WHEN mtu = 60 THEN period - 1
                            WHEN mtu = 15 THEN (period - 1) // 4
                            ELSE NULL END AS hour,
                       q2_mw * mtu / 60.0 AS q2_mwh
                FROM pibci_summed p JOIN units u USING (unit_code)
            )
            SELECT d, unit_code, parent, tech_group, zone, hour,
                   SUM(q2_mwh) AS q2_mwh
            FROM with_hour
            WHERE hour IS NOT NULL AND hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4,5,6
        """).df()
        df["post"] = 1 if label == "POST" else 0
        rows.append(df)
    panel = pd.concat(rows, ignore_index=True)
    panel["d"] = pd.to_datetime(panel["d"])
    panel["hour_class"] = panel["hour"].astype(int).apply(hour_class)
    panel["crit"] = (panel["hour_class"] == "critical").astype(int)
    panel["dow"] = panel["d"].dt.dayofweek
    panel["month"] = panel["d"].dt.month
    return panel[panel["hour_class"].isin(["critical", "flat"])].copy()


def build_vre(start, end):
    """Hourly Spanish wind + solar production in MW."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
               EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
               psr_type,
               AVG(quantity_mw) AS mw
        FROM '{WIND_SOLAR}'
        WHERE isp_start_utc >= TIMESTAMP '{start}'
          AND isp_start_utc <  TIMESTAMP '{end}'
        GROUP BY 1,2,3
    """).df()
    df = df.pivot_table(index=["d","hour"], columns="psr_type", values="mw").reset_index()
    df.columns = [str(c) for c in df.columns]
    keep = {"B16": "solar_mw", "B18": "wind_on_mw", "B19": "wind_off_mw"}
    rename = {old: new for old, new in keep.items() if old in df.columns}
    df = df.rename(columns=rename)
    for c in ["solar_mw", "wind_on_mw", "wind_off_mw"]:
        if c not in df.columns: df[c] = 0
    df["wind_mw"] = df["wind_on_mw"].fillna(0) + df["wind_off_mw"].fillna(0)
    df["solar_mw"] = df["solar_mw"].fillna(0)
    df["d"] = pd.to_datetime(df["d"])
    df["hour"] = df["hour"].astype(int)
    return df[["d","hour","wind_mw","solar_mw"]]


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    panel = build_q2_panel(units)
    print(f"q_2 panel rows: {len(panel):,}")

    # Restrict to pivotal firms (where the headline lives)
    panel = panel[panel["parent"].isin(TREATMENT_PARENTS)].copy()
    print(f"Pivotal subset: {len(panel):,}")

    vre = pd.concat([build_vre(PRE_START, PRE_END), build_vre(POST_START, POST_END)], ignore_index=True)
    print(f"VRE rows: {len(vre):,}")

    panel = panel.merge(vre, on=["d", "hour"], how="left")
    # Fill missing with sample mean (defensive)
    panel["wind_mw"] = panel["wind_mw"].fillna(panel["wind_mw"].mean())
    panel["solar_mw"] = panel["solar_mw"].fillna(panel["solar_mw"].mean())

    # Demean for interpretability of interactions
    panel["wind_z"] = panel["wind_mw"] - panel["wind_mw"].mean()
    panel["solar_z"] = panel["solar_mw"] - panel["solar_mw"].mean()

    panel["d_int"] = (panel["d"] - pd.Timestamp("1960-01-01")).dt.days
    panel["unit_code"] = panel["unit_code"].astype(str).str[:32]
    panel["parent"] = panel["parent"].astype(str).str[:16]
    panel["tech_group"] = panel["tech_group"].astype(str).str[:16]
    panel["hour"] = panel["hour"].astype(int)

    out = OUTDIR / "B4_cpt_panel.dta"
    panel[["d_int","unit_code","parent","tech_group","hour","hour_class",
           "crit","post","month","dow","q2_mwh","wind_mw","solar_mw","wind_z","solar_z"]].to_stata(
        out, version=118, write_index=False)
    print(f"Saved {out}  ({len(panel):,} obs)")


if __name__ == "__main__":
    main()
