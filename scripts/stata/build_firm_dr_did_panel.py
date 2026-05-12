# STATUS: ALIVE
# LAST-AUDIT: 2026-05-12
# FEEDS: scripts/stata/firm_dr_did.do (App. firm-partition DR DiD)
# CLAIM: Build a Stata .dta panel where the DiD partition is pivotal-vs-
#        non-pivotal firms (not critical-vs-flat hours). Outcome is the
#        within-day critical-flat differential of q_2 per (unit, date).
#        Strong overlap holds because firm class does not determine the
#        day-level weather/demand X.

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
    PLACEBO_PARENTS_SHORT as PLACEBO_PARENTS,
)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "stata_panels"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"


def build_unit_day_panel(units):
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code", "parent", "tech_group"]])
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
                       u.parent, u.tech_group,
                       CASE WHEN mtu = 60 THEN period - 1
                            WHEN mtu = 15 THEN (period - 1) // 4
                            ELSE NULL END AS hour,
                       q2_mw * mtu / 60.0 AS q2_mwh
                FROM pibci_summed p JOIN units u USING (unit_code)
            ),
            classified AS (
                SELECT d, unit_code, parent, tech_group,
                       CASE WHEN hour IN ({','.join(map(str, CRITICAL_HOURS))}) THEN 'critical'
                            WHEN hour IN ({','.join(map(str, FLAT_HOURS))})     THEN 'flat'
                            ELSE 'other' END AS hour_class,
                       q2_mwh
                FROM with_hour
                WHERE hour IS NOT NULL AND hour BETWEEN 0 AND 23
            )
            SELECT d, unit_code, parent, tech_group, hour_class,
                   SUM(q2_mwh) AS q2_mwh_sum,
                   COUNT(*) AS n_hours
            FROM classified
            WHERE hour_class IN ('critical', 'flat')
            GROUP BY 1,2,3,4,5
        """).df()
        df["window"] = label
        df["post"] = 1 if label == "POST" else 0
        rows.append(df)
    panel = pd.concat(rows, ignore_index=True)
    panel["d"] = pd.to_datetime(panel["d"])
    return panel


def build_xdaily(start, end):
    """Day-level Spanish wind + solar + demand in GW (averaged over hours of day)."""
    con = duckdb.connect()
    vre = con.execute(f"""
        SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d, psr_type,
               AVG(quantity_mw) / 1000.0 AS gw
        FROM '{WIND_SOLAR}'
        WHERE isp_start_utc >= TIMESTAMP '{start}'
          AND isp_start_utc <  TIMESTAMP '{end}'
        GROUP BY 1, 2
    """).df()
    vre = vre.pivot_table(index="d", columns="psr_type", values="gw").reset_index()
    vre.columns = [str(c) for c in vre.columns]
    keep = {"B16": "solar_gw", "B18": "wind_on", "B19": "wind_off"}
    for old, new in keep.items():
        if old not in vre.columns: vre[old] = 0
        vre = vre.rename(columns={old: new})
    vre["wind_gw"] = vre["wind_on"].fillna(0) + vre["wind_off"].fillna(0)
    vre["solar_gw"] = vre["solar_gw"].fillna(0)
    vre = vre[["d", "wind_gw", "solar_gw"]]

    dem = con.execute(f"""
        SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
               AVG(load_mw) / 1000.0 AS demand_gw
        FROM '{LOAD}'
        WHERE isp_start_utc >= TIMESTAMP '{start}'
          AND isp_start_utc <  TIMESTAMP '{end}'
        GROUP BY 1
    """).df()
    df = vre.merge(dem, on="d", how="inner")
    df["d"] = pd.to_datetime(df["d"])
    return df


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")

    panel = build_unit_day_panel(units)
    print(f"unit-day-hourclass rows: {len(panel):,}")

    # Pivot to wide: one row per (unit, date) with crit_sum and flat_sum
    wide = panel.pivot_table(index=["d", "unit_code", "parent", "tech_group", "post"],
                              columns="hour_class", values="q2_mwh_sum", aggfunc="sum").reset_index()
    wide.columns = [c if isinstance(c, str) else c for c in wide.columns]
    if "critical" not in wide.columns: wide["critical"] = 0
    if "flat" not in wide.columns:     wide["flat"] = 0
    wide["critical"] = wide["critical"].fillna(0)
    wide["flat"] = wide["flat"].fillna(0)
    wide["y_diff"] = wide["critical"] - wide["flat"]
    print(f"unit-day rows: {len(wide):,}")

    # Restrict to pivotal + non-pivotal sample (the DiD groups)
    wide = wide[wide["parent"].isin(list(TREATMENT_PARENTS) + list(PLACEBO_PARENTS))].copy()
    wide["pivotal"] = wide["parent"].isin(TREATMENT_PARENTS).astype(int)
    print(f"with pivotal/non-pivotal restriction: {len(wide):,}")

    # Merge day-level X
    xdaily = pd.concat([build_xdaily(PRE_START, PRE_END), build_xdaily(POST_START, POST_END)],
                        ignore_index=True)
    print(f"x-daily rows: {len(xdaily):,}")
    wide = wide.merge(xdaily, on="d", how="left")
    for c in ["wind_gw", "solar_gw", "demand_gw"]:
        wide[c] = wide[c].fillna(wide[c].mean())

    # Stata-friendly types
    wide["d_int"] = (wide["d"] - pd.Timestamp("1960-01-01")).dt.days
    wide["unit_code"] = wide["unit_code"].astype(str).str[:32]
    wide["parent"] = wide["parent"].astype(str).str[:16]
    wide["tech_group"] = wide["tech_group"].astype(str).str[:16]
    wide["dow"] = wide["d"].dt.dayofweek
    wide["month"] = wide["d"].dt.month

    out = OUTDIR / "firm_dr_did_panel.dta"
    cols = ["d_int", "unit_code", "parent", "tech_group", "post", "pivotal",
            "critical", "flat", "y_diff",
            "wind_gw", "solar_gw", "demand_gw", "dow", "month"]
    wide[cols].to_stata(out, version=118, write_index=False)
    print(f"saved {out} ({len(wide):,} unit-day obs)")

    # Quick balance check
    print("\nCounts by (post, pivotal):")
    print(wide.groupby(["post", "pivotal"]).size().to_string())
    print("\ny_diff means by (post, pivotal):")
    print(wide.groupby(["post", "pivotal"])["y_diff"].mean().to_string())


if __name__ == "__main__":
    main()
