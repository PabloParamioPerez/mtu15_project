# STATUS: ALIVE
# LAST-AUDIT: 2026-05-12
# FEEDS: scripts/stata/firm_ddd.do (App. triple-difference q_2 DiD)
# CLAIM: Take the headline B1 unit-day-hour panel (pivotal + non-pivotal
#        firms, critical + flat hours) and merge day-level Spain wind,
#        solar, and demand for the DDD covariate adjustment.

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"

PANELDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "stata_panels"
PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"


def build_xdaily(start, end):
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
    out = vre.merge(dem, on="d", how="inner")
    out["d"] = pd.to_datetime(out["d"])
    return out


def main():
    # Read the headline panel which already has pivotal+non-pivotal × critical+flat
    panel = pd.read_stata(PANELDIR / "B1_headline.dta")
    panel["d"] = pd.Timestamp("1960-01-01") + pd.to_timedelta(panel["d_int"], unit="D")
    panel["pivotal"] = (panel["treatment_group"] == "treatment").astype(int)
    panel = panel[panel["treatment_group"].isin(["treatment", "placebo"])].copy()
    print(f"DDD panel rows: {len(panel):,}")

    xdaily = pd.concat([build_xdaily(PRE_START, PRE_END), build_xdaily(POST_START, POST_END)],
                       ignore_index=True)
    print(f"x-daily rows: {len(xdaily):,}")

    panel = panel.merge(xdaily, on="d", how="left")
    for c in ["wind_gw", "solar_gw", "demand_gw"]:
        panel[c] = panel[c].fillna(panel[c].mean())

    panel["month"] = panel["d"].dt.month

    out = PANELDIR / "ddd_panel.dta"
    cols = ["d_int", "unit_code", "parent", "tech_group",
            "hour", "hour_class", "crit", "post", "pivotal",
            "dow", "month",
            "q2_mwh", "wind_gw", "solar_gw", "demand_gw"]
    panel[cols].to_stata(out, version=118, write_index=False)
    print(f"Saved {out}  ({len(panel):,} obs)")

    print("\nDDD cube (pivotal × post × crit):")
    print(panel.groupby(["pivotal","post","crit"]).size())


if __name__ == "__main__":
    main()
