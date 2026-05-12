# STATUS: ALIVE
# LAST-AUDIT: 2026-05-12
# FEEDS: scripts/stata/B1_q2_did.do (data prep for the Stata B1 replication)
# CLAIM: Build Stata-friendly .dta panels for the headline B1 q_2 DiD and the
#        three time-placebo windows. Saves to results/.../stata_panels/.

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
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "stata_panels"
OUTDIR.mkdir(parents=True, exist_ok=True)

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

WINDOWS = [
    ("B1_headline",    "2024-10-01", "2025-01-01", "2025-10-01", "2026-01-01"),
    ("P1_within2024",  "2024-07-01", "2024-10-01", "2024-10-01", "2025-01-01"),
    ("P2_within2025",  "2025-04-01", "2025-07-01", "2025-07-01", "2025-10-01"),
    ("P3_shifted1y",   "2023-10-01", "2024-01-01", "2024-10-01", "2025-01-01"),
]


def hour_class(h):
    if h in CRITICAL_HOURS: return "critical"
    if h in FLAT_HOURS:     return "flat"
    return "other"


def build(units, pre_s, pre_e, post_s, post_e):
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code", "parent", "tech_group", "zone"]])
    rows = []
    for label, start, end in [("PRE", pre_s, pre_e), ("POST", post_s, post_e)]:
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
    panel["treatment_group"] = panel["parent"].apply(
        lambda p: "treatment" if p in TREATMENT_PARENTS
        else ("placebo" if p in PLACEBO_PARENTS else "untagged")
    )
    panel = panel[panel["hour_class"].isin(["critical", "flat"])].copy()
    # Stata-friendly types
    panel["d_int"] = (panel["d"] - pd.Timestamp("1960-01-01")).dt.days  # Stata epoch
    panel["unit_code"] = panel["unit_code"].astype(str).str[:32]
    panel["parent"] = panel["parent"].astype(str).str[:16]
    panel["tech_group"] = panel["tech_group"].astype(str).str[:16]
    panel["treatment_group"] = panel["treatment_group"].astype(str).str[:16]
    panel["hour_class"] = panel["hour_class"].astype(str)
    panel["hour"] = panel["hour"].astype(int)
    return panel[["d_int", "unit_code", "parent", "tech_group", "treatment_group",
                  "hour", "hour_class", "crit", "post", "q2_mwh", "dow"]]


def main():
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    for name, pre_s, pre_e, post_s, post_e in WINDOWS:
        print(f"[{name}] pre {pre_s} -> {pre_e},  post {post_s} -> {post_e}")
        panel = build(units, pre_s, pre_e, post_s, post_e)
        out = OUTDIR / f"{name}.dta"
        panel.to_stata(out, version=118, write_index=False,
                       variable_labels={
                           "d_int": "Date (Stata internal, days since 1960-01-01)",
                           "unit_code": "OMIE unit code",
                           "parent": "Parent firm (short scheme)",
                           "tech_group": "Technology bucket",
                           "treatment_group": "treatment | placebo | untagged",
                           "hour": "Clock hour 0-23",
                           "hour_class": "critical | flat",
                           "crit": "1 if hour in critical_canonical",
                           "post": "1 if window=POST",
                           "q2_mwh": "Intraday upward sell adjustment (MWh, sum over IDA sessions in clock-hour)",
                           "dow": "Day of week 0=Mon",
                       })
        print(f"    saved {out}  ({len(panel):,} obs)")


if __name__ == "__main__":
    main()
