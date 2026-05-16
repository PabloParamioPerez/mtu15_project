# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: scripts/stata/reform_window/05_ddd_firm_q1.do, 06_ddd_firm_q2.do
# CLAIM: Build DDD + DDDD panels for the firm-level outcomes q_1 and q_2.
#   q_1 = DA-cleared MWh per (firm, unit, date, clock-hour), from PDBC.
#   q_2 = intraday programmed MWh (signed, all sessions summed) per
#         (firm, unit, date, clock-hour), from PIBCI.
#   Sample: CCGT units of pivotal firms (IB, GE, GN, HC, EDP-PT) and placebo
#         firms (Repsol, Engie, TotalEnergies, Moeve). Untagged firms dropped.
#   For each (window, outcome) pair we write {window}_{outcome}.dta. Missing
#   periods (unit offline) zero-filled via LEFT JOIN onto a complete grid.
# OUTPUT: 4 .dta files under results/regressions/firm/reform_window/panels/
#         da15_q1.dta, da15_q2.dta, ida15_q1.dta, ida15_q2.dta

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT,
    PLACEBO_PARENTS_SHORT,
)

PANELS = REPO / "results" / "regressions" / "firm" / "reform_window" / "panels"
PANELS.mkdir(parents=True, exist_ok=True)

PDBC  = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

CRIT_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
KEEP_HOURS = CRIT_HOURS + FLAT_HOURS

WINDOWS: dict[str, dict[str, pd.Timestamp]] = {
    "da15": {
        "y25_pre_s":  pd.Timestamp("2025-04-28"),
        "y25_pre_e":  pd.Timestamp("2025-09-30"),
        "y25_post_s": pd.Timestamp("2025-10-01"),
        "y25_post_e": pd.Timestamp("2026-02-13"),
        "y24_pre_s":  pd.Timestamp("2024-04-28"),
        "y24_pre_e":  pd.Timestamp("2024-09-30"),
        "y24_post_s": pd.Timestamp("2024-10-01"),
        "y24_post_e": pd.Timestamp("2025-02-13"),
    },
    "ida15": {
        "y25_pre_s":  pd.Timestamp("2024-12-09"),
        "y25_pre_e":  pd.Timestamp("2025-03-18"),
        "y25_post_s": pd.Timestamp("2025-03-19"),
        "y25_post_e": pd.Timestamp("2025-04-27"),
        "y24_pre_s":  pd.Timestamp("2023-12-09"),
        "y24_pre_e":  pd.Timestamp("2024-03-18"),
        "y24_post_s": pd.Timestamp("2024-03-19"),
        "y24_post_e": pd.Timestamp("2024-04-27"),
    },
}


def _load_ccgt_unit_firm() -> pd.DataFrame:
    """Return CCGT units with parent firm and pivotal indicator.
    Drops 'untagged' firms (we need a clean pivotal/placebo contrast)."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    ccgt = units[units["tech_group"] == "CCGT"][["unit_code", "parent"]].copy()
    ccgt = ccgt.rename(columns={"parent": "firm"})
    ccgt["pivotal"] = ccgt["firm"].apply(
        lambda f: 1 if f in TREATMENT_PARENTS_SHORT else (0 if f in PLACEBO_PARENTS_SHORT else -1)
    )
    ccgt = ccgt[ccgt["pivotal"] >= 0].reset_index(drop=True)
    print(f"  CCGT units: {len(ccgt)} ({(ccgt['pivotal']==1).sum()} pivotal, {(ccgt['pivotal']==0).sum()} placebo)")
    return ccgt


def _load_q1_hourly(units: pd.DataFrame, date_lo: pd.Timestamp, date_hi: pd.Timestamp) -> pd.DataFrame:
    """q_1 = DA-cleared MWh per (unit, date, clock-hour), from PDBC.
    PDBC is sparse: missing rows mean the unit was offline. We aggregate to
    clock-hour-MWh first (so the period-vs-quarter granularity collapses to a
    uniform MWh quantity), then LEFT JOIN onto the full (unit, date, hour) grid
    and zero-fill."""
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='8GB'")
    con.register("uft", units[["unit_code", "firm", "pivotal"]])
    q = f"""
    WITH q1 AS (
      SELECT CAST(date AS DATE) AS d,
             unit_code,
             CASE WHEN mtu_minutes = 60 THEN period - 1
                  WHEN mtu_minutes = 15 THEN (period - 1) / 4
                  ELSE NULL END AS clockhour,
             SUM(assigned_power_mw * mtu_minutes / 60.0) AS mwh
      FROM '{PDBC}' p
      JOIN uft u USING (unit_code)
      WHERE assigned_power_mw IS NOT NULL
        AND CAST(date AS DATE) BETWEEN DATE '{date_lo.date()}' AND DATE '{date_hi.date()}'
      GROUP BY 1, 2, 3
    )
    SELECT d, unit_code, clockhour, mwh
    FROM q1
    WHERE clockhour BETWEEN 0 AND 23
    """
    return con.execute(q).df()


def _load_q2_hourly(units: pd.DataFrame, date_lo: pd.Timestamp, date_hi: pd.Timestamp) -> pd.DataFrame:
    """q_2 = signed intraday programmed MWh per (unit, date, clock-hour),
    summed across all IDA sessions. From PIBCI."""
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='8GB'")
    con.register("uft", units[["unit_code", "firm", "pivotal"]])
    q = f"""
    WITH q2 AS (
      SELECT CAST(date AS DATE) AS d,
             unit_code,
             CASE WHEN mtu_minutes = 60 THEN period - 1
                  WHEN mtu_minutes = 15 THEN (period - 1) / 4
                  ELSE NULL END AS clockhour,
             SUM(assigned_power_mw * mtu_minutes / 60.0) AS mwh
      FROM '{PIBCI}' p
      JOIN uft u USING (unit_code)
      WHERE assigned_power_mw IS NOT NULL
        AND CAST(date AS DATE) BETWEEN DATE '{date_lo.date()}' AND DATE '{date_hi.date()}'
      GROUP BY 1, 2, 3
    )
    SELECT d, unit_code, clockhour, mwh
    FROM q2
    WHERE clockhour BETWEEN 0 AND 23
    """
    return con.execute(q).df()


def _build_grid(units: pd.DataFrame, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Full (unit, date, clockhour) Cartesian grid restricted to crit+flat hours."""
    hours = list(KEEP_HOURS)
    grid = pd.MultiIndex.from_product(
        [units["unit_code"].unique(), dates, hours],
        names=["unit_code", "date", "clockhour"],
    ).to_frame(index=False)
    grid = grid.merge(units[["unit_code", "firm", "pivotal"]], on="unit_code", how="left")
    return grid


def _add_treatment_indicators(df: pd.DataFrame, w: dict) -> pd.DataFrame:
    """Add crit, post, y25, pivotal indicators + all needed interactions for
    the DDD and the DDDD with pivotal moderator."""
    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    y25_pre  = (df["date"] >= w["y25_pre_s"])  & (df["date"] <= w["y25_pre_e"])
    y25_post = (df["date"] >= w["y25_post_s"]) & (df["date"] <= w["y25_post_e"])
    y24_pre  = (df["date"] >= w["y24_pre_s"])  & (df["date"] <= w["y24_pre_e"])
    y24_post = (df["date"] >= w["y24_post_s"]) & (df["date"] <= w["y24_post_e"])
    in_sample = y25_pre | y25_post | y24_pre | y24_post
    df = df.loc[in_sample].copy()
    df["y25"]  = (y25_pre | y25_post).loc[df.index].astype(int)
    df["post"] = (y25_post | y24_post).loc[df.index].astype(int)
    df["crit"] = df["clockhour"].isin(CRIT_HOURS).astype(int)
    # Pivotal already on df from the grid.

    # All needed interactions for DDDD
    df["crit_post"]     = df["crit"] * df["post"]
    df["crit_y25"]      = df["crit"] * df["y25"]
    df["post_y25"]      = df["post"] * df["y25"]
    df["crit_post_y25"] = df["crit"] * df["post"] * df["y25"]
    df["crit_piv"]      = df["crit"] * df["pivotal"]
    df["post_piv"]      = df["post"] * df["pivotal"]
    df["y25_piv"]       = df["y25"]  * df["pivotal"]
    df["crit_post_piv"]      = df["crit"] * df["post"] * df["pivotal"]
    df["crit_y25_piv"]       = df["crit"] * df["y25"]  * df["pivotal"]
    df["post_y25_piv"]       = df["post"] * df["y25"]  * df["pivotal"]
    df["crit_post_y25_piv"]  = df["crit"] * df["post"] * df["y25"] * df["pivotal"]

    df["dow"]   = df["date"].dt.dayofweek.astype(int)
    df["month"] = df["date"].dt.month.astype(int)
    df["year"]  = df["date"].dt.year.astype(int)
    df["date_stata"] = (df["date"] - pd.Timestamp("1960-01-01")).dt.days.astype(int)
    df["unit_id"] = df["unit_code"].astype("category").cat.codes.astype(int)
    df["firm_id"] = df["firm"].astype("category").cat.codes.astype(int)
    return df


def _build_panel(window_name: str, w: dict, outcome: str,
                  units: pd.DataFrame, raw: pd.DataFrame) -> pd.DataFrame:
    # Date list spanning all 4 sub-windows
    dates = pd.date_range(min(w["y24_pre_s"], w["y25_pre_s"]),
                          max(w["y24_post_e"], w["y25_post_e"]), freq="D")
    grid = _build_grid(units, dates)
    # LEFT JOIN raw onto grid; missing -> 0
    grid = grid.rename(columns={"date": "date"})
    raw = raw.rename(columns={"d": "date"})
    raw["date"] = pd.to_datetime(raw["date"])
    grid["date"] = pd.to_datetime(grid["date"])
    merged = grid.merge(raw, on=["unit_code", "date", "clockhour"], how="left")
    merged["mwh"] = merged["mwh"].fillna(0.0)
    merged = merged.rename(columns={"mwh": outcome})
    panel = _add_treatment_indicators(merged, w)
    keep = [
        "date", "date_stata", "unit_id", "firm_id", "unit_code", "firm",
        "clockhour", "dow", "month", "year",
        outcome, "crit", "post", "y25", "pivotal",
        "crit_post", "crit_y25", "post_y25", "crit_post_y25",
        "crit_piv", "post_piv", "y25_piv",
        "crit_post_piv", "crit_y25_piv", "post_y25_piv",
        "crit_post_y25_piv",
    ]
    panel = panel[keep].reset_index(drop=True)
    print(f"  [{window_name}-{outcome}] N={len(panel):,d}  "
          f"piv:{(panel['pivotal']==1).sum():,}  placebo:{(panel['pivotal']==0).sum():,}")
    return panel


def main():
    print("=== loading CCGT unit-firm map ===")
    units = _load_ccgt_unit_firm()

    for window_name, w in WINDOWS.items():
        date_lo = min(w["y24_pre_s"], w["y25_pre_s"])
        date_hi = max(w["y24_post_e"], w["y25_post_e"])
        print(f"\n=== window={window_name}  dates {date_lo.date()}..{date_hi.date()} ===")

        print("  loading q_1 from PDBC...")
        q1_raw = _load_q1_hourly(units, date_lo, date_hi)
        print(f"    {len(q1_raw):,} raw rows")
        panel_q1 = _build_panel(window_name, w, "q1", units, q1_raw)
        panel_q1.to_stata(PANELS / f"{window_name}_q1.dta", write_index=False, version=118)

        print("  loading q_2 from PIBCI...")
        q2_raw = _load_q2_hourly(units, date_lo, date_hi)
        print(f"    {len(q2_raw):,} raw rows")
        panel_q2 = _build_panel(window_name, w, "q2", units, q2_raw)
        panel_q2.to_stata(PANELS / f"{window_name}_q2.dta", write_index=False, version=118)

    print(f"\nAll 4 firm panels written to {PANELS}")


if __name__ == "__main__":
    main()
