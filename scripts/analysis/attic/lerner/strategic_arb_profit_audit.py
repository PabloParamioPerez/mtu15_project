# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: re-think B9 outcome. Strategic-profit measure = ΔQ × (DA-IDA wedge), not raw ΔQ.
# CLAIM: under-commitment "stopped completely at MTU15-DA" should be tested on captured arbitrage profit, not raw repositioning quantity.

"""Strategic arbitrage profit per firm-day, by regime.

Reasoning before running.

Raw ΔQ (B9 measure) conflates strategic withholding with operational
repositioning (forecast revision, demand updates). The user's claim
that under-commitment "stopped completely at MTU15-DA" doesn't hold
on raw ΔQ — Big-4 IB rebounds 2,319 → 3,328 MWh/day. But the MECHANISM
is about captured profit:

  arb_profit_firm_hour = ΔQ_firm_hour × (DA_price_hour − IDA_price_hour)

If the wedge collapses (MTU15-DA closes the granularity asymmetry that
the user's price model attributes the wedge to) AND ΔQ rebounds to
purely operational repositioning, the PRODUCT collapses → strategic
profit ≈ 0 even when ΔQ ≠ 0.

This better matches the mechanism. Pred:
  pre-IDA:    profit > 0 (positive wedge × positive ΔQ)
  3-sess:     profit > 0
  ISP15-win:  profit may INVERT (user noted DA-IDA wedge inverts)
              positive ΔQ × negative wedge = negative profit
  DA60/ID15:  profit peaks (max wedge × still substantial ΔQ)
  DA15/ID15:  profit ≈ 0 (wedge collapsed × ΔQ at operational level)

OVB: outcome is a product of two equilibrium objects; this is
descriptive, no instrument. Regime means tell us how the captured
profit pattern moves.

Output: results/regressions/strategic_arb_profit_audit.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PRICE_DA = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_IDA = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
OUT = PROJECT / "results" / "regressions" / "strategic_arb_profit_audit.csv"


def assign_regime(d) -> str:
    if d < pd.Timestamp("2024-06-14"):
        return "1.pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "2.3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "3.ISP15-win"
    if d < pd.Timestamp("2025-10-01"):
        return "4.DA60/ID15"
    return "5.DA15/ID15"


def main() -> None:
    print("[1/4] Build hourly DA price + IDA price (mean across sessions, hour-mapped)...")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET threads=4")

    con.execute(f"""
        CREATE TEMP TABLE da AS
        WITH hp AS (
            SELECT date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_DA}'
            WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_da FROM hp GROUP BY 1, 2
    """)
    con.execute(f"""
        CREATE TEMP TABLE ida AS
        WITH hp AS (
            SELECT date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   price_es_eur_mwh AS p
            FROM '{PRICE_IDA}'
            WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, AVG(p) AS p_ida FROM hp GROUP BY 1, 2
    """)

    # ΔQ per firm-hour (sum across IDA sessions of signed assigned MWh)
    print("[2/4] Build ΔQ per firm-hour from pibcie...")
    con.execute(f"""
        CREATE TEMP TABLE dq AS
        WITH hf AS (
            SELECT CAST(date AS DATE) AS date,
                   CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER ELSE period END AS hour,
                   CASE WHEN grupo_empresarial IN ('GE','IB','GN','HC') THEN grupo_empresarial
                        ELSE 'Fringe' END AS firm_group,
                   assigned_power_mw * mtu_minutes / 60.0 AS dq_mwh
            FROM '{PIBCIE}'
            WHERE assigned_power_mw IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, hour, firm_group, SUM(dq_mwh) AS dq_mwh
        FROM hf GROUP BY 1, 2, 3
    """)

    panel = con.sql("""
        SELECT q.date, q.hour, q.firm_group, q.dq_mwh,
               d.p_da, i.p_ida,
               (d.p_da - i.p_ida) AS wedge,
               q.dq_mwh * (d.p_da - i.p_ida) AS arb_profit_eur
        FROM dq q
        JOIN da d ON q.date = d.date AND q.hour = d.hour
        JOIN ida i ON q.date = i.date AND q.hour = i.hour
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])
    panel["regime"] = panel["date"].apply(assign_regime)
    print(f"   panel: {len(panel):,} firm-hour rows; range {panel.date.min().date()} → {panel.date.max().date()}")

    print()
    print("[3/4] Mean strategic arb_profit (€/firm-day, summed across hours) by firm-group × regime:")
    daily = panel.groupby(["date", "firm_group", "regime"], as_index=False).agg(
        profit_eur=("arb_profit_eur", "sum"),
        dq_mwh=("dq_mwh", "sum"),
        wedge_avg=("wedge", "mean"),
    )
    pivot = daily.pivot_table(index="firm_group", columns="regime", values="profit_eur", aggfunc="mean").round(0)
    print("Mean daily strategic arb_profit (€/firm-day):")
    print(pivot.to_string())
    print()

    pivot_q = daily.pivot_table(index="firm_group", columns="regime", values="dq_mwh", aggfunc="mean").round(0)
    print("Mean daily ΔQ (MWh/firm-day) — for comparison vs B9:")
    print(pivot_q.to_string())
    print()

    pivot_w = panel.pivot_table(index="firm_group", columns="regime", values="wedge", aggfunc="mean").round(2)
    print("Mean hourly DA-IDA wedge (€/MWh) — note ISP15-win inversion (user's anomaly):")
    print(pivot_w.iloc[:1].to_string())  # all firm-groups face same wedge
    print()

    # Per-month aggregate for Big-4 sum
    print("[4/4] Aggregate Big-4 monthly strategic profit (€M):")
    big4 = daily[daily["firm_group"].isin(["GE", "IB", "GN", "HC"])]
    big4["month"] = big4["date"].dt.to_period("M").dt.to_timestamp()
    monthly = big4.groupby(["month", "regime"], as_index=False)["profit_eur"].sum()
    by_regime = monthly.groupby("regime").agg(
        n_months=("month", "size"),
        mean_monthly_profit_M=("profit_eur", lambda x: x.mean() / 1e6),
        total_profit_M=("profit_eur", lambda x: x.sum() / 1e6),
    ).round(2)
    print(by_regime.to_string())
    print()

    # User's mechanism prediction check
    print("MECHANISM CHECK — user's prediction:")
    print("  pre-IDA → 3-sess: profit large positive (positive wedge × positive ΔQ)")
    print("  ISP15-win: may invert (per user, wedge inverts)")
    print("  DA60/ID15: peaks (max asymmetric-granularity wedge × strategic ΔQ)")
    print("  DA15/ID15: ≈ 0 (wedge collapses, no strategic motive)")
    print()
    print("Compare against the table above.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out = pd.concat([
        pivot.reset_index().assign(_table="profit_eur_per_firm_day"),
        pivot_q.reset_index().assign(_table="dq_mwh_per_firm_day"),
        by_regime.reset_index().assign(_table="big4_monthly_profit_M"),
    ], ignore_index=True, sort=False)
    out.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
