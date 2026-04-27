# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F12 seasonality robustness
# CLAIM: Test whether F12 pumped-storage arbitrage rate increase is reform-driven or solar-capacity-driven (same-cal-month).

"""F12 seasonality robustness — same-calendar-month comparison.

F12 reported MUEL/MUEB pure-pump cycle arb rate:
  pre-IDA (2018-01 → 2024-06, 6.5y): €41M/year
  post-MTU15-IDA (2025-03 → 2026-01, 10m): ~€181M/year (4.4× rise)

Confounders:
  - Solar capacity grew ~5-6× (5 GW → 28+ GW) over the period →
    duck-curve spread widens mechanically
  - Calendar mix differs (post-window doesn't sample winter as
    proportionally as pre-window)

Test: restrict pre-IDA to same calendar months as post-window. If
same-cal pre-IDA arb rate is comparable to post (e.g., €41M/yr →
€100M+/yr in latest pre years), the "4.4× rise" is mostly
capacity-driven, not reform-driven.

Output: data/derived/results/f12_seasonality_audit.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "f12_seasonality_audit.csv"


def main() -> None:
    print("[1/3] Build daily MUEL/MUEB cycle revenue...")
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    con.execute(f"""
        CREATE TEMP TABLE px AS
        WITH hp AS (
            SELECT date, period,
                   price_es_eur_mwh AS p,
                   mtu_minutes
            FROM '{PRICE}' WHERE price_es_eur_mwh IS NOT NULL
              AND CAST(date AS DATE) >= DATE '2018-01-01'
        )
        SELECT date, period, AVG(p) AS p_da, MAX(mtu_minutes) AS mtu_minutes
        FROM hp GROUP BY 1, 2
    """)
    con.execute(f"""
        CREATE TEMP TABLE puls AS
        SELECT CAST(date AS DATE) AS date, period, mtu_minutes,
               unit_code,
               assigned_power_mw * mtu_minutes / 60.0 AS mwh
        FROM '{PDBCE}'
        WHERE unit_code IN ('MUEL','MUEB')
          AND assigned_power_mw IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
    """)
    panel = con.sql("""
        SELECT p.date, p.period, p.unit_code, p.mwh, x.p_da
        FROM puls p JOIN px x ON p.date = x.date AND p.period = x.period
    """).df()
    panel["date"] = pd.to_datetime(panel["date"])

    # Daily aggregate per unit
    daily = panel.groupby(["date", "unit_code"], as_index=False).agg(
        mwh=("mwh", "sum"),
        revenue_eur=("p_da", lambda x: (x.values * panel.loc[x.index, "mwh"].values).sum()),
    )
    # Pivot
    pivot = daily.pivot(index="date", columns="unit_code", values=["mwh","revenue_eur"]).fillna(0)
    pivot.columns = [f"{a}_{b}" for a, b in pivot.columns]
    pivot = pivot.reset_index()
    # MUEB has negative mwh (pumping); MUEL has positive (gen)
    # Daily arb = MUEL revenue - (-MUEB revenue) = revenue from gen - cost of pump
    # MUEB revenue field = sum(p * mwh_negative) = negative (cost)
    # So daily_arb = MUEL_revenue + MUEB_revenue (since MUEB rev already negative = -cost)
    pivot["daily_arb_eur"] = pivot.get("revenue_eur_MUEL", 0) + pivot.get("revenue_eur_MUEB", 0)
    pivot["month"] = pivot["date"].dt.month
    pivot["year"] = pivot["date"].dt.year

    print(f"   panel: {len(pivot):,} days, {pivot.date.min().date()} → {pivot.date.max().date()}")

    print()
    print("[2/3] Annual aggregate arb rate by year:")
    yearly = pivot.groupby("year").agg(
        n_days=("date", "size"),
        arb_M=("daily_arb_eur", lambda x: x.sum() / 1e6),
    ).round(2)
    yearly["arb_per_year_M"] = yearly["arb_M"] / (yearly["n_days"] / 365)
    print(yearly.to_string())

    print()
    print("[3/3] Same-calendar-month comparison vs post-MTU15-IDA windows:")
    # Post-MTU15-IDA = 2025-03-19 onwards
    post = pivot[pivot["date"] >= pd.Timestamp("2025-03-19")].copy()
    pre = pivot[pivot["date"] < pd.Timestamp("2024-06-14")].copy()

    # DA60/ID15 period: 2025-03-19 to 2025-09-30 (Apr-Sep mostly + late Mar)
    da60 = post[post["date"] < pd.Timestamp("2025-10-01")]
    da15 = post[post["date"] >= pd.Timestamp("2025-10-01")]

    print(f"\n   POST DA60/ID15 (n={len(da60)} days, mostly Apr-Sep 2025): "
          f"daily mean €{da60.daily_arb_eur.mean():.0f}, "
          f"total €{da60.daily_arb_eur.sum()/1e6:.1f}M, "
          f"annualized €{da60.daily_arb_eur.sum()/1e6 / (len(da60)/365):.1f}M/yr")
    print(f"   POST DA15/ID15 (n={len(da15)} days, mostly Oct 2025-Jan 2026): "
          f"daily mean €{da15.daily_arb_eur.mean():.0f}, "
          f"total €{da15.daily_arb_eur.sum()/1e6:.1f}M, "
          f"annualized €{da15.daily_arb_eur.sum()/1e6 / (len(da15)/365):.1f}M/yr")

    # Same-calendar-month pre-IDA: take pre-IDA days with same calendar months as DA60
    da60_months = sorted(da60["month"].unique().tolist())
    da15_months = sorted(da15["month"].unique().tolist())
    pre_da60 = pre[pre["month"].isin(da60_months)]
    pre_da15 = pre[pre["month"].isin(da15_months)]
    print(f"\n   PRE-IDA same-cal-months as DA60 ({da60_months}, n={len(pre_da60)} days): "
          f"daily mean €{pre_da60.daily_arb_eur.mean():.0f}, "
          f"annualized €{pre_da60.daily_arb_eur.mean() * 365 / 1e6:.1f}M/yr")
    print(f"   PRE-IDA same-cal-months as DA15 ({da15_months}, n={len(pre_da15)} days): "
          f"daily mean €{pre_da15.daily_arb_eur.mean():.0f}, "
          f"annualized €{pre_da15.daily_arb_eur.mean() * 365 / 1e6:.1f}M/yr")
    print(f"   FULL pre-IDA (n={len(pre)} days): "
          f"daily mean €{pre.daily_arb_eur.mean():.0f}, "
          f"annualized €{pre.daily_arb_eur.mean() * 365 / 1e6:.1f}M/yr")

    print()
    print("Year-by-year by season for the relevant pre-IDA months:")
    for label, months in [("Apr-Sep", da60_months), ("Oct-Jan", da15_months)]:
        print(f"\n  Pre-IDA {label} months ({months}):")
        sub = pre[pre["month"].isin(months)]
        by_year = sub.groupby("year").agg(
            n=("date","size"),
            mean_daily=("daily_arb_eur","mean"),
            ann_M=("daily_arb_eur", lambda x: x.mean() * 365 / 1e6)
        ).round(2)
        print(by_year.to_string())

    OUT.parent.mkdir(parents=True, exist_ok=True)
    yearly.reset_index().to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
