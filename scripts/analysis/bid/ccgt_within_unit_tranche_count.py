# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: B8 (within-unit IB CCGT bid complexification 5.49→8.73 tpp, IB-specific); X14 (killed the prior aggregate "5-7→1-2 tranches" claim — MAV-format-change parser artefact)
# CLAIM: Within-unit DA bid tranche count for named complex-bidders (TAPOWER/SRI4R/ARCOS1/CTN4) pre vs post MTU15-IDA. Three of four COMPLEXIFY (not simplify), killing W3 aggregate claim → reborn as B8 IB-specific complexification.
"""W3 verification: within-unit tranche-count comparison for the four
named CCGT complex-bidders.

Context. The W3 wound rationale ('95% composition; only PALOS3 within-unit
shift') was contradicted by `ccgt_extensive_margin_exit.py` 2026-04-26:
the named units (TAPOWER, SRI4R, ARCOS1, CTN4) did NOT exit at MTU15-IDA.
So whatever drove the aggregate granularity drop must be within-unit
for these units. This script tests that directly: count tranches per
unit-period pre vs post MTU15-IDA.

Spec. Per (unit, year_month), count tranches in `det_all` (joined to
`cab_all` for unit_code lookup, sell-side only). Tranches per period
= total tranche rows / number of distinct (date, period) cells. Track
both 'tranches per active period' (granularity proxy) and 'total
tranche rows' (volume proxy).

Caveats:
  (a) The MAV format change at MTU15-IDA (per memory) means tranche
      counts have a definitional artefact pre vs post — the MAV column
      switches from mandatory-default to optional, and parser handling
      may inflate / deflate the row count. Acknowledge in interpretation.
  (b) Pre-2025-03-19 prices are 0-padded in det_all (parser artefact);
      this doesn't affect tranche COUNTS but means within-tranche price
      analysis is not possible pre-reform.
  (c) Post-MTU15-IDA periods are 15-min (period 1-96) vs 60-min pre
      (period 1-24). To get apples-to-apples granularity, we report
      tranches-per-active-period AND tranches-per-hour (where 'hour'
      collapses 4 post-reform 15-min periods).

Memory-conscious: DuckDB streams with 1.5GB cap, returns only the
small aggregated panel.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"

NAMED = ["TAPOWER", "SRI4R", "ARCOS1", "CTN4"]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")

    print("[1/2] cab_small (sell-side bids, named units only)...")
    placeholders = ",".join(f"'{u}'" for u in NAMED)
    con.execute(f"""
        CREATE TEMP TABLE cab_small AS
        SELECT date, offer_code, version, unit_code
        FROM '{CAB}'
        WHERE buy_sell = 'V'
          AND unit_code IN ({placeholders})
    """)
    n_cab = con.sql("SELECT COUNT(*) FROM cab_small").fetchone()[0]
    print(f"   cab rows for named units: {n_cab:,}")

    print("[2/2] aggregate tranches per (unit, year_month, period)...")
    df = con.sql(f"""
        WITH joined AS (
            SELECT c.unit_code,
                   d.date,
                   d.period,
                   d.quantity_mw
            FROM '{DET}' d
            JOIN cab_small c
              ON c.date = d.date
             AND c.offer_code = d.offer_code
             AND c.version = d.version
            WHERE d.quantity_mw > 0
        )
        SELECT unit_code,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               COUNT(*) AS total_tranche_rows,
               COUNT(DISTINCT (date, period)) AS active_periods,
               COUNT(DISTINCT date) AS active_days,
               -- Average tranche count per period
               COUNT(*) * 1.0
                 / NULLIF(COUNT(DISTINCT (date, period)), 0) AS tranches_per_period,
               -- Average periods per day (proxy: 24 pre / up to 96 post)
               COUNT(DISTINCT (date, period)) * 1.0
                 / NULLIF(COUNT(DISTINCT date), 0) AS periods_per_day
        FROM joined
        GROUP BY unit_code, month
        ORDER BY unit_code, month
    """).df()
    df["month"] = pd.to_datetime(df["month"])

    # Reform date
    REFORM = pd.Timestamp("2025-03-19")
    df["regime"] = pd.cut(
        df["month"],
        bins=[pd.Timestamp("2018-01-01"), REFORM, pd.Timestamp("2027-01-01")],
        labels=["pre-MTU15-IDA", "post-MTU15-IDA"],
    )

    print()
    print("=" * 100)
    print("Within-unit tranche-count comparison: TAPOWER, SRI4R, ARCOS1, CTN4")
    print("=" * 100)

    # Per-unit aggregate stats by regime
    print()
    print(f"{'unit':<10}  {'regime':<16}  {'months':>7}  {'days':>7}  {'periods/day':>13}  {'tranches/period':>17}  {'tranches/day':>14}")
    rows = []
    for unit in NAMED:
        for r in ["pre-MTU15-IDA", "post-MTU15-IDA"]:
            sub = df[(df["unit_code"] == unit) & (df["regime"] == r)]
            if len(sub) == 0:
                print(f"{unit:<10}  {r:<16}  (no data)")
                continue
            n_months = len(sub)
            total_days = sub["active_days"].sum()
            # Weighted by active days for fair averaging across months
            # tranches_per_period weighted by active_periods
            wp = (sub["tranches_per_period"] * sub["active_periods"]).sum() / sub["active_periods"].sum()
            ppd = (sub["periods_per_day"] * sub["active_days"]).sum() / sub["active_days"].sum()
            tpd = wp * ppd
            rows.append({
                "unit": unit, "regime": r, "months": n_months,
                "active_days": int(total_days),
                "tranches_per_period": float(wp),
                "periods_per_day": float(ppd),
                "tranches_per_day": float(tpd),
            })
            print(f"{unit:<10}  {r:<16}  {n_months:>7}  {total_days:>7,}  {ppd:>13.2f}  {wp:>17.2f}  {tpd:>14.2f}")
        print()

    tab = pd.DataFrame(rows)

    # Headline test: did tranches/period drop within-unit?
    print()
    print("Headline test: ratio of post / pre tranches-per-period")
    print(f"{'unit':<10}  {'pre tpp':>9}  {'post tpp':>10}  {'ratio (post/pre)':>18}  {'verdict'}")
    for unit in NAMED:
        try:
            pre = tab[(tab["unit"] == unit) & (tab["regime"] == "pre-MTU15-IDA")]["tranches_per_period"].iloc[0]
            post = tab[(tab["unit"] == unit) & (tab["regime"] == "post-MTU15-IDA")]["tranches_per_period"].iloc[0]
        except (IndexError, KeyError):
            continue
        ratio = post / pre if pre > 0 else float("nan")
        if ratio < 0.5:
            verdict = "SIMPLIFIED (within-unit drop)"
        elif ratio < 0.8:
            verdict = "modest decline"
        elif ratio < 1.2:
            verdict = "stable"
        else:
            verdict = "complexified"
        print(f"{unit:<10}  {pre:>9.2f}  {post:>10.2f}  {ratio:>18.3f}  {verdict}")

    out = PROJECT / "data" / "derived" / "results" / "ccgt_within_unit_tranche_count.csv"
    tab.to_csv(out, index=False)
    df.to_csv(PROJECT / "data" / "derived" / "results" / "ccgt_tranche_count_monthly.csv", index=False)
    print(f"\nwrote {out}")


if __name__ == "__main__":
    main()
