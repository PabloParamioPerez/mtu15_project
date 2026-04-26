# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: §0 cross-firm synthesis (vertical-integration mechanism for "why IB?")
# CLAIM: Per-firm monthly net seller position (sell - buy in DA), Big-4 only; tests if vertical-integration explains IB > GE in market-power tests
"""Vertical-integration / net-seller-position test for §0 IB-canonical synthesis.

Context. F1, F2, F5, F6, B8, F7 all converge on IB as the strategic firm,
not GE. Ciarreta-Espinosa (2010 Fig 5) tested whether net-seller position
explained EN > IB in 2002-2005 and found it didn't. We replicate the test
in reverse: does IB being more net-seller than GE explain IB > GE in our
2024-2026 sample?

Hypothesis. Firms with downstream retail arms (vertical integration)
internalize the cost of high spot prices when they're net buyers (their
retail arm pays the spot price). Net buyers should restrict their
incentive to push spot prices up. So:
  - net seller (more sell than buy) → strong incentive to raise spot prices
  - net buyer (more buy than sell) → weak / negative incentive

If IB > GE in market power tests AND IB is more net-seller than GE,
vertical integration provides a structural explanation.
If IB > GE but IB is not more net-seller, vertical integration doesn't
explain the pattern — would need a different mechanism story.

Spec. Per (firm, year_month):
  q_sell = sum of pdbce.assigned_power_mw where offer_type = 1 (sell)
  q_buy  = sum of pdbce.assigned_power_mw where offer_type = 2 (buy)
  net_seller_mwh = q_sell - q_buy

Restricted to Big-4 (GE, IB, GN, HC) and to 2024-01 onwards (matches
the analysis window).

Caveat. Per memory ref_rule_28_8_elimination.md: pre-2025-03-19 buy-side
data is largely artificial opportunity-cost bids for bilateral contracts.
Pre-MTU15-IDA net positions therefore overstate "real" buy positions.
We split the analysis at 2025-03-19 in the output for honest reading.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "net_seller_position.csv"

BIG4 = ("GE", "IB", "GN", "HC")


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    print("[1/2] Per-(firm, month, offer_type) cleared MWh from pdbce...")
    df = con.sql(f"""
        SELECT grupo_empresarial AS firm,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               offer_type,
               SUM(assigned_power_mw)
                 / CASE WHEN mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}'
        WHERE assigned_power_mw IS NOT NULL
          AND assigned_power_mw > 0
          AND grupo_empresarial IN ({",".join(f"'{f}'" for f in BIG4)})
          AND CAST(date AS DATE) >= DATE '2024-01-01'
          AND offer_type IN (1, 2)
        GROUP BY firm, month, offer_type, mtu_minutes
    """).df()
    df["month"] = pd.to_datetime(df["month"])

    # Pivot to wide: q_sell (offer_type=1) and q_buy (offer_type=2)
    wide = df.pivot_table(
        index=["firm", "month"], columns="offer_type", values="q_mwh", aggfunc="sum",
    ).reset_index().rename(columns={1: "q_sell_mwh", 2: "q_buy_mwh"})
    for c in ["q_sell_mwh", "q_buy_mwh"]:
        if c not in wide.columns:
            wide[c] = 0.0
        wide[c] = wide[c].fillna(0.0)
    wide["net_seller_mwh"] = wide["q_sell_mwh"] - wide["q_buy_mwh"]
    wide["net_seller_gwh"] = wide["net_seller_mwh"] / 1e3

    # ---- Print monthly time series (table) ----
    print()
    print("=" * 100)
    print("Monthly net-seller position per Big-4 firm (GWh per month)")
    print("Positive = net seller; Negative = net buyer")
    print("=" * 100)
    pivot = wide.pivot(index="month", columns="firm", values="net_seller_gwh").round(0)
    pivot = pivot[[f for f in BIG4 if f in pivot.columns]]
    print()
    print(pivot.to_string())

    # ---- Aggregate by era (pre vs post Rule 28.8 elimination) ----
    print()
    print("=" * 100)
    print("Mean monthly net-seller position by era (GWh/mo); Rule 28.8 elimination = 2025-03-19")
    print("=" * 100)
    wide["era"] = wide["month"].apply(
        lambda d: "post-Rule-28.8" if pd.Timestamp(d) >= pd.Timestamp("2025-03-01") else "pre-Rule-28.8"
    )
    print()
    print(f"{'firm':<6}  {'pre (GWh/mo)':>15}  {'post (GWh/mo)':>15}  {'Δ':>10}")
    summary_rows = []
    for f in BIG4:
        sub = wide[wide["firm"] == f]
        if len(sub) == 0:
            continue
        pre = sub[sub["era"] == "pre-Rule-28.8"]["net_seller_gwh"].mean()
        post = sub[sub["era"] == "post-Rule-28.8"]["net_seller_gwh"].mean()
        delta = post - pre
        summary_rows.append({"firm": f, "pre_gwh": pre, "post_gwh": post, "delta_gwh": delta})
        print(f"{f:<6}  {pre:>15,.0f}  {post:>15,.0f}  {delta:>+10,.0f}")

    # ---- Cross-firm comparison: post-Rule-28.8 (cleanest period) ----
    print()
    print("=" * 100)
    print("Cross-firm test: is IB more net-seller than GE in the post-Rule-28.8 period?")
    print("(if YES: vertical integration explains why IB > GE in market-power tests)")
    print("(if NO:  vertical integration does NOT explain the IB-canonical pattern)")
    print("=" * 100)
    summary = pd.DataFrame(summary_rows)
    if not summary.empty:
        ib = summary[summary["firm"] == "IB"]["post_gwh"].iloc[0] if "IB" in summary["firm"].values else float("nan")
        ge = summary[summary["firm"] == "GE"]["post_gwh"].iloc[0] if "GE" in summary["firm"].values else float("nan")
        print()
        print(f"  IB net seller (post-Rule-28.8): {ib:>+10,.0f} GWh/month")
        print(f"  GE net seller (post-Rule-28.8): {ge:>+10,.0f} GWh/month")
        print(f"  Difference (IB - GE):           {ib-ge:>+10,.0f} GWh/month")
        print()
        if ib > ge:
            verdict = "✓ YES — IB IS more net-seller than GE."
            implication = "Vertical-integration explanation supports IB-higher-Lerner pattern."
        else:
            verdict = "✗ NO — IB is NOT more net-seller than GE."
            implication = (
                "Vertical integration does NOT mechanically explain why IB > GE in market-power tests. "
                "The IB-canonical pattern needs a different mechanism story (portfolio composition, "
                "marginal-CCGT exposure, strategic conduct, ...)."
            )
        print(f"  {verdict}")
        print(f"  → {implication}")

    # ---- Persist ----
    OUT.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
