# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: §0 IB-canonical synthesis (mechanism test for "IB is the price-setter")
# CLAIM: Per-firm CCGT bid tranches concentrated at the marginal clearing price step (price-setter test)
"""Bid-curve concentration test for the IB-canonical price-setter claim.

Hypothesis. F7's per-firm decomposition shows IB carries ~98% of the
joint Big-4 €833M post-MTU15-IDA market-power transfer. The economic
interpretation is that IB is the marginal price-setter — its CCGT
tranches sit ON the marginal supply step, so removing/scaling them
shifts the clearing price; while GE's tranches sit AWAY from the
marginal step, so removing/scaling them doesn't.

Direct test: for each firm × ISP × tranche, compute the price gap
from the actual clearing price (= bid_price - clearing_price). Then:
  * fraction of firm's CCGT tranches within ±€5 of clearing → 'at the margin'
  * mean |price gap| → smaller for the price-setter
  * fraction of firm's CCGT *MWh volume* within ±€5

If IB's CCGT tranches are systematically more concentrated near the
clearing price than GE's, that's direct mechanism evidence for the
price-setter reading of F7 per-firm.

Memory-conscious: aggregate per (firm, month) in DuckDB.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MATCH = PROJECT / "data" / "derived" / "panels" / "synthetic_plant_match.parquet"
OUT = PROJECT / "results" / "regressions" / "marginal_price_step_concentration.csv"


def main() -> None:
    match = pd.read_parquet(MATCH)
    firm_units = {}
    for firm in ["GE", "IB", "GN", "HC"]:
        firm_units[firm] = match[
            (match["firm_L"] == firm) & (match["tech"] == "CCGT")
        ]["unit_L"].tolist()
    print("CCGT unit counts per firm:", {f: len(u) for f, u in firm_units.items()})

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    # Build a unit -> firm CCGT-only map
    rows_map = []
    for f, units in firm_units.items():
        for u in units:
            rows_map.append({"unit_code": u, "firm": f})
    unit_firm = pd.DataFrame(rows_map)
    con.register("unit_firm", unit_firm)

    print("[1/3] Pull CCGT sell-side bids for Big-4 (post-MTU15-IDA)...")
    bids = con.sql(f"""
        SELECT d.date,
               d.period,
               d.price_eur_mwh AS bid_price,
               d.quantity_mw   AS qty,
               c.unit_code,
               uf.firm
        FROM '{DET}' d
        JOIN '{CAB}' c
          ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        JOIN unit_firm uf USING (unit_code)
        WHERE c.buy_sell = 'V'
          AND d.quantity_mw > 0
          AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '2025-03-19'
    """).df()
    print(f"   {len(bids):,} CCGT tranche-rows post-MTU15-IDA")

    print("[2/3] Pull DA clearing price (marginalpdbc) and join...")
    px = con.sql(f"""
        SELECT date, period,
               AVG(price_es_eur_mwh) AS clear_price
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2025-03-19'
        GROUP BY date, period
    """).df()
    bids = bids.merge(px, on=["date", "period"], how="inner")
    bids["price_gap"] = bids["bid_price"] - bids["clear_price"]
    bids["near_margin"] = bids["price_gap"].abs() <= 5.0  # ±€5 of clearing
    print(f"   joined to clearing price: {len(bids):,} rows")

    # ---- Per-firm aggregate ----
    print()
    print("=" * 100)
    print("CCGT tranche concentration near the marginal clearing price (post-MTU15-IDA, pooled)")
    print("=" * 100)
    print()
    print(f"{'firm':<5}  {'n tranches':>12}  {'%tranches near ±€5':>20}  {'%MWh near ±€5':>16}  {'mean |gap|':>11}  {'median |gap|':>13}")
    rows = []
    for firm in ["GE", "IB", "GN", "HC"]:
        sub = bids[bids["firm"] == firm]
        if len(sub) == 0:
            continue
        pct_tr = sub["near_margin"].mean() * 100
        pct_q = sub.loc[sub["near_margin"], "qty"].sum() / sub["qty"].sum() * 100
        m_abs = sub["price_gap"].abs().mean()
        med_abs = sub["price_gap"].abs().median()
        rows.append({
            "firm": firm, "n_tranches": int(len(sub)),
            "pct_tranches_near_margin": float(pct_tr),
            "pct_mwh_near_margin": float(pct_q),
            "mean_abs_gap": float(m_abs),
            "median_abs_gap": float(med_abs),
        })
        print(
            f"{firm:<5}  {len(sub):>12,}  {pct_tr:>19.2f}%  {pct_q:>15.2f}%  "
            f"{m_abs:>11.2f}  {med_abs:>13.2f}"
        )

    # ---- Headline ----
    print()
    print("Headline test: IB CCGT vs GE CCGT bid concentration at marginal price step")
    print()
    rdf = pd.DataFrame(rows).set_index("firm")
    if "IB" in rdf.index and "GE" in rdf.index:
        ib_pct = rdf.loc["IB", "pct_mwh_near_margin"]
        ge_pct = rdf.loc["GE", "pct_mwh_near_margin"]
        ib_med = rdf.loc["IB", "median_abs_gap"]
        ge_med = rdf.loc["GE", "median_abs_gap"]
        print(f"  IB: {ib_pct:.1f}% of CCGT MWh within ±€5 of clearing (median |gap| = €{ib_med:.1f})")
        print(f"  GE: {ge_pct:.1f}% of CCGT MWh within ±€5 of clearing (median |gap| = €{ge_med:.1f})")
        print()
        if ib_pct > ge_pct + 5:
            verdict = "✓ IB CCGT tranches are MORE concentrated at the marginal step — supports price-setter reading."
        elif ib_pct > ge_pct:
            verdict = "≈ IB > GE but only marginally; weak directional support."
        else:
            verdict = "✗ IB is NOT more concentrated at the margin than GE — price-setter mechanism not via tranche placement."
        print(f"  {verdict}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
