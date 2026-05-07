# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 (per-IB-unit decomposition); §0 mechanism story
# CLAIM: Per-IB-unit attribution of the IB ~€820M post-MTU15-IDA market-power transfer
"""Per-IB-unit synthetic-firm decomposition.

F7 per-firm shows IB carries ~€820M of the joint Big-4 €833M transfer.
This script drills into IB to find WHICH IB units drive that.

For each IB plant L (matched in synthetic_plant_match.parquet):
  Replace ONLY L's offers with K_L/K_S × S's offers; keep everyone else
  actual (including other IB units). Re-clear. mp_L = p_actual - p_synth_L.

Aggregate per (unit, regime). The expectation given B8 (IB CCGT
complexification) + F7 per-firm (IB price-setter): the named complex-
bidder CCGT units (TAPOWER, ARCOS1, CTN4, CTN3) should carry most
of IB's price-setting power.

Memory-conscious: per-month chunking, DuckDB streaming.
"""
from __future__ import annotations

from pathlib import Path
import sys
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MATCH = PROJECT / "data" / "derived" / "panels" / "synthetic_plant_match.parquet"
OUT = PROJECT / "results" / "regressions" / "synthetic_firm_per_unit_ib.csv"


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2025-03-19"):
        return "pre-MTU15-IDA"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def process_month(con, year: int, month: int, ib_units: list[str]) -> pd.DataFrame:
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year+1:04d}-01-01"
    else:
        end = f"{year:04d}-{month+1:02d}-01"

    con.execute("DROP TABLE IF EXISTS match_tbl")
    con.execute(f"""
        CREATE TEMP TABLE match_tbl AS
        SELECT * FROM '{MATCH}' WHERE unit_S IS NOT NULL AND firm_L = 'IB'
    """)

    con.execute("DROP TABLE IF EXISTS sell_raw")
    con.execute(f"""
        CREATE TEMP TABLE sell_raw AS
        SELECT d.date, d.period, d.price_eur_mwh AS price,
               d.quantity_mw AS qty, c.unit_code
        FROM '{DET}' d
        JOIN '{CAB}' c
          ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE c.buy_sell = 'V' AND d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)
    if con.sql("SELECT COUNT(*) FROM sell_raw").fetchone()[0] == 0:
        return pd.DataFrame()

    con.execute("DROP TABLE IF EXISTS buy_raw")
    con.execute(f"""
        CREATE TEMP TABLE buy_raw AS
        SELECT d.date, d.period, d.price_eur_mwh AS price, d.quantity_mw AS qty
        FROM '{DET}' d
        JOIN '{CAB}' c
          ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE c.buy_sell = 'C' AND d.quantity_mw > 0 AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)

    con.execute("DROP TABLE IF EXISTS supply_actual")
    con.execute("""
        CREATE TEMP TABLE supply_actual AS
        SELECT date, period, price, SUM(qty) AS qty
        FROM sell_raw GROUP BY date, period, price
    """)
    con.execute("DROP TABLE IF EXISTS demand")
    con.execute("""
        CREATE TEMP TABLE demand AS
        SELECT date, period, price, SUM(qty) AS qty
        FROM buy_raw GROUP BY date, period, price
    """)
    con.execute("DROP TABLE IF EXISTS d_cum")
    con.execute("""
        CREATE TEMP TABLE d_cum AS
        SELECT date, period, price, qty,
               SUM(qty) OVER (PARTITION BY date, period
                              ORDER BY price DESC
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_demand
        FROM demand
    """)

    # Compute actual clearing once per month
    actual_clear = con.sql("""
        WITH sa AS (
            SELECT date, period, price,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
            FROM supply_actual
        ),
        sa_join AS (
            SELECT s.date, s.period, s.price, s.cum_supply,
                   COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
            FROM sa s LEFT JOIN d_cum d
              ON d.date = s.date AND d.period = s.period AND d.price >= s.price
            GROUP BY s.date, s.period, s.price, s.cum_supply
        )
        SELECT date, period, MIN(price) AS p_actual
        FROM sa_join
        WHERE cum_supply >= cum_demand_at_or_above
        GROUP BY date, period
    """).df()

    # Per IB unit: substitute and re-clear
    out_rows = []
    for unit in ib_units:
        # Build sell_synth_unit: replace ONLY this one unit
        con.execute("DROP TABLE IF EXISTS sell_synth_u")
        con.execute(f"""
            CREATE TEMP TABLE sell_synth_u AS
            SELECT date, period, price, qty
            FROM sell_raw
            WHERE unit_code <> '{unit}'
            UNION ALL
            SELECT s.date, s.period, s.price, s.qty * m.K_ratio AS qty
            FROM sell_raw s
            JOIN match_tbl m ON m.unit_S = s.unit_code AND m.unit_L = '{unit}'
        """)
        clear_u = con.sql("""
            WITH agg AS (
                SELECT date, period, price, SUM(qty) AS qty
                FROM sell_synth_u GROUP BY date, period, price
            ),
            cum AS (
                SELECT date, period, price,
                       SUM(qty) OVER (PARTITION BY date, period
                                      ORDER BY price
                                      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
                FROM agg
            ),
            jn AS (
                SELECT c.date, c.period, c.price, c.cum_supply,
                       COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
                FROM cum c LEFT JOIN d_cum d
                  ON d.date = c.date AND d.period = c.period AND d.price >= c.price
                GROUP BY c.date, c.period, c.price, c.cum_supply
            )
            SELECT date, period, MIN(price) AS p_synth
            FROM jn
            WHERE cum_supply >= cum_demand_at_or_above
            GROUP BY date, period
        """).df()
        merged = actual_clear.merge(clear_u, on=["date", "period"], how="inner")
        merged["unit_L"] = unit
        merged["mp"] = merged["p_actual"] - merged["p_synth"]
        out_rows.append(merged)

    if not out_rows:
        return pd.DataFrame()
    return pd.concat(out_rows, ignore_index=True)


def main() -> None:
    if not MATCH.exists():
        print(f"ERROR: {MATCH} not found.")
        sys.exit(1)

    match = pd.read_parquet(MATCH)
    ib_matched = match[
        (match["firm_L"] == "IB") & (match["unit_S"].notna())
    ]
    ib_units = ib_matched["unit_L"].tolist()
    print(f"IB matched units (CCGT + Hydro): {len(ib_units)}")
    print(ib_matched[["unit_L", "tech", "capacity_L", "unit_S", "K_ratio"]].to_string(index=False))

    months = [(y, m) for y in [2024, 2025, 2026] for m in range(1, 13)]
    months = [m for m in months if (m[0], m[1]) >= (2025, 3) and (m[0], m[1]) <= (2026, 1)]
    # Restrict to post-MTU15-IDA only (det_all bid prices interpretable)

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("SET threads=4")

    all_results = []
    for y, m in months:
        print(f"  processing {y:04d}-{m:02d}...", flush=True)
        try:
            df = process_month(con, y, m, ib_units)
        except Exception as e:
            print(f"    FAIL {y:04d}-{m:02d}: {e}")
            continue
        if len(df) == 0:
            continue
        all_results.append(df)

    if not all_results:
        print("No results.")
        return

    full = pd.concat(all_results, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])
    full["regime"] = full["date"].apply(assign_regime)

    # Per (unit, regime) attribution
    print()
    print("=" * 100)
    print("Per-IB-unit market-power attribution (post-MTU15-IDA)")
    print("=" * 100)
    print()
    print(f"{'unit':<10}  {'tech':<8}  {'n ISPs':>8}  {'mean MP €/MWh':>14}  {'~transfer M€':>14}")

    rows = []
    for unit in ib_units:
        sub = full[full["unit_L"] == unit]
        if len(sub) == 0:
            continue
        mean_mp = sub["mp"].mean()
        # Transfer estimate: 25 GWh / 4 per ISP15 (post-MTU15-IDA period 1-96)
        transfer_eur = (sub["mp"] * 25_000 / 4).sum() / 1e6
        tech = ib_matched[ib_matched["unit_L"] == unit]["tech"].iloc[0]
        rows.append({
            "unit": unit, "tech": tech, "n_isps": len(sub),
            "mean_mp": float(mean_mp),
            "transfer_eur_M": float(transfer_eur),
        })

    rdf = pd.DataFrame(rows).sort_values("transfer_eur_M", ascending=False)
    for _, r in rdf.iterrows():
        print(
            f"{r['unit']:<10}  {r['tech']:<8}  {r['n_isps']:>8,}  "
            f"{r['mean_mp']:>+14.3f}  {r['transfer_eur_M']:>+14.1f}"
        )

    print()
    total = rdf["transfer_eur_M"].sum()
    ccgt_total = rdf[rdf["tech"] == "CCGT"]["transfer_eur_M"].sum()
    hydro_total = rdf[rdf["tech"] == "Hydro"]["transfer_eur_M"].sum()
    print(f"  Sum across IB units (CCGT+Hydro): €{total:+.1f}M")
    print(f"  CCGT subtotal:  €{ccgt_total:+.1f}M  ({ccgt_total/total*100:.1f}% of IB-attributed total)")
    print(f"  Hydro subtotal: €{hydro_total:+.1f}M  ({hydro_total/total*100:.1f}% of IB-attributed total)")
    print()
    NAMED = {"TAPOWER", "ARCOS1", "ARCOS2", "ARCOS3", "CTN3", "CTN4", "CTJON2", "STC4"}
    named_total = rdf[rdf["unit"].isin(NAMED)]["transfer_eur_M"].sum()
    print(f"  Named complex-bidder CCGT units subtotal: €{named_total:+.1f}M  ({named_total/total*100:.1f}%)")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    rdf.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
