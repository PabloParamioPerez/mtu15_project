# STATUS: ALIVE
# LAST-AUDIT: 2026-04-26
# FEEDS: F7 (synthetic-firm method, Stages 2-3: synthetic supply + re-clearing)
# CLAIM: Per-ISP synthetic clearing price under Ciarreta-Espinosa Big-4 → Fringe substitution
"""Synthetic-firm method, Stages 2-3: synthetic supply + auction re-clearing.

Pipeline (per month, to bound memory):

  1. Load actual sell-side offers (cab + det) for the month.
  2. Load actual buy-side bids (cab + det) for the month.
  3. Apply plant-pair substitution (synthetic_plant_match.parquet):
     for each Big-4 plant L matched to Fringe plant S, REPLACE L's offers
     in the period with K_L/K_S × S's offers in the same period.
     Unmatched Big-4 plants (nuclear) keep actual offers.
  4. Per (date, period):
       p_actual  = clearing price under actual sell-side aggregate
       p_synth   = clearing price under synthetic sell-side aggregate
       q_actual / q_synth analogous
  5. Output per-(date, period) panel with both prices + quantities.

The clearing rule: find the smallest price p* such that cumulative
supply at or below p* >= cumulative demand at or above p*. Tied prices
resolved as in OMEL rules (price of the last accepted sell offer).
"""
from __future__ import annotations

from pathlib import Path
import sys
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
DET = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
MATCH = PROJECT / "data" / "derived" / "panels" / "synthetic_plant_match.parquet"
OUT = PROJECT / "data" / "derived" / "results" / "synthetic_firm_clearing.csv"

BIG4 = ["GE", "IB", "GN", "HC"]


def process_month(con: duckdb.DuckDBPyConnection, year: int, month: int) -> pd.DataFrame:
    """Process one calendar month; return per-(date, period) clearing prices."""
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year+1:04d}-01-01"
    else:
        end = f"{year:04d}-{month+1:02d}-01"

    # ---- Load matched-pair table into the connection ----
    con.execute("DROP TABLE IF EXISTS match_tbl")
    con.execute(f"""
        CREATE TEMP TABLE match_tbl AS
        SELECT * FROM '{MATCH}' WHERE unit_S IS NOT NULL
    """)

    # ---- Sell-side bids for the month, with unit→firm tagging ----
    # Get unit_code from cab; price/qty from det. Filter to date window.
    con.execute("DROP TABLE IF EXISTS sell_raw")
    con.execute(f"""
        CREATE TEMP TABLE sell_raw AS
        SELECT d.date,
               d.period,
               d.price_eur_mwh AS price,
               d.quantity_mw   AS qty,
               c.unit_code
        FROM '{DET}' d
        JOIN '{CAB}' c
          ON c.date = d.date
         AND c.offer_code = d.offer_code
         AND c.version = d.version
        WHERE c.buy_sell = 'V'
          AND d.quantity_mw > 0
          AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)
    n_sell = con.sql("SELECT COUNT(*) FROM sell_raw").fetchone()[0]
    if n_sell == 0:
        return pd.DataFrame()

    # ---- Buy-side bids for the month ----
    con.execute("DROP TABLE IF EXISTS buy_raw")
    con.execute(f"""
        CREATE TEMP TABLE buy_raw AS
        SELECT d.date,
               d.period,
               d.price_eur_mwh AS price,
               d.quantity_mw   AS qty
        FROM '{DET}' d
        JOIN '{CAB}' c
          ON c.date = d.date
         AND c.offer_code = d.offer_code
         AND c.version = d.version
        WHERE c.buy_sell = 'C'
          AND d.quantity_mw > 0
          AND d.price_eur_mwh IS NOT NULL
          AND CAST(d.date AS DATE) >= DATE '{start}'
          AND CAST(d.date AS DATE) <  DATE '{end}'
    """)

    # ---- Build synthetic sell-side ----
    # For each Big-4 plant L matched to S: drop L's offers, add S's offers
    # scaled by K_ratio. Unmatched plants (nuclear, etc.) keep their offers.
    con.execute("DROP TABLE IF EXISTS sell_synth")
    con.execute("""
        CREATE TEMP TABLE sell_synth AS
        SELECT date, period, price, qty, unit_code
        FROM sell_raw s
        WHERE NOT EXISTS (SELECT 1 FROM match_tbl m WHERE m.unit_L = s.unit_code)
        UNION ALL
        SELECT s.date, s.period, s.price, s.qty * m.K_ratio AS qty,
               m.unit_L AS unit_code
        FROM sell_raw s
        JOIN match_tbl m ON m.unit_S = s.unit_code
    """)

    # Sanity check: row counts
    n_synth = con.sql("SELECT COUNT(*) FROM sell_synth").fetchone()[0]

    # ---- Per (date, period), compute clearing price under each supply ----
    # Cumulative supply: sort by price ascending, running sum of qty.
    # Cumulative demand: sort by price descending, running sum of qty.
    # Clearing price = min price where cum_supply >= cum_demand.
    # Implementation: for each (date, period), do separate sort+cumsum.
    df = con.sql("""
        WITH
        supply_actual AS (
            SELECT date, period, price, SUM(qty) AS qty
            FROM sell_raw GROUP BY date, period, price
        ),
        supply_synth AS (
            SELECT date, period, price, SUM(qty) AS qty
            FROM sell_synth GROUP BY date, period, price
        ),
        demand AS (
            SELECT date, period, price, SUM(qty) AS qty
            FROM buy_raw GROUP BY date, period, price
        ),
        sa AS (
            SELECT date, period, price, qty,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
            FROM supply_actual
        ),
        ss AS (
            SELECT date, period, price, qty,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
            FROM supply_synth
        ),
        d_cum AS (
            SELECT date, period, price, qty,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price DESC
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_demand
            FROM demand
        ),
        -- Find clearing price for ACTUAL supply: smallest price where cum_supply at p
        --   meets demand at the same price level.
        -- Use pivot via join on price point: at each candidate price p, get cum_supply at <=p
        --   and cum_demand at >=p; clearing where they cross.
        ac_join AS (
            SELECT sa.date, sa.period, sa.price,
                   sa.cum_supply AS cum_supply,
                   COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
            FROM sa
            LEFT JOIN d_cum d
              ON d.date = sa.date AND d.period = sa.period AND d.price >= sa.price
            GROUP BY sa.date, sa.period, sa.price, sa.cum_supply
        ),
        ss_join AS (
            SELECT ss.date, ss.period, ss.price,
                   ss.cum_supply AS cum_supply,
                   COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
            FROM ss
            LEFT JOIN d_cum d
              ON d.date = ss.date AND d.period = ss.period AND d.price >= ss.price
            GROUP BY ss.date, ss.period, ss.price, ss.cum_supply
        ),
        ac_clear AS (
            SELECT date, period, MIN(price) AS p_actual
            FROM ac_join
            WHERE cum_supply >= cum_demand_at_or_above
            GROUP BY date, period
        ),
        ss_clear AS (
            SELECT date, period, MIN(price) AS p_synth
            FROM ss_join
            WHERE cum_supply >= cum_demand_at_or_above
            GROUP BY date, period
        )
        SELECT a.date, a.period,
               a.p_actual, b.p_synth,
               a.p_actual - b.p_synth AS market_power_eur_mwh
        FROM ac_clear a
        LEFT JOIN ss_clear b USING (date, period)
        ORDER BY date, period
    """).df()

    return df


def main() -> None:
    if not MATCH.exists():
        print(f"ERROR: {MATCH} not found. Run synthetic_firm_matching.py first.")
        sys.exit(1)

    # Determine month range from sell-side data: 2024-06 to 2026-04
    # (post-IDA window — the period of interest for the reform analysis)
    months = [(y, m) for y in [2024, 2025, 2026] for m in range(1, 13)]
    months = [m for m in months if (m[0], m[1]) >= (2024, 6) and (m[0], m[1]) <= (2026, 4)]

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")

    all_results = []
    for y, m in months:
        print(f"  processing {y:04d}-{m:02d}...", flush=True)
        try:
            df = process_month(con, y, m)
        except Exception as e:
            print(f"    FAIL {y:04d}-{m:02d}: {e}")
            continue
        if len(df) == 0:
            print(f"    (no data)")
            continue
        df["year"] = y
        df["month_id"] = m
        all_results.append(df)
        # Free memory between months
        con.execute("DROP TABLE IF EXISTS sell_raw")
        con.execute("DROP TABLE IF EXISTS sell_synth")
        con.execute("DROP TABLE IF EXISTS buy_raw")
        con.execute("DROP TABLE IF EXISTS match_tbl")

    if not all_results:
        print("No results produced.")
        return

    full = pd.concat(all_results, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])

    # Quick sanity check
    print()
    print(f"Total ISPs processed: {len(full):,}")
    print(f"Date range: {full['date'].min().date()} to {full['date'].max().date()}")
    print()
    print("Market power (p_actual - p_synth) summary:")
    print(full["market_power_eur_mwh"].describe().to_string())

    # Persist
    OUT.parent.mkdir(parents=True, exist_ok=True)
    full.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
