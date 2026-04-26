# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 (per-firm decomposition); §0 IB-canonical synthesis
# CLAIM: Per-firm Ciarreta-Espinosa market-power decomposition: which firm drives the joint Big-4 +13% market power?
"""Synthetic-firm method, per-firm decomposition (Ciarreta-Espinosa Tables 3-4 style).

The joint Big-4 synthetic re-clearing in synthetic_firm_clearing.py
shows ~13% market power post-MTU15-IDA (~€833M transfer). This script
decomposes that joint number into per-firm attributions:

  p_synth_GE  = clearing price if only GE's plants are replaced by their
                synthetic Fringe matches (IB/GN/HC offers actual)
  p_synth_IB  = analogous for IB only
  p_synth_GN  = analogous for GN only
  p_synth_HC  = analogous for HC only

Per-firm market power = p_actual - p_synth_F. If IB's market-power
contribution is larger than GE's despite GE being the larger firm,
that confirms the IB-canonical pattern (F1/F2/F5/F6/B8) extends to
the synthetic-firm method.

This is the cross-firm test Ciarreta-Espinosa run in their Tables 3-4:
they found IB > EN despite EN being larger, attributing it to CTC
regulation. For us in 2024-2026, CTC is gone — so the IB > GE pattern
(if confirmed) needs a different explanation (portfolio composition,
strategic conduct, etc.).
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
OUT = PROJECT / "data" / "derived" / "results" / "synthetic_firm_per_firm.csv"

BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"):
        return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"):
        return "3-sess"
    if d < pd.Timestamp("2025-03-19"):
        return "ISP15 win"
    if d < pd.Timestamp("2025-10-01"):
        return "DA60/ID15"
    return "DA15/ID15"


def process_month(con, year: int, month: int) -> pd.DataFrame:
    """Process one calendar month; return per-(date, period) clearing prices
    for actual + each per-firm synthetic substitution."""
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year+1:04d}-01-01"
    else:
        end = f"{year:04d}-{month+1:02d}-01"

    con.execute("DROP TABLE IF EXISTS match_tbl")
    con.execute(f"""
        CREATE TEMP TABLE match_tbl AS
        SELECT * FROM '{MATCH}' WHERE unit_S IS NOT NULL
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

    # Pre-aggregate demand and actual supply once
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

    def clearing(supply_table: str, alias: str) -> str:
        """SQL CTE expressions to clear `supply_table` against d_cum."""
        return f"""
        cum_{alias} AS (
            SELECT date, period, price,
                   SUM(qty) OVER (PARTITION BY date, period
                                  ORDER BY price
                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_supply
            FROM {supply_table}
        ),
        join_{alias} AS (
            SELECT s.date, s.period, s.price, s.cum_supply,
                   COALESCE(MAX(d.cum_demand), 0) AS cum_demand_at_or_above
            FROM cum_{alias} s
            LEFT JOIN d_cum d
              ON d.date = s.date AND d.period = s.period AND d.price >= s.price
            GROUP BY s.date, s.period, s.price, s.cum_supply
        ),
        clear_{alias} AS (
            SELECT date, period, MIN(price) AS p_{alias}
            FROM join_{alias}
            WHERE cum_supply >= cum_demand_at_or_above
            GROUP BY date, period
        )"""

    # Build sell_synth_<firm> tables: replace ONLY <firm>'s matched plants
    for firm in BIG4:
        con.execute(f"DROP TABLE IF EXISTS sell_synth_{firm}")
        con.execute(f"""
            CREATE TEMP TABLE sell_synth_{firm} AS
            SELECT date, period, price, qty, unit_code
            FROM sell_raw s
            WHERE NOT EXISTS (
                SELECT 1 FROM match_tbl m
                WHERE m.unit_L = s.unit_code AND m.firm_L = '{firm}'
            )
            UNION ALL
            SELECT s.date, s.period, s.price, s.qty * m.K_ratio AS qty,
                   m.unit_L AS unit_code
            FROM sell_raw s
            JOIN match_tbl m
              ON m.unit_S = s.unit_code AND m.firm_L = '{firm}'
        """)
        con.execute(f"DROP TABLE IF EXISTS supply_synth_{firm}")
        con.execute(f"""
            CREATE TEMP TABLE supply_synth_{firm} AS
            SELECT date, period, price, SUM(qty) AS qty
            FROM sell_synth_{firm} GROUP BY date, period, price
        """)

    # Now build the master query: clearing price under actual + per-firm synth
    ctes = [clearing("supply_actual", "actual")]
    for firm in BIG4:
        ctes.append(clearing(f"supply_synth_{firm}", firm))
    cte_block = ",\n        ".join(ctes)

    select_cols = ["a.date", "a.period", "a.p_actual"]
    join_block = ""
    for firm in BIG4:
        select_cols.append(f"{firm.lower()}.p_{firm}")
        join_block += f"\n        LEFT JOIN clear_{firm} {firm.lower()} USING (date, period)"

    sql = f"""
        WITH
        {cte_block}
        SELECT {', '.join(select_cols)}
        FROM clear_actual a{join_block}
        ORDER BY a.date, a.period
    """
    df = con.sql(sql).df()
    return df


def main() -> None:
    if not MATCH.exists():
        print(f"ERROR: {MATCH} not found.")
        sys.exit(1)

    months = [(y, m) for y in [2024, 2025, 2026] for m in range(1, 13)]
    months = [m for m in months if (m[0], m[1]) >= (2024, 6) and (m[0], m[1]) <= (2026, 4)]

    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
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
        all_results.append(df)
        for t in ["sell_raw", "buy_raw", "match_tbl", "supply_actual", "demand", "d_cum"]:
            con.execute(f"DROP TABLE IF EXISTS {t}")
        for firm in BIG4:
            con.execute(f"DROP TABLE IF EXISTS sell_synth_{firm}")
            con.execute(f"DROP TABLE IF EXISTS supply_synth_{firm}")

    if not all_results:
        print("No results.")
        return

    full = pd.concat(all_results, ignore_index=True)
    full["date"] = pd.to_datetime(full["date"])
    full["regime"] = full["date"].apply(assign_regime)

    # Per-firm market-power index per ISP
    for firm in BIG4:
        full[f"mp_{firm}"] = full["p_actual"] - full[f"p_{firm}"]

    # Drop ISPs with NaN in any per-firm column
    cols = [f"p_{f}" for f in BIG4] + ["p_actual"]
    full = full.dropna(subset=cols).copy()

    print()
    print("=" * 110)
    print("Per-firm market-power decomposition (post-MTU15-IDA only; pre-reform 0-padded prices excluded)")
    print("=" * 110)
    post = full[full["date"] >= pd.Timestamp("2025-03-19")].copy()

    # Per regime, per firm
    print()
    print(f"{'regime':<14}  {'n':>7}  ", end="")
    for f in BIG4:
        print(f"{'mp_'+f+' (€/MWh)':>14}  ", end="")
    print()

    rows = []
    for r in ["DA60/ID15", "DA15/ID15"]:
        sub = post[post["regime"] == r]
        if len(sub) == 0:
            continue
        print(f"{r:<14}  {len(sub):>7,}  ", end="")
        for f in BIG4:
            mean_mp = sub[f"mp_{f}"].mean()
            print(f"{mean_mp:>+14.3f}  ", end="")
            rows.append({
                "regime": r, "firm": f, "n_isps": len(sub),
                "mean_mp": float(mean_mp),
                "mean_p_actual": float(sub["p_actual"].mean()),
                "rel_mp_pct": float(mean_mp / sub["p_actual"].mean() * 100),
            })
        print()

    # Headline test
    print()
    print("Headline IB > GE test (post-MTU15-IDA pooled):")
    print(f"{'firm':<6}  {'mean MP €/MWh':>15}  {'rel MP %':>10}  {'~transfer M€ (n_isps × ~25 GWh × MP)':>40}")
    for f in BIG4:
        sub = post[post["regime"].isin(["DA60/ID15", "DA15/ID15"])]
        mean_mp = sub[f"mp_{f}"].mean()
        rel = mean_mp / sub["p_actual"].mean() * 100
        # Transfer estimate: per ISP volume ≈ 25 GWh (60-min equivalent); ISP15 means 25/4 GWh per 15-min ISP
        sub_post = sub.copy()
        sub_post["q_proxy"] = 25_000 / 4
        transfer_eur = (sub[f"mp_{f}"] * 25_000 / 4).sum() / 1e6
        print(f"{f:<6}  {mean_mp:>+15.3f}  {rel:>9.2f}%  {transfer_eur:>+40.1f}")

    # Verdict
    if rows:
        ge = next((r for r in rows if r["firm"] == "GE" and r["regime"] == "DA60/ID15"), None)
        ib = next((r for r in rows if r["firm"] == "IB" and r["regime"] == "DA60/ID15"), None)
        if ge and ib:
            print()
            print(
                f"DA60/ID15 mean MP: GE = {ge['mean_mp']:+.3f} €/MWh ({ge['rel_mp_pct']:.2f}%); "
                f"IB = {ib['mean_mp']:+.3f} €/MWh ({ib['rel_mp_pct']:.2f}%)"
            )
            if ib["mean_mp"] > ge["mean_mp"]:
                print("✓ IB > GE — synthetic-firm decomposition CONFIRMS IB-canonical pattern across the 4th independent test.")
            elif ib["mean_mp"] > 0 and ge["mean_mp"] > 0 and abs(ib["mean_mp"] - ge["mean_mp"]) < 0.3:
                print("≈ IB ≈ GE — synthetic-firm method does not strongly distinguish; market power is shared.")
            else:
                print("✗ GE > IB — synthetic-firm decomposition CONTRADICTS IB-canonical pattern.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(rows)
    out_df.to_csv(OUT, index=False)
    full.to_csv(OUT.parent / "synthetic_firm_per_firm_isp.csv", index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
