# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F8 / IB-canonical synthesis — longitudinal mechanism test
# CLAIM: Iberdrola's market-power signature (Q4 hydro-dispatch concentration) traced year-by-year 2018-2026 to test whether the +7pp post-MTU15-IDA widening is a reform-attributable jump, a continuous trend, or hydrology-dependent.
"""IB market-power year-by-year longitudinal decomposition.

The F8 alive claim says IB hydro is +21pp more Q4-concentrated than
Fringe post-MTU15-IDA, vs +14pp pre-reform — a +7pp widening
attributed to "reform amplification". The user pushed: is this
year-invariant up to the reform, then a sharp jump? A continuous
trend? Hydrology-dependent?

This script computes year-by-year (2018-2026):

  - IB hydro Q4 share (within-month price quartile)
  - Fringe hydro Q4 share
  - Gap (IB - Fringe)
  - IB hydro total cleared GWh
  - IB realized DA price (revenue / volume) vs system average

Then we look for:
  - Discontinuity at reform dates (2024-06 IDA, 2024-12 ISP15, 2025-03
    MTU15-IDA, 2025-10 MTU15-DA)
  - Continuous trend pre-reform
  - Hydrology dependence (Q4 share ~ IB hydro gen)

Output:
    data/derived/results/ib_market_power_year_by_year.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "results" / "ib_market_power_year_by_year.csv"


def main() -> None:
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    hydro_units = ref[
        ref["tech_low"].str.contains("hidr", regex=False)
    ]["unit_code"].tolist()

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.register("hydro_units", pd.DataFrame({"unit_code": hydro_units}))

    print("[1/3] DA hourly price + within-month price quartile, 2018+...")
    con.execute(f"""
        CREATE TEMP TABLE px AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2018-01-01'
        GROUP BY 1, 2
    """)
    con.execute("""
        CREATE TEMP TABLE px_q AS
        SELECT date, hour, p_da,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               EXTRACT(YEAR FROM CAST(date AS DATE))::INTEGER AS year,
               NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', CAST(date AS DATE))
                              ORDER BY p_da) AS price_q_in_month
        FROM px
    """)

    print("[2/3] Hydro cleared MWh per (firm, year, hour) with quartile...")
    con.execute(f"""
        CREATE TEMP TABLE hydro_clr AS
        SELECT p.unit_code,
               CASE WHEN p.grupo_empresarial IN ('GE','IB','GN','HC') THEN p.grupo_empresarial ELSE 'Fringe' END AS firm_group,
               p.date,
               CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER
                    ELSE p.period END AS hour,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        WHERE p.offer_type = 1 AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2018-01-01'
        GROUP BY p.unit_code, firm_group, p.date, hour, p.mtu_minutes
    """)

    print("[3/3] Aggregate per (firm, year, quartile)...")
    df = con.sql("""
        SELECT h.firm_group,
               q.year,
               q.price_q_in_month,
               SUM(h.q_mwh) AS q_mwh
        FROM hydro_clr h
        JOIN px_q q ON h.date = q.date AND h.hour = q.hour
        GROUP BY 1, 2, 3
    """).df()

    # Per (firm, year) totals + Q4 shares
    pivot = df.pivot_table(
        index=["firm_group", "year"], columns="price_q_in_month",
        values="q_mwh", aggfunc="sum",
    ).reset_index().fillna(0)
    pivot.columns.name = None
    pivot["total_gwh"] = (pivot[1] + pivot[2] + pivot[3] + pivot[4]) / 1e3
    pivot["Q4_pct"] = pivot[4] / (pivot[1] + pivot[2] + pivot[3] + pivot[4]) * 100
    pivot["Q1_pct"] = pivot[1] / (pivot[1] + pivot[2] + pivot[3] + pivot[4]) * 100
    pivot = pivot[["firm_group", "year", "total_gwh", "Q1_pct", "Q4_pct"]]

    # Print main table
    print()
    print("=" * 100)
    print("Year-by-year hydro Q4 dispatch share by firm (within-month price quartile)")
    print("=" * 100)
    print()

    rows = []
    for year in sorted(pivot["year"].unique()):
        ib = pivot[(pivot["firm_group"] == "IB") & (pivot["year"] == year)]
        fringe = pivot[(pivot["firm_group"] == "Fringe") & (pivot["year"] == year)]
        ge = pivot[(pivot["firm_group"] == "GE") & (pivot["year"] == year)]
        ib_q4 = ib["Q4_pct"].iloc[0] if len(ib) else float("nan")
        ib_gwh = ib["total_gwh"].iloc[0] if len(ib) else float("nan")
        fr_q4 = fringe["Q4_pct"].iloc[0] if len(fringe) else float("nan")
        ge_q4 = ge["Q4_pct"].iloc[0] if len(ge) else float("nan")
        gap = ib_q4 - fr_q4
        rows.append({
            "year": year, "ib_q4_pct": ib_q4, "fringe_q4_pct": fr_q4,
            "ge_q4_pct": ge_q4, "ib_total_gwh": ib_gwh, "ib_minus_fringe_pp": gap,
        })

    print(f"{'year':<6} {'IB Q4%':>8} {'Fringe Q4%':>12} {'gap (pp)':>10} {'GE Q4%':>8} {'IB hydro GWh/yr':>18}  flag")
    print("-" * 100)
    for r in rows:
        flag = ""
        if r["year"] == 2024: flag = "← IDA reform mid"
        if r["year"] == 2025: flag = "← MTU15-IDA + ISP15 (full year)"
        if r["year"] == 2026: flag = "← post-MTU15-DA (partial)"
        print(f"{r['year']:<6} {r['ib_q4_pct']:>7.1f}% {r['fringe_q4_pct']:>11.1f}%"
              f" {r['ib_minus_fringe_pp']:>+9.1f}  {r['ge_q4_pct']:>7.1f}%"
              f" {r['ib_total_gwh']:>17.0f}  {flag}")

    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)

    # Test for discontinuity at MTU15-IDA: pre-2025 mean vs 2025 vs 2026
    print()
    print("=" * 100)
    print("Discontinuity analysis")
    print("=" * 100)
    pre = [r for r in rows if r["year"] <= 2024]
    post_2025 = [r for r in rows if r["year"] == 2025]
    post_2026 = [r for r in rows if r["year"] == 2026]

    if pre and post_2025:
        pre_gap = sum(r["ib_minus_fringe_pp"] for r in pre) / len(pre)
        pre_min = min(r["ib_minus_fringe_pp"] for r in pre)
        pre_max = max(r["ib_minus_fringe_pp"] for r in pre)
        print(f"  Pre-MTU15-IDA (2018-2024) IB-Fringe gap:")
        print(f"    mean: {pre_gap:>+6.1f} pp;  range [{pre_min:+.1f}, {pre_max:+.1f}]")
        if post_2025:
            print(f"  2025 (mostly post-MTU15-IDA): {post_2025[0]['ib_minus_fringe_pp']:>+6.1f} pp")
            jump = post_2025[0]["ib_minus_fringe_pp"] - pre_gap
            print(f"    jump vs pre-mean: {jump:>+6.1f} pp")
            jump_max = post_2025[0]["ib_minus_fringe_pp"] - pre_max
            print(f"    jump vs pre-max: {jump_max:>+6.1f} pp  (positive = exceeds prior year-by-year max)")
        if post_2026:
            print(f"  2026 partial (post-MTU15-DA): {post_2026[0]['ib_minus_fringe_pp']:>+6.1f} pp")

    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
