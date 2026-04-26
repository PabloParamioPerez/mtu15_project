# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F7 per-IB-unit decomposition (mechanism for IB hydro market power)
# CLAIM: IB hydro DA cleared volume concentrates more in high-price hours than Fringe hydro — Bushnell-style strategic dispatch signature
"""IB hydro strategic-dispatch test.

The 2026-04-27 per-IB-unit decomposition shows IB hydro carries ~64% of
IB's €820M post-MTU15-IDA market-power transfer (TAMEGA, SIL, DUER, TAJO).
This script tests whether IB hydro DA dispatch is concentrated in
high-price hours more than Fringe hydro is — the empirical signature of
strategic withholding (Bushnell 2003-style).

Test. Per (firm, hour), compute the share of total daily DA cleared MWh
that is dispatched in price-quartile-k hours (k = Q1 lowest to Q4 highest).
Strategic dispatch predicts IB hydro Q4 share > Fringe hydro Q4 share.

The Q4 cutoff is computed within-month to control for monthly average
price drift (e.g., the 2025 Spanish duck curve has shifted).

Memory-conscious: DuckDB streaming, returns aggregated per-firm panel.
"""
from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "results" / "hydro_strategic_dispatch.csv"


def main() -> None:
    # Hydro unit list from reference file
    ref = pd.read_csv(REF, encoding="latin1")
    ref["tech_low"] = ref["technology"].fillna("").astype(str).str.lower()
    hydro_units = ref[
        ref["tech_low"].str.contains("hidr", regex=False)
        | ref["tech_low"].str.contains("hydro", regex=False)
    ]["unit_code"].tolist()
    print(f"Hydro units in registry: {len(hydro_units):,}")

    con = duckdb.connect()
    con.execute("SET memory_limit='2GB'")
    con.execute("SET threads=4")
    con.register("hydro_units", pd.DataFrame({"unit_code": hydro_units}))

    # Per-(date, hour) DA price
    print("[1/3] DA hourly price...")
    con.execute(f"""
        CREATE TEMP TABLE px AS
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{PRICE}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2024-01-01'
        GROUP BY 1, 2
    """)

    # Per-(date, hour) price quartile within month
    con.execute("""
        CREATE TEMP TABLE px_q AS
        SELECT date, hour, p_da,
               DATE_TRUNC('month', CAST(date AS DATE)) AS month,
               NTILE(4) OVER (PARTITION BY DATE_TRUNC('month', CAST(date AS DATE))
                              ORDER BY p_da) AS price_q_in_month
        FROM px
    """)

    # Per-(unit, date, hour) cleared MWh, hydro only
    print("[2/3] Hydro cleared MWh per (unit, date, hour)...")
    con.execute(f"""
        CREATE TEMP TABLE hydro_clr AS
        SELECT p.unit_code,
               p.grupo_empresarial AS firm,
               p.date,
               CASE WHEN p.mtu_minutes = 15 THEN CEIL(p.period / 4.0)::INTEGER
                    ELSE p.period END AS hour,
               SUM(p.assigned_power_mw)
                 / CASE WHEN p.mtu_minutes = 15 THEN 4.0 ELSE 1.0 END AS q_mwh
        FROM '{PDBCE}' p
        JOIN hydro_units h USING (unit_code)
        WHERE p.offer_type = 1
          AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2024-01-01'
        GROUP BY p.unit_code, p.grupo_empresarial, p.date, hour, p.mtu_minutes
    """)

    # Join + classify firm group + price quartile
    print("[3/3] Aggregate per (firm_group, price_q)...")
    df = con.sql("""
        SELECT h.unit_code,
               CASE WHEN h.firm IN ('GE','IB','GN','HC') THEN h.firm ELSE 'Fringe' END AS firm_group,
               h.date, h.hour, h.q_mwh,
               q.p_da, q.price_q_in_month, q.month
        FROM hydro_clr h
        JOIN px_q q USING (date, hour)
    """).df()
    df["date"] = pd.to_datetime(df["date"])
    df["era"] = df["date"].apply(
        lambda d: "post-MTU15-IDA" if pd.Timestamp(d) >= pd.Timestamp("2025-03-19") else "pre-MTU15-IDA"
    )

    print(f"   panel rows: {len(df):,}")

    # Per (firm_group, era, price_q): total MWh
    print()
    print("=" * 100)
    print("Hydro DA cleared MWh distribution by price quartile (within-month)")
    print("=" * 100)
    print()
    print(f"{'firm':<10}  {'era':<16}  {'Q1 low':>10}  {'Q2':>10}  {'Q3':>10}  {'Q4 high':>10}  {'Q4 share':>10}")
    rows = []
    for fg in ["IB", "GE", "GN", "HC", "Fringe"]:
        for era in ["pre-MTU15-IDA", "post-MTU15-IDA"]:
            sub = df[(df["firm_group"] == fg) & (df["era"] == era)]
            if len(sub) == 0:
                continue
            tot = sub.groupby("price_q_in_month")["q_mwh"].sum()
            tot_total = tot.sum()
            q1 = tot.get(1, 0); q2 = tot.get(2, 0); q3 = tot.get(3, 0); q4 = tot.get(4, 0)
            q4_share = q4 / tot_total * 100 if tot_total > 0 else 0.0
            print(
                f"{fg:<10}  {era:<16}  {q1/1e6:>10.2f}  {q2/1e6:>10.2f}  {q3/1e6:>10.2f}  "
                f"{q4/1e6:>10.2f}  {q4_share:>9.1f}%"
            )
            rows.append({
                "firm": fg, "era": era,
                "Q1_TWh": q1/1e6, "Q2_TWh": q2/1e6, "Q3_TWh": q3/1e6, "Q4_TWh": q4/1e6,
                "Q4_share_pct": q4_share,
            })

    # Headline test: IB hydro Q4 share vs Fringe hydro Q4 share, post-reform
    print()
    print("Headline test (post-MTU15-IDA): IB hydro vs Fringe hydro Q4 share")
    rdf = pd.DataFrame(rows).set_index(["firm", "era"])
    if ("IB", "post-MTU15-IDA") in rdf.index and ("Fringe", "post-MTU15-IDA") in rdf.index:
        ib_q4 = rdf.loc[("IB", "post-MTU15-IDA"), "Q4_share_pct"]
        fr_q4 = rdf.loc[("Fringe", "post-MTU15-IDA"), "Q4_share_pct"]
        print(f"  IB hydro Q4 share:     {ib_q4:.1f}%")
        print(f"  Fringe hydro Q4 share: {fr_q4:.1f}%")
        print(f"  Difference (IB - Fringe): {ib_q4 - fr_q4:+.1f} pp")
        if ib_q4 > fr_q4 + 5:
            print("  ✓ IB hydro is DISPROPORTIONATELY concentrated in Q4 high-price hours.")
            print("    Bushnell-style strategic-dispatch signature confirmed.")
        elif ib_q4 > fr_q4:
            print("  ≈ IB hydro is somewhat more concentrated in Q4 than Fringe, but not dramatically.")
        else:
            print("  ✗ IB hydro is NOT more concentrated in Q4 than Fringe — strategic dispatch not visible at this cut.")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
