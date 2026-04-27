# STATUS: ALIVE
# LAST-AUDIT: 2026-04-27
# FEEDS: F8 endogeneity sensitivity (red-team audit attack A3)
# CLAIM: F8's IB Q4 hydro-dispatch concentration survives the endogeneity concern: when re-defining Q4 quartiles using exogenous French DA prices instead of Spanish DA prices (which IB partly sets), the cross-firm gap is preserved.
"""F8 endogeneity sensitivity: French DA price as exogenous quartile reference.

Red-team audit attack A3. The baseline F8 finding (IB hydro 63% Q4
vs Fringe 42%, +21pp gap post-MTU15-IDA) defines within-month price
quartiles using the SPANISH DA clearing price. The Spanish price is
itself partly set by IB's bids — so when IB dispatches into a
high-price hour, IB's dispatch contributed to making it Q4. Q4 is
thus partly endogenous to IB's dispatch choice, which the attack
argues inflates the apparent +21pp gap.

This script re-runs F8 with within-month quartiles defined using
**French DA price** (ENTSO-E A44 for the FR domain). France:

  - Faces similar weather/demand patterns to Spain (climate spillover)
  - Is NOT set by IB (IB doesn't bid in France's market)
  - Therefore strictly exogenous to IB's Spanish dispatch decisions

If IB's hydro Q4 share against the French quartile is similar to
the baseline (Spanish quartile), the endogeneity concern is
data-defended. If it collapses, the +21pp gap was endogenous.

Two outputs:

  1. Q4-share table per (firm, era) using French quartiles —
     directly comparable to the baseline using Spanish quartiles.
  2. Concordance check: what % of Spain Q4 hours within each month
     are also France Q4 hours? High concordance → Q4 is essentially
     "high-European-demand hours" (exogenous to IB).

Output:
    data/derived/results/f8_endogeneity_sensitivity.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBCE = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PRICE_ES = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
PRICE_FR = PROJECT / "data" / "processed" / "entsoe" / "prices" / "fr_da_all.parquet"
REF = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT = PROJECT / "data" / "derived" / "results" / "f8_endogeneity_sensitivity.csv"


def main() -> None:
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

    # Load French DA price, hourly UTC
    print("[1/4] Loading French DA price (UTC hourly)...")
    fr_df = pd.read_parquet(PRICE_FR, columns=["isp_start_utc", "price_eur_per_mwh"])
    fr_df["isp_start_utc"] = pd.to_datetime(fr_df["isp_start_utc"])
    fr_df = fr_df.dropna(subset=["price_eur_per_mwh"])
    print(f"   FR DA: {len(fr_df):,} rows, range {fr_df['isp_start_utc'].min()} → {fr_df['isp_start_utc'].max()}")

    # Build the Spain (date, hour) ↔ UTC datetime mapping
    # OMIE uses Spain-local-hour 1..24 = local clock hour. Convert to UTC.
    # Spain is CET (UTC+1) winter, CEST (UTC+2) summer.
    print("[2/4] Spanish DA price (used for baseline Q4 reference)...")
    es_df = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes = 15 THEN CEIL(period / 4.0)::INTEGER
                    ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da_es
        FROM '{PRICE_ES}'
        WHERE price_es_eur_mwh IS NOT NULL
          AND CAST(date AS DATE) >= DATE '2024-01-01'
        GROUP BY 1, 2
    """).df()
    es_df["date"] = pd.to_datetime(es_df["date"])
    # Spain local datetime
    es_df["es_local_dt"] = es_df["date"] + pd.to_timedelta(es_df["hour"] - 1, unit="h")
    # Localize as Europe/Madrid then convert to UTC
    es_df["utc_dt"] = (
        es_df["es_local_dt"].dt.tz_localize("Europe/Madrid", ambiguous="NaT", nonexistent="NaT")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )
    es_df = es_df.dropna(subset=["utc_dt"])
    print(f"   ES DA: {len(es_df):,} rows post-2024, after DST cleanup")

    # Join French price at the same UTC moment
    fr_df = fr_df.rename(columns={"isp_start_utc": "utc_dt", "price_eur_per_mwh": "p_da_fr"})
    panel = es_df.merge(fr_df, on="utc_dt", how="inner")
    panel["month"] = panel["date"].dt.to_period("M").dt.to_timestamp()
    panel["era"] = panel["date"].apply(
        lambda d: "post-MTU15-IDA" if pd.Timestamp(d) >= pd.Timestamp("2025-03-19") else "pre-MTU15-IDA"
    )
    print(f"   ES↔FR aligned panel: {len(panel):,} (date, hour) rows")

    # Compute within-month quartiles for each (Spain-quartile, France-quartile)
    panel["q_es"] = panel.groupby("month")["p_da_es"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 4, labels=[1, 2, 3, 4]).astype(int)
    )
    panel["q_fr"] = panel.groupby("month")["p_da_fr"].transform(
        lambda s: pd.qcut(s.rank(method="first"), 4, labels=[1, 2, 3, 4]).astype(int)
    )

    # === Concordance check: how often does Spain Q4 = France Q4? ===
    print()
    print("=" * 90)
    print("Concordance: Spain Q4 hours ↔ France Q4 hours within month")
    print("=" * 90)
    crosstab = pd.crosstab(panel["q_es"], panel["q_fr"], normalize="index") * 100
    print("Cell value = % of (date, hour) rows with q_es=row that also have q_fr=col")
    print(crosstab.round(1).to_string())
    es_q4_fr_q4 = ((panel["q_es"] == 4) & (panel["q_fr"] == 4)).sum()
    es_q4_total = (panel["q_es"] == 4).sum()
    print()
    print(f"  Of all Spain-Q4 hours, {es_q4_fr_q4 / es_q4_total * 100:.1f}% are also France-Q4 hours.")
    print(f"  Of all Spain-Q4 hours, {((panel['q_es']==4) & (panel['q_fr']>=3)).sum() / es_q4_total * 100:.1f}% are France-Q3 or Q4.")

    # === Per-firm hydro dispatch by FRENCH quartile ===
    print()
    print("[3/4] Hydro cleared MWh per (unit, date, hour)...")
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
        WHERE p.offer_type = 1 AND p.assigned_power_mw IS NOT NULL
          AND p.assigned_power_mw > 0
          AND CAST(p.date AS DATE) >= DATE '2024-01-01'
        GROUP BY p.unit_code, p.grupo_empresarial, p.date, hour, p.mtu_minutes
    """)
    hydro = con.sql("SELECT * FROM hydro_clr").df()
    hydro["date"] = pd.to_datetime(hydro["date"])
    hydro = hydro.merge(panel[["date", "hour", "q_es", "q_fr", "era"]], on=["date", "hour"], how="inner")
    hydro["firm_group"] = hydro["firm"].apply(lambda f: f if f in ("GE","IB","GN","HC") else "Fringe")

    print(f"   joined hydro panel: {len(hydro):,} rows")
    print()
    print("[4/4] Q4 share by firm/era under SPANISH (baseline) vs FRENCH (exogenous) quartiles")
    print("=" * 90)
    print(f"{'firm':<8}  {'era':<16}  {'Q4_share_ES':>12}  {'Q4_share_FR':>12}  {'gap (FR vs Fringe FR)':>22}")

    rows = []
    fringe_q4_es = {}
    fringe_q4_fr = {}
    for era in ["pre-MTU15-IDA", "post-MTU15-IDA"]:
        sub_f = hydro[(hydro["firm_group"] == "Fringe") & (hydro["era"] == era)]
        if len(sub_f) > 0:
            fringe_q4_es[era] = sub_f.loc[sub_f["q_es"] == 4, "q_mwh"].sum() / sub_f["q_mwh"].sum() * 100
            fringe_q4_fr[era] = sub_f.loc[sub_f["q_fr"] == 4, "q_mwh"].sum() / sub_f["q_mwh"].sum() * 100

    for fg in ["IB", "GE", "GN", "HC", "Fringe"]:
        for era in ["pre-MTU15-IDA", "post-MTU15-IDA"]:
            sub = hydro[(hydro["firm_group"] == fg) & (hydro["era"] == era)]
            if len(sub) == 0:
                continue
            total = sub["q_mwh"].sum()
            q4_es = sub.loc[sub["q_es"] == 4, "q_mwh"].sum() / total * 100
            q4_fr = sub.loc[sub["q_fr"] == 4, "q_mwh"].sum() / total * 100
            gap_fr = q4_fr - fringe_q4_fr.get(era, 0)
            print(f"{fg:<8}  {era:<16}  {q4_es:>11.1f}%  {q4_fr:>11.1f}%  {gap_fr:>+21.1f} pp")
            rows.append({
                "firm": fg, "era": era,
                "Q4_share_ES_pct": q4_es,
                "Q4_share_FR_pct": q4_fr,
                "gap_vs_Fringe_FR_pp": gap_fr,
            })

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")

    # Summary headline
    rdf = pd.DataFrame(rows).set_index(["firm", "era"])
    if ("IB", "post-MTU15-IDA") in rdf.index and ("Fringe", "post-MTU15-IDA") in rdf.index:
        ib = rdf.loc[("IB", "post-MTU15-IDA")]
        fr = rdf.loc[("Fringe", "post-MTU15-IDA")]
        print()
        print("=" * 90)
        print("Headline (post-MTU15-IDA): IB hydro Q4 dispatch under exogenous (FR) reference")
        print("=" * 90)
        print(f"  Spanish-quartile-baseline gap:  {ib['Q4_share_ES_pct'] - fr['Q4_share_ES_pct']:+.1f} pp  "
              f"(IB {ib['Q4_share_ES_pct']:.1f}% vs Fringe {fr['Q4_share_ES_pct']:.1f}%)")
        print(f"  French-quartile-exogenous gap:  {ib['Q4_share_FR_pct'] - fr['Q4_share_FR_pct']:+.1f} pp  "
              f"(IB {ib['Q4_share_FR_pct']:.1f}% vs Fringe {fr['Q4_share_FR_pct']:.1f}%)")
        ratio = (ib['Q4_share_FR_pct'] - fr['Q4_share_FR_pct']) / (ib['Q4_share_ES_pct'] - fr['Q4_share_ES_pct']) * 100
        print(f"  Survival under exogenous FR ref: {ratio:.0f}% of the baseline gap")


if __name__ == "__main__":
    main()
