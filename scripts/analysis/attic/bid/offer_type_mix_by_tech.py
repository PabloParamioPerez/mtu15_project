# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (offer-types paragraph)
# CLAIM: Tells us, per technology, how much of the day-ahead sell-offer book
#        is a PURE SIMPLE offer (a bare price-quantity tranche ladder) versus
#        carries a complex condition. This matters for the per-curve
#        functionals (sigma_p, N_eff): they read the submitted geometry, and
#        a minimum-income condition or a block order changes what that
#        geometry means. Classification per (date, offer_code, unit_code),
#        last version, DA15/ID15 window:
#          MIC    -- cab fixed_term_eur > 0 (minimum-income condition)
#          block  -- any det tranche with block_number > 0
#          simple -- neither (priority: MIC > block > simple)
#        Result: Wind/Solar/Hydro/Hydro-pump are ~100% simple; Nuclear is
#        mostly simple with a MIC minority; CCGT is the outlier -- only a
#        small minority simple, the rest MIC or block.
#
# OUT: results/regressions/bid/per_curve_metrics/offer_type_mix.csv  (console)

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/per_curve_metrics/offer_type_mix.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"   # DA15/ID15 regime
TECH_ORDER = ["CCGT", "Nuclear", "Hydro", "Hydro_pump", "Wind", "Solar", "Other"]


def tech_bucket(t):
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "bombeo" in t: return "Hydro_pump"
    if "hidr" in t: return "Hydro"
    if "eólica" in t or "eolica" in t: return "Wind"
    if "solar" in t: return "Solar"
    return "Other"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u = u[["unit_code", "tech"]].drop_duplicates("unit_code")
    con.register("u", u)

    # One classified row per (date, offer_code, unit_code), last cab version.
    df = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code, version, fixed_term_eur FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code, version, fixed_term_eur,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det_agg AS (
      SELECT CAST(date AS DATE) d, offer_code, version,
             MAX((block_number>0)::INT) AS has_block
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
      GROUP BY 1,2,3),
    cls AS (
      SELECT u.tech,
             CASE WHEN c.fixed_term_eur > 0          THEN 'MIC'
                  WHEN COALESCE(d.has_block,0) = 1   THEN 'block'
                  ELSE 'simple' END AS offer_type
      FROM cab_l c
        JOIN u ON c.unit_code = u.unit_code
        LEFT JOIN det_agg d
          ON c.d=d.d AND c.offer_code=d.offer_code AND c.version=d.version)
    SELECT tech, offer_type, COUNT(*) AS n_offers
    FROM cls GROUP BY 1,2
    """).fetchdf()

    piv = (df.pivot(index="tech", columns="offer_type", values="n_offers")
             .fillna(0.0))
    for col in ["simple", "MIC", "block"]:
        if col not in piv.columns:
            piv[col] = 0.0
    piv["total"] = piv[["simple", "MIC", "block"]].sum(axis=1)
    for col in ["simple", "MIC", "block"]:
        piv[f"{col}_pct"] = 100.0 * piv[col] / piv["total"]
    piv = piv.reindex([t for t in TECH_ORDER if t in piv.index])

    piv.to_csv(OUT)
    print(f"\n=== DA sell-offer type mix by tech ({LO}..{HI}) ===")
    print(f"   share of offers that are pure simple / MIC / block\n")
    for tech, r in piv.iterrows():
        print(f"  {tech:11s}  simple {r['simple_pct']:5.1f}%   "
              f"MIC {r['MIC_pct']:5.1f}%   block {r['block_pct']:5.1f}%   "
              f"(n={int(r['total']):,})")
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
