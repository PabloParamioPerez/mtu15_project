# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (CCGT scarcity tier)
# CLAIM: Tests whether firms with a RICHER (more diversified) generation
#        portfolio bid a LOWER price in their CCGT scarcity tier (the block
#        withheld from the day-ahead auction). Hypothesis: a CCGT-dependent
#        firm parks its withheld block higher.
#        RESULT: no relationship (Spearman rho ~ -0.36 on non-CCGT share,
#        ~0 on tech breadth, N=13 firms). Note the DA scarcity-tier price is
#        NOT the Fase I payment -- Fase I is pay-as-bid of a SEPARATE
#        restriction offer (PO 14.4 apartado 20.1) -- so this price is a
#        firm-idiosyncratic non-clearing marker. The null is therefore
#        expected: portfolio richness should not predict a price that earns
#        nothing.
#
#        Portfolio richness per firm (firm = owner-agent group; majors mapped
#        by name): from cab_all registered capacity (MAX max_power_mw per
#        unit), generation units only, summed by technology:
#          non_ccgt_share = 1 - CCGT capacity / total generation capacity
#          n_techs        = number of generation techs the firm operates
#        Outcome: p_scar = the firm's MW-weighted mean CCGT scarcity-tier bid
#        price (tranches p > c_h + H, c_h = clock-hour-mean clearing price).
#        Cross-firm descriptive comparison (~12 CCGT-operating firms);
#        Spearman correlation, reported as suggestive given small N.
#
#        Caveat: renewables are often bid via representative/aggregator
#        agents, so non-CCGT share is measured with error; the dispatchable
#        side (hydro, pumped-storage, nuclear) is attributed cleanly.
#
# OUT: results/regressions/bid/granularity_response/ccgt_scarcity_portfolio.csv
#      figures/working/ccgt_scarcity_portfolio.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
MPDBC = P / "mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/granularity_response/ccgt_scarcity_portfolio.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/ccgt_scarcity_portfolio.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"
DST = ("2025-10-26", "2026-03-29")
H = 140.0
GEN_TECHS = ["CCGT", "Hydro", "Hydro_pump", "Hydro_RoR", "Wind", "Solar",
             "Nuclear"]


def tech_bucket(t):
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "bombeo" in t: return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "hidr" in t: return "Hydro_RoR"
    if "eólica" in t or "eolica" in t: return "Wind"
    if "solar" in t: return "Solar"
    return "Other"


def firm_id(o):
    o = str(o).lower()
    if "iberdrola" in o: return "Iberdrola"
    if "endesa" in o: return "Endesa"
    if "naturgy" in o or "gas natural" in o: return "Naturgy"
    if "edp" in o: return "EDP"
    if "engie" in o: return "Engie"
    if "repsol" in o: return "Repsol"
    if "moeve" in o or "cepsa" in o: return "Moeve"
    if "totalenergies" in o: return "TotalEnergies"
    if "axpo" in o: return "Axpo"
    if "bizkaia" in o: return "BizkaiaElec"
    if "ignis" in o: return "Ignis"
    if "alpiq" in o: return "Alpiq"
    if "alta eficiencia" in o: return "SEAE"
    return "Other"


def main():
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u["firm"] = u["owner_agent"].apply(firm_id)
    con.register("u", u[["unit_code", "tech", "firm"]])

    # --- portfolio: registered capacity by (firm, tech) ---------------------
    cap = con.execute(f"""
    WITH unit_cap AS (
      SELECT unit_code, MAX(max_power_mw) AS cap_mw
      FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V'
        AND max_power_mw > 0
      GROUP BY 1)
    SELECT u.firm, u.tech, SUM(uc.cap_mw) AS cap_mw, COUNT(*) AS n_units
    FROM unit_cap uc JOIN u ON uc.unit_code = u.unit_code
    WHERE u.tech IN ({','.join(f"'{t}'" for t in GEN_TECHS)})
    GROUP BY 1, 2
    """).fetchdf()

    port = cap.pivot_table(index="firm", columns="tech", values="cap_mw",
                           fill_value=0.0)
    for t in GEN_TECHS:
        if t not in port.columns:
            port[t] = 0.0
    port["total_gen_mw"] = port[GEN_TECHS].sum(axis=1)
    port["ccgt_mw"] = port["CCGT"]
    port = port[port["ccgt_mw"] > 0].copy()              # CCGT operators only
    port["non_ccgt_share"] = 1.0 - port["ccgt_mw"] / port["total_gen_mw"]
    port["n_techs"] = (port[GEN_TECHS] > 0).sum(axis=1)
    # effective number of techs (inverse Herfindahl of the capacity mix)
    shares = port[GEN_TECHS].div(port["total_gen_mw"], axis=0)
    port["tech_diversity"] = 1.0 / (shares ** 2).sum(axis=1)

    # --- outcome: CCGT scarcity-tier price per firm -------------------------
    dst = "(" + ",".join(f"'{d}'" for d in DST) + ")"
    psc = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code,
             CAST(FLOOR((period-1)/4.0) AS INT) AS clock_hour,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND quantity_mw > 0 AND period BETWEEN 1 AND 96),
    mp_h AS (
      SELECT CAST(date AS DATE) d,
             CAST(FLOOR((period-1)/4.0) AS INT) AS clock_hour,
             AVG(price_es_eur_mwh) AS c_h
      FROM '{MPDBC}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND price_es_eur_mwh IS NOT NULL GROUP BY 1,2)
    SELECT u.firm,
           SUM(dv.q*dv.p) / SUM(dv.q) AS p_scar,
           SUM(dv.q)                  AS scar_mw,
           COUNT(*)                   AS n_tranches
    FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN u    ON c.unit_code = u.unit_code AND u.tech='CCGT'
      JOIN mp_h ON mp_h.d=dv.d AND mp_h.clock_hour=dv.clock_hour
    WHERE dv.p > mp_h.c_h + {H} AND dv.d NOT IN {dst}
    GROUP BY 1
    """).fetchdf().set_index("firm")

    df = port.join(psc, how="inner")
    df = df[df["n_tranches"] >= 500].copy()              # enough scarcity data
    df = df.sort_values("non_ccgt_share", ascending=False)

    print(f"\n=== CCGT scarcity-tier price vs portfolio richness ({LO}..{HI}) ===")
    print("  non_ccgt_share = 1 - CCGT / total generation capacity (cab "
          "registered MW)")
    print("  p_scar = MW-weighted CCGT scarcity-tier bid price (p > c_h+140)\n")
    print(f"  {'firm':14s} {'CCGT MW':>8s} {'totgen MW':>10s} "
          f"{'non-CCGT%':>9s} {'n_tech':>6s} {'divers':>7s}  {'p_scar':>9s}")
    for fm, r in df.iterrows():
        print(f"  {fm:14s} {r['ccgt_mw']:>8.0f} {r['total_gen_mw']:>10.0f} "
              f"{r['non_ccgt_share']:>8.1%} {int(r['n_techs']):>6d} "
              f"{r['tech_diversity']:>7.2f}  {r['p_scar']:>9.1f}")

    # Spearman rank correlations (small N -> descriptive, not inferential)
    def spearman(a, b):
        ra, rb = pd.Series(a).rank(), pd.Series(b).rank()
        return np.corrcoef(ra, rb)[0, 1]

    print(f"\n  Spearman rank correlation with p_scar  (N = {len(df)} firms):")
    for col, lab in [("non_ccgt_share", "non-CCGT capacity share"),
                     ("n_techs", "number of generation techs"),
                     ("tech_diversity", "effective tech diversity")]:
        rho = spearman(df[col], df["p_scar"])
        print(f"    {lab:28s}  rho = {rho:+.2f}")

    df.to_csv(OUT)
    print(f"\nwrote {OUT}")

    # --- figure -------------------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(13, 5.2))
    for ax, xcol, xlab in [
            (axes[0], "non_ccgt_share", "non-CCGT share of generation capacity"),
            (axes[1], "tech_diversity", "effective number of technologies")]:
        ax.scatter(df[xcol], df["p_scar"],
                   s=40 + 0.06 * df["ccgt_mw"], color="#c44e52",
                   edgecolors="black", linewidths=0.5, zorder=3)
        for fm, r in df.iterrows():
            ax.annotate(fm, (r[xcol], r["p_scar"]), textcoords="offset points",
                        xytext=(6, 3), fontsize=7.5)
        # rank-fit guide line
        rho = spearman(df[xcol], df["p_scar"])
        ax.set_xlabel(xlab, fontsize=9.5)
        ax.set_ylabel("CCGT scarcity-tier bid price (EUR/MWh)", fontsize=9.5)
        ax.set_title(f"Spearman rho = {rho:+.2f}  (N={len(df)})", fontsize=9.5)
        ax.grid(alpha=0.3, lw=0.5)
    fig.suptitle("Do firms with a richer portfolio bid a lower CCGT scarcity "
                 "tier?  (DA15/ID15; point size ~ CCGT capacity)", fontsize=10.5)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
