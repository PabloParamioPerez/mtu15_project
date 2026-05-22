# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (granularity response)
# CLAIM: Ranks technologies by how much they USE the within-hour margin that
#        the MTU15 day-ahead reform created. Before MTU15-DA a clock-hour was
#        one bid; after, it is four quarter-bids. "Responsiveness to
#        granularity" = how different a unit's four within-hour quarter
#        curves are. We decompose it into two channels, per (unit, date,
#        clock-hour) with all four quarters bid (DA15/ID15 sell side):
#          PRICE channel  D_price -- SD across the 4 quarters of the quarter's
#            MW-weighted mean in-band bid price (EUR/MWh). Price-takers bid one
#            price every quarter; only a price-MAKER moves price across the
#            within-hour quarters -> D_price is the STRATEGIC signal.
#          QUANTITY channel D_qty -- coefficient of variation across the 4
#            quarters of the quarter's in-band offered MW. Tracks the
#            sub-hourly forecast / dispatch / arbitrage profile -> the
#            OPERATIONAL signal.
#        Band: in-band |p - MCP| <= H = 140, centered on the CLOCK-HOUR-MEAN
#        clearing price so the band is fixed within the hour (a quarter-
#        specific band would slide with scarcity and move D_price mechanically).
#        Within-hour by construction => seasonality differenced out.
#
# OUT: results/regressions/bid/granularity_response/responsiveness_by_tech.csv
#      figures/working/granularity_responsiveness_by_tech.pdf

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "results/regressions/bid/granularity_response/responsiveness_by_tech.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/granularity_responsiveness_by_tech.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"      # DA15/ID15 (MTU15 day-ahead)
DST = ("2025-10-26", "2026-03-29")
H = 140.0
TECH_ORDER = ["CCGT", "Hydro", "Hydro_pump", "Hydro_RoR", "Wind", "Solar",
              "Nuclear", "Other"]


def tech_bucket(t):
    # Hydro = RESERVOIR generation only (matches per_curve_metrics.py); the
    # small RE run-of-river units are split out as Hydro_RoR -- they are
    # price-takers and would otherwise dilute the reservoir-Hydro average.
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "nuclear" in t: return "Nuclear"
    if "bombeo" in t: return "Hydro_pump"
    if "hidráulica generación" in t: return "Hydro"
    if "hidr" in t: return "Hydro_RoR"
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
    dst = "(" + ",".join(f"'{d}'" for d in DST) + ")"

    res = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             ((period - 1) % 4) AS quarter,
             price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND quantity_mw > 0 AND period BETWEEN 1 AND 96),
    mp_h AS (
      SELECT CAST(date AS DATE) d,
             CAST(FLOOR((period - 1) / 4.0) AS INT) AS clock_hour,
             AVG(price_es_eur_mwh) AS p_clear_h
      FROM '{MPDBC}' WHERE date BETWEEN '{LO}' AND '{HI}'
        AND price_es_eur_mwh IS NOT NULL
      GROUP BY 1, 2),
    inband AS (
      SELECT c.unit_code, u.tech, dv.d, dv.clock_hour, dv.quarter, dv.q, dv.p
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN u    ON c.unit_code = u.unit_code
        JOIN mp_h ON mp_h.d=dv.d AND mp_h.clock_hour=dv.clock_hour
      WHERE dv.p BETWEEN mp_h.p_clear_h - {H} AND mp_h.p_clear_h + {H}
        AND dv.d NOT IN {dst}),
    -- one in-band curve per (unit, date, quarter)
    per_curve AS (
      SELECT unit_code, tech, d, clock_hour, quarter,
             SUM(q) AS mq, SUM(q * p) / SUM(q) AS pq
      FROM inband GROUP BY 1,2,3,4,5 HAVING SUM(q) > 0),
    -- within-hour dispersion across the 4 quarters
    per_hour AS (
      SELECT unit_code, tech, d, clock_hour,
             STDDEV_SAMP(pq)               AS d_price,
             STDDEV_SAMP(mq) / AVG(mq)     AS d_qty
      FROM per_curve GROUP BY 1,2,3,4 HAVING COUNT(*) = 4)
    SELECT tech,
           COUNT(*)                              AS n_cells,
           AVG(d_price)                          AS mean_d_price,
           AVG(d_qty)                            AS mean_d_qty,
           AVG((d_price > 0.001)::INT)           AS share_price_any,
           AVG((d_price > 0.5)::INT)             AS share_price_shaped,
           AVG((d_qty   > 0.001)::INT)           AS share_qty_any,
           AVG((d_qty   > 0.05)::INT)            AS share_qty_shaped,
           AVG((d_price > 0.5 OR d_qty > 0.05)::INT) AS share_uses_granularity
    FROM per_hour GROUP BY 1
    """).fetchdf()

    res["tech"] = pd.Categorical(res["tech"], TECH_ORDER, ordered=True)
    res = res.sort_values("tech")
    res.to_csv(OUT, index=False)

    print(f"\n=== Within-hour granularity responsiveness by technology "
          f"({LO}..{HI}, DA sell) ===")
    print("  D_price = SD across the 4 within-hour quarters of the quarter's "
          "in-band bid price (EUR/MWh) -- the STRATEGIC channel")
    print("  D_qty   = CV across the 4 quarters of in-band offered MW "
          "-- the OPERATIONAL channel\n")
    print(f"  {'tech':11s} {'n_cells':>10s}  {'D_price':>8s}  {'D_qty':>7s}  "
          f"{'price>0':>8s} {'price>.5':>8s}  {'qty>0':>7s} {'qty>5%':>7s} "
          f"{'uses%':>6s}")
    for _, r in res.iterrows():
        print(f"  {r['tech']:11s} {int(r['n_cells']):>10,}  "
              f"{r['mean_d_price']:>8.2f}  {r['mean_d_qty']:>7.3f}  "
              f"{r['share_price_any']:>7.1%} {r['share_price_shaped']:>8.1%}  "
              f"{r['share_qty_any']:>6.1%} {r['share_qty_shaped']:>6.1%} "
              f"{r['share_uses_granularity']:>5.1%}")

    print("\n  --- RANKINGS ---")
    print("  by PRICE channel (strategic):  " + " > ".join(
        res.sort_values("mean_d_price", ascending=False)["tech"].astype(str)))
    print("  by QUANTITY channel (operational):  " + " > ".join(
        res.sort_values("mean_d_qty", ascending=False)["tech"].astype(str)))
    print("  by USES-GRANULARITY at all:  " + " > ".join(
        res.sort_values("share_uses_granularity",
                        ascending=False)["tech"].astype(str)))
    print(f"\nwrote {OUT}")

    # --- figure: strategic (price) vs operational (quantity) plane ----------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    col = {"CCGT": "#d62728", "Hydro": "#1f77b4", "Hydro_pump": "#2ca02c",
           "Hydro_RoR": "#17becf", "Wind": "#9467bd", "Solar": "#ff7f0e",
           "Nuclear": "#8c564b", "Other": "#999999"}
    fig, ax = plt.subplots(figsize=(7.6, 6.0))
    for _, r in res.iterrows():
        t = str(r["tech"])
        ax.scatter(r["mean_d_qty"], r["mean_d_price"], s=160,
                   color=col.get(t, "#999"), edgecolors="black", linewidths=0.6,
                   zorder=3)
        ax.annotate(t.replace("_", " "),
                    (r["mean_d_qty"], r["mean_d_price"]),
                    textcoords="offset points", xytext=(9, 4), fontsize=9.5)
    ax.set_xlabel("operational channel  --  within-hour quantity dispersion "
                  r"$D_{\mathrm{qty}}$ (CV)", fontsize=10)
    ax.set_ylabel("strategic channel  --  within-hour price dispersion "
                  r"$D_{\mathrm{price}}$ (EUR/MWh)", fontsize=10)
    ax.set_title("Responsiveness to day-ahead granularity, by technology\n"
                 "(within-hour quarter-to-quarter bid dispersion, DA15/ID15)",
                 fontsize=10.5)
    ax.grid(alpha=0.3, lw=0.5)
    ax.margins(0.18)
    fig.tight_layout()
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
