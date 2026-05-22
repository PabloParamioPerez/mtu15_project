# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (two-tier CCGT bid curve)
# CLAIM: The CCGT day-ahead bid curve is two-tier. Each Big-4 CCGT fleet
#        offers a low-price "competing" tier that clears the day-ahead
#        auction, and a high-price tier parked at scarcity levels that does
#        NOT clear the auction but is the capacity REE recalls in pre-IDA
#        Fase I redispatch (pay-as-bid). Aggregating each firm's CCGT sell
#        tranches over a regime into one supply curve makes the two tiers,
#        and the near-vertical jump between them, directly visible.
#
# OUT: figures/working/ccgt_two_tier_bid_curve.pdf

from pathlib import Path
import duckdb
import numpy as np

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
MPDBC = P / "mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG = REPO / "figures/working/ccgt_two_tier_bid_curve.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

REGIME = ("DA15/ID15", "2025-10-01", "2026-05-15")
BINW = 5.0
FIRMS = {"IB": "#1f77b4", "GE": "#2ca02c", "GN": "#d62728", "HC": "#9467bd"}


def tech_bucket(t):
    t = str(t).lower()
    return "CCGT" if "ciclo combinado" in t else "Other"


def firm_bucket(o):
    o = str(o).lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    return "OTH"


def main():
    import pandas as pd
    con = duckdb.connect()
    con.execute("SET memory_limit='10GB'")
    con.execute("SET threads=4")
    u = pd.read_csv(UNITS)
    u["tech"] = u["technology"].apply(tech_bucket)
    u["firm"] = u["owner_agent"].apply(firm_bucket)
    u = u[(u["tech"] == "CCGT") & u["firm"].isin(FIRMS)][
        ["unit_code", "firm"]].drop_duplicates("unit_code")
    con.register("u", u)
    _, lo, hi = REGIME

    # MW-weighted histogram of bid price per firm (price binned at 5 EUR/MWh).
    hist = con.execute(f"""
    WITH cab_l AS (
      SELECT d, offer_code, unit_code FROM (
        SELECT CAST(date AS DATE) d, offer_code, unit_code,
               ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                                  ORDER BY version DESC) rn
        FROM '{CAB}' WHERE date BETWEEN '{lo}' AND '{hi}' AND buy_sell='V') WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) d, offer_code, price_eur_mwh p, quantity_mw q
      FROM '{DET}' WHERE date BETWEEN '{lo}' AND '{hi}' AND quantity_mw > 0)
    SELECT u.firm,
           ROUND(dv.p / {BINW}) * {BINW} AS pbin,
           SUM(dv.q) AS mw
    FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
      JOIN u ON c.unit_code = u.unit_code
    GROUP BY 1, 2
    """).fetchdf()

    mcp = con.execute(f"""
      SELECT AVG(price_es_eur_mwh) FROM '{MPDBC}'
      WHERE date BETWEEN '{lo}' AND '{hi}' AND price_es_eur_mwh IS NOT NULL
    """).fetchone()[0]

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(9, 5.5))
    for firm, col in FIRMS.items():
        h = hist[hist["firm"] == firm].sort_values("pbin")
        if h.empty:
            continue
        price = h["pbin"].values
        cum = np.cumsum(h["mw"].values)
        share = cum / cum[-1]
        # Step supply curve: over cumulative-share (share[i-1], share[i]] the
        # offered price is price[i]; share[-1]=0 implicitly is 0.
        ax.step(np.concatenate([[0.0], share]),
                np.concatenate([price, [price[-1]]]),
                where="post", color=col, lw=1.8, label=firm)
        # Share of the fleet's offered MW priced at or below mean MCP.
        da_share = share[price <= mcp][-1] if (price <= mcp).any() else 0.0
        ax.scatter([da_share], [mcp], color=col, s=28, zorder=5,
                   edgecolors="black", linewidths=0.4)

    ax.axhline(mcp, color="black", ls="--", lw=1.1)
    ax.text(0.012, mcp + 26, f"mean day-ahead clearing price $\\approx$ {mcp:.0f} EUR/MWh",
            fontsize=8.5)
    ax.set_ylim(-60, 1150)
    ax.set_xlim(0, 1)
    ax.set_xlabel("cumulative share of the firm's offered CCGT MW", fontsize=10)
    ax.set_ylabel("bid price (EUR/MWh)", fontsize=10)
    ax.set_title(f"Two-tier CCGT day-ahead bid curve, by firm ({REGIME[0]})\n"
                 "low tier clears the auction; high tier is parked for Fase I "
                 "redispatch", fontsize=10.5)
    ax.annotate("competing tier\n(clears day-ahead)", xy=(0.10, mcp),
                xytext=(0.16, 430), fontsize=8.5, ha="center",
                arrowprops=dict(arrowstyle="->", lw=0.8))
    ax.annotate("scarcity tier\n(withheld; recalled in Fase I)", xy=(0.55, 1000),
                xytext=(0.45, 720), fontsize=8.5, ha="center",
                arrowprops=dict(arrowstyle="->", lw=0.8))
    ax.legend(fontsize=9, loc="upper left", title="CCGT fleet")
    ax.grid(alpha=0.3, lw=0.5)
    fig.tight_layout()
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")

    # Console summary: DA-clearing share and scarcity-tier share per firm.
    print(f"\nmean MCP {REGIME[0]}: {mcp:.1f} EUR/MWh")
    for firm in FIRMS:
        h = hist[hist["firm"] == firm].sort_values("pbin")
        if h.empty:
            continue
        tot = h["mw"].sum()
        below = h[h["pbin"] <= mcp]["mw"].sum() / tot
        scarc = h[h["pbin"] >= 500]["mw"].sum() / tot
        print(f"  {firm}: {below:5.1%} of offered MW bids <= MCP (DA tier), "
              f"{scarc:5.1%} at >= 500 EUR/MWh (scarcity tier)")


if __name__ == "__main__":
    main()
