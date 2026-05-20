# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (strategic-band choice)
# CLAIM: Chooses the strategic-band half-width h in a DATA-DRIVEN way, instead
#        of asserting h = 50 EUR/MWh. For every DA sell tranche of the three
#        price-setting techs (CCGT, Hydro, Hydro_pump) of the Big-4 firms, it
#        builds the MW-weighted density of the bid-to-clearing distance
#        |p_bid - MCP|. CCGT bid stacks are bimodal: a dense near-MCP
#        "competing" cluster and a sparse "withholding" cluster parked at the
#        scarcity cap. h is set at the ANTIMODE -- the valley between the two
#        clusters -- so the band edge is where the data say in-contention
#        bidding ends. Estimated ONCE, pooled across all regimes, then frozen.
#
# OUT: results/regressions/bid/per_curve_metrics/strategic_band_h.json
#      figures/working/strategic_band_selection.pdf

from __future__ import annotations
from pathlib import Path
import json

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_JSON = REPO / "results/regressions/bid/per_curve_metrics/strategic_band_h.json"
OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
FIG = REPO / "figures/working/strategic_band_selection.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

START = "2024-06-14"
END = "2026-05-15"
BINW = 5.0          # EUR/MWh, distance histogram bin width
DMAX = 4500.0       # EUR/MWh, max distance tracked
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]


def tech_bucket(t):
    if t is None: return "Other"
    t = str(t).lower()
    if "ciclo combinado" in t: return "CCGT"
    if "hidráulica generación" in t: return "Hydro"
    if "bombeo" in t: return "Hydro_pump"
    return "Other"


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    return "OTH"


def build_histogram() -> pd.DataFrame:
    """MW-weighted histogram of |p_bid - MCP| per tech, pooled over all regimes."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "tech"]
    ].drop_duplicates("unit_code")
    con.register("u", units)
    sql = f"""
    WITH cab_last AS (
      SELECT CAST(date AS DATE) AS d, offer_code, unit_code,
             ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE), offer_code, unit_code
                                ORDER BY version DESC) AS rn
      FROM read_parquet('{CAB}')
      WHERE date >= '{START}' AND date <= '{END}' AND buy_sell='V'
    ),
    cab_l AS (SELECT d, offer_code, unit_code FROM cab_last WHERE rn=1),
    det AS (
      SELECT CAST(date AS DATE) AS d, offer_code, period,
             price_eur_mwh AS p, quantity_mw AS q
      FROM read_parquet('{DET}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear
      FROM read_parquet('{MPDBC}')
      WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    tr AS (
      SELECT u.tech, dv.q AS q, ABS(dv.p - mp.p_clear) AS dist
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN u ON c.unit_code = u.unit_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
    )
    SELECT tech,
           LEAST(FLOOR(dist / {BINW}) * {BINW}, {DMAX}) AS dist_bin,
           SUM(q)   AS mw,
           COUNT(*) AS n
    FROM tr
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return con.execute(sql).fetchdf()


def find_antimode(dist: np.ndarray, dens: np.ndarray):
    """Antimode = the valley that closes the near-MCP competing cluster.

    Smooth the MW-weighted density, locate the near-MCP peak, walk out until
    the density has fallen below 20% of that peak (the competing cluster has
    ended), then take the first local minimum: the floor of the valley before
    the next, sparser cluster. This is the band edge -- where in-contention
    bidding gives way to the scarcity-tail region.
    """
    # Moving-average smooth (window ~ 25 EUR/MWh).
    k = max(1, int(round(25.0 / BINW)))
    kernel = np.ones(2 * k + 1) / (2 * k + 1)
    sm = np.convolve(dens, kernel, mode="same")
    peak_i = int(np.argmax(sm))                       # near-MCP peak
    thr = 0.20 * sm[peak_i]
    below = np.where((sm < thr) & (dist > 30))[0]
    cluster_end = int(below[0])
    valley_i = cluster_end
    for i in range(cluster_end + 1, len(sm) - 1):
        if sm[i] <= sm[i - 1] and sm[i] < sm[i + 1]:
            valley_i = i
            break
    # Confirm a denser cluster rises after the valley (genuine bimodality).
    after = sm[valley_i:]
    rises = after.max() > 1.30 * sm[valley_i]
    return dist[valley_i], dist[peak_i], rises, sm


def main():
    print("Building bid-to-clearing distance histogram (DA sell tranches)...")
    hist = build_histogram()
    grid = np.arange(0, DMAX + BINW, BINW)
    # Per-tech and pooled MW-weighted density on the common grid.
    series = {}
    for tech in TECHS:
        s = hist[hist["tech"] == tech].set_index("dist_bin")["mw"]
        series[tech] = s.reindex(grid, fill_value=0.0).values
    pooled = np.sum([series[t] for t in TECHS], axis=0)
    pooled_dens = pooled / (pooled.sum() * BINW)

    h_anti, peak, rises, sm = find_antimode(grid, pooled_dens)
    total_mw = pooled.sum()
    in_band = pooled[grid < h_anti].sum() / total_mw
    print(f"  near-MCP peak at |p-MCP| ~ {peak:.0f} EUR/MWh")
    print(f"  ANTIMODE (band edge) h = {h_anti:.0f} EUR/MWh  "
          f"(cap cluster rises afterwards: {rises})")
    print(f"  share of sell-bid MW within +/- {h_anti:.0f}: {in_band:5.1%}")

    OUT_JSON.write_text(json.dumps({
        "h_eur_mwh": float(h_anti),
        "near_mcp_peak_eur_mwh": float(peak),
        "cap_cluster_present": bool(rises),
        "in_band_mw_share": float(in_band),
        "rule": "antimode of MW-weighted |p_bid - MCP| density, pooled over "
                "regimes and the three price-setting techs",
        "window": f"{START}..{END}",
    }, indent=2))
    print(f"wrote {OUT_JSON}")

    # ---- diagnostic figure -------------------------------------------------
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    tech_col = {"CCGT": "#d62728", "Hydro": "#1f77b4", "Hydro_pump": "#2ca02c"}
    # Left: zoom on 0-1000, per-tech MW-weighted density.
    ax = axes[0]
    for tech in TECHS:
        d = series[tech] / (series[tech].sum() * BINW)
        m = grid <= 1000
        ax.plot(grid[m], d[m], color=tech_col[tech], lw=1.4,
                label=tech.replace("_", " "))
    ax.axvline(h_anti, color="black", ls="--", lw=1.3)
    ax.text(h_anti + 12, ax.get_ylim()[1] * 0.85,
            rf"antimode $h={h_anti:.0f}$", fontsize=9)
    ax.axvline(50, color="grey", ls=":", lw=1.0)
    ax.text(52, ax.get_ylim()[1] * 0.70, "old $h=50$", fontsize=8, color="grey")
    ax.set_xlabel(r"$|p_{\mathrm{bid}} - \mathrm{MCP}|$ (EUR/MWh)", fontsize=10)
    ax.set_ylabel("MW-weighted density", fontsize=10)
    ax.set_title("Per-tech bid-distance density (zoom 0--1000)", fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, lw=0.5)
    # Right: pooled, full range, log-y, the two clusters visible.
    ax = axes[1]
    ax.semilogy(grid, np.maximum(pooled_dens, 1e-12), color="#333333", lw=1.0,
                label="pooled (raw)")
    ax.semilogy(grid, np.maximum(sm, 1e-12), color="#d62728", lw=1.6,
                label="pooled (smoothed)")
    ax.axvline(h_anti, color="black", ls="--", lw=1.3)
    ax.set_xlabel(r"$|p_{\mathrm{bid}} - \mathrm{MCP}|$ (EUR/MWh)", fontsize=10)
    ax.set_ylabel("MW-weighted density (log)", fontsize=10)
    ax.set_title("Pooled bid-distance density: competing vs cap cluster",
                 fontsize=10)
    ax.legend(fontsize=8)
    ax.grid(alpha=0.3, lw=0.5, which="both")
    fig.suptitle("Data-driven strategic band: antimode of the bid-to-clearing "
                 "distance density", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
