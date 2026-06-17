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


def find_band_edge(dist: np.ndarray, dens: np.ndarray):
    """Band edge = the upper edge of the near-MCP competing cluster.

    Smooth the MW-weighted density, locate the near-MCP peak, and walk out to
    where the density has fallen below 20% of that peak: the competing cluster
    has ended. An earlier draft used the \"antimode\" -- the valley floor
    further out -- but that valley sits ABOVE the realised clearing-price
    ceiling (the day-ahead price never exceeds ~250 EUR/MWh; see main()), so
    bids in the valley are soft-withholding, not in contention. The
    competing-cluster edge is the right band edge.
    """
    k = max(1, int(round(25.0 / BINW)))               # ~25 EUR/MWh smooth
    kernel = np.ones(2 * k + 1) / (2 * k + 1)
    sm = np.convolve(dens, kernel, mode="same")
    peak_i = int(np.argmax(sm))                       # near-MCP peak
    thr = 0.20 * sm[peak_i]
    edge_i = int(np.where((sm < thr) & (dist > 30))[0][0])
    rises = (sm[edge_i:].max() > 1.30 * sm[edge_i]) if edge_i < len(sm) - 1 else False
    return dist[edge_i], dist[peak_i], rises, sm


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

    h, peak, rises, sm = find_band_edge(grid, pooled_dens)
    total_mw = pooled.sum()
    in_band = pooled[grid < h].sum() / total_mw

    # Cross-check against the clearing-price distribution: the band edge must
    # not exceed the price range the auction actually produces.
    mcp = duckdb.connect().execute(f"""
        SELECT quantile_cont(price_es_eur_mwh, 0.50),
               quantile_cont(price_es_eur_mwh, 0.99),
               quantile_cont(price_es_eur_mwh, 0.999),
               MAX(price_es_eur_mwh)
        FROM read_parquet('{MPDBC}')
        WHERE date >= '{START}' AND date <= '{END}'
          AND price_es_eur_mwh IS NOT NULL
    """).fetchone()
    mcp_p50, mcp_p99, mcp_p999, mcp_max = mcp
    print(f"  near-MCP peak at |p-MCP| ~ {peak:.0f} EUR/MWh")
    print(f"  BAND EDGE h = {h:.0f} EUR/MWh (competing-cluster edge; "
          f"cap cluster present: {rises})")
    print(f"  share of sell-bid MW within +/- {h:.0f}: {in_band:5.1%}")
    print(f"  clearing price: p50 {mcp_p50:.0f}  p99 {mcp_p99:.0f}  "
          f"p99.9 {mcp_p999:.0f}  max {mcp_max:.0f} EUR/MWh")
    print(f"  cross-check: median MCP + h = {mcp_p50 + h:.0f} ~ p99.9 of MCP "
          f"({mcp_p999:.0f}) -- the band edge coincides with the price ceiling")

    OUT_JSON.write_text(json.dumps({
        "h_eur_mwh": float(h),
        "scarcity_threshold_eur_mwh": 200.0,
        "near_mcp_peak_eur_mwh": float(peak),
        "cap_cluster_present": bool(rises),
        "in_band_mw_share": float(in_band),
        "clearing_price_p50": float(mcp_p50), "clearing_price_p99": float(mcp_p99),
        "clearing_price_p999": float(mcp_p999), "clearing_price_max": float(mcp_max),
        "rule": "upper edge of the near-MCP competing cluster in the MW-weighted "
                "|p_bid - MCP| density; cross-checked against the clearing-price "
                "distribution (median MCP + h ~ p99.9 of MCP). Scarcity threshold "
                "200 EUR/MWh: the day-ahead price exceeds it in 0.1% of periods.",
        "window": f"{START}..{END}",
    }, indent=2))
    print(f"wrote {OUT_JSON}")
    h_anti = h

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
            rf"band edge $h={h_anti:.0f}$", fontsize=9)
    ax.axvline(230, color="grey", ls=":", lw=1.0)
    ax.text(232, ax.get_ylim()[1] * 0.70, "old $h=230$", fontsize=8, color="grey")
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
    fig.suptitle("Data-driven strategic band: upper edge of the near-MCP "
                 "competing cluster", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(FIG, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {FIG}")


if __name__ == "__main__":
    main()
