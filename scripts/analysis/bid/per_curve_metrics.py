# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: Two scalar metrics defined ON EACH SINGLE bid curve (not from a
#        comparison between curves -- per N. Fabra's advice, 2026-05-21).
#        For each (unit, date, period) day-ahead sell bid curve, restricted
#        to the DATA-DRIVEN strategic band |p - MCP| <= H. H is the antimode
#        of the MW-weighted bid-to-clearing distance density (the valley that
#        closes the near-MCP competing cluster), estimated once and frozen by
#        strategic_band_selection.py -- see strategic_band_h.json. H = 230.
#
#          PRICE metric  -- sigma_p: MW-weighted standard deviation of the
#            in-band tranche prices (EUR/MWh). How much the firm varies
#            price across the MW it offers near the clearing price. A flat
#            single block -> 0; a graded ladder spanning the band -> large.
#
#          QUANTITY metric -- N_eff: effective number of in-band tranches,
#            the inverse Herfindahl of the tranche MW shares,
#            N_eff = (sum q)^2 / sum(q^2). N_eff = 1 -> all MW in one
#            block; N_eff large -> MW split into many comparable pieces.
#
#        Both are per-curve. Cross-regime / cross-firm / cross-hour reading
#        is done by AGGREGATING these per-curve scalars (means, here), never
#        by differencing two curves. Computed raw -- seasonality, if it
#        matters, is handled at the aggregation step by same-calendar-month
#        windowing, not by deseasonalising the curve.
#
# OUT: data/derived/panels/per_curve_metrics_da.parquet   (per-cell)
#      results/regressions/bid/per_curve_metrics/tex/tab_per_curve_metrics.tex

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT_DIR = REPO / "data/derived/panels"
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT = OUT_DIR / "per_curve_metrics_da.parquet"
TEX_DIR = REPO / "results/regressions/bid/per_curve_metrics/tex"
TEX_DIR.mkdir(parents=True, exist_ok=True)

# Data-driven strategic-band half-width (EUR/MWh): antimode of the
# MW-weighted |p_bid - MCP| density, frozen by strategic_band_selection.py.
H = 230.0
START = "2024-06-14"
END = "2026-05-15"

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]
CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}


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
    if "repsol" in o: return "REP"
    return "OTH"


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def build_panel():
    """Per (date, clock_hour, unit) in-band price spread and effective tranche count."""
    con = duckdb.connect()
    con.execute("SET memory_limit='12GB'")
    con.execute("SET threads=4")
    units = pd.read_csv(UNITS)
    units["tech"] = units["technology"].apply(tech_bucket)
    units["firm"] = units["owner_agent"].apply(firm_bucket)
    units = units[units["tech"].isin(TECHS) & units["firm"].isin(FIRMS)][
        ["unit_code", "firm", "tech"]
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
             price_eur_mwh AS p, quantity_mw AS q, COALESCE(mtu_minutes, 60) AS mtu
      FROM read_parquet('{DET}')
      WHERE date >= '{START}' AND date <= '{END}' AND quantity_mw > 0
    ),
    mp AS (
      SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS p_clear,
             COALESCE(mtu_minutes, 60) AS mtu_p
      FROM read_parquet('{MPDBC}')
      WHERE date >= '{START}' AND date <= '{END}' AND price_es_eur_mwh IS NOT NULL
    ),
    inband AS (
      SELECT mp.d, mp.period, c.unit_code, dv.q, dv.p,
             CASE WHEN COALESCE(mp.mtu_p, dv.mtu) = 60 THEN mp.period - 1
                  ELSE CAST(FLOOR((mp.period - 1) / 4.0) AS INT) END AS clock_hour
      FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
        JOIN mp ON mp.d=dv.d AND mp.period=dv.period
      WHERE dv.p BETWEEN mp.p_clear - {H} AND mp.p_clear + {H}
    )
    -- One bid curve per (date, market period, unit): pre-MTU15 the period is
    -- a clock-hour, post-MTU15 it is a 15-min quarter. The metric is computed
    -- on each curve; clock_hour is kept only to label/aggregate later.
    SELECT i.d, i.period, i.clock_hour, i.unit_code, u.firm, u.tech,
           SUM(i.q)                       AS sum_w,
           SUM(i.q * i.p)                 AS sum_wp,
           SUM(i.q * i.p * i.p)           AS sum_wp2,
           SUM(i.q * i.q)                 AS sum_w2,
           COUNT(*)                       AS n_tranche
    FROM inband i JOIN u ON i.unit_code = u.unit_code
    GROUP BY 1, 2, 3, 4, 5, 6
    HAVING SUM(i.q) > 0
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])

    # PRICE metric: MW-weighted SD of in-band tranche prices.
    mean_p = df["sum_wp"] / df["sum_w"]
    var_p = (df["sum_wp2"] / df["sum_w"]) - mean_p ** 2
    df["sigma_p"] = np.sqrt(var_p.clip(lower=0))
    # QUANTITY metric: effective number of in-band tranches (inverse-HHI).
    df["n_eff"] = (df["sum_w"] ** 2) / df["sum_w2"]

    df["hour_class"] = df["clock_hour"].apply(hour_class)
    df["regime"] = "other"
    for label, lo, hi, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    df = df[df["regime"] != "other"].copy()
    return df[["d", "period", "clock_hour", "hour_class", "unit_code", "firm", "tech",
               "regime", "n_tranche", "sigma_p", "n_eff"]]


def write_summary_table(df):
    """Per-(tech, hour_class, regime) mean of the two per-curve metrics."""
    sub = df[df["hour_class"].isin(["Critical", "Flat"])]
    g = (sub.groupby(["tech", "hour_class", "regime"], observed=True)
            .agg(sigma_p=("sigma_p", "mean"), n_eff=("n_eff", "mean"),
                 n_cells=("sigma_p", "size"))
            .reset_index())
    regime_order = [r[0] for r in REGIME_DATES]
    regime_disp = {r[0]: r[3] for r in REGIME_DATES}
    lines = [r"\begin{tabular}{l l " + "r r " * len(regime_order) + r"}",
             r"\toprule",
             " & & " + " & ".join([rf"\multicolumn{{2}}{{c}}{{{regime_disp[r]}}}"
                                   for r in regime_order]) + r" \\",
             "Tech & Hour & " + " & ".join([r"$\sigma_p$ & $N_{\text{eff}}$"
                                            for _ in regime_order]) + r" \\",
             r"\midrule"]
    for tech in TECHS:
        for hc in ["Critical", "Flat"]:
            row = [tech.replace("_", " "), hc]
            for r in regime_order:
                cell = g[(g["tech"] == tech) & (g["hour_class"] == hc) & (g["regime"] == r)]
                if cell.empty:
                    row += ["---", "---"]
                else:
                    row += [f"{cell['sigma_p'].iloc[0]:.1f}", f"{cell['n_eff'].iloc[0]:.1f}"]
            lines.append(" & ".join(row) + r" \\")
        lines.append(r"\addlinespace")
    lines += [r"\bottomrule", r"\end{tabular}"]
    out = TEX_DIR / "tab_per_curve_metrics.tex"
    out.write_text("\n".join(lines))
    print(f"wrote {out}")


def write_figure(df):
    """2D figure (no 3D): the two per-curve metrics across regimes, per tech."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig_dir = REPO / "figures/working"
    fig_dir.mkdir(parents=True, exist_ok=True)
    regime_order = [r[0] for r in REGIME_DATES]
    regime_disp = [r[3] for r in REGIME_DATES]
    tech_col = {"CCGT": "#d62728", "Hydro": "#1f77b4", "Hydro_pump": "#2ca02c"}

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    for metric, ax, ylab in [("sigma_p", axes[0], r"Price spread $\sigma_p$ (EUR/MWh)"),
                             ("n_eff", axes[1], r"Effective tranches $N_{\mathrm{eff}}$")]:
        for tech in TECHS:
            for hc, ls, mk in [("Critical", "-", "o"), ("Flat", "--", "s")]:
                sub = df[(df["tech"] == tech) & (df["hour_class"] == hc)]
                vals = [sub[sub["regime"] == r][metric].mean() for r in regime_order]
                ax.plot(range(len(regime_order)), vals, ls=ls, marker=mk, ms=5,
                        color=tech_col[tech], lw=1.6,
                        label=f"{tech.replace('_',' ')} {hc.lower()}")
        ax.set_xticks(range(len(regime_order)))
        ax.set_xticklabels(regime_disp, rotation=30, ha="right", fontsize=8)
        ax.set_ylabel(ylab, fontsize=10)
        ax.grid(alpha=0.3, lw=0.5)
    axes[0].legend(fontsize=7, ncol=2, framealpha=0.9)
    fig.suptitle("Per-curve bid-shape metrics across regimes (DA, in-band): "
                 "price spread and effective tranche count", fontsize=11)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    out = fig_dir / "per_curve_metrics.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {out}")


def main():
    print("Building per-curve metric panel (DA sell bids, in-band)...")
    df = build_panel()
    print(f"  {len(df):,} (unit, date, hour) in-band curves")
    df.to_parquet(OUT, index=False)
    print(f"wrote {OUT}")
    write_summary_table(df)
    write_figure(df)
    # quick console read
    for tech in TECHS:
        s = df[(df["tech"] == tech) & (df["hour_class"] == "Critical")]
        print(f"  {tech} critical: sigma_p mean {s['sigma_p'].mean():.1f} EUR/MWh, "
              f"N_eff mean {s['n_eff'].mean():.1f}")


if __name__ == "__main__":
    main()
