# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Time series of daily REE-published ajuste-cost components (Fase I
#        up, Fase I dn, Fase II, TR up, TR dn, aFRR reserve, Imbalance),
#        14-day rolling mean, stacked positive (all are costs paid by REE,
#        no positive/negative sign distinction). Vertical lines mark the
#        five reform dates of the project timeline.
#
# Replaces the per-regime stacked bar chart
# (cascade_costs_official_per_regime.pdf) with the time-series view, as a
# companion to the broader cascade-costs table in
# thesis/provisional/additional_results.tex (still consumed there). The
# per-regime summary lives in the table; the figure adds the temporal
# evolution.
#
# IN:  data/processed/esios/indicators/indicators_all.parquet
# OUT: figures/working/cascade_costs_timeseries.pdf

from pathlib import Path

import duckdb
import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
IND = REPO / "data/processed/esios/indicators/indicators_all.parquet"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

WINDOW_START = "2024-01-01"
WINDOW_END = "2025-12-31"
ROLL = 14

# Stack order (bottom up). Same rollup as tab_cascade_costs_official.tex in
# scripts/analysis/regulatory/ree_full_cascade_v2.py.
COMPONENTS = [
    (1723, "TR up",         "#2c7fb8"),
    (1724, "TR dn",         "#1a5786"),
    (712,  "aFRR reserve up", "#7fcdbb"),
    (2127, "aFRR reserve dn", "#5a9a8b"),
    (1375, "Fase II up",    "#fdae6b"),
    (1376, "Fase II dn",    "#cf843f"),
    (1373, "Fase I up",     "#d7301f"),
    (1374, "Fase I dn",     "#9a2a1f"),
    (726,  "Imbalance excess", "#969696"),
    (727,  "Imbalance deficit","#525252"),
]

REFORM_DATES = [
    ("2024-06-14", "IDA reform\n6$\\to$3"),
    ("2024-12-11", "ISP15"),
    ("2025-03-19", "MTU15-IDA"),
    ("2025-04-28", "blackout"),
    ("2025-10-01", "MTU15-DA"),
]


def load_daily(con, indicator_id):
    q = f"""
    SELECT date, SUM(value) AS cost_eur
    FROM '{IND}'
    WHERE indicator_id = {indicator_id}
      AND date BETWEEN '{WINDOW_START}' AND '{WINDOW_END}'
      AND value IS NOT NULL
    GROUP BY date ORDER BY date
    """
    df = con.execute(q).fetchdf()
    df["cost_eur_m"] = df["cost_eur"] / 1e6
    return df[["date", "cost_eur_m"]]


def main():
    con = duckdb.connect()
    idx = pd.DataFrame({"date": pd.date_range(WINDOW_START, WINDOW_END, freq="D")})
    data = idx.copy()
    for ind, label, _ in COMPONENTS:
        d = load_daily(con, ind)
        d.columns = ["date", label]
        d["date"] = pd.to_datetime(d["date"])
        data = data.merge(d, on="date", how="left")
    data = data.fillna(0.0)
    # All components are REE-paid costs in absolute terms
    for _, label, _ in COMPONENTS:
        data[label] = data[label].abs()

    smooth = data.copy()
    for _, label, _ in COMPONENTS:
        smooth[label] = data[label].rolling(window=ROLL, center=True,
                                            min_periods=ROLL // 2).mean()

    fig, ax = plt.subplots(figsize=(13, 6.5))
    dates = smooth["date"]
    labels = [lab for _, lab, _ in COMPONENTS]
    colors = [c for _, _, c in COMPONENTS]
    series = [smooth[lab].values for lab in labels]
    ax.stackplot(dates, *series, labels=labels, colors=colors,
                 edgecolor="white", linewidth=0.3, alpha=0.95)

    ymax = max(np.nansum([s for s in series], axis=0)) * 1.05
    for date_str, txt in REFORM_DATES:
        d = pd.Timestamp(date_str)
        is_blk = "blackout" in txt
        ax.axvline(d, color="#9a2a1f" if is_blk else "gray",
                   ls=":", lw=1.4 if is_blk else 0.8, zorder=5)
        ax.text(d, ymax * 0.97, txt, ha="center", va="top", fontsize=7,
                color="gray", style="italic",
                bbox=dict(boxstyle="round,pad=0.15", fc="white",
                          ec="none", alpha=0.9))

    ax.set_ylabel(f"Per-day ajuste cost (EUR M, {ROLL}-day rolling mean)",
                  fontsize=10)
    ax.set_title("Evolution of daily REE ajuste-cost composition by service "
                 "(all stacked positive --- all are costs REE pays).",
                 fontsize=11)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.set_xlim(pd.Timestamp(WINDOW_START), pd.Timestamp(WINDOW_END))
    ax.set_ylim(0, ymax)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=30, ha="right")
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.15),
              fontsize=8, framealpha=0.92, ncol=5)
    ax.grid(axis="y", alpha=0.3, lw=0.5)
    ax.set_axisbelow(True)
    fig.tight_layout()
    out = FIG_DIR / "cascade_costs_timeseries.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
