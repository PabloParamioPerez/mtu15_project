# STATUS: ALIVE
# LAST-AUDIT: 2026-06-17
# FEEDS: thesis main-text figure -- critical-vs-flat pre-trends of the price-gap
#        Herfindahl HHI_p for CCGT, on the two own-market headline cells
#        (ID15 IDA, DA15 DA). 14-day rolling daily means; dashed line = cutover.
#        Companion to the HHI_p DiD table (hhi_price_did.py / tab:hhi-price).
#
# OUT: figures/thesis/fig_ccgt_hhi_price_pt.{pdf,png}

from pathlib import Path
import sys
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from hhi_price_did import build, CELLS  # noqa: E402

OUT = REPO / "figures/thesis/fig_ccgt_hhi_price_pt"


def daily_lines(panel, win):
    pre_lo, pre_hi, post_lo, post_hi = map(pd.Timestamp, win)
    q = panel[(panel.tech == "CCGT")].copy()
    q = q[q.hour_class.isin(["Critical", "Flat"])].dropna(subset=["hhi_price"])
    g = q.groupby(["d", "hour_class"])["hhi_price"].mean().unstack()
    g = g.sort_index()
    return g.rolling("14D").mean(), pd.Timestamp(post_lo)


def main():
    fig, axes = plt.subplots(1, 2, figsize=(8.2, 3.0), sharey=True)
    titles = {"ID15 IDA": "(a) ID15 --- intraday", "DA15 DA": "(b) DA15 --- day-ahead"}
    for ax, (label, cfg) in zip(axes, CELLS.items()):
        panel = build(cfg["market"], cfg["win"][0], cfg["win"][3], cfg["band"])
        roll, cut = daily_lines(panel, cfg["win"])
        ax.plot(roll.index, roll["Critical"], color="#b2182b", lw=1.6, label="Critical (ramps)")
        ax.plot(roll.index, roll["Flat"], color="#2166ac", lw=1.6, label="Flat (overnight)")
        ax.axvline(cut, color="black", ls="--", lw=1.0)
        ax.set_title(titles[label], fontsize=10)
        ax.set_ylabel(r"$\mathrm{HHI}_p \times 100$" if label == "ID15 IDA" else "")
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
        ax.tick_params(labelsize=8)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
    axes[0].legend(frameon=False, fontsize=8, loc="upper left")
    fig.tight_layout()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT.with_suffix(".pdf"))
    fig.savefig(OUT.with_suffix(".png"), dpi=150)
    print(f"Wrote {OUT.with_suffix('.pdf').relative_to(REPO)}")


if __name__ == "__main__":
    main()
