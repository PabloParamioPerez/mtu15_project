# STATUS: ALIVE
# LAST-AUDIT: 2026-05-11
# FEEDS: thesis paper.tex Figure 4 (pre-vs-post-MTU15-DA amplification)
# CLAIM: Per-firm critical-flat differential in CCGT bid-step count, before
#        and after the October 2025 day-ahead reform.

from __future__ import annotations

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "results" / "regressions" / "bid" / "perfirm_pre_vs_post_by_hour_class.csv"
OUTDIR = REPO / "figures" / "thesis"
PAPER_FIGDIR = REPO / "thesis" / "paper" / "figures"
OUTDIR.mkdir(parents=True, exist_ok=True)
PAPER_FIGDIR.mkdir(parents=True, exist_ok=True)

# Treatment firms shown in the figure. EDP-PT excluded: no Spanish-zone DET
# entries (its bids are in the Portuguese zone, parsed separately).
FIRM_ORDER = ["IB", "GE", "GN", "HC"]
FIRM_LABEL = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP-Spain"}

PRE_COLOR = "#9bb6d6"
POST_COLOR = "#d34a4a"


def main():
    df = pd.read_csv(SRC)
    # Compute per-firm differential: mean_n_tranches(critical) - mean_n_tranches(flat)
    pivot = df.pivot_table(index=["firm_class", "window"],
                           columns="hour_class",
                           values="mean_n_tranches").reset_index()
    pivot["diff"] = pivot["critical_h18_22"] - pivot["flat_h3_5"]
    diff = pivot.pivot_table(index="firm_class", columns="window", values="diff")
    diff = diff.reindex(FIRM_ORDER)

    fig, ax = plt.subplots(figsize=(9, 5.5))
    x = np.arange(len(FIRM_ORDER))
    width = 0.38
    pre = diff["PRE_2024_MTU60"].values
    post = diff["POST_2025_MTU15"].values
    ax.bar(x - width/2, pre,  width, color=PRE_COLOR,
           label="Pre-MTU15-DA (October--December 2024)")
    ax.bar(x + width/2, post, width, color=POST_COLOR,
           label="Post-MTU15-DA (October--December 2025)")

    # Multiplier annotations
    for xi, p_, q in zip(x, pre, post):
        if np.isfinite(p_) and abs(p_) > 0.1:
            mul = q / p_
            label = f"×{mul:.1f}" if mul > 0 else f"({mul:+.1f}×)"
        else:
            label = "(new)" if q > 0 else ""
        ax.annotate(label, xy=(xi + width/2, q), ha="center", va="bottom",
                    fontsize=10, fontweight="bold",
                    color="black" if (np.isfinite(p_) and abs(p_) > 0.1) else "#a04040")

    ax.axhline(0, color="black", linewidth=0.6)
    ax.set_xticks(x)
    ax.set_xticklabels([FIRM_LABEL[f] for f in FIRM_ORDER])
    ax.set_ylabel("Mean bid-step count, critical hours $-$ flat hours")
    ax.set_title("Per-firm bid-ladder enrichment in critical hours, pre vs post the October 2025 day-ahead reform")
    ax.legend(loc="upper right", fontsize=10, frameon=False)
    ax.grid(alpha=0.3, axis="y")

    fig.tight_layout()
    out = OUTDIR / "fig_per_firm_amplification"
    fig.savefig(f"{out}.png", dpi=160, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    fig.savefig(PAPER_FIGDIR / "fig_per_firm_amplification.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"wrote: {out}.png / .pdf  +  {PAPER_FIGDIR / 'fig_per_firm_amplification.pdf'}")
    print()
    print(diff.to_string())


if __name__ == "__main__":
    main()
