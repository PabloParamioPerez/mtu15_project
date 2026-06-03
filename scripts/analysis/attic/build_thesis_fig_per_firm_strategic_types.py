# STATUS: ALIVE
# LAST-AUDIT: 2026-05-11
# FEEDS: thesis paper.tex Figure 3 (per-firm bid-shape and granularity exploitation)
# CLAIM: Builds the 4-panel figure that contrasts dominant CCGT firms' bid-shape
#        between critical and flat hours (October-December 2025, post-MTU15-DA).
#        Input is perfirm_hourly_ccgt_bidshape_oct_dec_2025.csv.

from __future__ import annotations

from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CSV = REPO / "results" / "regressions" / "bid" / "perfirm_hourly_ccgt_bidshape_oct_dec_2025.csv"
OUTDIR = REPO / "figures" / "thesis"
# paper.tex reads PDFs from thesis/paper/figures/, so emit there as well.
PAPER_FIGDIR = REPO / "thesis" / "paper" / "figures"
OUTDIR.mkdir(parents=True, exist_ok=True)
PAPER_FIGDIR.mkdir(parents=True, exist_ok=True)

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}

FIRM_LABEL = {"IB": "Iberdrola", "GE": "Endesa", "GN": "Naturgy", "HC": "EDP-Spain"}
FIRM_ORDER = ["IB", "GE", "GN", "HC"]

CRIT_COLOR = "#d34a4a"
FLAT_COLOR = "#3a78b8"


def hour_class(h: int) -> str | None:
    if h in CRITICAL:
        return "critical"
    if h in FLAT:
        return "flat"
    return None


def collapse_by_class(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour_class"] = df["hour"].apply(hour_class)
    df = df[df["hour_class"].notna()]
    # Weight hour-level cells by the number of (date, unit, period) observations they aggregate.
    weighted = []
    for (firm, hc), g in df.groupby(["firm_class", "hour_class"]):
        n = g["n_obs"].sum()
        weighted.append({
            "firm_class": firm,
            "hour_class": hc,
            "mean_n_tranches": (g["mean_n_tranches"] * g["n_obs"]).sum() / n,
            "mech_strict_rate": (g["mech_strict_rate"] * g["n_obs"]).sum() / n,
            "mean_p_max":      (g["mean_p_max"]      * g["n_obs"]).sum() / n,
            "mean_p_min":      (g["mean_p_min"]      * g["n_obs"]).sum() / n,
        })
    return pd.DataFrame(weighted)


def grouped_bars(ax, summary: pd.DataFrame, column: str, ylabel: str, title: str,
                 scale: float = 1.0, annotate_cap_only: bool = False):
    x = np.arange(len(FIRM_ORDER))
    width = 0.38
    flat_vals = []
    crit_vals = []
    for firm in FIRM_ORDER:
        f = summary[(summary["firm_class"] == firm) & (summary["hour_class"] == "flat")]
        c = summary[(summary["firm_class"] == firm) & (summary["hour_class"] == "critical")]
        flat_vals.append(scale * (f[column].iloc[0] if len(f) else np.nan))
        crit_vals.append(scale * (c[column].iloc[0] if len(c) else np.nan))
    ax.bar(x - width/2, flat_vals, width, color=FLAT_COLOR, label="Flat hours")
    ax.bar(x + width/2, crit_vals, width, color=CRIT_COLOR, label="Critical hours")
    ax.set_xticks(x)
    ax.set_xticklabels([FIRM_LABEL[f] for f in FIRM_ORDER])
    ax.set_ylabel(ylabel)
    ax.set_title(title, fontsize=11)
    ax.grid(alpha=0.3, axis="y")
    ax.legend(fontsize=8, frameon=False)
    if annotate_cap_only:
        for xi, v in zip(x, crit_vals):
            if not np.isnan(v):
                ax.annotate(f"{v:.0f}", xy=(xi + width/2, v), ha="center", va="bottom", fontsize=8)


def main():
    raw = pd.read_csv(CSV)
    summary = collapse_by_class(raw)

    fig, axes = plt.subplots(2, 2, figsize=(11.5, 8))
    grouped_bars(axes[0, 0], summary, "mean_n_tranches",
                 "Mean bid steps per quarter",
                 "A. Number of bid steps")
    grouped_bars(axes[0, 1], summary, "mech_strict_rate",
                 "% of bids identical across the four quarters",
                 "B. Quarter-to-quarter bid repetition",
                 scale=100.0)
    grouped_bars(axes[1, 0], summary, "mean_p_max",
                 "Highest bid price (€/MWh)",
                 "C. Top of the bid ladder",
                 annotate_cap_only=True)
    grouped_bars(axes[1, 1], summary, "mean_p_min",
                 "Lowest bid price (€/MWh)",
                 "D. Bottom of the bid ladder")

    fig.suptitle("Per-firm bid behaviour, critical vs flat hours",
                 fontsize=13, y=0.995)

    explainer = (
        "A bid step is one price-quantity pair in a firm's supply offer for a given period; "
        "a firm with more steps offers a finer-grained supply curve. "
        "'Identical across quarters' = the firm submitted the exact same step "
        "(price and quantity) in all four quarters of the same clock-hour. "
        "Sample: combined-cycle gas (CCGT) units operated by the four largest "
        "Iberian groups, day-ahead market, October--December 2025 (post-reform window)."
    )
    fig.text(0.5, -0.02, explainer, ha="center", va="top",
             fontsize=8.5, wrap=True, style="italic")

    fig.tight_layout(rect=[0, 0.02, 1, 0.97])
    out = OUTDIR / "fig_per_firm_strategic_types"
    fig.savefig(f"{out}.png", dpi=160, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    # Mirror PDF into paper.tex include directory.
    fig.savefig(PAPER_FIGDIR / "fig_per_firm_strategic_types.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"wrote: {out}.png / .pdf  +  {PAPER_FIGDIR / 'fig_per_firm_strategic_types.pdf'}")


if __name__ == "__main__":
    main()
