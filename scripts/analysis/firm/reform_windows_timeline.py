# STATUS: ALIVE
# LAST-AUDIT: 2026-05-16
# FEEDS: provisional.tex §14 (Reform-window identification)
# CLAIM: Visual timeline showing the DA15 and IDA15 windows, the
#        regulatory event dates, and the reforzada zone. Companion to
#        the DDD identification section.

from __future__ import annotations

from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.patches as mpatches
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
FIGDIR = REPO / "figures" / "working"
FIGDIR.mkdir(parents=True, exist_ok=True)

ISP15            = pd.Timestamp("2024-12-09")
MTU15_IDA        = pd.Timestamp("2025-03-19")
BLACKOUT         = pd.Timestamp("2025-04-28")
MTU15_DA         = pd.Timestamp("2025-10-01")
SAMPLE_END       = pd.Timestamp("2026-02-13")

# DDD windows
IDA_PRE_START    = ISP15
IDA_PRE_END      = MTU15_IDA - pd.Timedelta(days=1)
IDA_POST_START   = MTU15_IDA
IDA_POST_END     = BLACKOUT - pd.Timedelta(days=1)

DA_PRE_START     = BLACKOUT
DA_PRE_END       = MTU15_DA - pd.Timedelta(days=1)
DA_POST_START    = MTU15_DA
DA_POST_END      = SAMPLE_END

# 2024 placebo windows (calendar-matched)
PLAC_IDA_PRE_S   = pd.Timestamp("2023-12-09")
PLAC_IDA_PRE_E   = pd.Timestamp("2024-03-18")
PLAC_IDA_POST_S  = pd.Timestamp("2024-03-19")
PLAC_IDA_POST_E  = pd.Timestamp("2024-04-27")

PLAC_DA_PRE_S    = pd.Timestamp("2024-04-28")
PLAC_DA_PRE_E    = pd.Timestamp("2024-09-30")
PLAC_DA_POST_S   = pd.Timestamp("2024-10-01")
PLAC_DA_POST_E   = pd.Timestamp("2025-02-13")


def main():
    fig, ax = plt.subplots(figsize=(13, 5.3))

    # Reforzada shading spans blackout → sample end
    ax.axvspan(BLACKOUT, SAMPLE_END, ymin=0.0, ymax=1.0, color="0.85",
                alpha=0.5, zorder=0, label="_nolegend_")
    ax.text(BLACKOUT + (SAMPLE_END - BLACKOUT) / 2, 5.7,
             "reforzada zone", ha="center", va="center", fontsize=10,
             color="0.35", fontweight="bold")

    # Treatment-year windows (2025)
    ax.barh(4, (IDA_PRE_END  - IDA_PRE_START).days,  left=IDA_PRE_START,
             color="tab:blue",   alpha=0.55, height=0.55, edgecolor="black",
             linewidth=0.5, label="IDA15 pre (Dec 9, 2024 → Mar 18, 2025)")
    ax.barh(4, (IDA_POST_END - IDA_POST_START).days, left=IDA_POST_START,
             color="tab:red",    alpha=0.55, height=0.55, edgecolor="black",
             linewidth=0.5, label="IDA15 post (Mar 19 → Apr 27, 2025)")
    ax.barh(3, (DA_PRE_END   - DA_PRE_START).days,   left=DA_PRE_START,
             color="tab:blue",   alpha=0.85, height=0.55, edgecolor="black",
             linewidth=0.5, label="DA15 pre (Apr 28 → Sep 30, 2025)")
    ax.barh(3, (DA_POST_END  - DA_POST_START).days,  left=DA_POST_START,
             color="tab:red",    alpha=0.85, height=0.55, edgecolor="black",
             linewidth=0.5, label="DA15 post (Oct 1, 2025 → Feb 13, 2026)")

    # 2024 placebo windows
    ax.barh(2, (PLAC_IDA_PRE_E  - PLAC_IDA_PRE_S).days,  left=PLAC_IDA_PRE_S,
             color="tab:blue",   alpha=0.25, height=0.55, edgecolor="black",
             linewidth=0.3, linestyle="--")
    ax.barh(2, (PLAC_IDA_POST_E - PLAC_IDA_POST_S).days, left=PLAC_IDA_POST_S,
             color="tab:red",    alpha=0.25, height=0.55, edgecolor="black",
             linewidth=0.3, linestyle="--")
    ax.barh(1, (PLAC_DA_PRE_E  - PLAC_DA_PRE_S).days,   left=PLAC_DA_PRE_S,
             color="tab:blue",   alpha=0.40, height=0.55, edgecolor="black",
             linewidth=0.3, linestyle="--")
    ax.barh(1, (PLAC_DA_POST_E - PLAC_DA_POST_S).days,  left=PLAC_DA_POST_S,
             color="tab:red",    alpha=0.40, height=0.55, edgecolor="black",
             linewidth=0.3, linestyle="--")

    # Reform-date markers (top to bottom)
    for d, lbl, color in [(ISP15,     "ISP15",     "gray"),
                          (MTU15_IDA, "MTU15-IDA", "tab:purple"),
                          (BLACKOUT,  "Blackout",  "black"),
                          (MTU15_DA,  "MTU15-DA",  "tab:red")]:
        ax.axvline(d, color=color, lw=1.2, ls=":", alpha=0.85, zorder=2)
        ax.text(d, 5.1, lbl, rotation=90, va="bottom", ha="right",
                 fontsize=8, color=color, fontweight="bold")

    ax.set_yticks([1, 2, 3, 4])
    ax.set_yticklabels(["DA15 placebo (2024)", "IDA15 placebo (2024)",
                         "DA15 treatment (2025)", "IDA15 treatment (2025)"],
                        fontsize=9)
    ax.set_ylim(0.4, 5.5)
    ax.set_xlim(pd.Timestamp("2023-11-01"), pd.Timestamp("2026-04-01"))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.setp(ax.get_xticklabels(), rotation=45, ha="right")
    ax.grid(True, alpha=0.3, axis="x")
    ax.set_title("Reform-window identification design (DA15 + IDA15) with 2024 placebo")
    ax.legend(loc="upper left", fontsize=7.5, frameon=False, ncol=2)

    plt.tight_layout()
    out = FIGDIR / "fig_reform_windows_timeline"
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf")


if __name__ == "__main__":
    main()
