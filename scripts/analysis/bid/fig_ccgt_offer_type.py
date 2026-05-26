# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 -- KEY figure addressing the offer-type
#        contamination concern for the CCGT bid-shape DiD.
#
# CLAIM: Two panels.
#   LEFT  -- offer-type composition of CCGT in-band sell offers,
#            pre vs post MTU15-DA, critical vs flat (stacked bars).
#            Shows: simple-offer share drops 29% -> 22%, block-order
#            share rises.
#   RIGHT -- Spec A sigma_p DiD by offer type, with 95% CI.
#            Shows: sigma_p widening is concentrated in simple offers
#            (+3.53***, t=3.60); pooled +1.25 dilutes it.
#
# OUT: figures/working/fig_ccgt_offer_type.pdf

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "figures/working/fig_ccgt_offer_type.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

# Composition (%) from the ccgt_offer_type_split_did.py console output:
# (these are stable enough to hard-code; rerun the script to refresh).
COMP = pd.DataFrame({
    "arm_hc": ["Pre\nCritical", "Pre\nFlat", "Post\nCritical", "Post\nFlat"],
    "simple": [29.4, 27.8, 21.9, 23.1],
    "MIC":    [40.4, 38.6, 42.8, 33.4],
    "block":  [30.2, 33.5, 35.3, 43.5],
})

# Spec A sigma_p DiD by offer type, from ccgt_offer_type_split.csv
SIGMA = pd.DataFrame({
    "offer_type": ["simple", "MIC", "block", "pooled"],
    "did":        [3.529, -0.897, 0.291, 1.248],
    "se":         [0.980,  1.208, 0.224, 0.566],
})


def main():
    fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.9))

    # === LEFT: composition stacked bars ===
    ax = axes[0]
    x = np.arange(len(COMP))
    bottom = np.zeros(len(COMP))
    colors = {"simple": "#1f77b4", "MIC": "#aaaaaa", "block": "#d62728"}
    for ot in ["simple", "MIC", "block"]:
        vals = COMP[ot].values
        ax.bar(x, vals, bottom=bottom, color=colors[ot], label=ot, width=0.62)
        for xi, v, b in zip(x, vals, bottom):
            ax.text(xi, b + v / 2, f"{v:.0f}", ha="center", va="center",
                    fontsize=7.5, color="white" if ot != "MIC" else "black")
        bottom += vals
    ax.set_xticks(x)
    ax.set_xticklabels(COMP["arm_hc"], fontsize=8.5)
    ax.set_ylabel("Share of CCGT in-band sell curves (\\%)", fontsize=9)
    ax.set_ylim(0, 105)
    ax.set_title("Offer-type composition shifts toward block orders",
                 fontsize=9.5)
    ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.18), ncol=3,
              frameon=False, fontsize=8.5)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="y", labelsize=8)

    # === RIGHT: sigma_p DiD by offer type ===
    ax = axes[1]
    y = np.arange(len(SIGMA))
    colors_did = {"simple": "#1f77b4", "MIC": "#aaaaaa",
                  "block": "#d62728", "pooled": "#000000"}
    ax.barh(y, SIGMA["did"].values,
            xerr=1.96 * SIGMA["se"].values,
            color=[colors_did[ot] for ot in SIGMA["offer_type"]],
            error_kw={"ecolor": "black", "elinewidth": 0.8, "capsize": 2.5})
    ax.axvline(0, color="black", lw=0.5)
    ax.set_yticks(y)
    ax.set_yticklabels(SIGMA["offer_type"], fontsize=9)
    ax.invert_yaxis()
    ax.set_xlabel(r"$\sigma_p$ DiD $\theta$ (EUR/MWh, 95\% CI)", fontsize=9)
    ax.set_title(r"$\sigma_p$ widening is concentrated in simple offers",
                 fontsize=9.5)
    for yi, v, s in zip(y, SIGMA["did"].values, SIGMA["se"].values):
        ax.text(v + (0.25 if v >= 0 else -0.25), yi,
                f"{v:+.2f}", va="center", fontsize=8,
                ha="left" if v >= 0 else "right")
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", labelsize=8)

    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
