# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 -- KEY figure: forest plot of per-tech
#        sigma_p DiD coefficients for ID15 and DA15 with 95% CIs.
#        Visualises the headline bid-shape widening pattern across techs
#        and reforms. CCGT-DA15 is shown twice: pooled (diluted) and
#        simple-offer-only (the cleaner causal estimate).
#
# OUT: figures/working/fig_sigma_p_forest.pdf

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "figures/working/fig_sigma_p_forest.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

# Numbers from results/regressions/bid/mtu15_critical_flat/specA_per_curve_did.csv
# and spec_a_wind.csv and ccgt_offer_type_split.csv (DA15 CCGT simple-only).
ID15 = pd.DataFrame({
    "tech": ["CCGT", "Hydro", "Hydro pump", "Wind"],
    "did":  [2.45,   1.17,    0.25,         0.32],
    "se":   [0.58,   0.44,    0.28,         0.10],
})

DA15 = pd.DataFrame({
    "tech": ["CCGT", "CCGT (simple-offer only)", "Hydro", "Hydro pump", "Wind"],
    "did":  [1.30,   3.53,                       1.69,    -0.63,        0.17],
    "se":   [0.75,   0.98,                       0.29,    0.36,         0.04],
    "is_simple_only": [False, True, False, False, False],
})


def panel(ax, df, title):
    y = np.arange(len(df))
    colors = ["#1f77b4"] * len(df)
    markers = ["o"] * len(df)
    # mark simple-only with open square
    if "is_simple_only" in df.columns:
        for i, simple in enumerate(df["is_simple_only"]):
            if simple:
                colors[i] = "#d62728"
                markers[i] = "s"
    ax.axvline(0, color="black", lw=0.5, zorder=1)
    for yi, (didv, sev, c, m) in enumerate(zip(df["did"], df["se"], colors, markers)):
        ci_lo = didv - 1.96 * sev
        ci_hi = didv + 1.96 * sev
        ax.plot([ci_lo, ci_hi], [yi, yi], color=c, lw=1.2, zorder=2)
        ax.plot(didv, yi, marker=m, color=c, markersize=6,
                markeredgewidth=0.8, markeredgecolor="black", zorder=3)
        ax.text(ci_hi + 0.15, yi, f"{didv:+.2f}", va="center", fontsize=8.5,
                color="black")
    ax.set_yticks(y)
    ax.set_yticklabels(df["tech"], fontsize=9)
    ax.invert_yaxis()
    ax.set_title(title, fontsize=10)
    ax.set_xlabel(r"$\sigma_p$ DiD $\theta$ (EUR/MWh, 95\% CI)", fontsize=9)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(axis="x", labelsize=8)
    ax.grid(axis="x", lw=0.3, color="black", alpha=0.15)


def main():
    fig, axes = plt.subplots(1, 2, figsize=(7.5, 2.6),
                              gridspec_kw={"width_ratios": [1, 1.05]})
    panel(axes[0], ID15, "ID15 (15-min intraday auctions, 2025-03-19)")
    panel(axes[1], DA15, "DA15 (15-min day-ahead, 2025-10-01)")

    # Set consistent x-axis for visual comparison
    xmin = min(min(ID15["did"] - 1.96 * ID15["se"]),
               min(DA15["did"] - 1.96 * DA15["se"])) - 0.5
    xmax = max(max(ID15["did"] + 1.96 * ID15["se"]),
               max(DA15["did"] + 1.96 * DA15["se"])) + 1.0
    for ax in axes:
        ax.set_xlim(xmin, xmax)

    fig.suptitle(
        r"Per-tech $\sigma_p$ DiD: bid-shape widens in critical hours across techs and reforms",
        fontsize=10.5, y=1.02,
    )
    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
