# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: thesis/provisional/advisor_memo.tex sec 4 Spec A cleared-MW DiD figure.
#        Forest plot of per-tech critical-flat DiD on day-ahead cleared MW
#        under DA15, two specs side by side: baseline (date-FE only) and
#        Fourier-SA (4 annual harmonics x crit + 6 DOW x crit interactions,
#        absorbing the crit-flat seasonal differential). Pump-storage is the
#        only tech whose effect survives the seasonal adjustment.
#
# INPUTS:
#   results/regressions/bid/mtu15_critical_flat/fourier_sa_cleared_mw.csv
# OUTPUT:
#   figures/thesis/fig_cleared_mw_forest.pdf

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
SRC = REPO / "results/regressions/bid/mtu15_critical_flat/fourier_sa_cleared_mw.csv"
OUT = REPO / "figures/thesis/fig_cleared_mw_forest.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

# Reading order top-to-bottom; group dispatchables first
TECH_ORDER = [
    "Pump-storage",
    "Combined-cycle gas",
    "Hydro (run)",
    "Hydro (RE)",
    "Coal+thermal",
    "Biomass",
    "Nuclear",
    "Wind",
    "Solar PV",
    "Solar thermal",
]
RENAME = {
    "Hydro_pump":         "Pump-storage",
    "CCGT":               "Combined-cycle gas",
    "Hydro_run":          "Hydro (run)",
    "Hydro_RE":           "Hydro (RE)",
    "Coal_other_thermal": "Coal+thermal",
    "Biomass_RE":         "Biomass",
    "Nuclear":            "Nuclear",
    "Wind":               "Wind",
    "Solar_PV":           "Solar PV",
    "Solar_thermal":      "Solar thermal",
}


def main():
    df = pd.read_csv(SRC)
    df["tech"] = df["tech"].map(RENAME)
    df = df[df["tech"].isin(TECH_ORDER)]
    df["ci_lo"] = df["DiD"] - 1.96 * df["se"]
    df["ci_hi"] = df["DiD"] + 1.96 * df["se"]
    df["sig"] = (df["t"].abs() > 1.96)

    fig, axes = plt.subplots(1, 2, figsize=(9.6, 4.6), sharey=True)
    spec_label = {
        "baseline":   "Baseline DiD (date FE)",
        "fourier_SA": "Fourier-SA DiD (4 harmonics x crit + 6 DOW x crit)",
    }
    for ax, spec in zip(axes, ["baseline", "fourier_SA"]):
        sub = df[df["spec"] == spec].set_index("tech").loc[TECH_ORDER]
        y = np.arange(len(sub))[::-1]
        # CI bars
        for yi, (_, r) in zip(y, sub.iterrows()):
            color = "#1f4e79" if r["sig"] else "0.55"
            lw = 2.0 if r["sig"] else 1.2
            ax.plot([r["ci_lo"], r["ci_hi"]], [yi, yi], color=color, lw=lw,
                    solid_capstyle="round")
            ax.plot([r["DiD"]], [yi], "o", color=color, markersize=6,
                    markeredgecolor="white", markeredgewidth=0.6, zorder=3)
        ax.axvline(0, color="black", lw=0.7, alpha=0.6)
        ax.set_yticks(y)
        ax.set_yticklabels(sub.index)
        ax.set_xlabel("DiD on cleared MW (critical $-$ flat)")
        ax.set_title(spec_label[spec], fontsize=10.5, loc="left")
        ax.grid(axis="x", alpha=0.25, lw=0.5)
        # Vertical span around zero (visual "null band")
        ax.axvspan(-50, 50, color="0.9", alpha=0.5, zorder=0)
        for spine in ("top", "right"):
            ax.spines[spine].set_visible(False)

    fig.suptitle("Per-tech critical-flat DiD on day-ahead cleared MW, DA15",
                 fontsize=12, y=1.00, x=0.02, ha="left")
    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight")
    print(f"Wrote {OUT}")


if __name__ == "__main__":
    main()
