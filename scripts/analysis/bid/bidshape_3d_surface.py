# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: 3D surface visualisation of in-band MW share, replacing the diurnal
#        heatmap. x = hour-of-day (0-23), y = (tech, firm) index, z = regime-mean
#        in-band share. One subplot per regime, 5 panels in a row.
#
# Same data as bidshape_diurnal_heatmap.pdf, but as a 3D surface for readers
# who prefer height-over-grid to colour-over-grid.
#
# OUT: figures/working/bidshape_3d_surface.pdf

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
CSV = REPO / "results/regressions/bid/bidshape_diurnal/per_firm_tech_hour_regime.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REGIME_ORDER = [
    ("3sess", "3-sess"),
    ("ISP15win", "ISP15-win"),
    ("MTU15IDA_pre", "DA60/ID15 pre-blk"),
    ("MTU15IDA_post", "DA60/ID15 post-blk"),
    ("DA15_ID15", "DA15/ID15"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC", "REP"]


def main():
    df = pd.read_csv(CSV)

    # Build (tech, firm) row ordering — only keep cells that have at least one regime present
    rowkeys = []
    for tech in TECHS:
        for firm in FIRMS:
            sub = df[(df["tech"] == tech) & (df["firm"] == firm)]
            if len(sub) > 0:
                rowkeys.append((tech, firm))
    n_rows = len(rowkeys)

    # Build (regime, hour, (tech, firm)) value matrix
    hours = list(range(24))
    n_hours = len(hours)

    fig = plt.figure(figsize=(16, 11))
    fig.suptitle("In-band MW share (±50 EUR/MWh around DA MCP) per (tech, firm) hour-of-day, by regime — 3D surface", fontsize=12)

    cmap = cm.viridis
    norm = matplotlib.colors.Normalize(vmin=0, vmax=1.0)

    for idx, (r_lab, r_disp) in enumerate(REGIME_ORDER):
        ax = fig.add_subplot(2, 3, idx + 1, projection="3d")
        Z = np.full((n_rows, n_hours), np.nan)
        for i, (tech, firm) in enumerate(rowkeys):
            for h in hours:
                cell = df[(df["tech"] == tech) & (df["firm"] == firm) &
                          (df["regime"] == r_lab) & (df["clock_hour"] == h)]
                if not cell.empty:
                    Z[i, h] = cell["in_band_share"].iloc[0]
        Zp = np.where(np.isnan(Z), 0.0, Z)
        X, Y = np.meshgrid(np.array(hours), np.arange(n_rows))
        surf = ax.plot_surface(X, Y, Zp, cmap=cmap, norm=norm, edgecolor="none",
                               rstride=1, cstride=1, alpha=0.95)
        ax.set_xlabel("Hour", fontsize=9)
        ax.set_xticks([0, 6, 12, 18, 23])
        ax.set_xticklabels(["0", "6", "12", "18", "23"], fontsize=8)
        ax.set_yticks(range(n_rows))
        ax.set_yticklabels([f"{t.replace('_',' ')[:5]}|{f}" for t, f in rowkeys], fontsize=7)
        ax.set_zlabel("Share", fontsize=9)
        ax.set_zlim(0, 1)
        ax.set_title(r_disp, fontsize=11)
        ax.view_init(elev=26, azim=-55)
    # Hide unused 6th subplot
    ax_unused = fig.add_subplot(2, 3, 6)
    ax_unused.axis("off")
    fig.subplots_adjust(left=0.03, right=0.95, top=0.94, bottom=0.04, wspace=0.10, hspace=0.18)
    cbar_ax = fig.add_axes([0.96, 0.15, 0.012, 0.7])
    fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), cax=cbar_ax, label="In-band share")
    out = FIG_DIR / "bidshape_3d_surface.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=110)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
