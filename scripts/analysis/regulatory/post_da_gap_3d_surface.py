# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: 3D surface visualisation of post-DA gap (PHF_last - PDBC, GWh/day),
#        replacing the diurnal heatmap. Mirror of bidshape_3d_surface.py.
#
# OUT: figures/working/post_da_gap_3d_surface.pdf

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
CSV = REPO / "results/regressions/regulatory/pdbf_to_phf_diurnal/per_firm_tech_hour_regime.csv"
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
    rowkeys = []
    for tech in TECHS:
        for firm in FIRMS:
            sub = df[(df["tech"] == tech) & (df["firm"] == firm)]
            if len(sub) > 0:
                rowkeys.append((tech, firm))
    n_rows = len(rowkeys)
    hours = list(range(24))
    n_hours = len(hours)

    # Determine z range from all data
    vmin = df["gap_gwh"].min()
    vmax = df["gap_gwh"].max()
    abs_max = max(abs(vmin), abs(vmax))

    fig = plt.figure(figsize=(16, 11))
    fig.suptitle(r"Post-DA gap (PHF$_{\text{last}}$ - PDBC, GWh/day) per (tech, firm) hour-of-day, by regime — 3D surface", fontsize=12)

    cmap = cm.RdBu_r
    norm = matplotlib.colors.Normalize(vmin=-abs_max, vmax=abs_max)

    for idx, (r_lab, r_disp) in enumerate(REGIME_ORDER):
        ax = fig.add_subplot(2, 3, idx + 1, projection="3d")
        Z = np.full((n_rows, n_hours), 0.0)
        for i, (tech, firm) in enumerate(rowkeys):
            for h in hours:
                cell = df[(df["tech"] == tech) & (df["firm"] == firm) &
                          (df["regime"] == r_lab) & (df["clock_hour"] == h)]
                if not cell.empty:
                    Z[i, h] = cell["gap_gwh"].iloc[0]
        X, Y = np.meshgrid(np.array(hours), np.arange(n_rows))
        surf = ax.plot_surface(X, Y, Z, cmap=cmap, norm=norm, edgecolor="none",
                               rstride=1, cstride=1, alpha=0.95)
        ax.set_xlabel("Hour", fontsize=9)
        ax.set_xticks([0, 6, 12, 18, 23])
        ax.set_xticklabels(["0", "6", "12", "18", "23"], fontsize=8)
        ax.set_yticks(range(n_rows))
        ax.set_yticklabels([f"{t.replace('_',' ')[:5]}|{f}" for t, f in rowkeys], fontsize=7)
        ax.set_zlabel("GWh/day", fontsize=9)
        ax.set_zlim(-abs_max, abs_max)
        ax.set_title(r_disp, fontsize=11)
        ax.view_init(elev=26, azim=-55)
    ax_unused = fig.add_subplot(2, 3, 6)
    ax_unused.axis("off")
    fig.subplots_adjust(left=0.03, right=0.95, top=0.94, bottom=0.04, wspace=0.10, hspace=0.18)
    cbar_ax = fig.add_axes([0.96, 0.15, 0.012, 0.7])
    fig.colorbar(cm.ScalarMappable(norm=norm, cmap=cmap), cax=cbar_ax, label="Post-DA gap (GWh/day)")
    out = FIG_DIR / "post_da_gap_3d_surface.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=110)
    plt.close(fig)
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
