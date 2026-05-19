# STATUS: ALIVE
# LAST-AUDIT: 2026-05-20
# CLAIM: Per-tech landscape 3D ridges of the per-unit near-MCP fPCA scalars
#        (Level shift and Tilt magnitude), substituting the per-(unit,
#        hour-class) reform tables (was Tables 18, 19, 20). Each per-tech
#        figure stacks 6 subplots (2 metrics x 3 hour-classes); each subplot
#        is a 3D ridge with the 3 reforms (ISP15, MTU15-IDA, MTU15-DA)
#        stacked along the y-axis. The distribution-across-units is the
#        substantive object (each unit has its OWN basis -- the visualization
#        focuses on the scalar projection summary).
#
# OUT: figures/working/fpca_per_unit_3d_<tech>.pdf  (one PDF per tech)

from pathlib import Path
import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.collections import PolyCollection
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
CSV = REPO / "results/regressions/bid/fpca/per_unit/coeffs_pairwise_nearmcp_H50_sa_per_unit.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REFORMS = [
    ("ISP15",     "ISP15 (Dec 2024)",       "#1f77b4"),
    ("MTU15-IDA", "MTU15-IDA (Mar 2025)",   "#ff7f0e"),
    ("MTU15-DA",  "MTU15-DA (Oct 2025)",    "#2ca02c"),
]
HOUR_CLASSES = ["Critical", "Flat", "Midday"]
METRICS = [
    ("level_shift", "Level shift (EUR/MWh)"),
    ("tilt_std",    "Tilt magnitude (EUR/MWh)"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]


def ridge_3d(ax, data_by_reform, x_range, title, xlabel, bw=None):
    """3D ridge: one ridge per reform stacked along y-axis."""
    x = np.linspace(x_range[0], x_range[1], 250)
    polys = []
    colors = []
    z_max = 0
    n_ridges = 0
    for i, (r_lab, r_disp, col) in enumerate(REFORMS):
        vals = data_by_reform.get(r_lab, np.array([]))
        vals = np.asarray(vals)
        vals = vals[np.isfinite(vals)]
        if len(vals) < 5 or vals.std() < 1e-6:
            # Too few obs or no spread; skip
            continue
        try:
            # Use Scott's bw with a floor relative to the x-range to avoid spikes
            x_span = x_range[1] - x_range[0]
            scott_bw = 1.06 * len(vals) ** (-0.2)
            bw_used = bw if bw is not None else max(0.25, scott_bw)
            kde = gaussian_kde(vals, bw_method=bw_used)
            z = kde(x)
            # Hard cap on z to avoid pathological spikes
            z = np.clip(z, 0, 5.0 / x_span)
        except (np.linalg.LinAlgError, ValueError):
            continue
        z_max = max(z_max, z.max())
        verts = [(x[0], 0)] + [(xi, zi) for xi, zi in zip(x, z)] + [(x[-1], 0)]
        polys.append((verts, col, r_disp, vals))
        colors.append(col)
        n_ridges += 1
    if not polys:
        ax.set_title(title + " (insufficient data)", fontsize=9)
        ax.set_axis_off()
        return
    poly_coll = PolyCollection([p[0] for p in polys], facecolors=[p[1] for p in polys],
                               edgecolors="black", linewidths=0.6, alpha=0.7)
    ax.add_collection3d(poly_coll, zs=list(range(len(polys))), zdir="y")
    # Add a rug of actual unit values along the bottom of each ridge
    for j, (_, col, _, vals) in enumerate(polys):
        ax.scatter(vals, np.full_like(vals, j), np.zeros_like(vals),
                   s=10, c=col, edgecolors="black", linewidths=0.3, zorder=10)
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, z_max * 1.2 if z_max > 0 else 1)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels([p[2] for p in polys], fontsize=7)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_zlabel("density", fontsize=8)
    ax.set_title(title, fontsize=9)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def main():
    df = pd.read_csv(CSV)
    print(f"Loaded {len(df):,} per-(unit, reform, hour_class) rows")

    for tech in TECHS:
        sub_t = df[df["tech"] == tech]
        if sub_t.empty:
            continue
        fig = plt.figure(figsize=(16, 11))
        fig.suptitle(f"{tech.replace('_',' ')}: per-unit near-MCP fPCA Level shift and Tilt magnitude, by reform and hour-class", fontsize=11)
        for col_idx, (metric, mlabel) in enumerate(METRICS):
            # Determine x range for this metric across all hour classes for this tech
            vals_all = sub_t[metric].dropna().values
            if len(vals_all) == 0:
                q_lo, q_hi = -1, 1
            else:
                q_lo, q_hi = np.quantile(vals_all, [0.02, 0.98])
                pad = 0.15 * (q_hi - q_lo + 1)
                q_lo -= pad; q_hi += pad
                if metric == "tilt_std":
                    q_lo = max(0, q_lo)
            for row_idx, hc in enumerate(HOUR_CLASSES):
                ax = fig.add_subplot(3, 2, row_idx * 2 + col_idx + 1, projection="3d")
                sub = sub_t[sub_t["hour_class"] == hc]
                data_by_reform = {r[0]: sub[sub["reform"] == r[0]][metric].dropna().values
                                  for r in REFORMS}
                ridge_3d(ax, data_by_reform, (q_lo, q_hi),
                         title=f"{hc} hours — {mlabel.split(' (')[0]}",
                         xlabel=mlabel)
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"fpca_per_unit_3d_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


if __name__ == "__main__":
    main()
