# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: Per-(firm, tech) 3D ridge plots of within-hour quarter-dissimilarity
#        D_w log(1+x), substituting the dense per-firm/tech overlay
#        log-histograms (was Figs 12-13 in the descriptive companion).
#
#        DA outputs use the post-MTU15-DA window (2025-10-01 to 2026-01-01,
#        from quarter_dissimilarity_ot.py); each per-tech landscape page has
#        4 panels (CCGT firms IB, GE, GN, HC) with 3 hour-class ridges per
#        panel (critical, flat, midday) stacked on the y-axis.
#
#        IDA outputs use the 3 MTU15-IDA regimes (pre-blk, post-blk, DA15/ID15
#        from quarter_dissimilarity_ida.py); same per-(tech, firm) landscape
#        layout but the y-axis stacks 3 regime ridges per panel rather than
#        hour-classes (the IDA pre-vs-post-vs-DA15 story is the substantive
#        comparison there).
#
#        Aggregator: mean of the 6 within-hour pairwise dissimilarities
#        (d_mean_w), per user feedback 2026-05-21.
#
# OUT: figures/working/dw_3d_per_firm_<tech>_DA.pdf
#      figures/working/dw_3d_per_firm_<tech>_IDA.pdf

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
DA_CSV = REPO / "results/regressions/bid/quarter_dissimilarity/quarter_dissimilarity_cells_2025Q4.csv"
IDA_DIR = REPO / "results/regressions/bid/quarter_dissimilarity_ida"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]

HOUR_CLASSES = [
    ("critical", "Critical",   "#d62728"),
    ("flat",     "Flat",       "#1f77b4"),
    ("midday",   "Midday",     "#2ca02c"),
]
IDA_REGIMES = [
    ("pre_blackout",  "DA60/ID15 pre",  "#2ca02c"),
    ("post_blackout", "DA60/ID15 post", "#d62728"),
    ("da15_id15",     "DA15/ID15",      "#9467bd"),
]


def ridge_3d(ax, data_by_key, x_range, key_labels, title, xlabel):
    """3D ridge: one ridge per key, stacked along y-axis."""
    x = np.linspace(x_range[0], x_range[1], 250)
    polys = []
    colors = []
    used_labels = []
    z_max = 0
    for i, (key, label, col) in enumerate(key_labels):
        vals = data_by_key.get(key, np.array([]))
        vals = np.asarray(vals)
        vals = vals[np.isfinite(vals)]
        if len(vals) < 30:
            continue
        try:
            kde = gaussian_kde(vals, bw_method=0.12)
            z = kde(x)
        except (np.linalg.LinAlgError, ValueError):
            continue
        z_max = max(z_max, z.max())
        verts = [(x[0], 0)] + [(xi, zi) for xi, zi in zip(x, z)] + [(x[-1], 0)]
        polys.append(verts)
        colors.append(col)
        used_labels.append(label)
    if not polys:
        ax.set_title(title + " (no data)", fontsize=9)
        ax.set_axis_off()
        return
    poly = PolyCollection(polys, facecolors=colors, edgecolors="black",
                          linewidths=0.6, alpha=0.7)
    ax.add_collection3d(poly, zs=list(range(len(polys))), zdir="y")
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, z_max * 1.15 if z_max > 0 else 1)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels(used_labels, fontsize=8)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_zlabel("density", fontsize=8)
    ax.set_title(title, fontsize=10)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def plot_da_per_tech():
    """DA post-MTU15-DA window: per-(firm, tech) panels, hour-class ridges."""
    df = pd.read_csv(DA_CSV)
    df["log_dw"] = np.log1p(df["d_mean_w"])
    # Drop "other" hour-class
    df = df[df["hour_class"].isin([k for k, _, _ in HOUR_CLASSES])].copy()

    for tech in TECHS:
        sub_t = df[df["tech_group"] == tech]
        if sub_t.empty:
            continue
        vals_all = sub_t["log_dw"].dropna().values
        if len(vals_all) == 0:
            continue
        q_hi = np.quantile(vals_all, 0.99)
        x_range = (0, q_hi * 1.05)

        fig = plt.figure(figsize=(16, 9.2))
        fig.suptitle(
            f"{tech.replace('_',' ')} --- within-hour quarter-dissimilarity "
            r"$\log(1+\bar D_w)$, DA Oct--Dec 2025, by firm and hour-class",
            fontsize=11)
        for idx, firm in enumerate(FIRMS):
            ax = fig.add_subplot(2, 2, idx + 1, projection="3d")
            sub = sub_t[sub_t["firm"] == firm]
            data_by_key = {hc: sub[sub["hour_class"] == hc]["log_dw"].dropna().values
                           for hc, _, _ in HOUR_CLASSES}
            ridge_3d(ax, data_by_key, x_range, HOUR_CLASSES,
                     title=firm, xlabel=r"$\log(1+\bar D_w)$ (EUR)")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"dw_3d_per_firm_{tech}_DA.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


def plot_ida_per_tech():
    """IDA across 3 MTU15-IDA regimes: per-(firm, tech) panels, regime ridges."""
    pieces = []
    for r_lab, _, _ in IDA_REGIMES:
        path = IDA_DIR / f"cells_{r_lab}.csv"
        if not path.exists():
            print(f"  WARN: {path} missing, skipping")
            continue
        d = pd.read_csv(path)
        d["regime"] = r_lab
        pieces.append(d)
    if not pieces:
        return
    df = pd.concat(pieces, ignore_index=True)
    df["log_dw"] = np.log1p(df["d_mean_w"])
    # Restrict to critical hours (the substantive comparison; pre-blk window
    # has very limited flat-hour data otherwise)
    df = df[df["hour_class"] == "critical"].copy()

    for tech in TECHS:
        sub_t = df[df["tech_group"] == tech]
        if sub_t.empty:
            continue
        vals_all = sub_t["log_dw"].dropna().values
        if len(vals_all) == 0:
            continue
        q_hi = np.quantile(vals_all, 0.99)
        x_range = (0, q_hi * 1.05)

        fig = plt.figure(figsize=(16, 9.2))
        fig.suptitle(
            f"{tech.replace('_',' ')} --- IDA within-hour quarter-dissimilarity "
            r"$\log(1+\bar D_w)$, critical hours, by firm and IDA-MTU15 regime",
            fontsize=11)
        for idx, firm in enumerate(FIRMS):
            ax = fig.add_subplot(2, 2, idx + 1, projection="3d")
            sub = sub_t[sub_t["firm"] == firm]
            data_by_key = {r: sub[sub["regime"] == r]["log_dw"].dropna().values
                           for r, _, _ in IDA_REGIMES}
            ridge_3d(ax, data_by_key, x_range, IDA_REGIMES,
                     title=firm, xlabel=r"$\log(1+\bar D_w)$ (EUR)")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"dw_3d_per_firm_{tech}_IDA.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


def main():
    print("DA per-(firm, tech) D_w ridges (hour-class y-axis)...")
    plot_da_per_tech()
    print("\nIDA per-(firm, tech) D_w ridges (regime y-axis, critical hours)...")
    plot_ida_per_tech()


if __name__ == "__main__":
    main()
