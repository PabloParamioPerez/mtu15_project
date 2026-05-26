# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: 3D ridge plots of the distribution of scalar variables across regimes.
#        For each tech, one panel per variable. Each panel: 5 KDE curves stacked
#        along the y-axis (one per regime), each filled in 3D. x = scalar value,
#        y = regime index, z = density. Reveals how the WHOLE distribution
#        shifts across regimes, not just the mean — and is more compact than
#        the previous EVR / level-shift tables.
#
# Outcomes covered:
#   1. PC1 score (functional-SA fPCA, per-tech basis)
#   2. PC2 score
#   3. PC3 score
#   4. In-band MW share
#   5. Post-DA gap (CCGT only; Hydro/Hydro_pump are also produced)
#
# OUT: figures/working/distribution_3d_<outcome>_<tech>.pdf  (one PDF per tech,
#      with subplots for the different scalar variables)

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
FPCA_DIR = REPO / "results/regressions/bid/fpca"
BIDSHAPE_SA_PANEL = REPO / "data/derived/panels/bidshape_sa_daily.parquet"
POSTDA_SA_PANEL = REPO / "data/derived/panels/post_da_gap_sa_daily.parquet"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REGIME_RANGES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30")),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18")),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27")),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30")),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15")),
]


def _assign_regime(df):
    df = df.copy()
    df["d"] = pd.to_datetime(df["d"])
    df["regime"] = "other"
    for label, lo, hi in REGIME_RANGES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    return df[df["regime"] != "other"].copy()

REGIME_ORDER = [
    ("3sess",         "3-sess",          "#1f77b4"),
    ("ISP15win",      "ISP15-win",       "#ff7f0e"),
    ("MTU15IDA_pre",  "DA60/ID15 pre",   "#2ca02c"),
    ("MTU15IDA_post", "DA60/ID15 post",  "#d62728"),
    ("DA15_ID15",     "DA15/ID15",       "#9467bd"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]


def ridge_3d(ax, data_by_regime, x_range, title, xlabel, bw=None):
    """Plot a 3D ridge plot: 5 KDE curves stacked along the y-axis."""
    x = np.linspace(x_range[0], x_range[1], 250)
    polys = []
    colors = []
    z_max = 0
    for i, (r_lab, r_disp, col) in enumerate(REGIME_ORDER):
        vals = data_by_regime.get(r_lab, np.array([]))
        if len(vals) < 30:
            continue
        try:
            kde = gaussian_kde(vals, bw_method=bw if bw is not None else "scott")
            z = kde(x)
        except (np.linalg.LinAlgError, ValueError):
            continue
        z_max = max(z_max, z.max())
        # 3D ridge: build a polygon with vertices (x, y_floor, 0), then (x, y_floor, z), closing back
        verts = [(x[0], 0)] + [(xi, zi) for xi, zi in zip(x, z)] + [(x[-1], 0)]
        polys.append(verts)
        colors.append(col)
    poly = PolyCollection(polys, facecolors=colors, edgecolors="black", linewidths=0.6, alpha=0.7)
    # Place polygons at y = regime index, in the xz plane
    # PolyCollection in 3D needs zs= and zdir= args
    ax.add_collection3d(poly, zs=list(range(len(polys))), zdir="y")
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, z_max * 1.15)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels([REGIME_ORDER[i][1] for i in range(len(polys))], fontsize=8)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_zlabel("density", fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def plot_pc_scores_per_tech():
    """For each tech, 3D ridge plot of PC1/PC2/PC3 across regimes."""
    for tech in TECHS:
        # Pre-MTU15 not in this dataset (window starts 2024-06-14); 5 reform regimes.
        fp = FPCA_DIR / f"pc_scores_{tech}_sa.parquet"
        if not fp.exists():
            print(f"  {fp.name} missing for {tech}")
            continue
        df = pd.read_parquet(fp)
        df["regime"] = df["regime"].astype(str)

        fig = plt.figure(figsize=(16, 5.5))
        fig.suptitle(f"{tech.replace('_',' ')}: distribution of functional-SA PC scores across regimes (per-tech basis, all (unit, day, period) cells)", fontsize=11)

        for k_idx, pc in enumerate(["PC1", "PC2", "PC3"]):
            ax = fig.add_subplot(1, 3, k_idx + 1, projection="3d")
            # Compute x range from data
            vals_all = df[pc].dropna().values
            q01, q99 = np.quantile(vals_all, [0.005, 0.995])
            data_by_regime = {r_lab: df[df["regime"] == r_lab][pc].dropna().values
                              for r_lab, _, _ in REGIME_ORDER}
            ridge_3d(ax, data_by_regime, (q01, q99), title=pc, xlabel=f"{pc} score")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"distribution_3d_PCscores_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


def plot_bidshare_per_tech():
    """3D ridge of SA daily in-band share across regimes, per tech.
    Reads the per-(unit, date, hour) SA panel and uses in_band_share_sa."""
    df = _assign_regime(pd.read_parquet(BIDSHAPE_SA_PANEL))
    df = df[df["in_band_share_sa"].notna()].copy()
    for tech in TECHS:
        sub = df[df["tech"] == tech]
        if sub.empty:
            continue
        data_by_regime = {r_lab: sub[sub["regime"] == r_lab]["in_band_share_sa"].dropna().values
                          for r_lab, _, _ in REGIME_ORDER}
        fig = plt.figure(figsize=(7, 5))
        ax = fig.add_subplot(111, projection="3d")
        ridge_3d(ax, data_by_regime, (0, 1), title=f"{tech.replace('_',' ')}", xlabel="SA in-band share")
        plt.tight_layout()
        out = FIG_DIR / f"distribution_3d_bidshare_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


def plot_postdagap_per_tech():
    """3D ridge of SA daily post-DA gap across regimes, per tech.
    Reads the per-(firm, tech, date) SA panel and uses gap_gwh_sa."""
    df = _assign_regime(pd.read_parquet(POSTDA_SA_PANEL))
    df = df[df["gap_gwh_sa"].notna()].copy()
    for tech in TECHS:
        sub = df[df["tech"] == tech]
        if sub.empty:
            continue
        data_by_regime = {r_lab: sub[sub["regime"] == r_lab]["gap_gwh_sa"].dropna().values
                          for r_lab, _, _ in REGIME_ORDER}
        vals_all = sub["gap_gwh_sa"].dropna().values
        q01, q99 = np.quantile(vals_all, [0.01, 0.99])
        fig = plt.figure(figsize=(7, 5))
        ax = fig.add_subplot(111, projection="3d")
        ridge_3d(ax, data_by_regime, (q01, q99), title=f"{tech.replace('_',' ')} post-DA gap (SA)", xlabel="SA gap GWh/day")
        plt.tight_layout()
        out = FIG_DIR / f"distribution_3d_postdagap_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


def main():
    print("PC score distributions per tech...")
    plot_pc_scores_per_tech()
    print("\nIn-band share distributions per tech...")
    plot_bidshare_per_tech()
    print("\nPost-DA gap distributions per tech...")
    plot_postdagap_per_tech()


if __name__ == "__main__":
    main()
