# STATUS: ALIVE
# LAST-AUDIT: 2026-05-20
# CLAIM: Hour-conditional 3D ridge plots of the per-cell kernel-weighted
#        within-hour quarter-dissimilarity (D_w) per tech. Same per-tech
#        landscape format as bidshape_3d_hourly.py: 4 panels per tech
#        (representative hours 03, 07, 13, 19), each with regime ridges
#        stacked along the y-axis. Reads the per-(unit, date, hour)
#        D_w cells computed by quarter_dissimilarity_ida.py for the three
#        regimes with MTU15-IDA data (pre-blk, post-blk, DA15/ID15).
#
#        Pre-MTU15-IDA regimes (3-sess, ISP15-win) have 60-min IDA periods
#        and no within-hour quarter structure, so D_w is undefined for
#        them.
#
# OUT: figures/working/dw_3d_hourly_<tech>.pdf  (one PDF per tech)

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
DW_DIR = REPO / "results/regressions/bid/quarter_dissimilarity_ida"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# Match the 5-regime palette used elsewhere; only the 3 IDA-MTU15 regimes have data.
REGIMES = [
    ("pre_blackout",  "DA60/ID15 pre",  "#2ca02c"),
    ("post_blackout", "DA60/ID15 post", "#d62728"),
    ("da15_id15",     "DA15/ID15",      "#9467bd"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
HOURS_REPRESENTATIVE = [
    (3,  "Hour 03  (Flat)"),
    (7,  "Hour 07  (Morning ramp, Critical)"),
    (13, "Hour 13  (Midday)"),
    (19, "Hour 19  (Evening peak, Critical)"),
]


def load_panel():
    pieces = []
    for r_lab, _, _ in REGIMES:
        path = DW_DIR / f"cells_{r_lab}.csv"
        if not path.exists():
            print(f"  WARN: {path} missing, skipping")
            continue
        df = pd.read_csv(path)
        df["regime"] = r_lab
        pieces.append(df)
    out = pd.concat(pieces, ignore_index=True)
    return out


def ridge_3d(ax, data_by_regime, x_range, title, xlabel):
    x = np.linspace(x_range[0], x_range[1], 250)
    polys = []
    colors = []
    z_max = 0
    for i, (r_lab, r_disp, col) in enumerate(REGIMES):
        vals = data_by_regime.get(r_lab, np.array([]))
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
    if not polys:
        ax.set_title(title + " (no data)", fontsize=9)
        ax.set_axis_off()
        return
    poly = PolyCollection(polys, facecolors=colors, edgecolors="black", linewidths=0.6, alpha=0.7)
    ax.add_collection3d(poly, zs=list(range(len(polys))), zdir="y")
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, z_max * 1.15 if z_max > 0 else 1)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels([REGIMES[i][1] for i in range(len(polys))], fontsize=8)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_zlabel("density", fontsize=9)
    ax.set_title(title, fontsize=10)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def main():
    print("Loading per-cell D_w from IDA quarter-dissimilarity outputs...")
    df = load_panel()
    print(f"  {len(df):,} (unit, day, hour) cells across {df['regime'].nunique()} regimes")

    # log1p transform so the spike at 0 doesn't dominate; keep zeros visible at left
    df["log_dw"] = np.log1p(df["d_max_w"])

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
        fig.suptitle(f"{tech.replace('_',' ')}: within-hour quarter-dissimilarity $\\log(1+D_w)$ distribution across IDA-MTU15 regimes — sliced by hour-of-day", fontsize=11)
        for idx, (h, h_label) in enumerate(HOURS_REPRESENTATIVE):
            ax = fig.add_subplot(2, 2, idx + 1, projection="3d")
            sub = sub_t[sub_t["hour"] == h]
            data_by_regime = {r[0]: sub[sub["regime"] == r[0]]["log_dw"].dropna().values
                              for r in REGIMES}
            ridge_3d(ax, data_by_regime, x_range,
                     title=h_label, xlabel=r"$\log(1+D_w)$ (EUR)")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"dw_3d_hourly_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


if __name__ == "__main__":
    main()
