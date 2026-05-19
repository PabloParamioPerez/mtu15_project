# STATUS: ALIVE
# LAST-AUDIT: 2026-05-19
# CLAIM: Hour-conditional 3D ridge plots of the SA daily in-band MW share —
#        for each tech, for a small set of representative hours of day, plot
#        the cross-regime distribution of the SA daily in-band MW share at
#        THAT hour. Reads the SA panel built by bidshape_sa_daily_panel.py
#        (per-(unit, hour) FWL on logit(share) with Fourier(K=4)+DOW stripped).
#
#        Hours chosen: 03 (flat), 07 (morning ramp), 13 (midday), 19 (evening
#        peak) — one from each hour-class plus a second critical-hour.
#
# OUT: figures/working/bidshape_3d_hourly_<tech>.pdf  (one PDF per tech,
#      4 panels per PDF, one per representative hour)

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
SA_PANEL = REPO / "data/derived/panels/bidshape_sa_daily.parquet"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess",          "#1f77b4"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win",       "#ff7f0e"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre",   "#2ca02c"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post",  "#d62728"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15",       "#9467bd"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
HOURS_REPRESENTATIVE = [
    (3,  "Hour 03  (Flat)"),
    (7,  "Hour 07  (Morning ramp, Critical)"),
    (13, "Hour 13  (Midday)"),
    (19, "Hour 19  (Evening peak, Critical)"),
]


def load_panel():
    df = pd.read_parquet(SA_PANEL)
    df["d"] = pd.to_datetime(df["d"])
    df["regime"] = "other"
    for label, lo, hi, _, _ in REGIME_DATES:
        m = (df["d"] >= lo) & (df["d"] <= hi)
        df.loc[m, "regime"] = label
    df = df[(df["regime"] != "other") & df["in_band_share_sa"].notna()].copy()
    return df


def ridge_3d(ax, data_by_regime, x_range, title, xlabel, ymax=4.0):
    """Plot a 3D ridge plot: 5 KDE curves stacked along the y-axis."""
    x = np.linspace(x_range[0], x_range[1], 200)
    polys = []
    colors = []
    z_max = 0
    for i, (r_lab, r_disp, col) in enumerate([(r[0], r[3], r[4]) for r in REGIME_DATES]):
        vals = data_by_regime.get(r_lab, np.array([]))
        if len(vals) < 30:
            continue
        try:
            vals = np.clip(vals, x_range[0] + 1e-3, x_range[1] - 1e-3)
            kde = gaussian_kde(vals, bw_method=0.10)
            z = np.clip(kde(x), 0, ymax)
        except (np.linalg.LinAlgError, ValueError):
            continue
        z_max = max(z_max, z.max())
        verts = [(x[0], 0)] + [(xi, zi) for xi, zi in zip(x, z)] + [(x[-1], 0)]
        polys.append(verts)
        colors.append(col)
    poly = PolyCollection(polys, facecolors=colors, edgecolors="black", linewidths=0.6, alpha=0.65)
    ax.add_collection3d(poly, zs=list(range(len(polys))), zdir="y")
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, min(ymax, z_max * 1.1) if z_max > 0 else ymax)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels([REGIME_DATES[i][3] for i in range(len(polys))], fontsize=7)
    ax.set_xlabel(xlabel, fontsize=8)
    ax.set_zlabel("density", fontsize=8)
    ax.set_title(title, fontsize=9)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def main():
    print(f"Loading SA panel {SA_PANEL.name}...")
    df = load_panel()
    print(f"  {len(df):,} cells with SA share")

    for tech in TECHS:
        sub_t = df[df["tech"] == tech]
        if sub_t.empty:
            continue
        fig = plt.figure(figsize=(16, 9.2))
        fig.suptitle(f"{tech.replace('_',' ')}: SA in-band MW share distribution across regimes — sliced by hour-of-day", fontsize=12)
        for idx, (h, h_label) in enumerate(HOURS_REPRESENTATIVE):
            ax = fig.add_subplot(2, 2, idx + 1, projection="3d")
            sub = sub_t[sub_t["clock_hour"] == h]
            data_by_regime = {r[0]: sub[sub["regime"] == r[0]]["in_band_share_sa"].dropna().values
                              for r in REGIME_DATES}
            ridge_3d(ax, data_by_regime, (0, 1), title=h_label, xlabel="SA in-band share")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"bidshape_3d_hourly_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


if __name__ == "__main__":
    main()
