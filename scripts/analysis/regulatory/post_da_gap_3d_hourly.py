# STATUS: ALIVE
# LAST-AUDIT: 2026-05-20
# CLAIM: Hour-conditional 3D ridge plots of the SA daily post-DA gap per
#        tech. Replaces the post-DA gap 3D surface diurnal figure with the
#        same per-tech landscape ridge format used for bidshape. Reads the
#        hourly SA panel (post_da_gap_sa_hourly.parquet); each tech gets 4
#        panels (representative hours 03, 07, 13, 19) of the cross-regime
#        gap distribution at that hour.
#
# OUT: figures/working/post_da_gap_3d_hourly_<tech>.pdf  (one PDF per tech)

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
SA_PANEL = REPO / "data/derived/panels/post_da_gap_sa_hourly.parquet"
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
    df = df[(df["regime"] != "other") & df["gap_gwh_sa"].notna()].copy()
    return df


def ridge_3d(ax, data_by_regime, x_range, title, xlabel):
    x = np.linspace(x_range[0], x_range[1], 250)
    polys = []
    colors = []
    z_max = 0
    for i, (r_lab, _, _, r_disp, col) in enumerate(REGIME_DATES):
        vals = data_by_regime.get(r_lab, np.array([]))
        vals = np.asarray(vals)
        vals = vals[np.isfinite(vals)]
        if len(vals) < 30:
            continue
        try:
            kde = gaussian_kde(vals, bw_method=0.10)
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
    poly = PolyCollection(polys, facecolors=colors, edgecolors="black", linewidths=0.6, alpha=0.65)
    ax.add_collection3d(poly, zs=list(range(len(polys))), zdir="y")
    ax.set_xlim(x_range[0], x_range[1])
    ax.set_ylim(-0.5, max(0, len(polys) - 0.5))
    ax.set_zlim(0, z_max * 1.15 if z_max > 0 else 1)
    ax.set_yticks(range(len(polys)))
    ax.set_yticklabels([REGIME_DATES[i][3] for i in range(len(polys))], fontsize=8)
    ax.set_xlabel(xlabel, fontsize=9)
    ax.set_zlabel("density", fontsize=9)
    ax.axvline = ax.plot([0, 0], [-0.5, max(0, len(polys) - 0.5)], [0, 0], color="black", lw=0.6, alpha=0.5)
    ax.set_title(title, fontsize=10)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)


def main():
    print(f"Loading SA hourly panel {SA_PANEL.name}...")
    df = load_panel()
    print(f"  {len(df):,} cells with SA gap")

    for tech in TECHS:
        sub_t = df[df["tech"] == tech]
        if sub_t.empty:
            continue
        vals_all = sub_t["gap_gwh_sa"].dropna().values
        if len(vals_all) == 0:
            continue
        q_lo, q_hi = np.quantile(vals_all, [0.01, 0.99])
        pad = 0.10 * (q_hi - q_lo + 1)
        q_lo -= pad
        q_hi += pad
        fig = plt.figure(figsize=(16, 9.2))
        fig.suptitle(f"{tech.replace('_',' ')}: SA post-DA gap (GWh/hour) distribution across regimes — sliced by hour-of-day", fontsize=12)
        for idx, (h, h_label) in enumerate(HOURS_REPRESENTATIVE):
            ax = fig.add_subplot(2, 2, idx + 1, projection="3d")
            sub = sub_t[sub_t["clock_hour"] == h]
            data_by_regime = {r[0]: sub[sub["regime"] == r[0]]["gap_gwh_sa"].dropna().values
                              for r in REGIME_DATES}
            ridge_3d(ax, data_by_regime, (q_lo, q_hi),
                     title=h_label, xlabel="SA gap GWh/hour")
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        out = FIG_DIR / f"post_da_gap_3d_hourly_{tech}.pdf"
        fig.savefig(out, bbox_inches="tight", dpi=110)
        plt.close(fig)
        print(f"  wrote {out.name}")


if __name__ == "__main__":
    main()
