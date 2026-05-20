# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: 3D waterfall plots of seasonality-adjusted (SA) aggregate bid
#        curves per (firm, tech), with 4 representative-hour ribbons
#        stacked along the y-axis and 5 regime overlays per ribbon
#        (color-coded). Substitutes the 2D per-quarter bid-curve grids
#        in the descriptive companion.
#
#        The SA bid curves come from the functional pre-deseasonalisation
#        pipeline (fpca_functional_sa.py), which writes per-cell
#        f_i^SA(q) on a fixed 99-point quantile grid into
#        results/regressions/bid/fpca/quantile_curves_<tech>_sa.parquet.
#        We map entity -> firm via lista_unidades.csv, derive clock_hour
#        from period (MTU60 pre-MTU15-DA, MTU15 post), and aggregate by
#        (firm, tech, clock_hour, regime) -> mean SA quantile curve.
#
#        x-axis: quantile of cumulative MW (q in [0.01, 0.99])
#        y-axis: representative hour (03, 07, 13, 19) stacked
#        z-axis: SA bid price (EUR/MWh)
#        Color: regime (5 ridges per hour-ribbon)
#
# OUT: figures/working/bid_curves_sa_3d_<tech>_<firm>.pdf

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
FPCA_DIR = REPO / "results/regressions/bid/fpca"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

# 5 regimes with palette consistent with the rest of the doc
REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess",          "#1f77b4"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win",       "#ff7f0e"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre",   "#2ca02c"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post",  "#d62728"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15",       "#9467bd"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]
HOURS_REPRESENTATIVE = [
    (3,  "Hour 03  (Flat)"),
    (7,  "Hour 07  (Morning ramp, Critical)"),
    (13, "Hour 13  (Midday)"),
    (19, "Hour 19  (Evening peak, Critical)"),
]
QUANTILES = np.linspace(0.01, 0.99, 99)


def firm_bucket(o):
    if not isinstance(o, str): return "OTH"
    o = o.lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    if "repsol" in o: return "REP"
    return "OTH"


def load_unit_map():
    u = pd.read_csv(UNITS_CSV)
    u["firm"] = u["owner_agent"].apply(firm_bucket)
    return u.set_index("unit_code")["firm"].to_dict()


def assign_regime(d):
    for label, lo, hi, _, _ in REGIME_DATES:
        if lo <= d <= hi:
            return label
    return "other"


def clock_hour_from_period(period, max_period):
    """1-based period in {1..24} (MTU60) or {1..96} (MTU15) -> 0..23 clock-hour."""
    if max_period <= 25:  # MTU60
        return period - 1
    return (period - 1) // 4


def build_sa_curves_per_group(tech):
    """Return aggregated SA curves: dict[(firm, hour, regime)] -> array of 99 quantile values."""
    path = FPCA_DIR / f"quantile_curves_{tech}_sa.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    unit_to_firm = load_unit_map()
    df["firm"] = df["entity"].map(unit_to_firm).fillna("OTH")
    df = df[df["firm"].isin(FIRMS)].copy()

    # Map period -> clock_hour. Period range varies per (date) due to MTU60/MTU15 mix.
    period_max_per_date = df.groupby("date")["period"].transform("max")
    df["clock_hour"] = np.where(period_max_per_date <= 25,
                                df["period"] - 1,
                                (df["period"] - 1) // 4)

    df["regime"] = df["date"].apply(assign_regime)
    df = df[df["regime"] != "other"].copy()

    qcols = [f"q{q:02d}" for q in range(1, 100)]
    print(f"  {len(df):,} (unit, date, period) cells for {tech}")
    grouped = (df.groupby(["firm", "clock_hour", "regime"], observed=True)[qcols]
                 .mean().reset_index())

    out = {}
    for _, row in grouped.iterrows():
        key = (row["firm"], int(row["clock_hour"]), row["regime"])
        out[key] = row[qcols].values.astype(float)
    return out


def plot_waterfall_per_firm(tech, sa_curves, firm):
    """3D waterfall: ribbons stacked along y=hour, regime overlays per ribbon."""
    fig = plt.figure(figsize=(15, 9))
    ax = fig.add_subplot(111, projection="3d")
    fig.suptitle(
        f"{tech.replace('_',' ')} --- {firm}: SA aggregate bid curves, "
        f"4 representative hours $\\times$ 5 regimes",
        fontsize=12)

    n_hours = len(HOURS_REPRESENTATIVE)
    y_offsets = np.arange(n_hours)

    z_max = 0
    has_data = False
    for hi, (h, h_label) in enumerate(HOURS_REPRESENTATIVE):
        y_val = y_offsets[hi]
        for r_lab, _, _, r_disp, col in REGIME_DATES:
            key = (firm, h, r_lab)
            if key not in sa_curves:
                continue
            curve = sa_curves[key]
            # x = quantile q, y = hour offset, z = SA price
            x = QUANTILES
            y = np.full_like(x, y_val, dtype=float)
            z = curve
            ax.plot(x, y, z, color=col, lw=1.6, alpha=0.85,
                    label=r_disp if hi == 0 else None)
            z_max = max(z_max, float(np.nanmax(z)))
            has_data = True
    if not has_data:
        plt.close(fig)
        return False

    ax.set_xlabel("Quantile of cumulative offered MW", fontsize=10)
    ax.set_ylabel("Clock hour", fontsize=10)
    ax.set_zlabel("SA bid price (EUR/MWh)", fontsize=10)
    ax.set_xlim(0, 1)
    ax.set_ylim(-0.5, n_hours - 0.5)
    ax.set_yticks(y_offsets)
    ax.set_yticklabels([h_label.split("(")[0].strip() for h, h_label in HOURS_REPRESENTATIVE], fontsize=8)
    # Clip z to a sensible domain so price-cap padding doesn't crush the visible band
    z_clip = min(z_max * 1.05, 400.0) if z_max > 0 else 200.0
    ax.set_zlim(0, z_clip)
    ax.view_init(elev=22, azim=-58)
    ax.grid(alpha=0.3)
    ax.legend(loc="upper left", fontsize=8, framealpha=0.7)

    out = FIG_DIR / f"bid_curves_sa_3d_{tech}_{firm}.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=110)
    plt.close(fig)
    print(f"  wrote {out.name}")
    return True


def main():
    for tech in TECHS:
        path = FPCA_DIR / f"quantile_curves_{tech}_sa.parquet"
        if not path.exists():
            print(f"  SKIP {tech}: {path} missing")
            continue
        print(f"Aggregating SA curves for {tech}...")
        sa_curves = build_sa_curves_per_group(tech)
        for firm in FIRMS:
            plot_waterfall_per_firm(tech, sa_curves, firm)


if __name__ == "__main__":
    main()
