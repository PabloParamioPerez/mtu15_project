# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: 3D surface plots of seasonality-adjusted (SA) aggregate bid curves
#        per (firm, tech), one panel per regime. The surface is the full
#        2D function bid_price(q, h) with x = quantile of cumulative MW
#        (q in [0.01, 0.99]), y = clock-hour (0..23), z = SA bid price
#        (EUR/MWh). User feedback 2026-05-21: include all 24 hours to form
#        a continuous "plane", not a 4-hour stack of ribbons.
#
#        The SA bid curves come from the functional pre-deseasonalisation
#        pipeline (fpca_functional_sa.py), which writes per-cell
#        f_i^SA(q) on a fixed 99-point quantile grid into
#        results/regressions/bid/fpca/quantile_curves_<tech>_sa.parquet.
#        Entity -> firm via lista_unidades.csv; clock_hour derived from
#        period (MTU60 pre-MTU15-DA, MTU15 post). Aggregate by
#        (firm, tech, clock_hour, regime) -> mean SA quantile curve, then
#        plot one 3D surface per regime in a 2x3 grid (5 regimes, 1 empty
#        slot reserved for the colorbar).
#
# OUT: figures/working/bid_curves_sa_3d_<tech>_<firm>.pdf

from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import cm
import matplotlib.colors as mcolors
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
FPCA_DIR = REPO / "results/regressions/bid/fpca"
UNITS_CSV = REPO / "data/external/omie_reference/lista_unidades.csv"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
FIG_DIR = REPO / "figures/working"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REGIME_DATES = [
    ("3sess",         pd.Timestamp("2024-06-14"), pd.Timestamp("2024-11-30"), "3-sess"),
    ("ISP15win",      pd.Timestamp("2024-12-01"), pd.Timestamp("2025-03-18"), "ISP15-win"),
    ("MTU15IDA_pre",  pd.Timestamp("2025-03-19"), pd.Timestamp("2025-04-27"), "DA60/ID15 pre"),
    ("MTU15IDA_post", pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"), "DA60/ID15 post"),
    ("DA15_ID15",     pd.Timestamp("2025-10-01"), pd.Timestamp("2026-05-15"), "DA15/ID15"),
]
TECHS = ["CCGT", "Hydro", "Hydro_pump"]
FIRMS = ["IB", "GE", "GN", "HC"]
QUANTILES = np.linspace(0.01, 0.99, 99)
HOURS = np.arange(24)
Z_CLIP = 400.0  # EUR/MWh hard cap so scarcity-tail spikes don't crush the visible band


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
    for label, lo, hi, _ in REGIME_DATES:
        if lo <= d <= hi:
            return label
    return "other"


def mcp_crossing_quantile(bid_curve, mcp_val):
    """Quantile q* where the (monotone) bid curve crosses MCP.

    Returns None if MCP is not finite or the bid curve is all-NaN.
    Returns 0.0 if the whole bid curve is above MCP (nothing clears),
    1.0 if the whole bid curve is below MCP (everything clears).
    """
    if not np.isfinite(mcp_val):
        return None
    bc = np.asarray(bid_curve, dtype=float)
    finite = np.isfinite(bc)
    if finite.sum() < 2:
        return None
    q = QUANTILES[finite]
    bc = bc[finite]
    if np.all(bc >= mcp_val):
        return float(q[0])
    if np.all(bc <= mcp_val):
        return float(q[-1])
    diff = bc - mcp_val
    sign_change = np.where(np.diff(np.sign(diff)) != 0)[0]
    if len(sign_change) == 0:
        return None
    i = sign_change[0]
    p_lo, p_hi = bc[i], bc[i + 1]
    q_lo, q_hi = q[i], q[i + 1]
    if p_hi == p_lo:
        return float(q_lo)
    frac = (mcp_val - p_lo) / (p_hi - p_lo)
    return float(q_lo + frac * (q_hi - q_lo))


def build_mcp_surfaces():
    """Return dict[regime] -> 24-vector of mean DA clearing price (EUR/MWh)."""
    df = pd.read_parquet(MPDBC, columns=["date", "period", "price_es_eur_mwh", "mtu_minutes"])
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["price_es_eur_mwh"].notna()].copy()
    df["regime"] = df["date"].apply(assign_regime)
    df = df[df["regime"] != "other"].copy()
    df["clock_hour"] = np.where(df["mtu_minutes"].fillna(60) == 60,
                                df["period"] - 1,
                                (df["period"] - 1) // 4)
    g = df.groupby(["regime", "clock_hour"])["price_es_eur_mwh"].mean().reset_index()
    out = {}
    for r_lab, _, _, _ in REGIME_DATES:
        arr = np.full(24, np.nan)
        for _, row in g[g["regime"] == r_lab].iterrows():
            h = int(row["clock_hour"])
            if 0 <= h < 24:
                arr[h] = float(row["price_es_eur_mwh"])
        out[r_lab] = arr
    return out


def build_surfaces_per_tech(tech):
    """Return dict[(firm, regime)] -> 2D array Z[h=0..23, q=0..98] of SA price."""
    path = FPCA_DIR / f"quantile_curves_{tech}_sa.parquet"
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"])
    unit_to_firm = load_unit_map()
    df["firm"] = df["entity"].map(unit_to_firm).fillna("OTH")
    df = df[df["firm"].isin(FIRMS)].copy()

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

    surfaces = {}
    for firm in FIRMS:
        for r_lab, _, _, _ in REGIME_DATES:
            sub = grouped[(grouped["firm"] == firm) & (grouped["regime"] == r_lab)]
            if sub.empty:
                continue
            Z = np.full((24, 99), np.nan)
            for _, row in sub.iterrows():
                h = int(row["clock_hour"])
                if 0 <= h < 24:
                    Z[h, :] = row[qcols].values.astype(float)
            surfaces[(firm, r_lab)] = Z
    return surfaces


def plot_surface_per_firm(tech, surfaces, mcp_by_regime, firm):
    """One figure per (firm, tech). 2x3 grid: 5 regime panels + 1 empty slot.

    Bid surface as a smooth lit surface (no mesh edges), z-axis clipped
    aggressively so the strategic band around MCP dominates the visual
    rather than the price-cap-padding plateau. MCP overlaid as an opaque
    red plane at z = MCP(h, regime).
    """
    keys = [(firm, r_lab) for r_lab, _, _, _ in REGIME_DATES if (firm, r_lab) in surfaces]
    if not keys:
        return False

    all_mcp = np.concatenate([m for m in mcp_by_regime.values() if m is not None])
    all_mcp = all_mcp[np.isfinite(all_mcp)]
    mcp_max = float(np.nanmax(all_mcp)) if len(all_mcp) else 100.0

    # Tight z-clip: 2 x max MCP, floored at 200, so the relevant strategic
    # band (bid prices near and just above MCP) dominates the visual.
    # Cap-padded bids well above this plateau visually at z_top, which is
    # the correct reading -- "bid > MCP by a lot".
    z_top = float(max(200.0, mcp_max * 2.0))
    z_bot = 0.0

    Xq, Yh = np.meshgrid(QUANTILES, HOURS)

    fig = plt.figure(figsize=(20, 12))
    fig.suptitle(
        f"{tech.replace('_',' ')} --- {firm}: SA aggregate bid surfaces, "
        f"24-hour $\\times$ 99-quantile, one panel per regime "
        f"(red plane = DA MCP; gold curve = bid$\\times$MCP intersection, the "
        f"marginal quantile; $z$ capped at {int(z_top)} EUR/MWh).",
        fontsize=12, y=0.97)

    for idx, (r_lab, _, _, r_disp) in enumerate(REGIME_DATES):
        ax = fig.add_subplot(2, 3, idx + 1, projection="3d")
        key = (firm, r_lab)
        if key not in surfaces:
            ax.set_title(f"{r_disp} (no data)", fontsize=11)
            ax.set_axis_off()
            continue
        Z = np.clip(surfaces[key], None, z_top)
        Zp = np.where(np.isfinite(Z), Z, np.nan)

        # MCP plane drawn FIRST so the bid surface renders on top of it.
        mcp_arr = mcp_by_regime.get(r_lab, np.full(24, np.nan))
        MCP_Z = np.tile(np.clip(mcp_arr, None, z_top)[:, None], (1, 99))
        MCP_Z = np.where(np.isfinite(MCP_Z), MCP_Z, np.nan)
        ax.plot_surface(Xq, Yh, MCP_Z,
                        color="#d62728",
                        edgecolor="none",
                        rstride=1, cstride=1,
                        alpha=0.70,
                        shade=False, antialiased=True)

        # Smooth monochrome bid surface, alpha slightly reduced so the MCP
        # plane stays visible where the bid surface clips below it.
        ax.plot_surface(Xq, Yh, Zp,
                        color="#9ec3e6",
                        edgecolor="none",
                        linewidth=0,
                        rstride=1, cstride=1,
                        alpha=0.85,
                        shade=True, antialiased=True)

        # Intersection curve: the marginal quantile q*(h) where the bid
        # surface crosses the MCP plane. MW below q* clear, MW above do not.
        cross_q, cross_h, cross_z = [], [], []
        for h in range(24):
            bc = surfaces[key][h, :]
            qstar = mcp_crossing_quantile(bc, mcp_arr[h])
            if qstar is not None:
                cross_q.append(qstar)
                cross_h.append(h)
                cross_z.append(min(float(mcp_arr[h]), z_top))
        if len(cross_q) >= 2:
            ax.plot(cross_q, cross_h, cross_z,
                    color="#111111", lw=2.6, zorder=10)
            ax.scatter(cross_q, cross_h, cross_z,
                       color="#ffd000", edgecolors="#111111", linewidths=0.5,
                       s=18, zorder=11, depthshade=False)

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 23)
        ax.set_zlim(z_bot, z_top)
        ax.set_xlabel("Quantile $q$", fontsize=10, labelpad=4)
        ax.set_ylabel("Hour", fontsize=10, labelpad=4)
        ax.set_zlabel("SA price (EUR/MWh)", fontsize=10, labelpad=4)
        ax.set_xticks([0, 0.25, 0.5, 0.75, 1.0])
        ax.set_yticks([0, 6, 12, 18, 23])
        ax.tick_params(axis="both", labelsize=8, pad=0)
        ax.set_title(r_disp, fontsize=12, pad=10)
        ax.view_init(elev=30, azim=-55)
        for pane in (ax.xaxis.pane, ax.yaxis.pane, ax.zaxis.pane):
            pane.set_edgecolor("lightgray")
            pane.set_alpha(0.10)

    fig.subplots_adjust(left=0.02, right=0.98, top=0.92, bottom=0.04,
                        wspace=0.10, hspace=0.18)
    out = FIG_DIR / f"bid_curves_sa_3d_{tech}_{firm}.pdf"
    fig.savefig(out, bbox_inches="tight", dpi=110)
    plt.close(fig)
    print(f"  wrote {out.name}")
    return True


def main():
    print("Building per-regime MCP curves from marginalpdbc...")
    mcp_by_regime = build_mcp_surfaces()
    for tech in TECHS:
        path = FPCA_DIR / f"quantile_curves_{tech}_sa.parquet"
        if not path.exists():
            print(f"  SKIP {tech}: {path} missing")
            continue
        print(f"Aggregating SA surfaces for {tech}...")
        surfaces = build_surfaces_per_tech(tech)
        for firm in FIRMS:
            plot_surface_per_firm(tech, surfaces, mcp_by_regime, firm)


if __name__ == "__main__":
    main()
