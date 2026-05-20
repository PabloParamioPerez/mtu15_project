# STATUS: ALIVE
# LAST-AUDIT: 2026-05-21
# CLAIM: 3D surface plots of seasonality-adjusted (SA) aggregate bid curves
#        per (firm, tech), one panel per regime. Surface = bid_price(q, h)
#        with x = quantile of cumulative MW (q in [0.01, 0.99]),
#        y = clock-hour (0..23), z = SA bid price (EUR/MWh).
#
#        The SA bid curves come from fpca_functional_sa.py
#        (quantile_curves_<tech>_sa.parquet). Per-cell SA curves are
#        non-monotone (functional deseasonalisation breaks monotonicity);
#        the per-(firm, tech, hour, regime) MEAN curve is monotone to
#        within ~0.1 EUR/MWh, and we apply a monotone rearrangement
#        (sort along q -- Chernozhukov-Fernandez-Val-Galichon 2010) to
#        guarantee a valid supply curve.
#
#        The DA MCP overlay is itself seasonality-adjusted (per-clock-hour
#        FWL on the daily clearing price: regime + Fourier(K=4) + DOW,
#        regime SA value at annual-mean Fourier and within-week DOW mean)
#        so the bid x MCP comparison is SA-vs-SA, not SA-vs-raw.
#
#        The bid x MCP intersection is drawn ONLY where the bid surface
#        genuinely crosses the MCP plane. Hours where the whole bid curve
#        is above MCP (clears nothing) or below MCP (clears everything)
#        have no intersection and are left as a gap in the curve.
#
# OUT: figures/working/bid_curves_sa_3d_<tech>_<firm>.pdf

from pathlib import Path
import sys

import numpy as np
import pandas as pd
import statsmodels.api as sm
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.analysis.sa_fwl import fourier_terms, dow_dummies, DEFAULT_K  # noqa: E402

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
K = DEFAULT_K

# Palette: neutral surface, distinct MCP plane, contrasting intersection.
COL_SURFACE = "#b9b9bd"   # neutral grey -- shape reads from shading alone
COL_MCP     = "#4d9aa8"   # muted teal MCP plane
COL_CROSS   = "#e6550d"   # orange intersection curve


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
    """Quantile q* where the monotone bid curve crosses MCP.

    Returns None when there is NO genuine crossing -- i.e. the whole bid
    curve is above MCP (clears nothing) or below MCP (clears everything).
    A returned value is a real intersection of the two surfaces.
    """
    if not np.isfinite(mcp_val):
        return None
    bc = np.asarray(bid_curve, dtype=float)
    finite = np.isfinite(bc)
    if finite.sum() < 2:
        return None
    q = QUANTILES[finite]
    bc = bc[finite]
    if np.all(bc >= mcp_val) or np.all(bc <= mcp_val):
        return None  # no genuine crossing
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


def build_mcp_sa():
    """Per-(regime, clock_hour) seasonality-adjusted DA clearing price.

    For each clock-hour, FWL on the daily MCP: regime + Fourier(K=4) + DOW.
    SA regime value = const + beta_regime + within-week DOW mean, at
    annual-mean Fourier (zero). Mirrors sa_fwl.py (identity link).
    """
    df = pd.read_parquet(MPDBC, columns=["date", "period", "price_es_eur_mwh", "mtu_minutes"])
    df["date"] = pd.to_datetime(df["date"])
    df = df[df["price_es_eur_mwh"].notna()].copy()
    df["regime"] = df["date"].apply(assign_regime)
    df = df[df["regime"] != "other"].copy()
    df["clock_hour"] = np.where(df["mtu_minutes"].fillna(60) == 60,
                                df["period"] - 1,
                                (df["period"] - 1) // 4)
    # daily mean MCP per (date, clock_hour)
    daily = df.groupby(["date", "clock_hour"])["price_es_eur_mwh"].mean().reset_index()
    doy = daily["date"].dt.dayofyear.values
    fk = fourier_terms(doy, K)
    dw = dow_dummies(daily["date"])
    daily = pd.concat([daily.reset_index(drop=True),
                       fk.reset_index(drop=True), dw.reset_index(drop=True)], axis=1)
    regime_labels = [r[0] for r in REGIME_DATES]
    for label, lo, hi, _ in REGIME_DATES:
        daily[f"D_{label}"] = ((daily["date"] >= lo) & (daily["date"] <= hi)).astype(float)

    fourier_cols = [f"{p}_{k}" for k in range(1, K + 1) for p in ("cos", "sin")]
    dow_cols = [f"dow_{i}" for i in range(1, 7)]
    regime_cols = [f"D_{lab}" for lab in regime_labels]

    out = {lab: np.full(24, np.nan) for lab in regime_labels}
    for h in range(24):
        sub = daily[daily["clock_hour"] == h]
        if len(sub) < 80:
            continue
        y = sub["price_es_eur_mwh"].astype(float).values
        X = sm.add_constant(sub[regime_cols + fourier_cols + dow_cols].astype(float).values)
        try:
            fit = sm.OLS(y, X, hasconst=True).fit()
        except (np.linalg.LinAlgError, ValueError):
            continue
        params = fit.params
        const = params[0]
        dow_mean = float(np.sum(params[1 + len(regime_cols) + len(fourier_cols):])) / 7.0
        for ri, lab in enumerate(regime_labels):
            out[lab][h] = const + params[1 + ri] + dow_mean
    return out


def build_surfaces_per_tech(tech):
    """dict[(firm, regime)] -> monotone 2D array Z[h=0..23, q=0..98]."""
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
                    # monotone rearrangement: sort the mean SA curve along q
                    Z[h, :] = np.sort(row[qcols].values.astype(float))
            surfaces[(firm, r_lab)] = Z
    return surfaces


def plot_surface_per_firm(tech, surfaces, mcp_sa, firm):
    """One figure per (firm, tech). 2x3 grid: 5 regime panels + 1 empty slot."""
    keys = [(firm, r_lab) for r_lab, _, _, _ in REGIME_DATES if (firm, r_lab) in surfaces]
    if not keys:
        return False

    all_mcp = np.concatenate([m for m in mcp_sa.values() if m is not None])
    all_mcp = all_mcp[np.isfinite(all_mcp)]
    mcp_max = float(np.nanmax(all_mcp)) if len(all_mcp) else 100.0
    z_top = float(max(200.0, mcp_max * 2.0))

    Xq, Yh = np.meshgrid(QUANTILES, HOURS)

    fig = plt.figure(figsize=(20, 12))
    fig.suptitle(
        f"{tech.replace('_',' ')} --- {firm}: SA aggregate bid surfaces, "
        f"24-hour $\\times$ 99-quantile, one panel per regime "
        f"(teal plane = SA DA MCP; orange curve = bid$\\times$MCP intersection; "
        f"$z$ capped at {int(z_top)} EUR/MWh).",
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
        mcp_arr = mcp_sa.get(r_lab, np.full(24, np.nan))

        # SA MCP plane (drawn first; bid surface renders on top)
        MCP_Z = np.tile(np.clip(mcp_arr, None, z_top)[:, None], (1, 99))
        MCP_Z = np.where(np.isfinite(MCP_Z), MCP_Z, np.nan)
        ax.plot_surface(Xq, Yh, MCP_Z, color=COL_MCP, edgecolor="none",
                        rstride=1, cstride=1, alpha=0.55,
                        shade=False, antialiased=True)

        # SA bid surface, neutral grey, shading reveals shape
        ax.plot_surface(Xq, Yh, Zp, color=COL_SURFACE, edgecolor="none",
                        linewidth=0, rstride=1, cstride=1, alpha=0.88,
                        shade=True, antialiased=True)

        # Intersection: only genuine crossings; gaps where bid never crosses MCP.
        # qstar is None where the whole bid curve is above MCP (clears nothing)
        # or below MCP (clears everything) -- those hours are left blank.
        qstar_by_h = {}
        for h in range(24):
            qs = mcp_crossing_quantile(surfaces[key][h, :], mcp_arr[h])
            if qs is not None:
                qstar_by_h[h] = qs
        if qstar_by_h:
            # connect consecutive hours with a line; markers at every crossing
            hs = sorted(qstar_by_h)
            seg_h, seg_q, seg_z = [], [], []
            for h in hs:
                seg_h.append(h); seg_q.append(qstar_by_h[h]); seg_z.append(float(mcp_arr[h]))
            # draw line only between adjacent hours
            for a, b in zip(hs[:-1], hs[1:]):
                if b - a == 1:
                    ax.plot([qstar_by_h[a], qstar_by_h[b]], [a, b],
                            [float(mcp_arr[a]), float(mcp_arr[b])],
                            color=COL_CROSS, lw=2.4, zorder=11)
            ax.scatter(seg_q, seg_h, seg_z, color=COL_CROSS,
                       edgecolors="#3a1602", linewidths=0.6, s=40,
                       depthshade=False, zorder=12)

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 23)
        ax.set_zlim(0, z_top)
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
    print("Building SA DA MCP curves (per-clock-hour FWL)...")
    mcp_sa = build_mcp_sa()
    for tech in TECHS:
        path = FPCA_DIR / f"quantile_curves_{tech}_sa.parquet"
        if not path.exists():
            print(f"  SKIP {tech}: {path} missing")
            continue
        print(f"Aggregating SA surfaces for {tech}...")
        surfaces = build_surfaces_per_tech(tech)
        for firm in FIRMS:
            plot_surface_per_firm(tech, surfaces, mcp_sa, firm)


if __name__ == "__main__":
    main()
