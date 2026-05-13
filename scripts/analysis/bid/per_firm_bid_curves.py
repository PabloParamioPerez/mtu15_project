# STATUS: ALIVE
# LAST-AUDIT: 2026-05-13
# FEEDS: thesis paper.tex §4.2 -- per-firm bid-shape table + bid-curve figure
# CLAIM: Compare DA bid curves and bid-shape moments across the four
#        dominant CCGT operators (IB, GE, GN, EDP-Spain) in critical vs
#        flat hours, October-December 2025 (post-MTU15-DA). Reuses the
#        nb15 atlas idea: average ladders by tranche rank instead of raw
#        curves.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import firm_unit_panel  # noqa: E402

DET = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "det_all.parquet"
CAB = REPO / "data" / "processed" / "omie" / "mercado_diario" / "ofertas" / "cab_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

FIGDIR = REPO / "thesis" / "paper" / "figures"
TABDIR = REPO / "thesis" / "paper" / "tables"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
MIDDAY_HOURS = (11, 12, 13, 14)

CLASS_COLOR = {"critical": "C3", "flat": "C0", "midday": "tab:green"}
CLASS_LABEL = {
    "critical": "Critical hours (05:00--09:00, 16:00--23:00)",
    "midday":   "Midday (11:00--14:00)",
    "flat":     "Flat hours (01:00--04:00)",
}

FIRM_DISPLAY = {
    "IB":     "Iberdrola",
    "GE":     "Endesa",
    "GN":     "Naturgy",
    "HC":     "EDP-Spain",
    "EDP-PT": "EDP-Portugal",
}
PIVOTAL_FIRMS = list(FIRM_DISPLAY.keys())

# Order to draw curves so the lightest-density class is on top
DRAW_ORDER = ("critical", "midday", "flat")
TECHS_APPENDIX = ("Hydro", "Nuclear", "Wind", "Solar PV", "Coal")


def load_tranches(techs=None):
    """Pull DET+CAB joined tranches for pivotal firms, sell-side, Oct-Dec 2025.
    If techs is None, all techs in the firm panel are returned (with the
    tech_group column preserved). If techs is a list, restrict to those."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    mask = units["parent"].isin(PIVOTAL_FIRMS)
    if techs is not None:
        mask &= units["tech_group"].isin(list(techs))
    uft = units[mask][["unit_code", "parent", "tech_group"]].rename(columns={"parent": "firm"})

    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("uft", uft)
    df = con.execute(
        f"""
        WITH cab AS (
            SELECT date::DATE AS d, offer_code, version, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, offer_code, unit_code
                                      ORDER BY version DESC) AS rn,
                   mtu_minutes
            FROM '{CAB}'
            WHERE buy_sell = 'V'
              AND date::DATE >= DATE '2025-10-01'
              AND date::DATE <  DATE '2026-01-01'
        ),
        cab_l AS (SELECT * FROM cab WHERE rn = 1),
        det AS (
            SELECT date::DATE AS d, offer_code, version, period,
                   price_eur_mwh AS price, quantity_mw AS qty
            FROM '{DET}'
            WHERE date::DATE >= DATE '2025-10-01'
              AND date::DATE <  DATE '2026-01-01'
              AND period BETWEEN 1 AND 96
              AND quantity_mw IS NOT NULL AND quantity_mw > 0
              AND price_eur_mwh IS NOT NULL
        )
        SELECT d.d, d.period, ((d.period - 1) // 4)::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               c.unit_code, u.firm, u.tech_group, d.price, d.qty
        FROM det d
          JOIN cab_l c USING (d, offer_code, version)
          JOIN uft u ON c.unit_code = u.unit_code
        """
    ).df()
    df["hour_class"] = np.select(
        [df["hour"].isin(CRITICAL_HOURS),
         df["hour"].isin(FLAT_HOURS),
         df["hour"].isin(MIDDAY_HOURS)],
        ["critical", "flat", "midday"],
        default="other",
    )
    return df


def build_per_hour_supply_curves(df):
    """EUPHEMIA aggregation per (firm, hour-of-day), normalised by the
    number of (date, period) cells in the window. This is unconditional
    on whether any of the firm's units submitted on a given day: dates
    where the firm bid nothing contribute zero MW. Curves of two firms
    in the same hour are therefore comparable as "average MW offered by
    the firm per period at bid price <= p".
    """
    df = df[df["hour_class"].isin(["critical", "midday", "flat"])].copy()
    # Total (date, period) cells in the window. Post-MTU15-DA: 4 periods
    # per clock-hour.
    n_dates = df["d"].nunique()
    n_periods_per_hour = n_dates * 4
    out = []
    for (firm, hour), g in df.groupby(["firm", "hour"]):
        g_binned = (
            g.assign(price_bin=g["price"].round(0))
            .groupby("price_bin", as_index=False)["qty"].sum()
            .rename(columns={"price_bin": "price"})
            .sort_values("price")
        )
        g_binned["cum_qty_per_period"] = g_binned["qty"].cumsum() / n_periods_per_hour
        g_binned["firm"] = firm
        g_binned["hour"] = int(hour)
        g_binned["hour_class"] = g["hour_class"].iloc[0]
        g_binned["n_periods_denom"] = n_periods_per_hour
        out.append(g_binned)
    return pd.concat(out, ignore_index=True)


def build_offer_diagnostics(df):
    """Per (firm, unit, hour-class), compute the fraction of (date, period)
    cells in the window in which the unit submitted at least one tranche.
    DET contains tranches from every offer that entered the auction
    (including unaccepted tranches at the price cap), so a unit-period
    cell missing from DET means the unit did not submit at all for that
    period (offline, maintenance, withdrawn before gate closure, etc.).

    Aggregates to (firm, hour-class) as the simple mean across units.
    If this rate differs sharply between hour classes, the supply curves
    include compositional changes (some units bidding more often in
    critical hours) on top of bid-strategy changes."""
    df = df[df["hour_class"].isin(["critical", "midday", "flat"])].copy()
    n_dates_total = df["d"].nunique()
    hours_per_class = {"critical": len(CRITICAL_HOURS),
                       "midday":   len(MIDDAY_HOURS),
                       "flat":     len(FLAT_HOURS)}
    # n_obs_periods per (firm, unit, hour-class)
    per_unit = (
        df.groupby(["firm", "unit_code", "hour_class"])[["d", "hour", "period"]]
        .apply(lambda g: g.drop_duplicates().shape[0])
        .reset_index(name="n_obs_periods")
    )
    per_unit["n_total_periods"] = per_unit["hour_class"].map(
        lambda hc: n_dates_total * hours_per_class[hc] * 4
    )
    per_unit["offer_rate"] = per_unit["n_obs_periods"] / per_unit["n_total_periods"]
    # Aggregate to (firm, hour-class): mean across units
    agg = (
        per_unit.groupby(["firm", "hour_class"], as_index=False)
        .agg(mean_offer_rate=("offer_rate", "mean"),
             min_offer_rate=("offer_rate", "min"),
             n_units=("unit_code", "count"))
    )
    return agg, per_unit


def build_per_quarter_curves(df):
    """EUPHEMIA aggregation per (firm, quarter-of-hour) within critical
    hours. One curve per quarter (q1..q4). Visualises whether the firm
    differentiates bids across the 4 quarters of a clock-hour."""
    df = df[df["hour_class"] == "critical"].copy()
    n_dates = df["d"].nunique()
    # Total (date, hour) cells for critical: n_dates * len(critical hours).
    # Each (date, hour) cell has one quarter-1, one quarter-2 etc.
    n_cells_per_quarter = n_dates * len(CRITICAL_HOURS)
    out = []
    for (firm, quarter), g in df.groupby(["firm", "quarter"]):
        g_binned = (
            g.assign(price_bin=g["price"].round(0))
            .groupby("price_bin", as_index=False)["qty"].sum()
            .rename(columns={"price_bin": "price"})
            .sort_values("price")
        )
        g_binned["cum_qty_per_cell"] = g_binned["qty"].cumsum() / n_cells_per_quarter
        g_binned["firm"] = firm
        g_binned["quarter"] = int(quarter)
        out.append(g_binned)
    return pd.concat(out, ignore_index=True)


def plot_quarter_curves(curves, tech_label, out_stem, ylim=None):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5))
    firms_to_plot = ["IB", "GE", "GN", "HC"]
    quarter_colors = {1: "#1f77b4", 2: "#2ca02c", 3: "#ff7f0e", 4: "#d62728"}
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        panel = curves[curves["firm"] == firm]
        for q in (1, 2, 3, 4):
            sub = panel[panel["quarter"] == q].sort_values("price")
            if len(sub) == 0:
                continue
            ax.step(sub["cum_qty_per_cell"], sub["price"], where="post",
                    color=quarter_colors[q], linewidth=1.2, alpha=0.85)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW offered per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        if ylim is not None:
            ax.set_ylim(*ylim)
        elif len(panel) > 0:
            qpp = panel.groupby("price")["qty"].sum().sort_index()
            cum = qpp.cumsum() / qpp.sum()
            p_low = float(qpp.index.min())
            p_hi_idx = cum[cum >= 0.80].index
            p_hi = float(p_hi_idx.min()) if len(p_hi_idx) else float(qpp.index.max())
            ymin = min(p_low - 10, 0)
            ymax = min(max(p_hi + 30, 50), 700)
            ax.set_ylim(ymin, ymax)
    handles = [plt.Line2D([0], [0], color=quarter_colors[q], linewidth=2.0,
                          label=f"Quarter {q} ({(q-1)*15:02d}--{q*15:02d} min)")
               for q in (1, 2, 3, 4)]
    fig.suptitle(rf"Aggregate DA supply curves by quarter within critical hours ({tech_label}, Oct--Dec 2025)",
                 fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight", dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def write_offer_diagnostic_table(diag, tech, out_path):
    """Write LaTeX table: mean per-unit offer rate by (firm, hour-class)."""
    pivot = diag.pivot(index="firm", columns="hour_class", values="mean_offer_rate")
    for col in ("critical", "midday", "flat"):
        if col not in pivot.columns:
            pivot[col] = float("nan")
    pivot = pivot[["critical", "midday", "flat"]]
    units = diag.pivot(index="firm", columns="hour_class", values="n_units").max(axis=1)
    lines = [
        r"\begin{tabular}{l c c c c}",
        r"\toprule",
        r"Firm & Critical & Midday & Flat & N units \\",
        r"\midrule",
    ]
    for firm in PIVOTAL_FIRMS:
        if firm not in pivot.index:
            continue
        row = pivot.loc[firm]
        n_u = int(units.loc[firm]) if firm in units.index and not pd.isna(units.loc[firm]) else 0
        lines.append(
            f"{FIRM_DISPLAY[firm]} & {row['critical']:.3f} & {row['midday']:.3f} & {row['flat']:.3f} & {n_u} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    Path(out_path).write_text("\n".join(lines) + "\n")
    print(f"  saved {out_path}")


def plot_bid_curves(curves, tech_label, out_stem):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5), sharex=False, sharey=False)
    firms_to_plot = ["IB", "GE", "GN", "HC"]
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        panel_curves = curves[curves["firm"] == firm]
        for hc in DRAW_ORDER:
            hours = sorted(panel_curves[panel_curves["hour_class"] == hc]["hour"].unique())
            for hour in hours:
                sub = panel_curves[panel_curves["hour"] == hour].sort_values("price")
                if len(sub) == 0:
                    continue
                ax.step(sub["cum_qty_per_period"], sub["price"], where="post",
                        color=CLASS_COLOR[hc], linewidth=0.7, alpha=0.55)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW offered per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        # Adaptive y-axis: focus on the "body" of the supply curve,
        # cropping out cap-reservation tail. y-high = quantity-weighted
        # 80th percentile of price, plus a small buffer, capped at 700.
        if len(panel_curves) > 0:
            qty_per_price = panel_curves.groupby("price")["qty"].sum().sort_index()
            cum = qty_per_price.cumsum() / qty_per_price.sum()
            p_low = float(qty_per_price.index.min())
            p_hi_idx = cum[cum >= 0.80].index
            p_hi = float(p_hi_idx.min()) if len(p_hi_idx) else float(qty_per_price.index.max())
            ymin = min(p_low - 10, 0)
            ymax = min(max(p_hi + 30, 50), 700)
            ax.set_ylim(ymin, ymax)
    handles = [plt.Line2D([0], [0], color=CLASS_COLOR[hc], linewidth=2.0,
                          alpha=0.7, label=CLASS_LABEL[hc])
               for hc in ("critical", "midday", "flat")]
    fig.suptitle(rf"Aggregate DA supply curves by pivotal firm and clock-hour ({tech_label}, Oct--Dec 2025)",
                 fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight", dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def build_table(df):
    """Per-firm bid-shape detail table aggregated to canonical hour classes.
    Always restricted to CCGT (matches the section's framing)."""
    df = df[(df["hour_class"].isin(["critical", "flat"])) &
            (df["tech_group"] == "CCGT")].copy()
    # Per-cell summaries: one row per (date, unit, period)
    by_cell = (
        df.groupby(["firm", "hour_class", "d", "unit_code", "period"], as_index=False)
        .agg(n_tranches=("price", "count"),
             p_min=("price", "min"),
             p_med=("price", "median"),
             p_max=("price", "max"),
             qty_total=("qty", "sum"))
    )
    # Mechanical-repeat detection within an hour: for each (firm, date, unit, hour),
    # do the four 15-min periods have identical (n_tranches, p_min, p_med, p_max)?
    by_cell["hour"] = ((by_cell["period"] - 1) // 4).astype(int)
    sig = by_cell.groupby(["firm", "hour_class", "d", "unit_code", "hour"]).agg(
        n_per=("n_tranches", "count"),
        n_unique=("n_tranches", "nunique"),
        p_med_unique=("p_med", "nunique"),
    ).reset_index()
    sig = sig[sig["n_per"] == 4]  # only hours with all 4 quarters
    sig["mech"] = ((sig["n_unique"] == 1) & (sig["p_med_unique"] == 1)).astype(int)
    mech_rate = (
        sig.groupby(["firm", "hour_class"])["mech"].mean().reset_index()
        .rename(columns={"mech": "mech_strict_rate"})
    )
    # Per (firm, hour_class) means over cells
    agg = (
        by_cell.groupby(["firm", "hour_class"], as_index=False)
        .agg(n_tranches=("n_tranches", "mean"),
             p_med=("p_med", "mean"),
             qty_total=("qty_total", "mean"),
             n_cells=("d", "count"))
    )
    agg = agg.merge(mech_rate, on=["firm", "hour_class"], how="left")
    return agg


def write_table_tex(agg):
    rows = []
    for firm in PIVOTAL_FIRMS:
        for hc in ("critical", "flat"):
            r = agg[(agg["firm"] == firm) & (agg["hour_class"] == hc)]
            if r.empty:
                continue
            r = r.iloc[0]
            rows.append((FIRM_DISPLAY[firm] if hc == "critical" else "",
                         hc.capitalize(),
                         r["n_tranches"], r["p_med"], r["qty_total"],
                         r["mech_strict_rate"], int(r["n_cells"])))
    lines = [
        r"\begin{tabular}{l l c c c c r}",
        r"\toprule",
        r"Firm & Hour-class & \makecell{Mean\\tranches} & \makecell{Median\\price (EUR/MWh)} & \makecell{Total qty\\(MW)} & \makecell{Mechanical\\repeat rate} & N \\",
        r"\midrule",
    ]
    last_firm = None
    for firm, hc, ntr, pmed, qty, mech, n in rows:
        if last_firm is not None and firm != "" and last_firm != firm:
            lines.append(r"\addlinespace")
        if firm:
            last_firm = firm
        lines.append(
            f"{firm} & {hc} & {ntr:.2f} & {pmed:.1f} & {qty:.1f} & {mech:.3f} & {n:,} \\\\"
        )
    lines += [r"\bottomrule", r"\end{tabular}"]
    out = TABDIR / "tab_per_firm_bid_shape_detail.tex"
    out.write_text("\n".join(lines) + "\n")
    print(f"saved {out}")


def main():
    print("loading all tranches for pivotal firms...")
    df = load_tranches(techs=None)
    print(f"  {len(df):,} tranche rows across {df['firm'].nunique()} firms and "
          f"{df['tech_group'].nunique()} tech groups")

    # Main-text figure: CCGT only
    print("CCGT curves (main text)...")
    ccgt = df[df["tech_group"] == "CCGT"].copy()
    plot_bid_curves(build_per_hour_supply_curves(ccgt),
                     "CCGT", str(FIGDIR / "fig_per_firm_bid_curves"))

    # Quarter-within-hour comparison (CCGT, critical hours)
    # Zoom y-axis to the relevant clearing band (50-200 EUR/MWh) so
    # quarter-to-quarter variation in the body of the supply curve
    # is visible.
    print("CCGT per-quarter curves (granularity exploitation)...")
    plot_quarter_curves(build_per_quarter_curves(ccgt),
                         "CCGT", str(FIGDIR / "fig_per_firm_bid_curves_quarters_ccgt"),
                         ylim=(50, 200))

    # CCGT offer-rate diagnostic (per-unit, averaged within firm-hour-class)
    diag, per_unit = build_offer_diagnostics(ccgt)
    print("CCGT mean per-unit offer rates by firm x hour-class:")
    print(diag.to_string(index=False))
    print("\nUnit-level rates (sample):")
    print(per_unit.head(15).to_string(index=False))
    write_offer_diagnostic_table(diag, "CCGT",
                                 TABDIR / "tab_per_firm_offer_rate.tex")

    # Appendix figures: per-hour AND per-quarter for each tech
    for tech in TECHS_APPENDIX:
        sub = df[df["tech_group"] == tech].copy()
        if sub.empty or sub.groupby(["firm", "hour"]).ngroups < 4:
            print(f"skipping {tech}: not enough data")
            continue
        slug = tech.lower().replace(" ", "_")
        print(f"{tech} curves (appendix, per-hour)...")
        plot_bid_curves(build_per_hour_supply_curves(sub), tech,
                         str(FIGDIR / f"fig_per_firm_bid_curves_{slug}"))
        print(f"{tech} curves (appendix, per-quarter)...")
        plot_quarter_curves(build_per_quarter_curves(sub), tech,
                             str(FIGDIR / f"fig_per_firm_bid_curves_quarters_{slug}"))

    print("building bid-shape detail table (CCGT)...")
    agg = build_table(df)
    write_table_tex(agg)


if __name__ == "__main__":
    main()
