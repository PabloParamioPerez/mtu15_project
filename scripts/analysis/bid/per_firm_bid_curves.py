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
    """EUPHEMIA aggregation per (firm, hour-of-day). Returns curves for
    critical, midday, and flat hour classes; other hours are dropped."""
    df = df[df["hour_class"].isin(["critical", "midday", "flat"])].copy()
    out = []
    for (firm, hour), g in df.groupby(["firm", "hour"]):
        n_cells = g.groupby(["d", "unit_code", "period"]).ngroups
        g_binned = (
            g.assign(price_bin=g["price"].round(0))
            .groupby("price_bin", as_index=False)["qty"].sum()
            .rename(columns={"price_bin": "price"})
            .sort_values("price")
        )
        g_binned["cum_qty_per_period"] = g_binned["qty"].cumsum() / n_cells
        g_binned["firm"] = firm
        g_binned["hour"] = int(hour)
        g_binned["hour_class"] = g["hour_class"].iloc[0]
        g_binned["n_cells"] = n_cells
        out.append(g_binned)
    return pd.concat(out, ignore_index=True)


def plot_bid_curves(curves, tech_label, out_stem):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5), sharex=False, sharey=False)
    firms_to_plot = ["IB", "GE", "GN", "HC"]
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        # Draw critical first, then midday, then flat -- so flat (3 hours)
        # is on top and visible even where it overlaps with critical.
        for hc in DRAW_ORDER:
            hours = sorted(curves[(curves["firm"] == firm) &
                                  (curves["hour_class"] == hc)]["hour"].unique())
            for hour in hours:
                sub = curves[(curves["firm"] == firm) & (curves["hour"] == hour)].sort_values("price")
                if len(sub) == 0:
                    continue
                ax.step(sub["cum_qty_per_period"], sub["price"], where="post",
                        color=CLASS_COLOR[hc], linewidth=0.7, alpha=0.55)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW offered per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        ax.set_ylim(-50, 500)
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

    # Appendix figures: one per tech in TECHS_APPENDIX
    for tech in TECHS_APPENDIX:
        sub = df[df["tech_group"] == tech].copy()
        if sub.empty or sub.groupby(["firm", "hour"]).ngroups < 4:
            print(f"skipping {tech}: not enough data")
            continue
        print(f"{tech} curves (appendix)...")
        stem = FIGDIR / f"fig_per_firm_bid_curves_{tech.lower().replace(' ', '_')}"
        plot_bid_curves(build_per_hour_supply_curves(sub), tech, str(stem))

    print("building bid-shape detail table (CCGT)...")
    agg = build_table(df)
    write_table_tex(agg)


if __name__ == "__main__":
    main()
