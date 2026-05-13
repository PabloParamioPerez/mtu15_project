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

FIRM_DISPLAY = {
    "IB":     "Iberdrola",
    "GE":     "Endesa",
    "GN":     "Naturgy",
    "HC":     "EDP-Spain",
    "EDP-PT": "EDP-Portugal",
}
PIVOTAL_FIRMS = list(FIRM_DISPLAY.keys())


def load_tranches():
    """Pull DET+CAB joined tranches for pivotal CCGT, sell-side, Oct-Dec 2025."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    uft = units[units["parent"].isin(PIVOTAL_FIRMS) & (units["tech_group"] == "CCGT")][
        ["unit_code", "parent"]
    ].rename(columns={"parent": "firm"})

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
               c.unit_code, u.firm, d.price, d.qty
        FROM det d
          JOIN cab_l c USING (d, offer_code, version)
          JOIN uft u ON c.unit_code = u.unit_code
        """
    ).df()
    df["hour_class"] = np.where(
        df["hour"].isin(CRITICAL_HOURS), "critical",
        np.where(df["hour"].isin(FLAT_HOURS), "flat", "other"),
    )
    return df


def build_aggregate_supply_curves(df):
    """OMIE / EUPHEMIA aggregation: pool all sell tranches by (firm,
    hour-class), sort ascending by price, cumulate quantities. Normalise
    by the number of (date, unit, period) cells so the y-axis reads as
    "average MW offered per period at bid price <= p."

    Public-description of EUPHEMIA, sec. 7.1 (p.43-44): supply tranches
    are pooled, sorted by ascending price, and inverse functions added
    pointwise. For pure step orders this is equivalent to sorting (price,
    quantity) pairs by price and cumulating quantities. We restrict to
    CCGT here to match the rest of section 4.2.
    """
    df = df[df["hour_class"].isin(["critical", "flat"])].copy()
    out = []
    for (firm, hc), g in df.groupby(["firm", "hour_class"]):
        n_cells = g.groupby(["d", "unit_code", "period"]).ngroups
        # Bin to 1 EUR/MWh price grid to keep the curve plottable
        g_binned = (
            g.assign(price_bin=g["price"].round(0))
            .groupby("price_bin", as_index=False)["qty"].sum()
            .rename(columns={"price_bin": "price"})
            .sort_values("price")
        )
        g_binned["cum_qty_per_period"] = g_binned["qty"].cumsum() / n_cells
        g_binned["firm"] = firm
        g_binned["hour_class"] = hc
        g_binned["n_cells"] = n_cells
        out.append(g_binned)
    return pd.concat(out, ignore_index=True)


def plot_bid_curves(curves):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5), sharex=False, sharey=False)
    firms_to_plot = ["IB", "GE", "GN", "HC"]
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        for hc, color in [("critical", "C3"), ("flat", "C0")]:
            sub = curves[(curves["firm"] == firm) & (curves["hour_class"] == hc)].sort_values("price")
            if len(sub) == 0:
                continue
            ax.step(sub["cum_qty_per_period"], sub["price"], where="post",
                    color=color, linewidth=1.8,
                    label=("Critical hours" if hc == "critical" else "Flat hours"))
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW offered per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        ax.set_ylim(-50, 700)  # focus on relevant clearing range
        ax.legend(fontsize=8, frameon=False, loc="upper left")
    fig.suptitle(r"Aggregate DA supply curves by pivotal firm (CCGT, Oct--Dec 2025)",
                 fontsize=12, y=1.00)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        out = FIGDIR / f"fig_per_firm_bid_curves.{ext}"
        fig.savefig(out, bbox_inches="tight", dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"saved {FIGDIR / 'fig_per_firm_bid_curves.pdf'}")


def build_table(df):
    """Per-firm bid-shape detail table aggregated to canonical hour classes."""
    df = df[df["hour_class"].isin(["critical", "flat"])].copy()
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
    print("loading tranches...")
    df = load_tranches()
    print(f"  {len(df):,} tranche rows across {df['firm'].nunique()} firms")

    print("building aggregate supply curves (OMIE methodology)...")
    curves = build_aggregate_supply_curves(df)
    plot_bid_curves(curves)

    print("building bid-shape detail table...")
    agg = build_table(df)
    write_table_tex(agg)


if __name__ == "__main__":
    main()
