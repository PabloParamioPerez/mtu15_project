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


def build_bid_curves(df):
    """For each (firm, hour-class), compute the average ladder across
    (date, unit, period) cells, indexed by tranche rank within the cell."""
    df = df[df["hour_class"].isin(["critical", "flat"])].copy()
    df = df.sort_values(["d", "unit_code", "period", "price"])
    df["rank"] = df.groupby(["d", "unit_code", "period"]).cumcount() + 1
    df["cum_qty"] = df.groupby(["d", "unit_code", "period"])["qty"].cumsum()
    # Average by (firm, hour_class, rank)
    avg = (
        df.groupby(["firm", "hour_class", "rank"], as_index=False)
        .agg(price=("price", "mean"),
             cum_qty=("cum_qty", "mean"),
             qty=("qty", "mean"),
             n_cells=("d", "count"))
    )
    # Only keep ranks with enough support (>= 5% of cells at rank 1)
    n_rank1 = avg[avg["rank"] == 1].set_index(["firm", "hour_class"])["n_cells"]
    avg = avg.merge(n_rank1.rename("n_rank1"), on=["firm", "hour_class"])
    avg = avg[avg["n_cells"] >= 0.05 * avg["n_rank1"]]
    return avg


def plot_bid_curves(avg):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5), sharex=False, sharey=False)
    firms_to_plot = ["IB", "GE", "GN", "HC"]  # pivotal CCGT operators with data
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        for hc, color in [("critical", "C3"), ("flat", "C0")]:
            sub = avg[(avg["firm"] == firm) & (avg["hour_class"] == hc)].sort_values("rank")
            if len(sub) == 0:
                continue
            ax.step(sub["cum_qty"], sub["price"], where="post", color=color, linewidth=1.8,
                    label=("Critical hours" if hc == "critical" else "Flat hours"))
            ax.scatter(sub["cum_qty"], sub["price"], color=color, s=18, zorder=3)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("Cumulative quantity (MW)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8, frameon=False, loc="lower right")
    fig.suptitle("Average day-ahead bid curves by pivotal firm, October--December 2025",
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

    print("building average ladders...")
    avg = build_bid_curves(df)
    plot_bid_curves(avg)

    print("building bid-shape detail table...")
    agg = build_table(df)
    write_table_tex(agg)


if __name__ == "__main__":
    main()
