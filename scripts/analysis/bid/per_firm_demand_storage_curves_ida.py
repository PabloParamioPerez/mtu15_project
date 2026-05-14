# STATUS: ALIVE
# LAST-AUDIT: 2026-05-14
# FEEDS: thesis paper.tex §4.4 IDA mirror -- per-firm IDA demand/storage curves
# CLAIM: EUPHEMIA-style stepwise aggregate buy curves for pump-storage and
#        demand-side bids of pivotal firms in the intraday auctions
#        (ICAB+IDET), pooled across the 3 IDA sessions per day. Mirrors
#        per_firm_demand_storage_curves.py for the day-ahead market.

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

ICAB = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "icab_all.parquet"
IDET = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "idet_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

FIGDIR = REPO / "thesis" / "paper" / "figures"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
MIDDAY_HOURS = (11, 12, 13, 14)

TECH_YLIM = {
    "Retailer":           (0, 250),
    "Direct_consumer":    (0, 250),
    "Pump_load":          (-10, 100),
    "Hydro_pump":         (50, 250),
    "Hybrid_RES_storage": (-10, 200),
}

TECH_SIDE = {
    "Retailer":           "C",
    "Direct_consumer":    "C",
    "Pump_load":          "C",
    "Hybrid_RES_storage": "V",
    "Hydro_pump":         "V",
}

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
DRAW_ORDER = ("critical", "midday", "flat")

MAIN_TECH = "Pump_load"

TECHS_GRID_DEMAND   = ("Pump_load", "Retailer", "Direct_consumer")
TECHS_GRID_DISCHARGE = ("Hydro_pump", "Hybrid_RES_storage")


def load_tranches():
    """ICAB+IDET join, pivotal firms, demand+storage techs, Oct-Dec 2025,
    pooled across the 3 IDA sessions. ICAB carries the buy_sell flag."""
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    keep_techs = list(TECH_YLIM.keys())
    mask = units["parent"].isin(PIVOTAL_FIRMS) & units["tech_group"].isin(keep_techs)
    uft = units[mask][["unit_code", "parent", "tech_group"]].rename(columns={"parent": "firm"})

    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("uft", uft)
    df = con.execute(
        f"""
        WITH icab AS (
            SELECT date::DATE AS d, session_number, offer_code, version, unit_code, buy_sell,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{ICAB}'
            WHERE date::DATE >= DATE '2025-10-01' AND date::DATE < DATE '2026-01-01'
        ),
        icab_l AS (SELECT * FROM icab WHERE rn = 1),
        idet AS (
            SELECT date::DATE AS d, session_number, offer_code, version, period,
                   price_eur_mwh AS price, quantity_mw AS qty
            FROM '{IDET}'
            WHERE date::DATE >= DATE '2025-10-01' AND date::DATE < DATE '2026-01-01'
              AND period BETWEEN 1 AND 96
              AND quantity_mw IS NOT NULL AND quantity_mw > 0
              AND price_eur_mwh IS NOT NULL
        )
        SELECT d.d, d.session_number, d.period,
               ((d.period - 1) // 4)::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               c.unit_code, u.firm, u.tech_group, c.buy_sell,
               d.price, d.qty
        FROM idet d
          JOIN icab_l c USING (d, session_number, offer_code, version)
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
    df = df[df.apply(lambda r: r["buy_sell"] == TECH_SIDE.get(r["tech_group"], "C"), axis=1)]
    return df


def _aggregate_curve(g, n_denom, side):
    binned = (
        g.assign(price_bin=g["price"].round(0))
        .groupby("price_bin", as_index=False)["qty"].sum()
        .rename(columns={"price_bin": "price"})
    )
    asc = (side == "V")
    binned = binned.sort_values("price", ascending=asc)
    binned["cum_qty_per_cell"] = binned["qty"].cumsum() / n_denom
    return binned


def build_per_hour_curves(df, side):
    df = df[df["hour_class"].isin(["critical", "midday", "flat"])].copy()
    n_periods_per_hour_session = df["d"].nunique() * 4 * 3  # 4 quarters x 3 sessions
    out = []
    for (firm, hour), g in df.groupby(["firm", "hour"]):
        b = _aggregate_curve(g, n_periods_per_hour_session, side)
        b["firm"] = firm
        b["hour"] = int(hour)
        b["hour_class"] = g["hour_class"].iloc[0]
        out.append(b)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def build_per_quarter_curves(df, side, hour_class="critical"):
    df = df[df["hour_class"] == hour_class].copy()
    hours_in_class = {"critical": len(CRITICAL_HOURS),
                      "flat":     len(FLAT_HOURS),
                      "midday":   len(MIDDAY_HOURS)}[hour_class]
    n_cells_per_quarter = df["d"].nunique() * hours_in_class * 3  # 3 sessions
    out = []
    for (firm, quarter), g in df.groupby(["firm", "quarter"]):
        b = _aggregate_curve(g, n_cells_per_quarter, side)
        b["firm"] = firm
        b["quarter"] = int(quarter)
        out.append(b)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def _set_panel_limits(ax, panel, ylim):
    if ylim is None:
        return True
    ax.set_ylim(*ylim)
    in_band = panel[(panel["price"] >= ylim[0]) & (panel["price"] <= ylim[1])]
    if len(in_band) == 0:
        return False
    x_lo = float(in_band["cum_qty_per_cell"].min())
    x_hi = float(in_band["cum_qty_per_cell"].max())
    buf = max(20.0, 0.05 * (x_hi - x_lo))
    ax.set_xlim(max(0, x_lo - buf), x_hi + buf)
    return True


def _blank_panel(ax, label):
    ax.set_xticks([]); ax.set_yticks([])
    ax.text(0.5, 0.5, label, ha="center", va="center",
            transform=ax.transAxes, fontsize=8, color="gray")


def plot_per_firm_curves(curves, tech_label, out_stem, side):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5))
    for ax, firm in zip(axes.flatten(), ["IB", "GE", "GN", "HC"]):
        panel = curves[curves["firm"] == firm]
        for hc in DRAW_ORDER:
            for hour in sorted(panel[panel["hour_class"] == hc]["hour"].unique()):
                s = panel[panel["hour"] == hour].sort_values("cum_qty_per_cell")
                if len(s) == 0:
                    continue
                ax.step(s["cum_qty_per_cell"], s["price"], where="post",
                        color=CLASS_COLOR[hc], linewidth=0.7, alpha=0.55)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW " + ("offered" if side == "V" else "demanded") + " per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        _set_panel_limits(ax, panel, TECH_YLIM.get(tech_label))
    handles = [plt.Line2D([0], [0], color=CLASS_COLOR[hc], linewidth=2.0,
                          alpha=0.7, label=CLASS_LABEL[hc])
               for hc in ("critical", "midday", "flat")]
    label_side = "demand" if side == "C" else "supply"
    fig.suptitle(rf"Aggregate IDA {label_side} curves by pivotal firm and clock-hour ({tech_label}, Oct--Dec 2025)",
                 fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight", dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def plot_per_firm_quarter_curves(curves, tech_label, out_stem, side, hour_class_label, ylim=None):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5))
    quarter_colors = {1: "#1f77b4", 2: "#2ca02c", 3: "#ff7f0e", 4: "#d62728"}
    for ax, firm in zip(axes.flatten(), ["IB", "GE", "GN", "HC"]):
        panel = curves[curves["firm"] == firm]
        for q in (1, 2, 3, 4):
            s = panel[panel["quarter"] == q].sort_values("cum_qty_per_cell")
            if len(s) == 0:
                continue
            ax.step(s["cum_qty_per_cell"], s["price"], where="post",
                    color=quarter_colors[q], linewidth=1.2, alpha=0.85)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW " + ("offered" if side == "V" else "demanded") + " per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        _set_panel_limits(ax, panel, ylim if ylim is not None else TECH_YLIM.get(tech_label))
    handles = [plt.Line2D([0], [0], color=quarter_colors[q], linewidth=2.0,
                          label=f"Quarter {q} ({(q-1)*15:02d}--{q*15:02d} min)")
               for q in (1, 2, 3, 4)]
    label_side = "demand" if side == "C" else "supply"
    fig.suptitle(rf"Aggregate IDA {label_side} curves by quarter within {hour_class_label} hours ({tech_label}, Oct--Dec 2025)",
                 fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight", dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def plot_compact_grid(df, techs, out_stem, mode, side_label):
    firms = ["IB", "GE", "GN", "HC"]
    quarter_colors = {1: "#1f77b4", 2: "#2ca02c", 3: "#ff7f0e", 4: "#d62728"}
    n_rows, n_cols = len(firms), len(techs)
    fig, axes = plt.subplots(n_rows, n_cols,
                              figsize=(2.6 * n_cols, 2.2 * n_rows),
                              sharex=False, sharey=False)
    for i, firm in enumerate(firms):
        for j, tech in enumerate(techs):
            ax = axes[i, j]
            sub = df[(df["firm"] == firm) & (df["tech_group"] == tech)].copy()
            side = TECH_SIDE.get(tech, "C")
            placeholder = None
            if sub.empty:
                placeholder = "(no units)"
            else:
                if mode == "per_hour":
                    curves = build_per_hour_curves(sub, side)
                    panel = curves[curves["firm"] == firm]
                    plot_iter = [(hour, CLASS_COLOR[hc], 0.5, 0.55)
                                 for hc in DRAW_ORDER
                                 for hour in sorted(panel[panel["hour_class"] == hc]["hour"].unique())]
                    selector = lambda key: panel[panel["hour"] == key[0]]
                else:
                    curves = build_per_quarter_curves(sub, side, "critical")
                    panel = curves[curves["firm"] == firm]
                    plot_iter = [(q, quarter_colors[q], 0.9, 0.85) for q in (1, 2, 3, 4)]
                    selector = lambda key: panel[panel["quarter"] == key[0]]
                in_band = _set_panel_limits(ax, panel, TECH_YLIM.get(tech))
                if not in_band:
                    placeholder = "(out of band:\nbids at cap)"
                else:
                    for key in plot_iter:
                        s = selector(key).sort_values("cum_qty_per_cell")
                        if len(s):
                            ax.step(s["cum_qty_per_cell"], s["price"], where="post",
                                    color=key[1], linewidth=key[2], alpha=key[3])
            if placeholder is not None:
                _blank_panel(ax, placeholder)
            else:
                ax.grid(alpha=0.3)
                ax.tick_params(labelsize=7)
                ax.set_xlabel("MW", fontsize=7)
            if i == 0:
                ax.set_title(tech, fontsize=10)
            if j == 0:
                ax.set_ylabel(FIRM_DISPLAY.get(firm, firm), fontsize=9)
    if mode == "per_hour":
        handles = [plt.Line2D([0], [0], color=CLASS_COLOR[hc], linewidth=2.0,
                              alpha=0.7, label=CLASS_LABEL[hc])
                   for hc in ("critical", "midday", "flat")]
        ncol = 3
        title = f"Aggregate IDA {side_label} curves by firm and tech-group (Oct--Dec 2025)"
    else:
        handles = [plt.Line2D([0], [0], color=quarter_colors[q], linewidth=2.0,
                              label=f"Q{q}") for q in (1, 2, 3, 4)]
        ncol = 4
        title = f"Aggregate IDA {side_label} curves by quarter within critical hours, by firm and tech"
    fig.legend(handles=handles, loc="upper center", ncol=ncol, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.985))
    fig.suptitle(title, fontsize=11, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=140 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def main():
    print("loading IDA demand+storage tranches for pivotal firms...")
    df = load_tranches()
    print(f"  {len(df):,} tranche rows, {df['firm'].nunique()} firms, "
          f"{df['tech_group'].nunique()} techs, {df['session_number'].nunique()} sessions")

    main_df = df[df["tech_group"] == MAIN_TECH].copy()
    side = TECH_SIDE[MAIN_TECH]
    print(f"{MAIN_TECH} per-hour curves (main text, IDA)...")
    plot_per_firm_curves(build_per_hour_curves(main_df, side), MAIN_TECH,
                          str(FIGDIR / "fig_per_firm_demand_curves_pump_load_ida"), side)
    print(f"{MAIN_TECH} per-quarter curves, critical hours (IDA granularity exploitation)...")
    plot_per_firm_quarter_curves(build_per_quarter_curves(main_df, side, "critical"),
                                   MAIN_TECH,
                                   str(FIGDIR / "fig_per_firm_demand_curves_quarters_pump_load_ida"),
                                   side, "critical")
    print(f"{MAIN_TECH} per-quarter curves, flat hours (IDA falsification)...")
    plot_per_firm_quarter_curves(build_per_quarter_curves(main_df, side, "flat"),
                                   MAIN_TECH,
                                   str(FIGDIR / "fig_per_firm_demand_curves_quarters_pump_load_ida_flat"),
                                   side, "flat")

    print("compact grid (IDA demand-side, buy bids): per-hour...")
    plot_compact_grid(df, list(TECHS_GRID_DEMAND),
                       str(FIGDIR / "fig_demand_grid_per_hour_ida"),
                       "per_hour", "demand")
    print("compact grid (IDA demand-side, buy bids): per-quarter (critical hours)...")
    plot_compact_grid(df, list(TECHS_GRID_DEMAND),
                       str(FIGDIR / "fig_demand_grid_per_quarter_ida"),
                       "per_quarter", "demand")

    print("compact grid (IDA storage discharge, sell bids): per-hour...")
    plot_compact_grid(df, list(TECHS_GRID_DISCHARGE),
                       str(FIGDIR / "fig_storage_discharge_grid_per_hour_ida"),
                       "per_hour", "storage discharge")
    print("compact grid (IDA storage discharge, sell bids): per-quarter (critical hours)...")
    plot_compact_grid(df, list(TECHS_GRID_DISCHARGE),
                       str(FIGDIR / "fig_storage_discharge_grid_per_quarter_ida"),
                       "per_quarter", "storage discharge")


if __name__ == "__main__":
    main()
