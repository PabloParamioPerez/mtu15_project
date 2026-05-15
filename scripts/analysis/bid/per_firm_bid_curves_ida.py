# STATUS: ALIVE
# LAST-AUDIT: 2026-05-13
# FEEDS: thesis paper.tex §4.2 IDA mirror -- per-firm IDA supply curves
# CLAIM: Same EUPHEMIA aggregation rule applied to IDA bids
#        (ICAB+IDET), pooled across the 3 IDA sessions per day.
#        Mirrors per_firm_bid_curves.py for the day-ahead market.

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
from mtu.analysis.clearing_prices import overlay_clearing_prices  # noqa: E402

WINDOW = ("2025-10-01", "2026-01-01")

ICAB = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "icab_all.parquet"
IDET = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "ofertas" / "idet_all.parquet"
MARGINALPIBC = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

FIGDIR = REPO / "figures" / "thesis"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)
MIDDAY_HOURS = (11, 12, 13, 14)

TECH_YLIM = {
    "CCGT":     (50, 200),
    "Hydro":    (0, 250),
    "Nuclear":  (-500, 50),
    "Wind":     (-15, 50),
    "Solar PV": (-15, 50),
    "Coal":     (0, 200),
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
TECHS_GRID = ("CCGT", "Hydro", "Nuclear", "Wind", "Solar PV")


def compute_clearing_prices_ida(start_date: str = WINDOW[0],
                                end_date: str = WINDOW[1]) -> dict:
    """Mean IDA clearing price (€/MWh) over the analysis window, pooled
    across the 3 IDA sessions, by hour-class. Returns dict for use by
    `_draw_clearing_line()` overlay."""
    con = duckdb.connect()
    df = con.execute(f"""
        SELECT
          (CAST(((period - 1) / 4) AS INT)) AS hour,
          AVG(price_es_eur_mwh) AS mean_price
        FROM '{MARGINALPIBC}'
        WHERE date::DATE >= DATE '{start_date}' AND date::DATE < DATE '{end_date}'
          AND price_es_eur_mwh IS NOT NULL
        GROUP BY 1
    """).df()
    out = {}
    for hc, hours in (("critical", CRITICAL_HOURS),
                      ("midday",   MIDDAY_HOURS),
                      ("flat",     FLAT_HOURS)):
        sub = df[df["hour"].isin(hours)]
        out[hc] = float(sub["mean_price"].mean()) if len(sub) else float("nan")
    return out


def _draw_clearing_line(ax, y_value, color, label=None, alpha=0.85):
    """Horizontal dashed line at the mean clearing price IF in y-axis range."""
    if y_value is None or not np.isfinite(y_value):
        return False
    ymin, ymax = ax.get_ylim()
    if y_value < ymin or y_value > ymax:
        return False
    ax.axhline(y_value, color=color, linestyle="--",
               linewidth=1.0, alpha=alpha, zorder=10)
    if label:
        ax.text(ax.get_xlim()[1], y_value, f" {label}: {y_value:.0f}",
                va="bottom", ha="right", fontsize=7, color=color,
                alpha=alpha, zorder=11)
    return True


def load_tranches(techs=None):
    """Pull IDET tranches for pivotal firms, sell-side, Oct-Dec 2025,
    pooled across the 3 IDA sessions. Joins ICAB only to filter
    buy_sell='V' (sell side) since IDET has unit_code directly."""
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
        WITH icab AS (
            SELECT date::DATE AS d, session_number, offer_code, version, unit_code,
                   ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, offer_code, unit_code
                                      ORDER BY version DESC) AS rn
            FROM '{ICAB}'
            WHERE buy_sell = 'V'
              AND date::DATE >= DATE '2025-10-01'
              AND date::DATE <  DATE '2026-01-01'
        ),
        icab_l AS (SELECT * FROM icab WHERE rn = 1),
        idet AS (
            SELECT date::DATE AS d, session_number, offer_code, version, period,
                   price_eur_mwh AS price, quantity_mw AS qty
            FROM '{IDET}'
            WHERE date::DATE >= DATE '2025-10-01'
              AND date::DATE <  DATE '2026-01-01'
              AND period BETWEEN 1 AND 96
              AND quantity_mw IS NOT NULL AND quantity_mw > 0
              AND price_eur_mwh IS NOT NULL
        )
        SELECT d.d, d.session_number, d.period,
               ((d.period - 1) // 4)::INT AS hour,
               (((d.period - 1) % 4) + 1)::INT AS quarter,
               c.unit_code, u.firm, u.tech_group, d.price, d.qty
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
    return df


def build_per_hour_supply_curves(df):
    """EUPHEMIA aggregation per (firm, hour-of-day), pooling all 3 IDA
    sessions and all units of the firm. Normalised by N_dates * 4
    quarters * 3 sessions so the y-axis reads as firm-level MW offered
    per (date, period, session)."""
    df = df[df["hour_class"].isin(["critical", "midday", "flat"])].copy()
    n_dates = df["d"].nunique()
    n_periods_per_hour_session = n_dates * 4 * 3  # 4 quarters x 3 sessions
    out = []
    for (firm, hour), g in df.groupby(["firm", "hour"]):
        g_binned = (
            g.assign(price_bin=g["price"].round(0))
            .groupby("price_bin", as_index=False)["qty"].sum()
            .rename(columns={"price_bin": "price"})
            .sort_values("price")
        )
        g_binned["cum_qty_per_period"] = g_binned["qty"].cumsum() / n_periods_per_hour_session
        g_binned["firm"] = firm
        g_binned["hour"] = int(hour)
        g_binned["hour_class"] = g["hour_class"].iloc[0]
        out.append(g_binned)
    return pd.concat(out, ignore_index=True)


def build_per_quarter_curves(df, hour_class="critical"):
    """EUPHEMIA per (firm, quarter), restricted to one hour-class,
    pooling all sessions."""
    df = df[df["hour_class"] == hour_class].copy()
    n_dates = df["d"].nunique()
    hours_in_class = {"critical": len(CRITICAL_HOURS),
                      "flat":     len(FLAT_HOURS),
                      "midday":   len(MIDDAY_HOURS)}[hour_class]
    n_cells_per_quarter = n_dates * hours_in_class * 3  # 3 sessions
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


def plot_bid_curves(curves, tech_label, out_stem):
    fig, axes = plt.subplots(2, 2, figsize=(11, 7.5))
    firms_to_plot = ["IB", "GE", "GN", "HC"]
    for ax, firm in zip(axes.flatten(), firms_to_plot):
        panel = curves[curves["firm"] == firm]
        for hc in DRAW_ORDER:
            hours = sorted(panel[panel["hour_class"] == hc]["hour"].unique())
            for hour in hours:
                sub = panel[panel["hour"] == hour].sort_values("price")
                if len(sub) == 0:
                    continue
                ax.step(sub["cum_qty_per_period"], sub["price"], where="post",
                        color=CLASS_COLOR[hc], linewidth=0.7, alpha=0.55)
        ax.set_title(FIRM_DISPLAY.get(firm, firm))
        ax.set_xlabel("MW offered per period (cumulative)")
        ax.set_ylabel("Bid price (EUR/MWh)")
        ax.grid(alpha=0.3)
        if len(panel) > 0:
            qpp = panel.groupby("price")["qty"].sum().sort_index()
            cum = qpp.cumsum() / qpp.sum()
            p_low = float(qpp.index.min())
            p_hi_idx = cum[cum >= 0.80].index
            p_hi = float(p_hi_idx.min()) if len(p_hi_idx) else float(qpp.index.max())
            ymin = min(p_low - 10, 0)
            ymax = min(max(p_hi + 30, 50), 700)
            ax.set_ylim(ymin, ymax)
        overlay_clearing_prices(ax, "ida", *WINDOW, mode="per_hour")
    handles = [plt.Line2D([0], [0], color=CLASS_COLOR[hc], linewidth=2.0,
                          alpha=0.7, label=CLASS_LABEL[hc])
               for hc in ("critical", "midday", "flat")]
    fig.suptitle(rf"Aggregate IDA supply curves by pivotal firm and clock-hour ({tech_label}, Oct--Dec 2025)",
                 fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def plot_quarter_curves(curves, tech_label, out_stem, ylim=None, suptitle=None,
                        hour_class: str = "critical"):
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
            in_band = panel[(panel["price"] >= ylim[0]) & (panel["price"] <= ylim[1])]
            if len(in_band) > 0:
                x_lo = float(in_band["cum_qty_per_cell"].min())
                x_hi = float(in_band["cum_qty_per_cell"].max())
                buf = max(50.0, 0.05 * (x_hi - x_lo))
                ax.set_xlim(max(0, x_lo - buf), x_hi + buf)
        overlay_clearing_prices(ax, "ida", *WINDOW, mode="per_quarter",
                                hour_class=hour_class)
    handles = [plt.Line2D([0], [0], color=quarter_colors[q], linewidth=2.0,
                          label=f"Quarter {q} ({(q-1)*15:02d}--{q*15:02d} min)")
               for q in (1, 2, 3, 4)]
    if suptitle is None:
        suptitle = rf"Aggregate IDA supply curves by quarter within critical hours ({tech_label}, Oct--Dec 2025)"
    fig.suptitle(suptitle, fontsize=12, y=0.99)
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.955))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=120 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def plot_compact_grid_per_hour(df, techs, out_stem):
    firms = ["IB", "GE", "GN", "HC"]
    n_rows, n_cols = len(firms), len(techs)
    fig, axes = plt.subplots(n_rows, n_cols,
                              figsize=(2.6 * n_cols, 2.2 * n_rows),
                              sharex=False, sharey=False)
    for i, firm in enumerate(firms):
        for j, tech in enumerate(techs):
            ax = axes[i, j]
            sub = df[(df["firm"] == firm) & (df["tech_group"] == tech)].copy()
            if len(sub) == 0:
                ax.set_xticks([]); ax.set_yticks([])
                ax.text(0.5, 0.5, "(no units)", ha="center", va="center",
                        transform=ax.transAxes, fontsize=8, color="gray")
            else:
                curves = build_per_hour_supply_curves(sub)
                panel = curves[curves["firm"] == firm]
                for hc in DRAW_ORDER:
                    hours = sorted(panel[panel["hour_class"] == hc]["hour"].unique())
                    for hour in hours:
                        s = panel[panel["hour"] == hour].sort_values("price")
                        if len(s) == 0:
                            continue
                        ax.step(s["cum_qty_per_period"], s["price"], where="post",
                                color=CLASS_COLOR[hc], linewidth=0.5, alpha=0.55)
                ylim = TECH_YLIM.get(tech)
                if ylim is not None:
                    ax.set_ylim(*ylim)
                    in_band = panel[(panel["price"] >= ylim[0]) & (panel["price"] <= ylim[1])]
                    if len(in_band) > 0:
                        x_lo = float(in_band["cum_qty_per_period"].min())
                        x_hi = float(in_band["cum_qty_per_period"].max())
                        buf = max(20.0, 0.05 * (x_hi - x_lo))
                        ax.set_xlim(max(0, x_lo - buf), x_hi + buf)
                overlay_clearing_prices(ax, "ida", *WINDOW, mode="per_hour", annotate=False)
            ax.grid(alpha=0.3)
            ax.tick_params(labelsize=7)
            if i == 0:
                ax.set_title(tech, fontsize=10)
            if j == 0:
                ax.set_ylabel(FIRM_DISPLAY.get(firm, firm), fontsize=9)
            ax.set_xlabel("MW", fontsize=7)
    handles = [plt.Line2D([0], [0], color=CLASS_COLOR[hc], linewidth=2.0,
                          alpha=0.7, label=CLASS_LABEL[hc])
               for hc in ("critical", "midday", "flat")]
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.985))
    fig.suptitle("Aggregate IDA supply curves by firm and technology (Oct--Dec 2025)",
                 fontsize=11, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=140 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def plot_compact_grid_per_quarter(df, techs, out_stem, hour_class="critical"):
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
            if len(sub) == 0:
                ax.set_xticks([]); ax.set_yticks([])
                ax.text(0.5, 0.5, "(no units)", ha="center", va="center",
                        transform=ax.transAxes, fontsize=8, color="gray")
            else:
                curves = build_per_quarter_curves(sub, hour_class=hour_class)
                panel = curves[curves["firm"] == firm]
                for q in (1, 2, 3, 4):
                    s = panel[panel["quarter"] == q].sort_values("price")
                    if len(s) == 0:
                        continue
                    ax.step(s["cum_qty_per_cell"], s["price"], where="post",
                            color=quarter_colors[q], linewidth=0.9, alpha=0.85)
                ylim = TECH_YLIM.get(tech)
                if ylim is not None:
                    ax.set_ylim(*ylim)
                    in_band = panel[(panel["price"] >= ylim[0]) & (panel["price"] <= ylim[1])]
                    if len(in_band) > 0:
                        x_lo = float(in_band["cum_qty_per_cell"].min())
                        x_hi = float(in_band["cum_qty_per_cell"].max())
                        buf = max(20.0, 0.05 * (x_hi - x_lo))
                        ax.set_xlim(max(0, x_lo - buf), x_hi + buf)
                overlay_clearing_prices(ax, "ida", *WINDOW, mode="per_quarter",
                                        hour_class=hour_class, annotate=False)
            ax.grid(alpha=0.3)
            ax.tick_params(labelsize=7)
            if i == 0:
                ax.set_title(tech, fontsize=10)
            if j == 0:
                ax.set_ylabel(FIRM_DISPLAY.get(firm, firm), fontsize=9)
            ax.set_xlabel("MW", fontsize=7)
    handles = [plt.Line2D([0], [0], color=quarter_colors[q], linewidth=2.0,
                          label=f"Q{q}") for q in (1, 2, 3, 4)]
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False,
               fontsize=9, bbox_to_anchor=(0.5, 0.985))
    hc_label = {"critical": "critical hours", "flat": "flat hours",
                "midday": "midday hours"}[hour_class]
    fig.suptitle(f"Aggregate IDA supply curves by quarter within {hc_label}, by firm and tech",
                 fontsize=11, y=0.995)
    fig.tight_layout(rect=[0, 0, 1, 0.94])
    for ext in ("pdf", "png"):
        fig.savefig(f"{out_stem}.{ext}", bbox_inches="tight",
                    dpi=140 if ext == "png" else None)
    plt.close(fig)
    print(f"  saved {out_stem}.pdf")


def main():
    print("loading all IDA tranches for pivotal firms...")
    df = load_tranches(techs=None)
    print(f"  {len(df):,} tranche rows across {df['firm'].nunique()} firms, "
          f"{df['tech_group'].nunique()} tech groups, "
          f"{df['session_number'].nunique()} sessions")

    # Main: CCGT per-hour and per-quarter (critical + flat)
    print("CCGT per-hour curves (IDA)...")
    ccgt = df[df["tech_group"] == "CCGT"].copy()
    plot_bid_curves(build_per_hour_supply_curves(ccgt),
                     "CCGT", str(FIGDIR / "fig_per_firm_bid_curves_ida"))

    print("CCGT per-quarter curves (critical hours, IDA)...")
    plot_quarter_curves(build_per_quarter_curves(ccgt, hour_class="critical"),
                         "CCGT",
                         str(FIGDIR / "fig_per_firm_bid_curves_quarters_ccgt_ida"),
                         ylim=(50, 200))

    print("CCGT per-quarter curves (flat hours, IDA falsification)...")
    plot_quarter_curves(build_per_quarter_curves(ccgt, hour_class="flat"),
                         "CCGT",
                         str(FIGDIR / "fig_per_firm_bid_curves_quarters_ccgt_ida_flat"),
                         ylim=(50, 200),
                         hour_class="flat",
                         suptitle=r"Aggregate IDA supply curves by quarter within flat hours (CCGT, Oct--Dec 2025)")

    # Compact grids: per-hour and per-quarter (critical) for all techs
    print("compact grid: IDA per-hour, all techs...")
    plot_compact_grid_per_hour(df, list(TECHS_GRID),
                                str(FIGDIR / "fig_bid_curves_grid_per_hour_ida"))
    print("compact grid: IDA per-quarter (critical), all techs...")
    plot_compact_grid_per_quarter(df, list(TECHS_GRID),
                                    str(FIGDIR / "fig_bid_curves_grid_per_quarter_ida"),
                                    hour_class="critical")


if __name__ == "__main__":
    main()
