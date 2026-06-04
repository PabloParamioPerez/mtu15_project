# STATUS: ALIVE
# LAST-AUDIT: 2026-06-03
# FEEDS: thesis/paper/thesis.tex --- bid-curves panel figures showing
#        representative DA bid step functions for one unit per technology.
#        Two settings: critical-hour evening ramp and flat-hour overnight
#        on a windy night. Panels that have bid prices far below the MCP
#        band are drawn with a broken y-axis so the bid curve is readable
#        and the band/MCP context is preserved in a top sub-panel.
#
# OUT: figures/thesis/fig_bid_curves_by_tech.pdf      (critical hour)
#      figures/thesis/fig_bid_curves_by_tech_flat.pdf (flat hour, windy night)

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec
import numpy as np

REPO = Path(__file__).resolve().parents[3]
CAB = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT = REPO / "figures/thesis/fig_bid_curves_by_tech"

H_BAND = 50.0         # in-band bandwidth h = p90-p50 of MCP for DA15 real window

SETTINGS = [
    {
        "out_suffix": "",
        "date": "2025-12-17",
        "period": 68,        # hour 17 quarter 4, MCP ~ EUR 122/MWh
        "scene": "evening-ramp critical hour",
        "fallback_periods": [67, 69, 66, 70, 65],
        "units": [
            ("LARES2",   "CCGT",        "tab:red",     "CCGT"),
            ("ASC2",     "Nuclear",     "tab:purple",  "Nuclear"),
            ("DUER",     "Hydro",       "tab:blue",    "Hydro"),
            ("MUEL",     "Hydro_pump",  "tab:cyan",    "Pump-storage"),
            ("EGEDRE1",  "Wind",        "tab:green",   "Wind (aggregator)"),
            ("EGVD503",  "Solar PV",    "tab:orange",  "Solar PV (aggregator)"),
        ],
    },
    {
        "out_suffix": "_flat",
        "date": "2025-12-04",
        "period": 4,         # hour 01 quarter 4, flat overnight hour, windy night
        "scene": "flat overnight hour on a windy night",
        "fallback_periods": [3, 5, 2, 6, 1, 7, 8],
        "units": [
            ("LARES2",   "CCGT",        "tab:red",     "CCGT"),
            ("ASC2",     "Nuclear",     "tab:purple",  "Nuclear"),
            ("DUER",     "Hydro",       "tab:blue",    "Hydro"),
            ("MUEL",     "Hydro_pump",  "tab:cyan",    "Pump-storage"),
            ("WMVD088",  "Wind",        "tab:green",   "Wind (aggregator)"),
            ("EGVD503",  "Solar PV",    "tab:orange",  "Solar PV (aggregator)"),
        ],
    },
]


def period_to_hour_quarter(period):
    """MTU15 period (1-96) -> (hour 0-23, quarter 1-4)."""
    return (period - 1) // 4, ((period - 1) % 4) + 1


def fetch_tranches(unit_code, date, period, fallback_periods):
    con = duckdb.connect(); con.execute("SET threads=4")
    for p_try in [period, *fallback_periods]:
        df = con.execute(f"""
        SELECT d.price_eur_mwh AS price, d.quantity_mw AS qty
        FROM '{CAB}' c
        JOIN '{DET}' d ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
        WHERE c.date = DATE '{date}' AND c.unit_code = '{unit_code}'
          AND c.buy_sell = 'V' AND d.period = {p_try}
          AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw IS NOT NULL AND d.quantity_mw > 0
        ORDER BY d.price_eur_mwh
        """).df()
        if len(df) > 0:
            return df, p_try
    return df, None


def fetch_mcp(date, period):
    con = duckdb.connect()
    MCP = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
    df = con.execute(f"""
    SELECT period, price_es_eur_mwh AS price FROM '{MCP}'
    WHERE date = DATE '{date}' AND price_es_eur_mwh IS NOT NULL
    """).df()
    if len(df) == 0:
        return None
    row = df[df["period"] == period]
    if len(row) > 0:
        return float(row["price"].iloc[0])
    return float(df["price"].median())


def step_curve(prices, quantities):
    order = np.argsort(prices)
    p = np.asarray(prices)[order]
    q = np.asarray(quantities)[order]
    cum = np.concatenate([[0], np.cumsum(q)])
    xs, ys = [0.0], [p[0]]
    for i in range(len(p)):
        xs.extend([cum[i], cum[i + 1]])
        ys.extend([p[i], p[i]])
    return xs, ys


def draw_band_and_mcp(ax, mcp, color="orange"):
    if mcp is None:
        return
    ax.axhspan(mcp - H_BAND, mcp + H_BAND, color=color, alpha=0.18, zorder=0)
    ax.axhline(mcp, color="red", lw=1.0, ls="--", alpha=0.8, zorder=1)


def draw_break_marks(ax_top, ax_bot, d=0.015):
    """Diagonal break marks across the gap between two stacked axes."""
    kwargs = dict(transform=ax_top.transAxes, color="k", clip_on=False, lw=0.8)
    ax_top.plot((-d, +d), (-d, +d), **kwargs)
    ax_top.plot((1 - d, 1 + d), (-d, +d), **kwargs)
    kwargs.update(transform=ax_bot.transAxes)
    ax_bot.plot((-d, +d), (1 - d, 1 + d), **kwargs)
    ax_bot.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)


def needs_break(df, mcp):
    """True if the bid-price range is far enough below the MCP band that a
    full-range y-axis would squash the bid curve."""
    if mcp is None or len(df) == 0:
        return False
    p_max = df["price"].max()
    return p_max < (mcp - H_BAND - 20)   # at least 20 EUR/MWh below the band


def render_panel(parent_gs, fig, unit_info, mcp, df, p_used, period_requested):
    unit, tech, color, label = unit_info
    title = f"{label} --- {unit}"
    if df is None or len(df) == 0:
        ax = fig.add_subplot(parent_gs)
        ax.text(0.5, 0.5, f"(no bids --- {tech.lower()} absent\nat this hour)",
                transform=ax.transAxes, ha="center", va="center",
                color="gray", fontsize=10, style="italic")
        ax.set_title(title, fontsize=11, weight="bold")
        ax.set_xticks([]); ax.set_yticks([])
        return
    xs, ys = step_curve(df["price"].values, df["qty"].values)
    n, sum_mw = len(df), df["qty"].sum()
    title = f"{label} --- {unit} ({n} tranche{'s' if n != 1 else ''})"

    if needs_break(df, mcp):
        # Broken-axis panel: top shows band & MCP, bottom shows bid curve
        inner = GridSpecFromSubplotSpec(2, 1, subplot_spec=parent_gs,
                                         height_ratios=[1, 2.4], hspace=0.08)
        ax_top = fig.add_subplot(inner[0])
        ax_bot = fig.add_subplot(inner[1])
        # Top: shaded band, MCP line, no bid curve
        draw_band_and_mcp(ax_top, mcp)
        ax_top.set_ylim(mcp - H_BAND - 10, mcp + H_BAND + 10)
        ax_top.set_xticklabels([])
        ax_top.tick_params(axis="x", which="both", bottom=False, top=False)
        ax_top.text(0.99, mcp + 3, f"MCP {mcp:.0f}", transform=ax_top.get_yaxis_transform(),
                    fontsize=8, color="red", ha="right", va="bottom")
        ax_top.text(0.99, mcp + H_BAND - 4, "+h",
                    transform=ax_top.get_yaxis_transform(),
                    fontsize=7, color="darkorange", ha="right", va="top")
        ax_top.text(0.99, mcp - H_BAND + 4, "-h",
                    transform=ax_top.get_yaxis_transform(),
                    fontsize=7, color="darkorange", ha="right", va="bottom")
        ax_top.spines["bottom"].set_visible(False)
        ax_top.grid(alpha=0.3)
        ax_top.tick_params(labelsize=8)
        # Bottom: bid curve
        ax_bot.plot(xs, ys, color=color, lw=2.4, drawstyle="steps-pre")
        ax_bot.fill_between(xs, ax_bot.get_ylim()[0], ys, color=color, alpha=0.10, step="pre")
        y_pad = max(2.0, (df["price"].max() - df["price"].min()) * 0.20)
        ax_bot.set_ylim(df["price"].min() - y_pad, df["price"].max() + y_pad)
        ax_bot.spines["top"].set_visible(False)
        ax_bot.set_xlabel("Cumulative offered MW", fontsize=9)
        ax_bot.set_ylabel("Bid price (EUR/MWh)", fontsize=9)
        ax_bot.grid(alpha=0.3)
        ax_bot.tick_params(labelsize=8)
        draw_break_marks(ax_top, ax_bot)
        ax_top.set_title(title, fontsize=11, weight="bold")
    else:
        # Single-axis panel: band + MCP + bid curve in same frame
        ax = fig.add_subplot(parent_gs)
        draw_band_and_mcp(ax, mcp)
        ax.plot(xs, ys, color=color, lw=2.4, drawstyle="steps-pre")
        ax.fill_between(xs, -10, ys, color=color, alpha=0.10, step="pre")
        if mcp is not None:
            ax.text(0.99, mcp + 3, f"MCP {mcp:.0f}", transform=ax.get_yaxis_transform(),
                    fontsize=8, color="red", ha="right", va="bottom")
            ax.text(0.99, mcp + H_BAND - 4, "+h", transform=ax.get_yaxis_transform(),
                    fontsize=7, color="darkorange", ha="right", va="top")
            ax.text(0.99, mcp - H_BAND + 4, "-h", transform=ax.get_yaxis_transform(),
                    fontsize=7, color="darkorange", ha="right", va="bottom")
        y_min, y_max = -10, 200
        clipped = (df["price"] > y_max).sum()
        if clipped > 0:
            ax.annotate(f"↑ {clipped} tranche{'s' if clipped > 1 else ''} above {y_max} EUR/MWh",
                        xy=(0.98, 0.95), xycoords="axes fraction", ha="right", va="top",
                        fontsize=7, color="gray",
                        bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="gray", alpha=0.7))
        ax.set_ylim(y_min, y_max)
        ax.set_xlabel("Cumulative offered MW", fontsize=9)
        ax.set_ylabel("Bid price (EUR/MWh)", fontsize=9)
        ax.set_title(title, fontsize=11, weight="bold")
        ax.grid(alpha=0.3)
        ax.tick_params(labelsize=8)


def render_setting(setting):
    DATE = setting["date"]
    PERIOD = setting["period"]
    UNITS = setting["units"]
    FALLBACK = setting["fallback_periods"]
    OUT_SUFFIX = setting["out_suffix"]
    fig = plt.figure(figsize=(14, 7.5))
    outer = GridSpec(2, 3, figure=fig)
    mcp = fetch_mcp(DATE, PERIOD)
    cell_data = []
    for i, unit_info in enumerate(UNITS):
        row, col = i // 3, i % 3
        df, p_used = fetch_tranches(unit_info[0], DATE, PERIOD, FALLBACK)
        render_panel(outer[row, col], fig, unit_info, mcp, df, p_used, PERIOD)
        if df is not None and len(df) > 0:
            cell_data.append({"unit": unit_info[0], "tech": unit_info[1],
                               "n_tranches": len(df), "sum_mw": df["qty"].sum()})
    h, q = period_to_hour_quarter(PERIOD)
    fig.suptitle(f"Representative day-ahead supply curves by technology, {DATE} hour {h:02d} quarter {q} ({setting['scene']})",
                 fontsize=12, weight="bold", y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    out = Path(str(OUT) + OUT_SUFFIX)
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.savefig(f"{out}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {out}.pdf / .png")
    for c in cell_data:
        print(f"  {c['unit']:>10}  {c['tech']:<11}  n_tranches={c['n_tranches']:>3}  sum_mw={c['sum_mw']:>7.1f}")


def main():
    for setting in SETTINGS:
        render_setting(setting)


if __name__ == "__main__":
    main()
