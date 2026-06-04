# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: thesis/paper/thesis.tex Appendix C.4 - representative buy vs sell
#        bid curves per technology in IDA pre-ID15 (when buy-side activity
#        was largest). One representative (unit, date, session, period) per
#        tech; sell and buy curves overlaid on the same panel.
#
# OUT: figures/thesis/fig_buy_vs_sell_curves.{pdf,png}

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
from matplotlib.gridspec import GridSpec
import numpy as np

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
OUT = REPO / "figures/thesis/fig_buy_vs_sell_curves"

# Representative units per tech (high dual-direction activity pre-ID15)
UNITS = [
    ("CTGN3",   "CCGT",        "tab:red"),
    ("ACAVADO", "Hydro",       "tab:blue"),
    ("MUEL",    "Hydro_pump",  "tab:cyan"),
    ("COF1",    "Nuclear",     "tab:purple"),
    ("GESTVD2", "Solar PV",    "tab:orange"),
    ("GSVD116", "Wind",        "tab:green"),
]
LABELS = {"CCGT":"CCGT","Hydro":"Hydro","Hydro_pump":"Pump-storage",
          "Nuclear":"Nuclear","Solar PV":"Solar PV (aggregator)","Wind":"Wind (aggregator)"}


def find_period(unit, con):
    """Find a (date, session, period) pre-ID15 where this unit has both
    sell and buy offers."""
    q = f"""
    SELECT c.date, c.session_number, dd.period,
           SUM(CASE WHEN c.buy_sell='V' THEN dd.quantity_mw ELSE 0 END) AS sell_mw,
           SUM(CASE WHEN c.buy_sell='C' THEN dd.quantity_mw ELSE 0 END) AS buy_mw,
           SUM(CASE WHEN c.buy_sell='V' THEN 1 ELSE 0 END) AS sell_n,
           SUM(CASE WHEN c.buy_sell='C' THEN 1 ELSE 0 END) AS buy_n
    FROM '{ICAB}' c JOIN '{IDET}' dd
      ON c.date=dd.date AND c.offer_code=dd.offer_code AND c.version=dd.version
    WHERE c.unit_code='{unit}'
      AND c.date BETWEEN '2024-06-14' AND '2025-03-18'
      AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    GROUP BY 1,2,3
    HAVING sell_n >= 1 AND buy_n >= 1
    ORDER BY (sell_n + buy_n) DESC LIMIT 1
    """
    df = con.execute(q).df()
    if len(df) == 0:
        return None
    r = df.iloc[0]
    return str(r["date"]), int(r["session_number"]), int(r["period"])


def fetch(unit, date, session, period, side, con):
    q = f"""
    SELECT dd.price_eur_mwh AS p, dd.quantity_mw AS q
    FROM '{ICAB}' c JOIN '{IDET}' dd
      ON c.date=dd.date AND c.offer_code=dd.offer_code AND c.version=dd.version
    WHERE c.unit_code='{unit}' AND c.buy_sell='{side}'
      AND c.date=DATE '{date}' AND c.session_number={session} AND dd.period={period}
      AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    ORDER BY dd.price_eur_mwh
    """
    return con.execute(q).df()


def step(prices, qty, ascending=True):
    order = np.argsort(prices) if ascending else np.argsort(-np.asarray(prices))
    p = np.asarray(prices)[order]; q = np.asarray(qty)[order]
    cum = np.concatenate([[0], np.cumsum(q)])
    xs, ys = [0.0], [p[0]]
    for i in range(len(p)):
        xs.extend([cum[i], cum[i+1]]); ys.extend([p[i], p[i]])
    return xs, ys


def fetch_mcp(date, session, period, con):
    q = f"""
    SELECT price_es_eur_mwh FROM '{MCPIDA}'
    WHERE date=DATE '{date}' AND session_number={session} AND period={period}
    LIMIT 1
    """
    df = con.execute(q).df()
    return float(df.iloc[0,0]) if len(df) else None


def main():
    con = duckdb.connect(); con.execute("SET threads=4")
    fig = plt.figure(figsize=(13, 7.5))
    gs = GridSpec(2, 3, figure=fig)
    for i, (unit, tech, color) in enumerate(UNITS):
        ax = fig.add_subplot(gs[i // 3, i % 3])
        result = find_period(unit, con)
        if result is None:
            ax.text(0.5, 0.5, f"(no dual-direction obs)", transform=ax.transAxes,
                    ha="center", va="center", color="gray", fontsize=10, style="italic")
            ax.set_title(f"{LABELS[tech]} - {unit}", fontsize=11, weight="bold")
            ax.set_xticks([]); ax.set_yticks([]); continue
        date, session, period = result
        sell_df = fetch(unit, date, session, period, "V", con)
        buy_df  = fetch(unit, date, session, period, "C", con)
        mcp = fetch_mcp(date, session, period, con)
        # SELL curve: ascending price (supply)
        if len(sell_df) > 0:
            xs, ys = step(sell_df["p"].values, sell_df["q"].values, ascending=True)
            ax.plot(xs, ys, color="tab:blue", lw=2.2, drawstyle="steps-pre",
                    label=f"Sell ({len(sell_df)} tr.)")
        # BUY curve: descending price (demand)
        if len(buy_df) > 0:
            xs, ys = step(buy_df["p"].values, buy_df["q"].values, ascending=False)
            ax.plot(xs, ys, color="tab:red", lw=2.2, drawstyle="steps-pre",
                    label=f"Buy ({len(buy_df)} tr.)")
        if mcp is not None:
            ax.axhline(mcp, color="black", ls="--", lw=0.8, alpha=0.6)
            ax.text(0.98, mcp + 2, f"MCP {mcp:.0f}", transform=ax.get_yaxis_transform(),
                    fontsize=8, ha="right", va="bottom", color="black")
        ax.set_title(f"{LABELS[tech]} --- {unit} (S{session} p{period}, {date})",
                     fontsize=10, weight="bold")
        ax.set_xlabel("Cumulative MW", fontsize=9)
        ax.set_ylabel("Bid price (EUR/MWh)", fontsize=9)
        ax.grid(alpha=0.3); ax.tick_params(labelsize=8)
        ax.legend(loc="best", fontsize=8, frameon=True)
        # Reasonable y limits
        all_prices = list(sell_df["p"].values) + list(buy_df["p"].values)
        if mcp is not None: all_prices.append(mcp)
        if all_prices:
            ymin, ymax = min(all_prices) - 10, max(all_prices) + 10
            # cap parked tier
            ymax = min(ymax, mcp + 80 if mcp else ymax)
            ax.set_ylim(ymin, ymax)
    fig.suptitle("Representative IDA sell vs buy bid curves by technology (pre-ID15)",
                 fontsize=12, weight="bold", y=1.00)
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(f"{OUT}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUT}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {OUT}.pdf / .png")


if __name__ == "__main__":
    main()
