# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- per-session IDA parallel-trends
#        figure for CCGT sigma_p. The pooled-IDA panel in
#        fig_parallel_trends_sigma_p hides the IDA1/IDA2/IDA3 session-
#        order pattern that NeuroDATE_II (3 Dec 2025) explicitly
#        flags: late-IDA prices are more extreme.
#
# Panels (4 stacked): DA, IDA1, IDA2, IDA3.
# IDA3 covers only hours 13-24 (afternoon), so we report critical-only
# for IDA3 (hours 16-22 = evening peak) and flag the coverage in the
# caption.
#
# OUT: figures/thesis/fig_parallel_trends_sigma_p_per_session.{pdf,png}

from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB    = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET    = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
ICAB   = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET   = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MCPDA  = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MCPIDA = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP   = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT    = REPO / "figures/thesis/fig_parallel_trends_sigma_p_per_session"

H_BAND     = 50.0
DATE_START = "2024-06-14"
DATE_END   = "2026-05-18"
ID15 = pd.Timestamp("2025-03-19")
BLK  = pd.Timestamp("2025-04-28")
DA15 = pd.Timestamp("2025-10-01")
CRIT = set(range(5, 9)) | set(range(16, 23))
FLAT = {1, 2, 3}


def compute_sigma_da(con):
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
         mcp AS (
           SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp
           FROM '{MCPDA}'
           WHERE date BETWEEN '{DATE_START}' AND '{DATE_END}'
             AND price_es_eur_mwh IS NOT NULL
         ),
         offers AS (
           SELECT CAST(c.date AS DATE) AS d, c.offer_code, c.version, c.unit_code,
                  d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
           FROM '{CAB}' c JOIN '{DET}' d
             ON c.date = d.date AND c.offer_code = d.offer_code AND c.version = d.version
           WHERE c.buy_sell = 'V'
             AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw > 0
             AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
         )
    SELECT o.d, o.unit_code, o.period,
           CASE WHEN o.period <= 24 THEN o.period
                ELSE CAST(CEIL(o.period / 4.0) AS INT) END AS hour,
           SUM(o.q) AS sw, SUM(o.q*o.p) AS swp, SUM(o.q*o.p*o.p) AS swpp,
           COUNT(*) AS n
    FROM offers o JOIN mcp m USING (d, period) JOIN u USING (unit_code)
    WHERE ABS(o.p - m.mcp) <= {H_BAND}
    GROUP BY 1, 2, 3, 4
    """
    return con.execute(q).df()


def compute_sigma_ida(con, sess):
    q = f"""
    WITH u AS (SELECT unit_code FROM '{UMAP}' WHERE tech_group = 'CCGT'),
         mcp_raw AS (
           SELECT CAST(date AS DATE) AS d, session_number, period,
                  price_es_eur_mwh AS mcp,
                  ROW_NUMBER() OVER (PARTITION BY date::DATE, session_number, period
                                      ORDER BY mtu_minutes ASC) AS rn
           FROM '{MCPIDA}'
           WHERE session_number = {sess}
             AND date BETWEEN '{DATE_START}' AND '{DATE_END}'
             AND price_es_eur_mwh IS NOT NULL
         ),
         mcp AS (SELECT d, session_number, period, mcp FROM mcp_raw WHERE rn=1),
         offers AS (
           SELECT CAST(c.date AS DATE) AS d, c.session_number AS sess, c.offer_code,
                  c.unit_code, d.period, d.price_eur_mwh AS p, d.quantity_mw AS q
           FROM '{ICAB}' c JOIN '{IDET}' d
             ON c.date = d.date AND c.session_number = d.session_number
              AND c.offer_code = d.offer_code AND c.version = d.version
           WHERE c.buy_sell = 'V'
             AND c.session_number = {sess}
             AND c.block_order_avg_price_eur IS NULL
             AND d.price_eur_mwh IS NOT NULL AND d.quantity_mw > 0
             AND c.date BETWEEN '{DATE_START}' AND '{DATE_END}'
         )
    SELECT o.d, o.unit_code, o.period,
           CASE WHEN o.period <= 24 THEN o.period
                ELSE CAST(CEIL(o.period / 4.0) AS INT) END AS hour,
           SUM(o.q) AS sw, SUM(o.q*o.p) AS swp, SUM(o.q*o.p*o.p) AS swpp,
           COUNT(*) AS n
    FROM offers o JOIN mcp m ON o.d = m.d AND o.sess = m.session_number
                            AND o.period = m.period
    JOIN u USING (unit_code)
    WHERE ABS(o.p - m.mcp) <= {H_BAND}
    GROUP BY 1, 2, 3, 4
    """
    return con.execute(q).df()


def to_sigma(df):
    df = df.copy()
    p_bar = df["swp"] / df["sw"]
    var_p = (df["swpp"] - df["swp"] * df["swp"] / df["sw"]) / df["sw"]
    df["sigma_p"] = np.sqrt(np.clip(var_p, 0, None))
    return df


def weekly(df):
    df = df.copy()
    df["d"] = pd.to_datetime(df["d"])
    df["week"] = df["d"] - pd.to_timedelta(df["d"].dt.weekday, unit="D")
    df["hour_class"] = df["hour"].apply(
        lambda h: "critical" if h in CRIT else ("flat" if h in FLAT else None))
    df = df.dropna(subset=["hour_class"])
    return (df.groupby(["week", "hour_class"], observed=True)["sigma_p"]
              .median().reset_index())


def plot_panel(ax, w, title, show_legend=False):
    has_data = False
    for cls, col in [("critical", "tab:red"), ("flat", "tab:blue")]:
        sub = w[w["hour_class"] == cls].sort_values("week")
        if len(sub) == 0:
            continue
        ax.plot(sub["week"], sub["sigma_p"], lw=1.5, color=col,
                label=cls.capitalize(), marker="o", markersize=2.0)
        has_data = True
    for d, label, color in [(ID15, "ID15", "tab:purple"),
                             (BLK,  "Blackout", "tab:gray"),
                             (DA15, "DA15", "tab:green")]:
        ax.axvline(d, color=color, ls="--", lw=0.9, alpha=0.7)
        ymax = ax.get_ylim()[1] if ax.get_ylim()[1] > 0 else 1.0
        ax.text(d, ymax * 0.97, "  " + label,
                rotation=90, va="top", ha="left", fontsize=7, color=color)
    ax.set_title(title, fontsize=10, weight="bold")
    ax.set_ylabel(r"$\sigma_p$ (EUR/MWh)", fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(labelsize=7)
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    if show_legend and has_data:
        ax.legend(loc="best", fontsize=7, frameon=False)


def main():
    con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='6GB'")
    print("Fetching DA ...", flush=True)
    da = weekly(to_sigma(compute_sigma_da(con)))
    print(f"  DA weeks: {len(da):,}", flush=True)
    ida = {}
    for sess in (1, 2, 3):
        print(f"Fetching IDA{sess} ...", flush=True)
        ida[sess] = weekly(to_sigma(compute_sigma_ida(con, sess)))
        print(f"  IDA{sess} weeks: {len(ida[sess]):,}", flush=True)
    con.close()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(4, 1, figsize=(11.5, 11), sharex=True)
    plot_panel(axes[0], da, "Day-ahead", show_legend=True)
    plot_panel(axes[1], ida[1], "IDA1 (intraday auction 1)")
    plot_panel(axes[2], ida[2], "IDA2 (intraday auction 2)")
    plot_panel(axes[3], ida[3],
               "IDA3 (intraday auction 3, covers hours 13--24; critical only)")
    axes[-1].set_xlabel("Week", fontsize=9)
    fig.tight_layout()
    plt.savefig(f"{OUT}.pdf", bbox_inches="tight")
    plt.savefig(f"{OUT}.png", bbox_inches="tight", dpi=130)
    plt.close(fig)
    print(f"saved {OUT}.pdf")


if __name__ == "__main__":
    main()
