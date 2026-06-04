# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: thesis/paper/thesis.tex §6.2 (production-unit arbitrageurs).
#        Two-panel figure: sell-share = sell_MW / (sell_MW + buy_MW) per
#        (date, tech) for DA (top panel) and IDA (bottom panel), weekly
#        smoothed, with vertical lines at ID15, blackout, DA15.
#
# Also prints a compact %-dual sessions table per (market, tech) restricted
# to the same window (no more 2018-2026 spread).
#
# OUT: figures/thesis/fig_sell_share_over_time.{pdf,png}
#      results/regressions/bid/mtu15_critical_flat/pct_dual_focused.csv

from pathlib import Path
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
CAB  = REPO / "data/processed/omie/mercado_diario/ofertas/cab_all.parquet"
DET  = REPO / "data/processed/omie/mercado_diario/ofertas/det_all.parquet"
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"

OUT_FIG = REPO / "figures/thesis/fig_sell_share_over_time"
OUT_CSV = REPO / "results/regressions/bid/mtu15_critical_flat/pct_dual_focused.csv"

DATE_LO = "2024-06-14"   # post-IDA-reform (matches Spec A long pre-window)
DATE_HI = "2026-04-30"
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Nuclear", "Solar PV", "Wind"]
TECH_LABEL = {"CCGT":"CCGT", "Hydro":"Hydro", "Hydro_pump":"Pump-storage",
              "Nuclear":"Nuclear", "Solar PV":"Solar PV", "Wind":"Wind"}
TECH_COLOR = {"CCGT":"tab:red", "Hydro":"tab:blue", "Hydro_pump":"tab:cyan",
              "Nuclear":"tab:purple", "Solar PV":"tab:orange", "Wind":"tab:green"}

REFORMS = [
    ("2024-12-11", "ISP15", "gray"),
    ("2025-03-19", "ID15",  "tab:purple"),
    ("2025-04-28", "blackout", "black"),
    ("2025-10-01", "DA15",  "tab:green"),
]


def build(market, cab_path, det_path):
    con = duckdb.connect(); con.execute("SET threads=4; SET memory_limit='6GB'")
    q = f"""
    WITH u AS (SELECT unit_code, tech_group FROM '{UMAP}')
    SELECT CAST(c.date AS DATE) AS d, u.tech_group AS tech, c.buy_sell,
           SUM(dd.quantity_mw) AS sum_mw
    FROM '{cab_path}' c JOIN '{det_path}' dd
      ON c.date = dd.date AND c.offer_code = dd.offer_code AND c.version = dd.version
    JOIN u ON c.unit_code = u.unit_code
    WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
      AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
    GROUP BY 1, 2, 3
    """
    df = con.execute(q).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    p = df.pivot_table(index=["d","tech"], columns="buy_sell",
                       values="sum_mw", fill_value=0).reset_index()
    p["sell_mw"] = p.get("V", 0); p["buy_mw"] = p.get("C", 0)
    p["sell_share"] = p["sell_mw"] / (p["sell_mw"] + p["buy_mw"]).replace(0, pd.NA)
    p["market"] = market
    return p[["d","market","tech","sell_mw","buy_mw","sell_share"]]


print("Building DA panel ..."); da  = build("DA",  CAB,  DET)
print("Building IDA panel ..."); ida = build("IDA", ICAB, IDET)


# === % dual sessions table (focused window) ===
def pct_dual(cab_path, market):
    con = duckdb.connect(); con.execute("SET threads=4")
    if market == "IDA":
        sess_q = f"""
        SELECT c.date, c.session_number, c.unit_code, u.tech_group AS tech,
               SUM(CASE WHEN c.buy_sell='V' THEN 1 ELSE 0 END) AS n_sell,
               SUM(CASE WHEN c.buy_sell='C' THEN 1 ELSE 0 END) AS n_buy
        FROM '{cab_path}' c JOIN '{UMAP}' u ON c.unit_code=u.unit_code
        WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        GROUP BY c.date, c.session_number, c.unit_code, u.tech_group
        """
    else:
        sess_q = f"""
        SELECT c.date, c.unit_code, u.tech_group AS tech,
               SUM(CASE WHEN c.buy_sell='V' THEN 1 ELSE 0 END) AS n_sell,
               SUM(CASE WHEN c.buy_sell='C' THEN 1 ELSE 0 END) AS n_buy
        FROM '{cab_path}' c JOIN '{UMAP}' u ON c.unit_code=u.unit_code
        WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
        GROUP BY c.date, c.unit_code, u.tech_group
        """
    q = f"""
    WITH sessions AS ({sess_q})
    SELECT tech, COUNT(*) AS n_sessions,
           SUM(CASE WHEN n_sell > 0 AND n_buy > 0 THEN 1 ELSE 0 END) AS n_dual,
           100.0 * SUM(CASE WHEN n_sell > 0 AND n_buy > 0 THEN 1 ELSE 0 END) / COUNT(*) AS pct_dual
    FROM sessions WHERE tech IN ({','.join("'"+t+"'" for t in TECHS)})
    GROUP BY 1
    """
    df = con.execute(q).fetchdf()
    df["market"] = market
    return df

pct_da  = pct_dual(CAB,  "DA")
pct_ida = pct_dual(ICAB, "IDA")
pct = pd.concat([pct_da, pct_ida], ignore_index=True)
pct.to_csv(OUT_CSV, index=False)
print(f"\nsaved {OUT_CSV}")
print("\n=== % dual sessions, window 2024-06-14 to 2026-04-30 ===\n")
print(pct.pivot_table(index="tech", columns="market",
                     values="pct_dual").round(1).reindex(TECHS).to_string())


# 21-day rolling mean per tech
def smooth(df):
    df = df.copy().sort_values(["tech","d"])
    df["sell_share_smooth"] = (df.groupby("tech")["sell_share"]
                                  .transform(lambda x: x.rolling(21, min_periods=7).mean()))
    return df


ida_w = smooth(ida)

# === Figure: IDA only with %-dual mini-table embedded below ===
from matplotlib.gridspec import GridSpec
fig = plt.figure(figsize=(12, 6.0))
gs = GridSpec(2, 1, height_ratios=[6, 1], hspace=0.55, figure=fig)
ax = fig.add_subplot(gs[0])
for tech in TECHS:
    sub = ida_w[ida_w["tech"] == tech].sort_values("d")
    ax.plot(sub["d"], sub["sell_share_smooth"], lw=1.6,
            color=TECH_COLOR[tech], label=TECH_LABEL[tech])
for date_str, label, color in REFORMS:
    d = pd.to_datetime(date_str)
    ax.axvline(d, color=color, ls="--", lw=0.9, alpha=0.7)
    ax.text(d, 1.04, label, rotation=0, va="bottom", ha="center",
            fontsize=8, color=color, weight="bold")
ax.set_title("Intraday-auction sell-share (21-day rolling mean): "
              "$\\mathrm{sell\\;MW}/(\\mathrm{sell\\;MW}+\\mathrm{buy\\;MW})$",
              fontsize=11, weight="bold")
ax.set_ylabel("Sell-share", fontsize=10)
ax.set_xlabel("Date", fontsize=10)
ax.set_ylim(-0.02, 1.08)
ax.grid(alpha=0.3)
ax.tick_params(labelsize=9)
ax.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
ax.legend(loc="lower center", ncol=6, fontsize=9, frameon=True,
          bbox_to_anchor=(0.5, -0.32))

# Compact %-dual table below the plot (one row, one column per tech)
pct_lookup = dict(zip(pct_ida["tech"], pct_ida["pct_dual"]))
tax = fig.add_subplot(gs[1])
tax.axis("off")
col_labels = [TECH_LABEL[t] for t in TECHS]
row_values = [[f"{pct_lookup[t]:.1f}\\%" if t in pct_lookup else "--" for t in TECHS]]
tbl = tax.table(cellText=row_values, colLabels=col_labels,
                rowLabels=["IDA \\% dual sessions"],
                loc="center", cellLoc="center", colLoc="center")
tbl.auto_set_font_size(False)
tbl.set_fontsize(9)
tbl.scale(1.0, 1.4)
# Colour the column-header cells per-tech to match the line colours
for j, t in enumerate(TECHS):
    cell = tbl[(0, j)]
    cell.set_facecolor(TECH_COLOR[t])
    cell.set_text_props(color="white", weight="bold")

OUT_FIG.parent.mkdir(parents=True, exist_ok=True)
plt.savefig(f"{OUT_FIG}.pdf", bbox_inches="tight")
plt.savefig(f"{OUT_FIG}.png", bbox_inches="tight", dpi=130)
plt.close(fig)
print(f"saved {OUT_FIG}.pdf / .png")

