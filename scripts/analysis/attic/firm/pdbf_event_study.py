# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B10, B11, F23 — visual time-series evidence
# CLAIM: Monthly bilateral volume by Big-4 firm with reform-date and blackout
#        markers shows the regulatory shocks (Rule 28.8 elimination 2025-03-19,
#        blackout 2025-04-28) cleanly; reinforces B11 and F23 with event-study
#        visualisation.
"""PDBF event-study: monthly bilateral volume by Big-4 with regime markers."""
from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_CSV = PROJECT / "results" / "regressions" / "pdbf_event_study_monthly.csv"
OUT_PNG = PROJECT / "figures" / "working" / "pdbf_event_study.png"

REFORM_DATES = {
    "6→3 IDA":           "2024-06-14",
    "ISP15":             "2024-12-01",
    "MTU15-IDA + Rule 28.8 elim": "2025-03-19",
    "Blackout":          "2025-04-28",
    "MTU15-DA":          "2025-10-01",
}
BIG4 = ["IB", "GE", "GN", "HC"]
COLORS = {"IB": "tab:blue", "GE": "tab:orange", "GN": "tab:green", "HC": "tab:red"}


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")

    # Unit→firm mapping
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    con.register("uf", firms[["unit_code", "firm"]])

    # Monthly bilateral + auction sell volumes by firm
    monthly = con.execute(f"""
        SELECT date_trunc('month', CAST(p.date AS DATE)) AS month, uf.firm,
               SUM(CASE WHEN p.offer_type = 4
                        THEN ABS(p.assigned_power_mw) * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_abs_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_sell_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2 ORDER BY 1, 2
    """).df()
    monthly["bilateral_TWh"] = monthly["bilateral_abs_mwh"] / 1e6
    monthly["auction_TWh"]   = monthly["auction_sell_mwh"]   / 1e6
    monthly["total_TWh"]     = monthly["bilateral_TWh"] + monthly["auction_TWh"]
    monthly["bilat_share"]   = monthly["bilateral_TWh"] / monthly["total_TWh"]
    monthly.to_csv(OUT_CSV, index=False)
    print(f"wrote {OUT_CSV}")

    # Plot
    fig, axes = plt.subplots(2, 2, figsize=(13, 8.5), sharex=True)
    titles = {
        "bilateral_TWh": "Monthly bilateral volume (TWh) — Big-4",
        "auction_TWh":   "Monthly auction-cleared sell volume (TWh) — Big-4",
        "total_TWh":     "Monthly total sell volume (TWh) — Big-4",
        "bilat_share":   "Bilateral share of total sell volume (%) — Big-4",
    }
    panels = [
        (0, 0, "bilateral_TWh"),
        (0, 1, "auction_TWh"),
        (1, 0, "total_TWh"),
        (1, 1, "bilat_share"),
    ]
    for r, c, col in panels:
        ax = axes[r][c]
        for f in BIG4:
            sub = monthly[monthly.firm == f].sort_values("month")
            y = sub[col] * (100 if col == "bilat_share" else 1)
            ax.plot(sub["month"], y, label=f, color=COLORS[f], lw=1.6)
        for label, dt in REFORM_DATES.items():
            ax.axvline(pd.Timestamp(dt), color="grey", lw=0.8, ls="--", alpha=0.7)
        ax.set_title(titles[col], fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        if col == "bilat_share":
            ax.set_ylabel("%")
            ax.set_ylim(0, 100)
        else:
            ax.set_ylabel("TWh / month")
        if r == 1 and c == 0:
            ax.legend(loc="upper left", fontsize=9)

    # Annotate reform-date labels on the top-left panel only
    ax_top = axes[0][0]
    ymax = ax_top.get_ylim()[1]
    for label, dt in REFORM_DATES.items():
        ax_top.annotate(label, xy=(pd.Timestamp(dt), ymax * 0.97),
                        rotation=90, va="top", ha="right", fontsize=7,
                        color="dimgrey", alpha=0.85)

    fig.suptitle("Big-4 bilateral vs auction commitment volumes — PDBF panel 2018-2026",
                 fontsize=12, weight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.97])
    OUT_PNG.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUT_PNG, dpi=150, bbox_inches="tight")
    print(f"wrote {OUT_PNG}")

    # Print summary stats around the 2025-03-19 break
    print("\n=== 6-month windows around the 2025-03-19 break ===")
    pre = monthly[(monthly.month >= "2024-09-01") & (monthly.month < "2025-03-01")]
    post = monthly[(monthly.month >= "2025-03-01") & (monthly.month < "2025-09-01")]
    pre_g = pre.groupby("firm")[["bilateral_TWh", "auction_TWh"]].mean()
    post_g = post.groupby("firm")[["bilateral_TWh", "auction_TWh"]].mean()
    delta = (post_g - pre_g) / pre_g * 100
    print("\nPRE (Sep 2024 – Feb 2025) mean monthly volumes (TWh):")
    print(pre_g.round(2).to_string())
    print("\nPOST (Mar – Aug 2025) mean monthly volumes (TWh):")
    print(post_g.round(2).to_string())
    print("\nΔ% (POST − PRE) / PRE:")
    print(delta.round(1).to_string())


if __name__ == "__main__":
    main()
