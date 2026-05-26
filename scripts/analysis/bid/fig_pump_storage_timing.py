# STATUS: ALIVE
# LAST-AUDIT: 2026-05-25
# FEEDS: advisor_memo.tex sec 4 -- the key figure for the pump-storage
#        within-day arbitrage finding under MTU15-DA same-calendar.
#
# CLAIM: Recompute mean DA-cleared pump-storage MW by (hour-class, pre/post)
#        from pdbc_all.parquet under the same-calendar windows
#        (Oct-Dec 2024 vs Oct-Dec 2025) and plot as grouped bars: one panel
#        for generation, one for pumping. The visual shows the timing flip
#        without changing total daily energy.
#
# OUT: figures/working/fig_pump_storage_timing.pdf

from pathlib import Path
import sys

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "scripts/analysis/bid"))
from mtu15_critical_flat_did import CRITICAL, FLAT, hour_class_label  # noqa: E402

PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
OUT = REPO / "figures/working/fig_pump_storage_timing.pdf"
OUT.parent.mkdir(parents=True, exist_ok=True)

SAMECAL = {
    "pre": (pd.Timestamp("2024-10-01"), pd.Timestamp("2024-12-31")),
    "post": (pd.Timestamp("2025-10-01"), pd.Timestamp("2025-12-31")),
}


def load_pump_panel(lo, hi):
    units = pd.read_csv(UNITS)
    units = units[units["technology"].str.lower().str.contains("bombeo", na=False)][
        ["unit_code"]
    ].drop_duplicates()
    con = duckdb.connect()
    con.register("u", units)
    sql = f"""
    SELECT CAST(p.date AS DATE) d, p.period,
           SUM(CASE WHEN p.assigned_power_mw > 0 THEN p.assigned_power_mw ELSE 0 END) AS gen,
           SUM(CASE WHEN p.assigned_power_mw < 0 THEN -p.assigned_power_mw ELSE 0 END) AS pump,
           COALESCE(p.mtu_minutes, 60) mtu
    FROM '{PDBC}' p JOIN u ON p.unit_code = u.unit_code
    WHERE p.date BETWEEN '{lo.date()}' AND '{hi.date()}'
    GROUP BY 1, p.period, mtu
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(
        df["mtu"] == 60, df["period"] - 1, ((df["period"] - 1) // 4).astype(int)
    )
    df["hour_class"] = df["clock_hour"].map(hour_class_label)
    return df[df["hour_class"].isin(["Critical", "Flat"])].copy()


def main():
    rows = []
    for arm, (lo, hi) in SAMECAL.items():
        panel = load_pump_panel(lo, hi)
        for hc in ["Flat", "Critical"]:
            sub = panel[panel["hour_class"] == hc]
            rows.append({
                "arm": arm, "hc": hc,
                "gen": sub["gen"].mean(),
                "pump": sub["pump"].mean(),
            })
    df = pd.DataFrame(rows)
    print(df)

    fig, axes = plt.subplots(1, 2, figsize=(7.0, 2.8), sharey=True)
    width = 0.36
    x = np.array([0, 1])
    colors_pre = "#888888"
    colors_post = "#1f77b4"

    for ax, outcome, title in [(axes[0], "gen", "Generation (sell)"),
                                (axes[1], "pump", "Pumping (buy)")]:
        pre = df[df["arm"] == "pre"].set_index("hc").loc[["Flat", "Critical"], outcome].values
        post = df[df["arm"] == "post"].set_index("hc").loc[["Flat", "Critical"], outcome].values
        ax.bar(x - width / 2, pre, width, color=colors_pre,
               label="Pre MTU15-DA (Oct--Dec 2024)")
        ax.bar(x + width / 2, post, width, color=colors_post,
               label="Post MTU15-DA (Oct--Dec 2025)")
        for xi, p_pre, p_post in zip(x, pre, post):
            ax.text(xi - width / 2, p_pre + 25, f"{p_pre:.0f}", ha="center", fontsize=8)
            ax.text(xi + width / 2, p_post + 25, f"{p_post:.0f}", ha="center", fontsize=8)
        ax.set_xticks(x)
        ax.set_xticklabels(["Flat hours\n(overnight)", "Critical hours\n(ramp + peak)"],
                           fontsize=9)
        ax.set_title(title, fontsize=10)
        ax.set_ylim(0, max(pre.max(), post.max()) * 1.22)
        ax.spines[["top", "right"]].set_visible(False)
        ax.tick_params(axis="y", labelsize=8)

    axes[0].set_ylabel("Mean DA cleared power (MW)", fontsize=9)
    axes[0].legend(loc="upper left", frameon=False, fontsize=8)
    fig.suptitle(
        "Pump-storage critical-hour generation rises disproportionately after MTU15-DA",
        fontsize=10, y=1.04,
    )
    fig.tight_layout()
    fig.savefig(OUT, bbox_inches="tight")
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
