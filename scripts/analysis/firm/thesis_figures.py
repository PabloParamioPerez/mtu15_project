# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §3.4, §5.1
# CLAIM: Produces the priority thesis figures from existing results:
#   F1: hourly load + DA price + VRE profile (§3.4 calibrates critical hours)
#   F2: tech-stratified β₃ coefficient plot from B1 (§5.1)
#   F3: q_2 trajectory by firm × hour-class same-cal-month (§5.1)

from __future__ import annotations

from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

REPO = Path(__file__).resolve().parents[3]
THESIS_FIG = REPO / "figures" / "thesis"
THESIS_FIG.mkdir(parents=True, exist_ok=True)

LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
MARGPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
B1_OUT = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "B1_q2_did.csv"
B1_PANEL = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis" / "B1_panel.parquet"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)  # canonical: demand surge ∪ VRE transition
FLAT_HOURS = (1, 2, 3)


def fig_calibration():
    """F1: hourly load + price + VRE profile, Oct-Dec 2025 average.
    Highlights critical h{18-22} and flat h{3-5} bands."""
    con = duckdb.connect()
    print("F1: calibration figure...")
    load_df = con.execute(f"""
        WITH t AS (
            SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   load_mw
            FROM '{LOAD}'
            WHERE isp_start_utc >= TIMESTAMP '2025-10-01'
              AND isp_start_utc <  TIMESTAMP '2026-01-01'
        )
        SELECT hour, AVG(load_mw)/1000.0 AS load_gw FROM t GROUP BY 1 ORDER BY 1
    """).df()

    price_df = con.execute(f"""
        WITH t AS (
            SELECT date::DATE AS d, period, mtu_minutes, price_es_eur_mwh
            FROM '{MARGPDBC}'
            WHERE date::DATE >= DATE '2025-10-01' AND date::DATE < DATE '2026-01-01'
        ),
        h AS (
            SELECT d, CASE WHEN mtu_minutes = 60 THEN period - 1
                          ELSE (period - 1) // 4 END AS hour,
                   AVG(price_es_eur_mwh) AS p
            FROM t WHERE period IS NOT NULL GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        )
        SELECT hour, AVG(p) AS price FROM h GROUP BY 1 ORDER BY 1
    """).df()

    vre_df = con.execute(f"""
        WITH per_isp AS (
            SELECT isp_start_utc,
                   (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   SUM(CASE WHEN psr_type='B16' THEN quantity_mw ELSE 0 END) AS solar_mw,
                   SUM(CASE WHEN psr_type IN ('B18','B19') THEN quantity_mw ELSE 0 END) AS wind_mw
            FROM '{WIND_SOLAR}'
            WHERE isp_start_utc >= TIMESTAMP '2025-10-01'
              AND isp_start_utc <  TIMESTAMP '2026-01-01'
            GROUP BY 1,2,3
        ),
        per_day_hour AS (
            SELECT d, hour, AVG(solar_mw) AS solar_mw, AVG(wind_mw) AS wind_mw
            FROM per_isp GROUP BY 1,2
        )
        SELECT hour,
               AVG(solar_mw)/1000.0 AS solar_gw,
               AVG(wind_mw)/1000.0 AS wind_gw
        FROM per_day_hour GROUP BY 1 ORDER BY 1
    """).df()

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(9, 6), sharex=True)

    # Top panel: load + price
    color_load = "C0"
    ax1.plot(load_df["hour"], load_df["load_gw"], marker="o", color=color_load, label="Spanish load (GW)")
    ax1.set_ylabel("Load (GW)", color=color_load)
    ax1.tick_params(axis="y", labelcolor=color_load)
    ax1.grid(alpha=0.3)
    ax1b = ax1.twinx()
    color_p = "C3"
    ax1b.plot(price_df["hour"], price_df["price"], marker="s", color=color_p, linewidth=1.5, label="DA clearing price (€/MWh)")
    ax1b.set_ylabel("DA price (€/MWh)", color=color_p)
    ax1b.tick_params(axis="y", labelcolor=color_p)
    # shading
    for h in CRITICAL_HOURS:
        ax1.axvspan(h-0.5, h+0.5, alpha=0.15, color="red")
    for h in FLAT_HOURS:
        ax1.axvspan(h-0.5, h+0.5, alpha=0.15, color="blue")
    ax1.set_title("Hourly average load and DA clearing price, Spain Oct–Dec 2025")
    ax1.text(0.99, 0.05, "red = critical h{5–7, 16–19}\nblue = flat h{1–3}",
             transform=ax1.transAxes, ha="right", fontsize=8, va="bottom",
             bbox=dict(boxstyle="round", facecolor="white", alpha=0.7))

    # Bottom panel: VRE
    ax2.plot(vre_df["hour"], vre_df["wind_gw"], marker="o", color="green", label="Wind (GW)")
    ax2.plot(vre_df["hour"], vre_df["solar_gw"], marker="s", color="orange", label="Solar (GW)")
    ax2.set_ylabel("VRE (GW)")
    ax2.set_xlabel("Hour of day (Madrid local)")
    ax2.legend()
    ax2.grid(alpha=0.3)
    for h in CRITICAL_HOURS:
        ax2.axvspan(h-0.5, h+0.5, alpha=0.15, color="red")
    for h in FLAT_HOURS:
        ax2.axvspan(h-0.5, h+0.5, alpha=0.15, color="blue")
    ax2.set_title("Hourly average wind and solar production")
    ax2.set_xticks(range(0, 24, 2))

    fig.tight_layout()
    out = THESIS_FIG / "fig_critical_hours_calibration"
    fig.savefig(f"{out}.png", dpi=120, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}.png / .pdf")


def fig_tech_beta3():
    """F2: tech-stratified β₃ coefficient plot from B1.
    Shows the dispatchable / non-dispatchable split."""
    print("F2: tech-stratified β₃ plot...")
    df = pd.read_csv(B1_OUT)
    # Treatment group, by tech
    sub = df[df["label"].str.startswith("treatment_") & ~df["label"].isin(["treatment_only"])].copy()
    sub["tech"] = sub["label"].str.replace("treatment_", "")
    # Filter to clean techs (drop spurious Solar PV result)
    sub = sub[sub["tech"].isin(["CCGT","Hydro","Hydro_pump","Coal","Nuclear","Wind","Biomass"])].copy()
    sub["beta_3"] = sub["beta_3"].astype(float)
    sub["se"] = sub["se"].astype(float)
    sub["ci_lo"] = sub["beta_3"] - 1.96 * sub["se"]
    sub["ci_hi"] = sub["beta_3"] + 1.96 * sub["se"]
    sub = sub.sort_values("beta_3")

    # Tech category coloring
    dispatchable = {"CCGT","Hydro","Hydro_pump","Coal"}
    must_run = {"Nuclear"}
    nondisp = {"Wind","Solar PV","Biomass"}
    def tech_color(t):
        if t in dispatchable: return "C3"
        if t in must_run: return "C1"
        return "C0"
    colors = sub["tech"].map(tech_color).values

    fig, ax = plt.subplots(figsize=(9, 5))
    ypos = np.arange(len(sub))
    ax.errorbar(sub["beta_3"], ypos, xerr=1.96*sub["se"], fmt="none", ecolor="gray", capsize=3)
    ax.scatter(sub["beta_3"], ypos, c=colors, s=80, zorder=3)
    ax.axvline(0, color="black", linewidth=0.8)
    ax.set_yticks(ypos)
    ax.set_yticklabels(sub["tech"])
    ax.set_xlabel(r"$\beta_3$: critical $\times$ post DiD coefficient on $q_2$ (MWh / unit-clock-hour)")
    ax.set_title("Treatment-effect heterogeneity by technology\n(treatment group: pivotal firms only, same-cal-month Oct-Dec 2024 vs Oct-Dec 2025)")
    ax.grid(axis="x", alpha=0.3)
    # Legend
    handles = [
        plt.Line2D([0],[0], marker="o", color="w", markerfacecolor="C3", markersize=10, label="dispatchable strategic"),
        plt.Line2D([0],[0], marker="o", color="w", markerfacecolor="C1", markersize=10, label="must-run"),
        plt.Line2D([0],[0], marker="o", color="w", markerfacecolor="C0", markersize=10, label="price-taker / VRE"),
    ]
    ax.legend(handles=handles, loc="lower right", fontsize=8)
    fig.tight_layout()
    out = THESIS_FIG / "fig_tech_stratified_beta3"
    fig.savefig(f"{out}.png", dpi=120, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}.png / .pdf")


def fig_q2_trajectory():
    """F3: q_2 monthly trajectory by firm × hour-class for the two same-cal-month windows."""
    print("F3: q_2 same-cal-month trajectory by firm × hour-class...")
    panel = pd.read_parquet(B1_PANEL)
    panel["d"] = pd.to_datetime(panel["d"])
    panel["year_month"] = panel["d"].dt.to_period("M").dt.to_timestamp()
    # Aggregate per (parent, year_month, hour_class)
    agg = (panel.groupby(["parent","year_month","hour_class"])["q2_mwh_clock_hour"]
           .mean().reset_index())

    fig, axes = plt.subplots(2, 3, figsize=(13, 6.5), sharex=True, sharey=False)
    for ax, parent in zip(axes.flat, ["IB","GE","GN","HC","Repsol","TotalEnergies"]):
        sub = agg[agg["parent"] == parent]
        for hc, color, marker, label in [
            ("critical_canonical", "C3", "o", "critical h{5-7,16-19}"),
            ("flat_canonical", "C0", "s", "flat h{1-3}"),
        ]:
            s = sub[sub["hour_class"]==hc].sort_values("year_month")
            if len(s):
                ax.plot(s["year_month"], s["q2_mwh_clock_hour"], color=color, marker=marker, label=label, linewidth=1.3)
        ax.set_title(parent)
        ax.set_ylabel("$q_2$ MWh / unit-clock-hour")
        ax.tick_params(axis="x", rotation=45, labelsize=7)
        ax.grid(alpha=0.3)
        ax.axhline(0, color="black", linewidth=0.5)
        if parent == "IB":
            ax.legend(fontsize=7)
    fig.suptitle("$q_2$ monthly mean by firm × hour-class (same-cal-month windows)",
                 fontsize=11)
    fig.tight_layout()
    out = THESIS_FIG / "fig_q2_samecal_trajectory"
    fig.savefig(f"{out}.png", dpi=110, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"  saved: {out}.png / .pdf")


def main():
    fig_calibration()
    fig_tech_beta3()
    fig_q2_trajectory()
    print("\nDone.")


if __name__ == "__main__":
    main()
