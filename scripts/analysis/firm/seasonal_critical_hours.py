# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: identification refinement (season-specific critical hours)
# CLAIM: Tests whether the canonical critical-hours definition h{18-22}
#        is season-invariant. Computes hourly load and DA price profiles
#        for Q1, Q2, Q3, Q4 of 2025 separately and looks for shifts
#        in the peak hour.

from __future__ import annotations

from pathlib import Path
import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
MARGPDBC = REPO / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
FIGDIR = REPO / "figures" / "thesis"
FIGDIR.mkdir(parents=True, exist_ok=True)
OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)

SEASONS = {
    "Winter (Jan-Mar 2025)": ("2025-01-01", "2025-04-01"),
    "Spring (Apr-Jun 2025)": ("2025-04-01", "2025-07-01"),
    "Summer (Jul-Sep 2025)": ("2025-07-01", "2025-10-01"),
    "Autumn (Oct-Dec 2025)": ("2025-10-01", "2026-01-01"),
}


def hourly_profile(start, end):
    con = duckdb.connect()
    load = con.execute(f"""
        WITH t AS (
            SELECT (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   load_mw
            FROM '{LOAD}'
            WHERE isp_start_utc >= TIMESTAMP '{start}' AND isp_start_utc < TIMESTAMP '{end}'
        )
        SELECT hour, AVG(load_mw)/1000.0 AS load_gw FROM t GROUP BY 1 ORDER BY 1
    """).df()
    price = con.execute(f"""
        WITH t AS (
            SELECT date::DATE AS d, period, mtu_minutes, price_es_eur_mwh
            FROM '{MARGPDBC}'
            WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
        ),
        h AS (
            SELECT d, CASE WHEN mtu_minutes = 60 THEN period - 1 ELSE (period-1)/4 END AS hour,
                   AVG(price_es_eur_mwh) AS p
            FROM t WHERE period IS NOT NULL GROUP BY 1,2 HAVING hour BETWEEN 0 AND 23
        )
        SELECT hour, AVG(p) AS price FROM h GROUP BY 1 ORDER BY 1
    """).df()
    return load, price


def top_hours_by(df, col, n=5):
    return sorted(df.nlargest(n, col)["hour"].astype(int).tolist())


def main():
    profiles = {}
    rankings = []
    for season_name, (start, end) in SEASONS.items():
        print(f"--- {season_name} ---")
        load, price = hourly_profile(start, end)
        profiles[season_name] = (load, price)
        # Top 5 by price (canonical critical-hours method)
        top_p = top_hours_by(price, "price")
        # Top 5 by demand (raw-load method)
        top_l = top_hours_by(load, "load_gw")
        # Min 3 hours by load (flat)
        flat = sorted(load.nsmallest(3, "load_gw")["hour"].astype(int).tolist())
        rankings.append({
            "season": season_name,
            "top5_price": top_p,
            "top5_load": top_l,
            "flat3_load": flat,
            "max_price_hour": int(price.iloc[price["price"].idxmax()]["hour"]),
            "max_load_hour": int(load.iloc[load["load_gw"].idxmax()]["hour"]),
            "max_price": float(price["price"].max()),
            "max_load": float(load["load_gw"].max()),
        })
        print(f"  top-5 by PRICE: {top_p}  (peak hour: h{int(price.iloc[price['price'].idxmax()]['hour'])})")
        print(f"  top-5 by LOAD:  {top_l}  (peak hour: h{int(load.iloc[load['load_gw'].idxmax()]['hour'])})")
        print(f"  bottom-3 LOAD:  {flat}")

    df = pd.DataFrame(rankings)
    df.to_csv(OUTDIR / "seasonal_critical_hours.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'seasonal_critical_hours.csv'}")

    # 4-panel figure
    fig, axes = plt.subplots(2, 2, figsize=(13, 8), sharex=True)
    for ax, (season, (load, price)) in zip(axes.flat, profiles.items()):
        ax.plot(load["hour"], load["load_gw"], color="C0", marker="o", label="Load (GW)")
        ax.set_ylabel("Load (GW)", color="C0")
        ax.tick_params(axis="y", labelcolor="C0")
        axb = ax.twinx()
        axb.plot(price["hour"], price["price"], color="C3", marker="s", label="DA price (€/MWh)")
        axb.set_ylabel("DA price (€/MWh)", color="C3")
        axb.tick_params(axis="y", labelcolor="C3")
        # Highlight canonical critical h{18-22} and flat h{3-5}
        for h in (18, 19, 20, 21, 22):
            ax.axvspan(h-0.5, h+0.5, alpha=0.10, color="red")
        for h in (3, 4, 5):
            ax.axvspan(h-0.5, h+0.5, alpha=0.10, color="blue")
        ax.set_title(season)
        ax.set_xlabel("Hour of day (Madrid local)")
        ax.set_xticks(range(0, 24, 2))
        ax.grid(alpha=0.3)
    fig.suptitle("Hourly load and DA price by season, Spain 2025\n"
                 "Red bands: canonical critical h{18-22}.  Blue bands: canonical flat h{3-5}.",
                 fontsize=11)
    fig.tight_layout()
    out = FIGDIR / "fig_seasonal_critical_hours"
    fig.savefig(f"{out}.png", dpi=120, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"\nFigure: {out}.png / .pdf")


if __name__ == "__main__":
    main()
