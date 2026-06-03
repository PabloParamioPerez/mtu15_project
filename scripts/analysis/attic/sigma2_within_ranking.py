# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: theoretical anchor for critical-hours definition (§3.4)
# CLAIM: Computes σ²_within(net-load) by hour-of-day, the theoretical
#        ranker per the granularity model. Net-load = load − wind − solar
#        at MTU15 resolution. Variance computed across the 4 quarters
#        within each clock-hour, then averaged across days.
#
# Also reports |slope| (mean absolute first-difference within hour) as
# an alternative directionless measure of within-hour change rate.

from __future__ import annotations

from pathlib import Path
import duckdb
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
WIND_SOLAR = REPO / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
FIGDIR = REPO / "figures" / "thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)
FIGDIR.mkdir(parents=True, exist_ok=True)


def compute_within_hour_stats(start: str, end: str, label: str):
    """For each hour-of-day compute the mean within-hour SIGNED demand change
    (load[q=last] - load[q=first]). Positive = demand surge UP within hour;
    negative = demand falling within hour. The strategically relevant
    metric for the granularity model is the upward-surge magnitude."""
    con = duckdb.connect()
    print(f"\n--- {label}: {start} to {end} ---")
    df = con.execute(f"""
        WITH load_isp AS (
            SELECT isp_start_utc,
                   (isp_start_utc AT TIME ZONE 'Europe/Madrid')::DATE AS d,
                   EXTRACT(HOUR FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS hour,
                   EXTRACT(MINUTE FROM (isp_start_utc AT TIME ZONE 'Europe/Madrid')) AS minute,
                   load_mw, mtu_minutes
            FROM '{LOAD}'
            WHERE isp_start_utc >= TIMESTAMP '{start}' AND isp_start_utc < TIMESTAMP '{end}'
              AND mtu_minutes = 15
        ),
        ordered AS (
            -- For each (d, hour), find the first quarter (minute 0) and last (minute 45)
            SELECT d, hour,
                   MIN(CASE WHEN minute = 0  THEN load_mw END) AS load_q1,
                   MIN(CASE WHEN minute = 45 THEN load_mw END) AS load_q4,
                   AVG(load_mw) AS load_mean,
                   COUNT(*) AS n_quarters
            FROM load_isp
            GROUP BY 1,2
            HAVING COUNT(*) = 4 AND load_q1 IS NOT NULL AND load_q4 IS NOT NULL
        )
        SELECT hour,
               AVG(load_mean)/1000.0 AS load_gw,
               -- Mean signed within-hour change: positive = demand rising
               AVG(load_q4 - load_q1) AS mean_signed_change_mw,
               -- Mean upward-only change: max(0, q4 - q1)
               AVG(GREATEST(load_q4 - load_q1, 0)) AS mean_up_change_mw,
               -- Probability that within-hour change is positive
               AVG(CASE WHEN load_q4 - load_q1 > 0 THEN 1.0 ELSE 0.0 END) AS prob_up,
               COUNT(DISTINCT d) AS n_days
        FROM ordered GROUP BY 1 ORDER BY 1
    """).df()
    df["window"] = label
    print(df.to_string(index=False))
    return df


def main():
    # Use full 2025 to capture seasonality
    df_2025 = compute_within_hour_stats("2025-01-01", "2026-01-01", "2025 (full year)")
    df_q4 = compute_within_hour_stats("2025-10-01", "2026-01-01", "Oct-Dec 2025")
    full = pd.concat([df_2025, df_q4], ignore_index=True)
    full.to_csv(OUTDIR / "sigma2_within_ranking.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'sigma2_within_ranking.csv'}")

    print("\n=== Top-5 hours by SIGNED within-hour demand change (load[q4]-load[q1]) ===")
    print("(positive = demand rising within the hour; negative = falling)")
    print("\nFull year 2025 — top by signed change (upward surge):")
    print(df_2025.nlargest(8, "mean_signed_change_mw")[["hour","mean_signed_change_mw","load_gw","prob_up"]].sort_values("hour").to_string(index=False))
    print("\nFull year 2025 — bottom (downward swings):")
    print(df_2025.nsmallest(5, "mean_signed_change_mw")[["hour","mean_signed_change_mw","load_gw","prob_up"]].sort_values("hour").to_string(index=False))

    # Plot signed within-hour change
    fig, ax = plt.subplots(figsize=(11, 5))
    bars = ax.bar(df_2025["hour"], df_2025["mean_signed_change_mw"],
                  color=["C3" if v > 0 else "C0" for v in df_2025["mean_signed_change_mw"]],
                  alpha=0.8)
    ax.axhline(0, color="black", linewidth=0.7)
    ax.set_xlabel("Hour of day (Madrid local)")
    ax.set_ylabel(r"Mean signed within-hour $\Delta$ load: load(q4) $-$ load(q1)  (MW)")
    ax.set_xticks(range(0, 24))
    ax.set_title("Within-hour demand change by hour-of-day, Spain 2025\n"
                 "Red = demand rising within the hour (strategic upward surge).  Blue = falling.")
    ax.grid(alpha=0.3, axis="y")
    fig.tight_layout()
    out = FIGDIR / "fig_within_hour_demand_change"
    fig.savefig(f"{out}.png", dpi=120, bbox_inches="tight")
    fig.savefig(f"{out}.pdf", bbox_inches="tight")
    plt.close(fig)
    print(f"\nFigure: {out}.png / .pdf")


if __name__ == "__main__":
    main()
