# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Example-day plot of residual demand RD = load - wind - solar against the
#        OMIE day-ahead clearing price MCP, plus the per-window OLS slope of
#        MCP on RD restricted to MCP within a bandwidth of its window-mean.
#
# OUT:
#   figures/working/fig_residual_demand_example.pdf
#   results/regressions/bid/mtu15_critical_flat/residual_demand_slope_summary.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
GEN  = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"

FIG_OUT = REPO / "figures/working/fig_residual_demand_example.pdf"
SUM_OUT = REPO / "results/regressions/bid/mtu15_critical_flat/residual_demand_slope_summary.csv"

# Bandwidth around the window-mean MCP, in EUR/MWh, for the slope calc
H_BAND_EUR = 50.0

WINDOWS = [
    ("pre_ID15",   "2024-06-14", "2025-03-18"),  # IDA hourly, DA hourly
    ("post_ID15",  "2025-03-19", "2025-04-27"),  # IDA 15-min, DA hourly
    ("pre_DA15",   "2025-04-28", "2025-09-30"),  # IDA 15-min, DA hourly
    ("post_DA15",  "2025-10-01", "2026-03-06"),  # IDA 15-min, DA 15-min
]


def main() -> None:
    con = duckdb.connect()
    # Hourly DA panel: aggregate load (sum 15-min within hour), aggregate VRE
    # (sum 15-min within hour, sum across psr B16/B18/B19), and MCP per hour.
    panel = con.execute(f"""
    WITH load_h AS (
      SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
             AVG(load_mw) AS load_mw
      FROM '{LOAD}'
      GROUP BY 1
    ),
    vre_h AS (
      SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
             SUM(CASE WHEN psr_type IN ('B16','B18') THEN quantity_mw ELSE 0 END) / 4.0 AS wind_mw,
             SUM(CASE WHEN psr_type = 'B19'           THEN quantity_mw ELSE 0 END) / 4.0 AS solar_mw
      FROM '{GEN}'
      WHERE psr_type IN ('B16','B18','B19')
      GROUP BY 1
    ),
    mcp_h AS (
      -- MCP per (date, clock-hour). When MTU=15 the clock-hour MCP is the
      -- mean of the four quarter prices (period in [4(h-1)+1, 4h]).
      SELECT CAST(date AS DATE) AS d,
             CASE WHEN COALESCE(mtu_minutes,60)=60 THEN period
                  ELSE CAST(FLOOR((period-1)/4.0) AS INT) + 1 END AS clock_hour,
             AVG(price_es_eur_mwh) AS mcp
      FROM '{MPDBC}'
      WHERE price_es_eur_mwh IS NOT NULL
        AND date >= '2024-06-01' AND date <= '2026-03-31'
      GROUP BY 1, 2
    )
    SELECT
      CAST(l.hour_utc AS DATE) AS d,
      EXTRACT('hour' FROM l.hour_utc) + 1 AS clock_hour,
      l.load_mw, v.wind_mw, v.solar_mw,
      (l.load_mw - v.wind_mw - v.solar_mw) AS rd_mw,
      m.mcp
    FROM load_h l
      JOIN vre_h v USING (hour_utc)
      LEFT JOIN mcp_h m ON CAST(l.hour_utc AS DATE) = m.d
                       AND EXTRACT('hour' FROM l.hour_utc) + 1 = m.clock_hour
    WHERE l.hour_utc >= '2024-06-01' AND l.hour_utc < '2026-04-01'
      AND v.wind_mw IS NOT NULL AND v.solar_mw IS NOT NULL
      AND m.mcp IS NOT NULL
    ORDER BY l.hour_utc
    """).df()
    panel["d"] = pd.to_datetime(panel["d"])

    # ----- Example day plot -----
    example_day = pd.Timestamp("2025-11-04")  # pick a representative post-DA15 day
    day = panel[panel["d"] == example_day].sort_values("clock_hour")
    if day.empty:
        # Fallback: pick first day with full 24 hours in post_DA15
        candidates = (panel[panel["d"] >= "2025-10-15"]
                      .groupby("d").size().loc[lambda s: s == 24].index)
        if len(candidates) > 0:
            example_day = candidates[0]
            day = panel[panel["d"] == example_day].sort_values("clock_hour")
    print(f"Example day: {example_day.date()}, {len(day)} hours")

    fig, axes = plt.subplots(2, 1, figsize=(8, 7), sharex=True)
    ax = axes[0]
    ax.plot(day["clock_hour"], day["load_mw"], "k-", label="Load")
    ax.plot(day["clock_hour"], day["wind_mw"], "g-", alpha=0.7, label="Wind")
    ax.plot(day["clock_hour"], day["solar_mw"], "orange", label="Solar")
    ax.plot(day["clock_hour"], day["rd_mw"], "b-", lw=2,
            label="Residual demand (load - wind - solar)")
    ax.set_ylabel("MW")
    ax.set_title(f"Example day {example_day.date()}: load, VRE, and residual demand")
    ax.legend(loc="best", fontsize=8); ax.grid(alpha=0.3)

    ax = axes[1]
    ax.scatter(day["rd_mw"], day["mcp"], c=day["clock_hour"], cmap="viridis", s=40)
    ax.set_xlabel("Residual demand (MW)")
    ax.set_ylabel("DA clearing price (EUR/MWh)")
    ax.set_title("MCP vs residual demand, hourly within the day")
    ax.grid(alpha=0.3)
    # Hour-label annotations
    for _, row in day.iterrows():
        ax.annotate(f"{int(row['clock_hour']):02d}", (row["rd_mw"], row["mcp"]),
                    fontsize=7, alpha=0.7)
    plt.tight_layout()
    FIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_OUT)
    plt.close(fig)
    print(f"Wrote {FIG_OUT}")

    # ----- Per-window slope of MCP on RD inside bandwidth around mean MCP -----
    rows = []
    for label, lo, hi in WINDOWS:
        sub = panel[(panel["d"] >= lo) & (panel["d"] <= hi)].copy()
        if sub.empty:
            continue
        mcp_mean = sub["mcp"].mean()
        # restrict to bandwidth around the window-mean MCP
        in_band = sub[sub["mcp"].between(mcp_mean - H_BAND_EUR, mcp_mean + H_BAND_EUR)]
        x = in_band["rd_mw"].to_numpy()
        y = in_band["mcp"].to_numpy()
        # OLS: MCP = a + slope * RD
        if len(x) < 30:
            continue
        x_dev = x - x.mean(); y_dev = y - y.mean()
        slope = (x_dev * y_dev).sum() / (x_dev**2).sum()
        # residual SE
        a = y.mean() - slope * x.mean()
        e = y - (a + slope * x)
        rss = (e**2).sum()
        s_e = np.sqrt(rss / (len(x) - 2))
        se_slope = s_e / np.sqrt((x_dev**2).sum())
        rows.append({
            "window": label, "lo": lo, "hi": hi,
            "n_hours": len(sub), "n_in_band": len(in_band),
            "mcp_mean": mcp_mean, "rd_mean": in_band["rd_mw"].mean(),
            "rd_sd": in_band["rd_mw"].std(),
            "slope_eur_mwh_per_MW": slope, "se_slope": se_slope,
            "t_slope": slope / se_slope, "h_band_eur": H_BAND_EUR,
        })
    out = pd.DataFrame(rows)
    SUM_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(SUM_OUT, index=False)
    print(f"\nResidual-demand slope by window (MCP per MW of RD):")
    print(out.round(5).to_string(index=False))


if __name__ == "__main__":
    main()
