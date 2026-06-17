# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Per-QUARTER residual-demand vs MCP analysis for post-MTU15 windows.
#        For each (date, quarter) computes RD = load - wind - solar from
#        15-min ENTSO-E data and pairs with the OMIE quarter-MCP. Then for
#        each (date, market, clock-hour) with all four quarters observed,
#        computes the within-hour SD of RD and the within-hour SD of MCP.
#        The ratio sd(MCP) / sd(RD) is a per-hour empirical slope of MCP
#        on RD within the hour, which is the M3 thinness-asymmetry primitive
#        identified at the system level instead of the bid-stack proxy.
#
# OUT:
#   figures/working/fig_residual_demand_per_quarter.pdf
#   results/regressions/bid/mtu15_critical_flat/residual_demand_per_quarter.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
GEN  = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"

FIG_OUT = REPO / "figures/working/fig_residual_demand_per_quarter.pdf"
SUM_OUT = REPO / "results/regressions/bid/mtu15_critical_flat/residual_demand_per_quarter.csv"

WINDOWS = [
    # (label, market, lo, hi, period_mtu, source)
    ("post_ID15_IDA", "IDA", "2025-03-19", "2025-04-27", 15),
    ("pre_DA15_IDA",  "IDA", "2025-04-28", "2025-09-30", 15),
    ("post_DA15_DA",  "DA",  "2025-10-01", "2026-03-06", 15),
    ("post_DA15_IDA", "IDA", "2025-10-01", "2026-03-06", 15),
]


def main() -> None:
    con = duckdb.connect()
    # 15-min residual-demand panel from ENTSO-E
    rd = con.execute(f"""
    WITH load_q AS (
      SELECT isp_start_utc, mtu_minutes, load_mw
      FROM '{LOAD}'
      WHERE mtu_minutes = 15
        AND isp_start_utc >= '2025-03-01' AND isp_start_utc < '2026-04-01'
    ),
    vre_q AS (
      SELECT isp_start_utc,
             SUM(CASE WHEN psr_type IN ('B16','B18') THEN quantity_mw ELSE 0 END) AS wind_mw,
             SUM(CASE WHEN psr_type = 'B19'           THEN quantity_mw ELSE 0 END) AS solar_mw
      FROM '{GEN}'
      WHERE psr_type IN ('B16','B18','B19') AND mtu_minutes = 15
        AND isp_start_utc >= '2025-03-01' AND isp_start_utc < '2026-04-01'
      GROUP BY 1
    )
    SELECT
      CAST(l.isp_start_utc AS DATE) AS d,
      EXTRACT('hour' FROM l.isp_start_utc) + 1 AS clock_hour,
      (EXTRACT('minute' FROM l.isp_start_utc) / 15) + 1 AS quarter,
      l.load_mw, v.wind_mw, v.solar_mw,
      (l.load_mw - v.wind_mw - v.solar_mw) AS rd_mw
    FROM load_q l JOIN vre_q v USING (isp_start_utc)
    WHERE v.wind_mw IS NOT NULL AND v.solar_mw IS NOT NULL
    """).df()
    rd["d"] = pd.to_datetime(rd["d"])
    rd["period"] = (rd["clock_hour"] - 1) * 4 + rd["quarter"]

    # MCP per period for DA and IDA when MTU=15
    mcp_da = con.execute(f"""
    SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp_da
    FROM '{MPDBC}'
    WHERE mtu_minutes = 15 AND price_es_eur_mwh IS NOT NULL
      AND date >= '2025-10-01' AND date <= '2026-03-31'
    """).df()
    mcp_da["d"] = pd.to_datetime(mcp_da["d"])

    mcp_ida = con.execute(f"""
    WITH q AS (
      SELECT CAST(date AS DATE) AS d, session_number, period, mtu_minutes,
             price_es_eur_mwh AS mcp,
             ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                ORDER BY session_number DESC) AS rn
      FROM '{MPIBC}'
      WHERE mtu_minutes = 15 AND price_es_eur_mwh IS NOT NULL
        AND date >= '2025-03-19' AND date <= '2026-03-31'
    )
    SELECT d, period, mcp AS mcp_ida FROM q WHERE rn = 1
    """).df()
    mcp_ida["d"] = pd.to_datetime(mcp_ida["d"])

    # ----- Per-hour within-hour stats -----
    rows_out = []
    detail_rows = []
    for label, market, lo, hi, mtu in WINDOWS:
        sub_rd = rd[(rd["d"] >= lo) & (rd["d"] <= hi)].copy()
        mcp_src = mcp_da if market == "DA" else mcp_ida
        mcp_col = "mcp_da" if market == "DA" else "mcp_ida"
        sub_mcp = mcp_src[(mcp_src["d"] >= lo) & (mcp_src["d"] <= hi)]
        m = sub_rd.merge(sub_mcp, on=["d", "period"], how="inner")
        if m.empty:
            print(f"{label}: NO DATA"); continue

        # Within-hour aggregation: SD across the 4 quarters per (date, clock-hour)
        wh = (m.groupby(["d", "clock_hour"])
                .agg(n_q=("quarter", "size"),
                     mcp_sd=(mcp_col, "std"),
                     rd_sd=("rd_mw", "std"),
                     mcp_mean=(mcp_col, "mean"),
                     rd_mean=("rd_mw", "mean"))
                .query("n_q == 4 and rd_sd > 0")
                .reset_index())
        # Per-hour slope: sd(MCP) / sd(RD) — local price-impact within the hour
        wh["local_slope"] = wh["mcp_sd"] / wh["rd_sd"]
        # Hour classes
        critical_set = {5,6,7,8,16,17,18,19,20,21,22}
        flat_set     = {1,2,3}
        wh["hour_class"] = wh["clock_hour"].apply(
            lambda h: "critical" if h in critical_set else (
                       "flat" if h in flat_set else "other"))
        detail_rows.append((label, wh))

        # Summary per (hour_class)
        for cls, g in wh.groupby("hour_class"):
            rows_out.append({
                "window": label, "market": market, "hour_class": cls,
                "n_hours": len(g),
                "mcp_sd_mean": g["mcp_sd"].mean(),
                "rd_sd_mean": g["rd_sd"].mean(),
                "local_slope_mean": g["local_slope"].mean(),
                "local_slope_median": g["local_slope"].median(),
            })

    summary = pd.DataFrame(rows_out)
    SUM_OUT.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(SUM_OUT, index=False)
    print("\nWithin-hour SD and local slope by (window, hour_class):")
    print(summary.round(4).to_string(index=False))

    # ----- Plot: scatter of within-hour sd(MCP) vs sd(RD), one panel per window -----
    fig, axes = plt.subplots(2, 2, figsize=(11, 9), sharex=False, sharey=False)
    for (label, wh), ax in zip(detail_rows, axes.flatten()):
        for cls, color in [("critical", "tab:red"), ("flat", "tab:blue"),
                            ("other", "tab:gray")]:
            g = wh[wh["hour_class"] == cls]
            ax.scatter(g["rd_sd"], g["mcp_sd"], s=12, alpha=0.4, color=color,
                        label=f"{cls} (n={len(g)})")
        # OLS fit (all hours)
        x = wh["rd_sd"].to_numpy(); y = wh["mcp_sd"].to_numpy()
        if len(x) > 30:
            slope = np.cov(x, y, ddof=1)[0, 1] / np.var(x, ddof=1)
            intercept = y.mean() - slope * x.mean()
            xx = np.linspace(0, x.max(), 100)
            ax.plot(xx, intercept + slope * xx, "k-", lw=1.5,
                    label=f"slope = {slope:.4f}")
        ax.set_xlabel("within-hour SD of residual demand (MW)")
        ax.set_ylabel("within-hour SD of MCP (EUR/MWh)")
        ax.set_title(label)
        ax.legend(fontsize=7); ax.grid(alpha=0.3)
    plt.tight_layout()
    FIG_OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(FIG_OUT)
    plt.close(fig)
    print(f"\nWrote {FIG_OUT}")


if __name__ == "__main__":
    main()
