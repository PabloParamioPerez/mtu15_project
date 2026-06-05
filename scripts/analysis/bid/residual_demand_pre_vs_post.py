# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Apples-to-apples comparison of the system residual-demand-slope
#        pre vs post each MTU15 reform. For each window:
#          * pre-reform: 1 obs per hour (hourly product cleared the auction)
#          * post-reform: 4 obs per hour (per-quarter product)
#        Compute slope of MCP on RD = load - wind - solar by OLS restricted
#        to MCP within +/- 50 EUR/MWh of the window-mean MCP. Higher slope
#        = more inelastic per MW.
#
# OUT: results/regressions/bid/mtu15_critical_flat/residual_demand_pre_vs_post.csv

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
LOAD = REPO / "data/processed/entsoe/load/load_actual_all.parquet"
GEN  = REPO / "data/processed/entsoe/generation/gen_actual_per_type_all.parquet"
MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/residual_demand_pre_vs_post.csv"

H_BAND_EUR = 50.0


def _fetch_load_vre(con, granularity: str):
    """Return DataFrame at the given granularity (15 or 60 min). For 60-min
    we average over the 4 quarters in the hour."""
    mtu = 15 if granularity == "15" else 60
    if granularity == "15":
        # 15-min native
        sql = f"""
        WITH load_q AS (
          SELECT isp_start_utc, load_mw
          FROM '{LOAD}' WHERE mtu_minutes = 15
        ),
        vre_q AS (
          SELECT isp_start_utc,
                 SUM(CASE WHEN psr_type IN ('B16','B18') THEN quantity_mw ELSE 0 END) AS wind_mw,
                 SUM(CASE WHEN psr_type = 'B19'           THEN quantity_mw ELSE 0 END) AS solar_mw
          FROM '{GEN}'
          WHERE psr_type IN ('B16','B18','B19') AND mtu_minutes = 15
          GROUP BY 1
        )
        SELECT CAST(l.isp_start_utc AS DATE) AS d,
               EXTRACT('hour' FROM l.isp_start_utc) + 1 AS clock_hour,
               (EXTRACT('minute' FROM l.isp_start_utc) / 15) + 1 AS quarter,
               l.load_mw, v.wind_mw, v.solar_mw,
               (l.load_mw - v.wind_mw - v.solar_mw) AS rd_mw
        FROM load_q l JOIN vre_q v USING (isp_start_utc)
        WHERE v.wind_mw IS NOT NULL AND v.solar_mw IS NOT NULL
        """
    else:
        # 60-min aggregate (mean MW)
        sql = f"""
        WITH load_h AS (
          SELECT date_trunc('hour', isp_start_utc) AS hour_utc, AVG(load_mw) AS load_mw
          FROM '{LOAD}' WHERE mtu_minutes = 15
          GROUP BY 1
        ),
        vre_h AS (
          SELECT date_trunc('hour', isp_start_utc) AS hour_utc,
                 SUM(CASE WHEN psr_type IN ('B16','B18') THEN quantity_mw ELSE 0 END) / 4.0 AS wind_mw,
                 SUM(CASE WHEN psr_type = 'B19'           THEN quantity_mw ELSE 0 END) / 4.0 AS solar_mw
          FROM '{GEN}'
          WHERE psr_type IN ('B16','B18','B19') AND mtu_minutes = 15
          GROUP BY 1
        )
        SELECT CAST(l.hour_utc AS DATE) AS d,
               EXTRACT('hour' FROM l.hour_utc) + 1 AS clock_hour,
               l.load_mw, v.wind_mw, v.solar_mw,
               (l.load_mw - v.wind_mw - v.solar_mw) AS rd_mw
        FROM load_h l JOIN vre_h v USING (hour_utc)
        WHERE v.wind_mw IS NOT NULL AND v.solar_mw IS NOT NULL
        """
    df = con.execute(sql).df()
    df["d"] = pd.to_datetime(df["d"])
    return df


def _fetch_mcp(con, market: str, mtu: int):
    src = MPDBC if market == "DA" else MPIBC
    if market == "DA":
        sql = f"""
        SELECT CAST(date AS DATE) AS d, period, price_es_eur_mwh AS mcp,
               COALESCE(mtu_minutes, 60) AS mtu_actual
        FROM '{src}' WHERE price_es_eur_mwh IS NOT NULL AND COALESCE(mtu_minutes, 60) = {mtu}
        """
    else:
        sql = f"""
        WITH q AS (
          SELECT CAST(date AS DATE) AS d, session_number, period, mtu_minutes,
                 price_es_eur_mwh AS mcp,
                 ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                                    ORDER BY session_number DESC) AS rn
          FROM '{src}' WHERE price_es_eur_mwh IS NOT NULL AND COALESCE(mtu_minutes, 60) = {mtu}
        )
        SELECT d, period, mcp, mtu_minutes AS mtu_actual FROM q WHERE rn = 1
        """
    df = con.execute(sql).df()
    df["d"] = pd.to_datetime(df["d"])
    return df


def _ols_slope(x, y):
    if len(x) < 30:
        return np.nan, np.nan, len(x)
    x_dev = x - x.mean(); y_dev = y - y.mean()
    if (x_dev ** 2).sum() <= 0:
        return np.nan, np.nan, len(x)
    slope = (x_dev * y_dev).sum() / (x_dev ** 2).sum()
    a = y.mean() - slope * x.mean()
    e = y - (a + slope * x)
    rss = (e ** 2).sum()
    s_e = np.sqrt(rss / (len(x) - 2)) if len(x) > 2 else np.nan
    se = s_e / np.sqrt((x_dev ** 2).sum()) if s_e == s_e else np.nan
    return float(slope), float(se), len(x)


CONFIG = [
    # (window_label, market, lo, hi, granularity)
    ("pre_ID15_IDA",   "IDA", "2024-06-14", "2025-03-18", "60"),
    ("post_ID15_IDA",  "IDA", "2025-03-19", "2025-04-27", "15"),
    ("pre_DA15_DA",    "DA",  "2024-06-14", "2025-09-30", "60"),
    ("post_DA15_DA",   "DA",  "2025-10-01", "2026-03-06", "15"),
    ("pre_DA15_IDA",   "IDA", "2025-04-28", "2025-09-30", "15"),
    ("post_DA15_IDA",  "IDA", "2025-10-01", "2026-03-06", "15"),
]


def main() -> None:
    con = duckdb.connect()
    rows = []
    for label, market, lo, hi, gran in CONFIG:
        mtu_int = 15 if gran == "15" else 60
        rd = _fetch_load_vre(con, gran)
        mcp = _fetch_mcp(con, market, mtu_int)
        if gran == "15":
            rd["period"] = (rd["clock_hour"] - 1) * 4 + rd["quarter"]
        else:
            rd["period"] = rd["clock_hour"]
        m = rd.merge(mcp, on=["d", "period"], how="inner")
        sub = m[(m["d"] >= lo) & (m["d"] <= hi)]
        if sub.empty:
            print(f"{label}: NO DATA"); continue
        mcp_mean = sub["mcp"].mean()
        in_band = sub[sub["mcp"].between(mcp_mean - H_BAND_EUR, mcp_mean + H_BAND_EUR)]
        x = in_band["rd_mw"].to_numpy(); y = in_band["mcp"].to_numpy()
        slope, se, n = _ols_slope(x, y)
        rows.append({
            "window": label, "market": market, "granularity_min": mtu_int,
            "n_periods": len(sub), "n_in_band": n,
            "mcp_mean": mcp_mean, "rd_mean": in_band["rd_mw"].mean(),
            "rd_sd": in_band["rd_mw"].std(),
            "slope_eur_mwh_per_MW": slope, "se_slope": se,
            "t_slope": slope / se if (se and se > 0) else np.nan,
            "h_band_eur": H_BAND_EUR,
        })
    out = pd.DataFrame(rows)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT, index=False)
    print("\n=== Residual-demand slope: pre-reform hourly vs post-reform per-quarter ===")
    print(out.round(5).to_string(index=False))

    # Pretty pairs
    print("\n=== Pre vs post (same market) ===")
    pairs = [
        ("ID15 IDA", "pre_ID15_IDA",  "post_ID15_IDA"),
        ("DA15 DA",  "pre_DA15_DA",   "post_DA15_DA"),
        ("DA15 IDA", "pre_DA15_IDA",  "post_DA15_IDA"),
    ]
    for name, prelbl, postlbl in pairs:
        pre = out[out.window == prelbl].iloc[0]["slope_eur_mwh_per_MW"]
        post = out[out.window == postlbl].iloc[0]["slope_eur_mwh_per_MW"]
        ratio = post / pre if pre and not np.isnan(pre) else float("nan")
        sign = "MORE inelastic" if post > pre else "MORE elastic" if post < pre else "same"
        print(f"  {name}: pre={pre:.5f} -> post={post:.5f}  (ratio {ratio:.2f})  per-quarter is {sign}")


if __name__ == "__main__":
    main()
