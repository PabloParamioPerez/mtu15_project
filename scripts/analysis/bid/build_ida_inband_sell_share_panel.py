# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: scripts/analysis/bid/bsts_ida_sell_share.R --- per-tech daily IDA
#        in-band sell-share = in-band sell MW / (in-band sell + in-band buy MW).
#        In-band defined as |bid_price - MCP| <= H_BAND for each period; uses
#        the IDA period-specific MCP (latest session per period).
#
# OUT: data/derived/panels/ida_inband_sell_share_daily.parquet

from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
MPIBC = REPO / "data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT  = REPO / "data/derived/panels/ida_inband_sell_share_daily.parquet"

DATE_LO = "2023-01-01"
DATE_HI = "2026-04-30"
H_BAND  = 60.0   # uniform in-band bandwidth (close to window-specific p90 values 58/62)
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Nuclear"]

con = duckdb.connect()
con.execute("SET threads=4; SET memory_limit='8GB'")

tech_filter = "(" + ",".join(f"'{t}'" for t in TECHS) + ")"

q = f"""
WITH u AS (SELECT unit_code, tech_group AS tech FROM '{UMAP}' WHERE tech_group IN {tech_filter}),
mcp AS (
  SELECT CAST(date AS DATE) AS d, session_number, period, price_es_eur_mwh AS mcp,
         ROW_NUMBER() OVER (PARTITION BY date::DATE, period
                            ORDER BY session_number DESC) AS rn
  FROM '{MPIBC}'
  WHERE date BETWEEN '{DATE_LO}' AND '{DATE_HI}' AND price_es_eur_mwh IS NOT NULL
),
mcp_l AS (SELECT d, period, mcp FROM mcp WHERE rn = 1),
offers AS (
  SELECT CAST(c.date AS DATE) AS d, c.session_number, dd.period,
         c.unit_code, c.buy_sell, dd.price_eur_mwh AS p, dd.quantity_mw AS q
  FROM '{ICAB}' c JOIN '{IDET}' dd
    ON c.date = dd.date AND c.offer_code = dd.offer_code AND c.version = dd.version
  WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
    AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
),
inband AS (
  SELECT o.d, u.tech, o.buy_sell, o.q
  FROM offers o JOIN mcp_l m USING (d, period) JOIN u ON o.unit_code = u.unit_code
  WHERE ABS(o.p - m.mcp) <= {H_BAND}
)
SELECT d, tech,
       SUM(CASE WHEN buy_sell='V' THEN q ELSE 0 END) AS sell_mw,
       SUM(CASE WHEN buy_sell='C' THEN q ELSE 0 END) AS buy_mw,
       SUM(CASE WHEN buy_sell='V' THEN q ELSE 0 END)
         / NULLIF(SUM(q), 0) AS sell_share
FROM inband
GROUP BY 1, 2 ORDER BY 1, 2
"""
df = con.execute(q).fetchdf()
df.to_parquet(OUT)
print(f"Saved {OUT}: {len(df):,} rows")
print(f"Date range: {df['d'].min()} -> {df['d'].max()}")

print("\nMean in-band sell-share by tech (full sample):")
print(df.groupby("tech")["sell_share"].mean().round(3).to_string())

# Spot-check pre-windows
import pandas as pd
df["d"] = pd.to_datetime(df["d"])
for label, lo, hi in [
    ("ID15 placebo 2024 pre (2023-06-14 to 2024-03-18)", "2023-06-14", "2024-03-18"),
    ("ID15 real pre (2024-06-14 to 2025-03-18)", "2024-06-14", "2025-03-18"),
    ("DA15 placebo 2024 pre (2024-04-28 to 2024-09-30)", "2024-04-28", "2024-09-30"),
    ("DA15 real pre (2025-04-28 to 2025-09-30)", "2025-04-28", "2025-09-30"),
]:
    print(f"\n{label}:")
    sub = df[(df["d"] >= lo) & (df["d"] <= hi)]
    print(sub.groupby("tech").agg(
        n=("d", "nunique"),
        sell_share_mean=("sell_share", "mean"),
        sell_share_sd=("sell_share", "std"),
    ).round(3).to_string())
