# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: scripts/analysis/bid/bsts_ida_buy_share.R --- per-tech daily
#        IDA buy-share = sum buy MW / (sum buy MW + sum sell MW).
#        Latest-session-per-period selection on IDA (consistent with the
#        Spec A IDA-side construction).
#
# OUT: data/derived/panels/ida_buy_share_daily.parquet

from pathlib import Path
import duckdb

REPO = Path(__file__).resolve().parents[3]
ICAB = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/icab_all.parquet"
IDET = REPO / "data/processed/omie/mercado_intradiario_subastas/ofertas/idet_all.parquet"
UMAP = REPO / "data/derived/panels/bid_shape_critical_flat/_unit_map.parquet"
OUT  = REPO / "data/derived/panels/ida_buy_share_daily.parquet"

DATE_LO = "2023-01-01"   # extend back to allow 2024 same-calendar placebo
DATE_HI = "2026-04-30"   # through current data
TECHS = ["CCGT", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Nuclear"]

con = duckdb.connect()
con.execute("SET threads=4; SET memory_limit='6GB'")

tech_filter = "(" + ",".join(f"'{t}'" for t in TECHS) + ")"

# Aggregate by (date, tech, buy_sell): sum quantity_mw across all sessions/units
# Note: IDA has up to 3 sessions per day; we count all offers across sessions.
q = f"""
WITH u AS (SELECT unit_code, tech_group AS tech FROM '{UMAP}' WHERE tech_group IN {tech_filter}),
agg AS (
  SELECT CAST(c.date AS DATE) AS d, u.tech, c.buy_sell,
         SUM(dd.quantity_mw) AS sum_mw
  FROM '{ICAB}' c JOIN '{IDET}' dd
    ON c.date = dd.date AND c.offer_code = dd.offer_code AND c.version = dd.version
  JOIN u ON c.unit_code = u.unit_code
  WHERE c.date BETWEEN '{DATE_LO}' AND '{DATE_HI}'
    AND dd.quantity_mw > 0 AND dd.price_eur_mwh IS NOT NULL
  GROUP BY 1, 2, 3
)
SELECT d, tech,
       SUM(CASE WHEN buy_sell='V' THEN sum_mw ELSE 0 END) AS sell_mw,
       SUM(CASE WHEN buy_sell='C' THEN sum_mw ELSE 0 END) AS buy_mw,
       SUM(CASE WHEN buy_sell='C' THEN sum_mw ELSE 0 END)
         / NULLIF(SUM(sum_mw), 0) AS buy_share
FROM agg
GROUP BY 1, 2 ORDER BY 1, 2
"""
df = con.execute(q).fetchdf()
df.to_parquet(OUT)
print(f"Saved {OUT}: {len(df):,} (date, tech) rows")
print(df.head(10).to_string())
print(f"\nDate range: {df['d'].min()} -> {df['d'].max()}")
print(f"\nMean buy_share by tech (full sample):")
print(df.groupby("tech")["buy_share"].mean().round(3).to_string())
