# STATUS: ONE-OFF
# CLAIM-CHECK: "GN CCGT bid at scarcity prices to stay out of the PDBC and be
#   included via post-clearing RT." Reconstructs the program chain
#   PDBC -> PDBF -> PIBCA(max session) -> PHF(max session) per GN CCGT unit-day
#   (energy MWh, robust to the MTU60/MTU15 period mismatch), and the DA bid
#   side (MW-weighted bid price vs MCP). RT2 = PHF - PIBCA (post-IDA RT);
#   pre-IDA Fase I + IDA clearing land in PIBCA - PDBF.

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
P = REPO / "data/processed/omie"
PDBC = P / "mercado_diario/programas/pdbc_all.parquet"
PDBF = P / "mercado_diario/programas/pdbf_all.parquet"
PIBCA = P / "mercado_intradiario_subastas/programas/pibca_all.parquet"
PHF = P / "mercado_intradiario_subastas/programas/phf_all.parquet"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
MPDBC = P / "mercado_diario/precios/marginalpdbc_all.parquet"

GN_CCGT = ['ACE4', 'BES4', 'CAMGI10', 'CTGN1', 'CTGN2', 'CTGN3', 'MALA1',
           'PALOS1', 'PALOS2', 'PALOS3', 'PBCN1', 'PBCN2', 'PVENT1',
           'SAGU1', 'SAGU2', 'SAGU3', 'SBO3', 'SROQ1']
UN = "(" + ",".join(f"'{u}'" for u in GN_CCGT) + ")"

REGIMES = [
    ("3-sess",         "2024-06-14", "2024-11-30"),
    ("ISP15-win",      "2024-12-01", "2025-03-18"),
    ("MTU15-IDA pre",  "2025-03-19", "2025-04-27"),
    ("MTU15-IDA post", "2025-04-28", "2025-09-30"),
    ("DA15/ID15",      "2025-10-01", "2026-05-15"),
]
CASE = " ".join(
    f"WHEN d BETWEEN DATE '{lo}' AND DATE '{hi}' THEN '{r}'"
    for r, lo, hi in REGIMES
)

con = duckdb.connect()
con.execute("SET memory_limit='10GB'")
con.execute("SET threads=4")

# ---- program chain: daily energy (MWh) per GN CCGT unit-day --------------
chain = con.execute(f"""
WITH pdbc AS (
  SELECT unit_code, CAST(date AS DATE) d,
         SUM(assigned_power_mw*mtu_minutes/60.0) mwh
  FROM '{PDBC}' WHERE unit_code IN {UN} AND date >= '2024-06-14' GROUP BY 1,2),
pdbf AS (
  SELECT unit_code, CAST(date AS DATE) d,
         SUM(assigned_power_mw*mtu_minutes/60.0) mwh
  FROM '{PDBF}' WHERE unit_code IN {UN} AND date >= '2024-06-14' GROUP BY 1,2),
pibca_ms AS (
  SELECT unit_code, date, period, assigned_power_mw, mtu_minutes,
         ROW_NUMBER() OVER (PARTITION BY date,period,unit_code
                            ORDER BY session_number DESC) rn
  FROM '{PIBCA}' WHERE unit_code IN {UN} AND date >= '2024-06-14'),
pibca AS (
  SELECT unit_code, CAST(date AS DATE) d,
         SUM(assigned_power_mw*mtu_minutes/60.0) mwh
  FROM pibca_ms WHERE rn=1 GROUP BY 1,2),
phf_ms AS (
  SELECT unit_code, date, period, assigned_power_mw, mtu_minutes,
         ROW_NUMBER() OVER (PARTITION BY date,period,unit_code
                            ORDER BY session_number DESC) rn
  FROM '{PHF}' WHERE unit_code IN {UN} AND date >= '2024-06-14'),
phf AS (
  SELECT unit_code, CAST(date AS DATE) d,
         SUM(assigned_power_mw*mtu_minutes/60.0) mwh
  FROM phf_ms WHERE rn=1 GROUP BY 1,2),
allp AS (
  SELECT 'pdbc' prog, * FROM pdbc UNION ALL
  SELECT 'pdbf', * FROM pdbf UNION ALL
  SELECT 'pibca', * FROM pibca UNION ALL
  SELECT 'phf', * FROM phf),
ud AS (
  SELECT unit_code, d,
         SUM(CASE WHEN prog='pdbc'  THEN mwh ELSE 0 END) pdbc,
         SUM(CASE WHEN prog='pdbf'  THEN mwh ELSE 0 END) pdbf,
         SUM(CASE WHEN prog='pibca' THEN mwh ELSE 0 END) pibca,
         SUM(CASE WHEN prog='phf'   THEN mwh ELSE 0 END) phf
  FROM allp GROUP BY 1,2)
SELECT CASE {CASE} END regime,
       COUNT(DISTINCT d) n_days,
       AVG(pdbc)          da_cleared,
       AVG(pdbf-pdbc)     bilateral,
       AVG(pibca-pdbf)    ida_plus_faseI,
       AVG(phf-pibca)     rt2_postida,
       AVG(phf)           final_phf
FROM ud GROUP BY 1
""").df()

# ---- DA bid side: MW-weighted bid price vs MCP ---------------------------
bids = con.execute(f"""
WITH cab_l AS (
  SELECT CAST(date AS DATE) d, offer_code, unit_code FROM (
    SELECT CAST(date AS DATE) date, offer_code, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                              ORDER BY version DESC) rn
    FROM '{CAB}' WHERE date >= '2024-06-14' AND buy_sell='V'
                 AND unit_code IN {UN}) WHERE rn=1),
det AS (
  SELECT CAST(date AS DATE) d, offer_code, period,
         price_eur_mwh p, quantity_mw q
  FROM '{DET}' WHERE date >= '2024-06-14' AND quantity_mw > 0),
mp AS (
  SELECT CAST(date AS DATE) d, period, price_es_eur_mwh mcp
  FROM '{MPDBC}' WHERE date >= '2024-06-14' AND price_es_eur_mwh IS NOT NULL),
tr AS (
  SELECT mp.d, dv.q, dv.p, mp.mcp
  FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
  JOIN mp ON mp.d=dv.d AND mp.period=dv.period)
SELECT CASE {CASE} END regime,
       SUM(q*p)/SUM(q)                                  mw_wtd_bid_price,
       SUM(CASE WHEN p>mcp      THEN q ELSE 0 END)/SUM(q) share_above_mcp,
       SUM(CASE WHEN p>=500     THEN q ELSE 0 END)/SUM(q) share_scarcity_ge500,
       AVG(mcp)                                          mean_mcp
FROM tr GROUP BY 1
""").df()

order = [r[0] for r in REGIMES]
chain["__o"] = chain["regime"].map({r: i for i, r in enumerate(order)})
bids["__o"] = bids["regime"].map({r: i for i, r in enumerate(order)})
chain = chain.sort_values("__o").drop(columns="__o")
bids = bids.sort_values("__o").drop(columns="__o")

pd.set_option("display.width", 160, "display.float_format", lambda v: f"{v:.1f}")
print("\n=== GN CCGT program chain, mean MWh per unit-day ===")
print(chain.to_string(index=False))
print("\n  da_cleared = PDBC | bilateral = PDBF-PDBC | "
      "ida_plus_faseI = PIBCA-PDBF | rt2_postida = PHF-PIBCA")
print("\n=== GN CCGT DA bid side ===")
pd.set_option("display.float_format", lambda v: f"{v:.2f}")
print(bids.to_string(index=False))
print("\n  share_above_mcp / share_scarcity_ge500 are MW-weighted fractions "
      "of offered DA volume")
