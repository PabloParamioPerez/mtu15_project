# STATUS: ONE-OFF
# CLAIM-CHECK: is the CCGT firm-level bidding split (GN withholds at scarcity
#   prices, IB bids competitively) a constant behaviour, or a post-blackout
#   one? Computes per-(firm, regime) the day-ahead MW-weighted bid, the
#   scarcity share of the bid, and the Fase I recall volume, across all five
#   reform-window regimes (the blackout falls between MTU15-IDA pre and post).
#
# OUT: figures/working/ccgt_withholding_over_time.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
PDBF = P / "mercado_diario/programas/pdbf_all.parquet"
PIBCA = P / "mercado_intradiario_subastas/programas/pibca_all.parquet"
PIBCI = P / "mercado_intradiario_subastas/programas/pibci_all.parquet"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG = REPO / "figures/working/ccgt_withholding_over_time.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

REGIMES = [
    ("3-sess",         "2024-06-14", "2024-11-30"),
    ("ISP15-win",      "2024-12-01", "2025-03-18"),
    ("MTU15-IDA pre",  "2025-03-19", "2025-04-27"),
    ("MTU15-IDA post", "2025-04-28", "2025-09-30"),
    ("DA15/ID15",      "2025-10-01", "2026-05-15"),
]
ORDER = [r[0] for r in REGIMES]
# The 28-Apr-2025 blackout falls between regime index 2 and 3.
BLACKOUT_X = 2.5
FIRMS = {"IB": "#1f77b4", "GE": "#2ca02c", "GN": "#d62728", "HC": "#9467bd"}


def case_on(col):
    return "CASE " + " ".join(
        f"WHEN {col} BETWEEN DATE '{lo}' AND DATE '{hi}' THEN '{r}'"
        for r, lo, hi in REGIMES) + " END"


def tech_bucket(t):
    return "CCGT" if "ciclo combinado" in str(t).lower() else "Other"


def firm_bucket(o):
    o = str(o).lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o or "hidroel" in o: return "HC"
    return "OTH"


con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=false")

u = pd.read_csv(UNITS)
u["tech"] = u["technology"].apply(tech_bucket)
u["firm"] = u["owner_agent"].apply(firm_bucket)
u = u[(u["tech"] == "CCGT") & u["firm"].isin(FIRMS)][
    ["unit_code", "firm"]].drop_duplicates("unit_code")
con.register("u", u)
UN = "(" + ",".join(f"'{c}'" for c in sorted(u["unit_code"])) + ")"

# ---- DA bid: MW-weighted price + scarcity share, per (firm, regime) ------
bids = con.execute(f"""
WITH cab_l AS (
  SELECT d, offer_code, unit_code FROM (
    SELECT CAST(date AS DATE) d, offer_code, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                              ORDER BY version DESC) rn
    FROM '{CAB}' WHERE date >= '2024-06-14' AND buy_sell='V'
                 AND unit_code IN {UN}) WHERE rn=1),
det AS (
  SELECT CAST(date AS DATE) d, offer_code, price_eur_mwh p, quantity_mw q
  FROM '{DET}' WHERE date >= '2024-06-14' AND quantity_mw > 0)
SELECT u.firm, {case_on('dv.d')} regime,
       SUM(dv.q*dv.p)/SUM(dv.q)                                 da_bid,
       SUM(CASE WHEN dv.p>=500 THEN dv.q ELSE 0 END)/SUM(dv.q)   scarcity
FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
  JOIN u ON c.unit_code = u.unit_code
WHERE {case_on('dv.d')} IS NOT NULL GROUP BY 1,2
""").df()

# ---- Fase I recall volume per (firm, regime), mean MWh per unit-day ------
def daily(parquet, maxsession=False):
    if maxsession:
        return (f"SELECT unit_code, CAST(date AS DATE) d, "
                f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM ("
                f" SELECT unit_code,date,period,assigned_power_mw,mtu_minutes,"
                f" ROW_NUMBER() OVER (PARTITION BY date,period,unit_code "
                f" ORDER BY session_number DESC) rn FROM '{parquet}' "
                f" WHERE date >= '2024-06-14' AND unit_code IN {UN}) WHERE rn=1 "
                f"GROUP BY 1,2")
    return (f"SELECT unit_code, CAST(date AS DATE) d, "
            f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM '{parquet}' "
            f"WHERE date >= '2024-06-14' AND unit_code IN {UN} GROUP BY 1,2")


fase = con.execute(f"""
WITH pdbf  AS ({daily(PDBF)}),
     pibca AS ({daily(PIBCA, True)}),
     pibci AS ({daily(PIBCI)}),
allp AS (
  SELECT 'pdbf' p,* FROM pdbf UNION ALL SELECT 'pibca' p,* FROM pibca
  UNION ALL SELECT 'pibci' p,* FROM pibci),
ud AS (
  SELECT unit_code, d,
         SUM(CASE WHEN p='pdbf'  THEN mwh ELSE 0 END) pdbf,
         SUM(CASE WHEN p='pibca' THEN mwh ELSE 0 END) pibca,
         SUM(CASE WHEN p='pibci' THEN mwh ELSE 0 END) pibci
  FROM allp GROUP BY 1,2)
SELECT u.firm, {case_on('d')} regime, AVG(pibca-pdbf-pibci) fase_i
FROM ud JOIN u ON ud.unit_code = u.unit_code
WHERE {case_on('d')} IS NOT NULL GROUP BY 1,2
""").df()

m = bids.merge(fase, on=["firm", "regime"], how="outer")
m["x"] = m["regime"].map({r: i for i, r in enumerate(ORDER)})
m = m.sort_values(["firm", "x"])

print("\n=== CCGT day-ahead withholding and Fase I recall, by firm and regime ===")
for firm in FIRMS:
    s = m[m["firm"] == firm]
    print(f"  {firm}")
    for r in s.itertuples():
        print(f"    {r.regime:16s}  DA bid {r.da_bid:7.0f}  ({r.scarcity:4.0%} scarcity)"
              f"  |  Fase I {r.fase_i:7.0f} MWh/unit-day")

# ---- figure ---------------------------------------------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

fig, (axA, axB) = plt.subplots(1, 2, figsize=(14, 5))
for ax, col, lab in [(axA, "scarcity", "scarcity share of DA bid (MW $\\geq$500 EUR/MWh)"),
                     (axB, "fase_i", "Fase I recall (mean MWh per CCGT unit-day)")]:
    for firm, c in FIRMS.items():
        s = m[m["firm"] == firm]
        yv = s[col].values * (100 if col == "scarcity" else 1)
        ax.plot(s["x"], yv, "-o", color=c, lw=2, ms=6, label=firm)
    ax.axvspan(BLACKOUT_X, len(ORDER) - 0.5, color="#d62728", alpha=0.06, zorder=0)
    ax.axvline(BLACKOUT_X, color="#d62728", ls="--", lw=1.2)
    ax.text(BLACKOUT_X + 0.06, ax.get_ylim()[1] if col != "scarcity" else 97,
            "28-Apr blackout\n+ operación reforzada", fontsize=8, color="#d62728",
            va="top")
    ax.set_xticks(range(len(ORDER)))
    ax.set_xticklabels(ORDER, rotation=30, ha="right", fontsize=8.5)
    ax.set_ylabel(lab, fontsize=9.5)
    ax.grid(alpha=0.3, lw=0.5)
axA.set_ylim(-5, 100)
axA.set_title("(A) Day-ahead withholding --- constant, pre-dates the blackout",
              fontsize=10)
axB.set_title("(B) Fase I recall --- scales up under operación reforzada",
              fontsize=10)
axA.legend(title="CCGT fleet", fontsize=8.5, loc="center left")
fig.suptitle("Is the CCGT bidding split constant over time? "
             "Withholding yes; the recall volume scales with reforzada",
             fontsize=11, y=1.03)
fig.tight_layout()
fig.savefig(FIG, bbox_inches="tight", dpi=130)
plt.close(fig)
print(f"\nwrote {FIG}")
