# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 6 (q1 drop / Fase I)
# CLAIM: Per-firm CCGT program chain, confirming GN re-enters via REE Fase I.
#        For each Big-4 firm's CCGT fleet, reconstructs the OMIE program chain
#        as daily energy (MWh, robust to the MTU60/MTU15 period mismatch) and
#        decomposes the final post-IDA program PHF into:
#          DA cleared  = PDBC
#          Bilateral   = PDBF - PDBC
#          Fase I      = PIBCA - PDBF - PIBCI   (pre-IDA REE redispatch)
#          IDA auction = PIBCI                  (intraday-auction clearing)
#          RT2         = PHF - PIBCA            (post-IDA REE redispatch)
#        PIBCA is the accumulated post-IDA program (carries Fase I); PIBCI is
#        the intraday-auction-cleared program only. So PIBCA - PDBF - PIBCI
#        isolates the pre-IDA Fase I redispatch. GN CCGT clears almost nothing
#        in the IDA auctions (PIBCI ~ 0) despite bidding competitively there:
#        its program is rebuilt by REE Fase I redispatch.
#
# OUT: figures/working/ccgt_program_chain_by_firm.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
PDBC = P / "mercado_diario/programas/pdbc_all.parquet"
PDBF = P / "mercado_diario/programas/pdbf_all.parquet"
PIBCA = P / "mercado_intradiario_subastas/programas/pibca_all.parquet"
PIBCI = P / "mercado_intradiario_subastas/programas/pibci_all.parquet"
PHF = P / "mercado_intradiario_subastas/programas/phf_all.parquet"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
IDET = P / "mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = P / "mercado_intradiario_subastas/ofertas/icab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG = REPO / "figures/working/ccgt_program_chain_by_firm.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

REGIMES = [
    ("3-sess",         "2024-06-14", "2024-11-30"),
    ("ISP15-win",      "2024-12-01", "2025-03-18"),
    ("MTU15-IDA pre",  "2025-03-19", "2025-04-27"),
    ("MTU15-IDA post", "2025-04-28", "2025-09-30"),
    ("DA15/ID15",      "2025-10-01", "2026-05-15"),
]
ORDER = [r[0] for r in REGIMES]


def case_on(col):
    return "CASE " + " ".join(
        f"WHEN {col} BETWEEN DATE '{lo}' AND DATE '{hi}' THEN '{r}'"
        for r, lo, hi in REGIMES) + " END"


CASE_D, CASE_DV, CASE_I = case_on("d"), case_on("dv.d"), case_on("i.d")
FIRMS = {"IB": "#1f77b4", "GE": "#2ca02c", "GN": "#d62728", "HC": "#9467bd"}


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
n_units = u.groupby("firm").size().to_dict()
# Inline unit-code filter so the program-table window functions never
# materialise the full (33M-row) intraday tables.
UN = "(" + ",".join(f"'{c}'" for c in sorted(u["unit_code"])) + ")"


def daily_mwh(parquet):
    """Daily energy per (unit, day): SUM(power * mtu/60), filtered to UN."""
    return (f"SELECT unit_code, CAST(date AS DATE) d, "
            f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM '{parquet}' "
            f"WHERE date >= '2024-06-14' AND unit_code IN {UN} GROUP BY 1,2")


def daily_mwh_maxsession(parquet):
    """Daily energy per (unit, day) keeping the max-session row per period."""
    return (f"SELECT unit_code, CAST(date AS DATE) d, "
            f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM ("
            f"  SELECT unit_code, date, period, assigned_power_mw, mtu_minutes,"
            f"  ROW_NUMBER() OVER (PARTITION BY date,period,unit_code "
            f"  ORDER BY session_number DESC) rn FROM '{parquet}' "
            f"  WHERE date >= '2024-06-14' AND unit_code IN {UN}) WHERE rn=1 "
            f"GROUP BY 1,2")


# ---- program chain: daily energy (MWh) per (firm, unit, day) -------------
chain = con.execute(f"""
WITH pdbc  AS ({daily_mwh(PDBC)}),
     pdbf  AS ({daily_mwh(PDBF)}),
     pibca AS ({daily_mwh_maxsession(PIBCA)}),
     pibci AS ({daily_mwh(PIBCI)}),
     phf   AS ({daily_mwh_maxsession(PHF)}),
allp AS (
  SELECT 'pdbc' prog,* FROM pdbc UNION ALL SELECT 'pdbf',* FROM pdbf
  UNION ALL SELECT 'pibca',* FROM pibca UNION ALL SELECT 'pibci',* FROM pibci
  UNION ALL SELECT 'phf',* FROM phf),
ud AS (
  SELECT unit_code, d,
         SUM(CASE WHEN prog='pdbc'  THEN mwh ELSE 0 END) pdbc,
         SUM(CASE WHEN prog='pdbf'  THEN mwh ELSE 0 END) pdbf,
         SUM(CASE WHEN prog='pibca' THEN mwh ELSE 0 END) pibca,
         SUM(CASE WHEN prog='pibci' THEN mwh ELSE 0 END) pibci,
         SUM(CASE WHEN prog='phf'   THEN mwh ELSE 0 END) phf
  FROM allp GROUP BY 1,2)
SELECT u.firm, {CASE_D} regime,
       AVG(pdbc)                  da_cleared,
       AVG(pdbf-pdbc)             bilateral,
       AVG(pibca-pdbf-pibci)      faseI,
       AVG(pibci)                 ida_auction,
       AVG(phf-pibca)             rt2
FROM ud JOIN u ON ud.unit_code = u.unit_code
WHERE {CASE_D} IS NOT NULL
GROUP BY 1,2
""").df()

# ---- DA and IDA MW-weighted bid price per (firm, regime) -----------------
da_bids = con.execute(f"""
WITH cab_l AS (
  SELECT d, offer_code, unit_code FROM (
    SELECT CAST(date AS DATE) d, offer_code, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                              ORDER BY version DESC) rn
    FROM '{CAB}' WHERE date >= '2024-06-14' AND buy_sell='V') WHERE rn=1),
det AS (
  SELECT CAST(date AS DATE) d, offer_code, price_eur_mwh p, quantity_mw q
  FROM '{DET}' WHERE date >= '2024-06-14' AND quantity_mw > 0)
SELECT u.firm, {CASE_DV} regime,
       SUM(dv.q*dv.p)/SUM(dv.q)                                  da_bid,
       SUM(CASE WHEN dv.p>=500 THEN dv.q ELSE 0 END)/SUM(dv.q)    da_scarcity
FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
  JOIN u ON c.unit_code = u.unit_code
WHERE {CASE_DV} IS NOT NULL GROUP BY 1,2
""").df()

ida_bids = con.execute(f"""
WITH icab_l AS (
  SELECT d, session_number, offer_code, version, unit_code FROM (
    SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),session_number,
                              offer_code,unit_code ORDER BY version DESC) rn
    FROM '{ICAB}' WHERE date >= '2024-06-14' AND buy_sell='V') WHERE rn=1),
idet AS (
  SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
         price_eur_mwh p, quantity_mw q
  FROM '{IDET}' WHERE date >= '2024-06-14' AND quantity_mw > 0)
SELECT u.firm, {CASE_I} regime,
       SUM(i.q*i.p)/SUM(i.q)                                 ida_bid,
       SUM(CASE WHEN i.p>=500 THEN i.q ELSE 0 END)/SUM(i.q)   ida_scarcity
FROM idet i JOIN icab_l c
  ON i.d=c.d AND i.session_number=c.session_number
 AND i.offer_code=c.offer_code AND i.version=c.version AND i.unit_code=c.unit_code
JOIN u ON i.unit_code = u.unit_code
WHERE {CASE_I} IS NOT NULL GROUP BY 1,2
""").df()

# ---- console tables ------------------------------------------------------
pd.set_option("display.width", 175)
for df in (chain, da_bids, ida_bids):
    df["__o"] = df["regime"].map({r: i for i, r in enumerate(ORDER)})

print("\n=== Per-firm CCGT program chain, mean MWh per CCGT unit-day ===")
print("DA cleared=PDBC | Bilateral=PDBF-PDBC | Fase I=PIBCA-PDBF-PIBCI "
      "| IDA auction=PIBCI | RT2=PHF-PIBCA\n")
for firm in FIRMS:
    sub = chain[chain["firm"] == firm].sort_values("__o")
    if sub.empty:
        continue
    print(f"  {firm}  ({n_units.get(firm,0)} CCGT units)")
    for r in sub.itertuples():
        final = r.da_cleared + r.bilateral + r.faseI + r.ida_auction + r.rt2
        print(f"    {r.regime:16s}  DA {r.da_cleared:6.0f} | bilat {r.bilateral:6.0f}"
              f" | Fase I {r.faseI:7.0f} | IDA {r.ida_auction:6.0f}"
              f" | RT2 {r.rt2:6.0f} | PHF {final:7.0f}")

print("\n=== Per-firm CCGT MW-weighted bid price (EUR/MWh): DA vs IDA ===")
m = da_bids.merge(ida_bids, on=["firm", "regime", "__o"], how="outer")
for firm in FIRMS:
    sub = m[m["firm"] == firm].sort_values("__o")
    if sub.empty:
        continue
    print(f"  {firm}")
    for r in sub.itertuples():
        print(f"    {r.regime:16s}  DA bid {r.da_bid:7.0f} ({r.da_scarcity:4.0%} scarcity)"
              f" | IDA bid {r.ida_bid:6.0f} ({r.ida_scarcity:4.0%} scarcity)")

# ---- figure: per-firm stacked program chain ------------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

comps = [("da_cleared",  "DA cleared (PDBC)",                 "#4c72b0"),
         ("bilateral",   "Bilateral",                          "#bbbbbb"),
         ("faseI",       "Fase I redispatch (pre-IDA, REE)",    "#d62728"),
         ("ida_auction", "IDA auction clearing",                "#dd8452"),
         ("rt2",         "RT2 (post-IDA, REE)",                 "#8c564b")]
fig, axes = plt.subplots(1, 4, figsize=(15, 4.8), sharey=True)
for ax, firm in zip(axes, FIRMS):
    sub = chain[chain["firm"] == firm].sort_values("__o")
    x = np.arange(len(sub))
    bottom = np.zeros(len(sub))
    for col, lab, c in comps:
        vals = sub[col].clip(lower=0).values
        ax.bar(x, vals, bottom=bottom, color=c, width=0.62,
               label=lab if firm == "IB" else None)
        bottom += vals
    ax.set_xticks(x)
    ax.set_xticklabels(sub["regime"], rotation=35, ha="right", fontsize=7.5)
    ax.set_title(f"{firm}  ({n_units.get(firm,0)} CCGT units)", fontsize=10)
    ax.grid(axis="y", alpha=0.3, lw=0.5)
axes[0].set_ylabel("mean MWh per CCGT unit-day", fontsize=9.5)
fig.legend(loc="upper center", ncol=5, fontsize=8.3, bbox_to_anchor=(0.5, 1.03),
           frameon=False)
fig.suptitle("CCGT program chain by firm: where each fleet's dispatched energy "
             "enters", fontsize=11, y=1.10)
fig.tight_layout()
fig.savefig(FIG, bbox_inches="tight", dpi=130)
plt.close(fig)
print(f"\nwrote {FIG}")
