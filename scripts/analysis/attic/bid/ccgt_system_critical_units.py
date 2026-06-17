# STATUS: ONE-OFF
# CLAIM-CHECK: which CCGT units are system-critical, geographically, and do
#   they bid differently from non-critical units. "System-critical" is read
#   from REE's revealed need: the Fase I-up redispatch share of a unit's
#   final dispatch (PHF). A unit REE keeps rebuilding via pre-IDA Fase I
#   redispatch is one the grid needs locally for security. Per CCGT unit,
#   DA15/ID15 window, joined to the zonal map; reports Fase I reliance and
#   day-ahead bid behaviour (MW-weighted bid, scarcity share) per zone.
#
# OUT: figures/working/ccgt_system_critical_units.pdf

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
ZMAP = REPO / "data/external/ccgt_zonal_map.csv"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG = REPO / "figures/working/ccgt_system_critical_units.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"   # DA15/ID15 window (operación reforzada)
# Zones ordered roughly south/Mediterranean corridor -> north interior.
ZONE_ORDER = ["Galicia", "Sur", "Levante", "Cataluna", "Centro", "Aragon", "Norte"]


def firm_bucket(o):
    o = str(o).lower()
    if "iberdrola" in o: return "IB"
    if "endesa" in o: return "GE"
    if "naturgy" in o or "gas natural" in o: return "GN"
    if "edp" in o: return "HC"
    if "moeve" in o: return "Moeve"
    if "engie" in o: return "Engie"
    if "total" in o: return "Total"
    if "axpo" in o: return "Axpo"
    if "repsol" in o: return "Repsol"
    if "ignis" in o: return "Ignis"
    if "alpiq" in o: return "Alpiq"
    return "Other"


con = duckdb.connect()
con.execute("SET memory_limit='12GB'")
con.execute("SET threads=4")
con.execute("SET preserve_insertion_order=false")

zmap = pd.read_csv(ZMAP)
units = pd.read_csv(UNITS)[["unit_code", "owner_agent"]].drop_duplicates("unit_code")
zmap = zmap.merge(units, on="unit_code", how="left")
zmap["firm"] = zmap["owner_agent"].apply(firm_bucket)
con.register("z", zmap[["unit_code", "plant", "zone", "firm"]])
UN = "(" + ",".join(f"'{c}'" for c in zmap["unit_code"]) + ")"


def daily(parquet, maxsession=False):
    if maxsession:
        return (f"SELECT unit_code, CAST(date AS DATE) d, "
                f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM ("
                f" SELECT unit_code,date,period,assigned_power_mw,mtu_minutes,"
                f" ROW_NUMBER() OVER (PARTITION BY date,period,unit_code "
                f" ORDER BY session_number DESC) rn FROM '{parquet}' "
                f" WHERE date BETWEEN '{LO}' AND '{HI}' AND unit_code IN {UN}) "
                f"WHERE rn=1 GROUP BY 1,2")
    return (f"SELECT unit_code, CAST(date AS DATE) d, "
            f"SUM(assigned_power_mw*mtu_minutes/60.0) mwh FROM '{parquet}' "
            f"WHERE date BETWEEN '{LO}' AND '{HI}' AND unit_code IN {UN} GROUP BY 1,2")


chain = con.execute(f"""
WITH pdbc  AS ({daily(PDBC)}),
     pdbf  AS ({daily(PDBF)}),
     pibca AS ({daily(PIBCA, True)}),
     pibci AS ({daily(PIBCI)}),
     phf   AS ({daily(PHF, True)}),
allp AS (
  SELECT 'pdbc' p,* FROM pdbc UNION ALL SELECT 'pdbf' p,* FROM pdbf
  UNION ALL SELECT 'pibca' p,* FROM pibca UNION ALL SELECT 'pibci' p,* FROM pibci
  UNION ALL SELECT 'phf' p,* FROM phf),
ud AS (
  SELECT unit_code, d,
         SUM(CASE WHEN p='pdbc'  THEN mwh ELSE 0 END) pdbc,
         SUM(CASE WHEN p='pdbf'  THEN mwh ELSE 0 END) pdbf,
         SUM(CASE WHEN p='pibca' THEN mwh ELSE 0 END) pibca,
         SUM(CASE WHEN p='pibci' THEN mwh ELSE 0 END) pibci,
         SUM(CASE WHEN p='phf'   THEN mwh ELSE 0 END) phf
  FROM allp GROUP BY 1,2)
SELECT unit_code,
       AVG(pdbc)             da_cleared,
       AVG(pibca-pdbf-pibci) fase_i,
       AVG(pibci)            ida_auction,
       AVG(phf-pibca)        rt2,
       AVG(phf)              phf
FROM ud GROUP BY 1
""").df()

bids = con.execute(f"""
WITH cab_l AS (
  SELECT d, offer_code, unit_code FROM (
    SELECT CAST(date AS DATE) d, offer_code, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),offer_code,unit_code
                              ORDER BY version DESC) rn
    FROM '{CAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V'
                 AND unit_code IN {UN}) WHERE rn=1),
det AS (
  SELECT CAST(date AS DATE) d, offer_code, price_eur_mwh p, quantity_mw q
  FROM '{DET}' WHERE date BETWEEN '{LO}' AND '{HI}' AND quantity_mw > 0)
SELECT c.unit_code,
       SUM(dv.q*dv.p)/SUM(dv.q)                                da_bid,
       SUM(CASE WHEN dv.p>=500 THEN dv.q ELSE 0 END)/SUM(dv.q)  scarcity_share
FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
GROUP BY 1
""").df()

df = (zmap[["unit_code", "plant", "zone", "firm"]]
      .merge(chain, on="unit_code", how="left")
      .merge(bids, on="unit_code", how="left"))
df = df[df["phf"].fillna(0) > 1].copy()           # drop units that never run
df["fase_i_share"] = (df["fase_i"] / df["phf"]).clip(0, 1)
df["da_share"] = (df["da_cleared"] / df["phf"]).clip(0, 1)
df["zone"] = pd.Categorical(df["zone"], ZONE_ORDER, ordered=True)
df = df.sort_values(["zone", "fase_i_share"], ascending=[True, False])

pd.set_option("display.width", 170)
print(f"\n=== CCGT units, system-criticality by zone ({LO}..{HI}, DA15/ID15) ===")
print("Fase I share = REE pre-IDA redispatch / final dispatch (PHF). "
      "scarcity = % of DA-offered MW at >= 500 EUR/MWh.\n")
for z in ZONE_ORDER:
    sub = df[df["zone"] == z]
    if sub.empty:
        continue
    print(f"  --- {z} ---")
    for r in sub.itertuples():
        bid = f"{r.da_bid:6.0f}" if pd.notna(r.da_bid) else "    --"
        scar = f"{r.scarcity_share:4.0%}" if pd.notna(r.scarcity_share) else "  --"
        print(f"    {r.plant:24s} {r.firm:7s} PHF {r.phf:6.0f} MWh/d | "
              f"Fase I {r.fase_i_share:5.0%} | DA {r.da_share:5.0%} | "
              f"bid {bid} ({scar} scarcity)")

print("\n=== Per-zone summary (MWh/day totals, dispatch-weighted shares) ===")
g = df.groupby("zone", observed=True).apply(lambda s: pd.Series({
    "n_units": len(s),
    "phf_total": s["phf"].sum(),
    "fase_i_total": s["fase_i"].sum(),
    "fase_i_share": s["fase_i"].sum() / s["phf"].sum(),
    "da_share": s["da_cleared"].sum() / s["phf"].sum(),
    "scarcity_wtd": np.average(s["scarcity_share"].fillna(0),
                               weights=s["phf"].clip(lower=1)),
})).reset_index()
print(g.to_string(index=False,
                   formatters={"phf_total": "{:.0f}".format,
                               "fase_i_total": "{:.0f}".format,
                               "fase_i_share": "{:.0%}".format,
                               "da_share": "{:.0%}".format,
                               "scarcity_wtd": "{:.0%}".format}))

# ---- figure: (A) criticality by zone, (B) withholding is firm-driven -----
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

fig, (axA, axB) = plt.subplots(1, 2, figsize=(15, 5.6),
                               gridspec_kw={"width_ratios": [1, 1.35]})

# Panel A: dispatch-weighted Fase I share by zone (geography of criticality).
zg = (df.groupby("zone", observed=True)
        .apply(lambda s: pd.Series({
            "fase_i": s["fase_i"].sum() / s["phf"].sum(),
            "n": len(s)}))
        .reindex(ZONE_ORDER))
y = np.arange(len(zg))
axA.barh(y, zg["fase_i"].values, color="#c44e52", height=0.66)
axA.set_yticks(y)
axA.set_yticklabels([f"{z}  (n={int(n)})" for z, n in zip(zg.index, zg["n"])],
                    fontsize=9)
axA.invert_yaxis()
axA.set_xlabel("Fase I share of CCGT dispatch (zone total)", fontsize=9.5)
axA.set_xlim(0, 0.75)
for i, v in enumerate(zg["fase_i"].values):
    axA.text(v + 0.015, i, f"{v:.0%}", va="center", fontsize=8.5)
axA.set_title("(A) System-criticality by zone\nREE Fase I redispatch reliance",
              fontsize=10)
axA.grid(axis="x", alpha=0.3, lw=0.5)

# Panel B: Fase I reliance vs day-ahead withholding, coloured by firm.
fcol = {"GN": "#d62728", "IB": "#1f77b4", "GE": "#2ca02c", "HC": "#9467bd"}
for firm, col in list(fcol.items()) + [("other", "#9a9a9a")]:
    s = df[df["da_bid"].notna()]
    s = s[s["firm"] == firm] if firm != "other" else s[~s["firm"].isin(fcol)]
    if s.empty:
        continue
    axB.scatter(s["fase_i_share"], s["scarcity_share"], s=np.sqrt(s["phf"]) * 6,
                color=col, alpha=0.82, edgecolors="black", linewidths=0.4,
                label=firm)
axB.set_xlabel("Fase I share of final dispatch  (system-criticality)", fontsize=9.5)
axB.set_ylabel("scarcity share of day-ahead bid  (MW at $\\geq$500 EUR/MWh)",
               fontsize=9.5)
axB.set_title("(B) Day-ahead withholding does not track system-criticality\n"
              "it tracks the firm; marker size $\\propto$ dispatched MWh",
              fontsize=10)
axB.grid(alpha=0.3, lw=0.5)
axB.legend(title="owner", fontsize=8.5, loc="center right")
axB.set_xlim(-0.03, 1.03)
axB.set_ylim(-0.05, 1.0)
fig.suptitle("CCGT units key for system stability, and how they bid "
             f"({LO[:7]} onward, DA15/ID15)", fontsize=11.5, y=1.02)
fig.tight_layout()
fig.savefig(FIG, bbox_inches="tight", dpi=130)
plt.close(fig)
print(f"\nwrote {FIG}")
