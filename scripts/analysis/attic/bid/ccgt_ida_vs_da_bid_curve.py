# STATUS: ALIVE
# LAST-AUDIT: 2026-05-22
# FEEDS: thesis/provisional/descriptive_facts.tex sec 9 (CCGT bidding)
# CLAIM: How CCGT plants bid in the intraday auctions (IDA) vs the day-ahead
#        (DA). The pre-IDA Fase I redispatch resolves the CCGT's grid-security
#        role before the IDAs clear, so the IDA is a market in which the
#        Fase-I-recall game is no longer in play. We aggregate each Big-4 CCGT
#        fleet's DA and IDA sell tranches into supply curves (DA15/ID15) and
#        compare. Result: in the DA the curves are two-tier (a withholding
#        shelf far above marginal cost); in the IDA the same fleets bid a
#        single competitive tier around marginal cost. The DA withholding is
#        therefore pure strategy, not cost -- the same units, same days, bid
#        at cost in the IDA and at multiples of it in the DA.
#
# OUT: figures/working/ccgt_ida_vs_da_bid_curve.pdf

from pathlib import Path
import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
P = REPO / "data/processed/omie"
DET = P / "mercado_diario/ofertas/det_all.parquet"
CAB = P / "mercado_diario/ofertas/cab_all.parquet"
IDET = P / "mercado_intradiario_subastas/ofertas/idet_all.parquet"
ICAB = P / "mercado_intradiario_subastas/ofertas/icab_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
FIG = REPO / "figures/working/ccgt_ida_vs_da_bid_curve.pdf"
FIG.parent.mkdir(parents=True, exist_ok=True)

LO, HI = "2025-10-01", "2026-05-15"      # DA15/ID15
BINW = 5.0
MC_LO, MC_HI = 100.0, 110.0               # CCGT marginal cost (sec 6.5)
SCARCITY = 200.0                          # withholding threshold (price ceiling)
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
UN = "(" + ",".join(f"'{c}'" for c in sorted(u["unit_code"])) + ")"

# ---- DA sell-bid price histogram per firm --------------------------------
da = con.execute(f"""
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
SELECT u.firm, ROUND(dv.p/{BINW})*{BINW} pbin, SUM(dv.q) mw
FROM det dv JOIN cab_l c ON dv.d=c.d AND dv.offer_code=c.offer_code
  JOIN u ON c.unit_code = u.unit_code
GROUP BY 1,2
""").fetchdf()

# ---- IDA sell-bid price histogram per firm (pooled over the 3 sessions) --
ida = con.execute(f"""
WITH icab_l AS (
  SELECT d, session_number, offer_code, version, unit_code FROM (
    SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
           ROW_NUMBER() OVER (PARTITION BY CAST(date AS DATE),session_number,
                              offer_code,unit_code ORDER BY version DESC) rn
    FROM '{ICAB}' WHERE date BETWEEN '{LO}' AND '{HI}' AND buy_sell='V'
                  AND unit_code IN {UN}) WHERE rn=1),
idet AS (
  SELECT CAST(date AS DATE) d, session_number, offer_code, version, unit_code,
         price_eur_mwh p, quantity_mw q
  FROM '{IDET}' WHERE date BETWEEN '{LO}' AND '{HI}' AND quantity_mw > 0)
SELECT u.firm, ROUND(i.p/{BINW})*{BINW} pbin, SUM(i.q) mw
FROM idet i JOIN icab_l c
  ON i.d=c.d AND i.session_number=c.session_number AND i.offer_code=c.offer_code
 AND i.version=c.version AND i.unit_code=c.unit_code
JOIN u ON i.unit_code = u.unit_code
GROUP BY 1,2
""").fetchdf()


def supply_curve(hist, firm):
    """Return (cumulative-share, price) step arrays for a firm's bid book."""
    h = hist[hist["firm"] == firm].sort_values("pbin")
    if h.empty:
        return None, None, None
    price = h["pbin"].values
    cum = np.cumsum(h["mw"].values)
    share = cum / cum[-1]
    tot = h["mw"].sum()
    mwwtd = (h["pbin"] * h["mw"]).sum() / tot
    scar = h[h["pbin"] >= SCARCITY]["mw"].sum() / tot
    x = np.concatenate([[0.0], share])
    y = np.concatenate([price, [price[-1]]])
    return (x, y), mwwtd, scar


print(f"\n=== CCGT bid: day-ahead vs intraday auctions ({LO}..{HI}) ===")
print(f"   MW-weighted bid (EUR/MWh) and withholding share (>= {SCARCITY:.0f})\n")
for firm in FIRMS:
    _, da_bid, da_s = supply_curve(da, firm)
    _, ida_bid, ida_s = supply_curve(ida, firm)
    print(f"  {firm}:  DA bid {da_bid:7.0f} ({da_s:5.1%} withheld)  |  "
          f"IDA bid {ida_bid:6.0f} ({ida_s:5.1%} withheld)")

# ---- figure: per-firm DA vs IDA supply curves ----------------------------
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

fig, axes = plt.subplots(1, 4, figsize=(15, 4.6), sharey=True)
for ax, firm in zip(axes, FIRMS):
    da_c, _, _ = supply_curve(da, firm)
    ida_c, _, _ = supply_curve(ida, firm)
    if da_c is not None:
        ax.step(da_c[0], da_c[1], where="post", color="#c44e52", lw=2.0,
                label="day-ahead")
    if ida_c is not None:
        ax.step(ida_c[0], ida_c[1], where="post", color="#4c72b0", lw=2.0,
                label="intraday auction")
    ax.axhspan(MC_LO, MC_HI, color="#7a7a7a", alpha=0.30, zorder=0, lw=0)
    ax.axhline(SCARCITY, color="#cc6600", ls="-.", lw=1.0)
    ax.set_ylim(-60, 1120)
    ax.set_xlim(0, 1)
    ax.set_title(firm, fontsize=11)
    ax.set_xlabel("cumulative share of offered MW", fontsize=8.5)
    ax.grid(alpha=0.3, lw=0.5)
axes[0].set_ylabel("bid price (EUR/MWh)", fontsize=10)
axes[0].legend(fontsize=8.5, loc="center left")
axes[0].text(0.03, MC_HI + 30, "marginal cost", fontsize=7.5, color="#555555")
axes[0].text(0.03, SCARCITY + 30, "withholding threshold", fontsize=7.5,
             color="#cc6600")
fig.suptitle("CCGT bid curves: day-ahead vs intraday auction, by firm "
             "(DA15/ID15)\nthe day-ahead withholding shelf is absent in the "
             "intraday auction --- the same fleets bid around marginal cost there",
             fontsize=10.5, y=1.07)
fig.tight_layout()
fig.savefig(FIG, bbox_inches="tight", dpi=130)
plt.close(fig)
print(f"\nwrote {FIG}")
