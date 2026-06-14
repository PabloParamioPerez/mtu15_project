# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis §6.3 -- tests the "bounded per-product arbitrage capacity"
#        mechanism behind the non-collapse of the within-hour wedge, using the
#        continuous (XBID) market as the arbitrage venue, and controlling for
#        the cross-border net flow that bounds how much arbitrage is feasible.
#
# Idea: if continuous-market arbitrage were unbounded it would scale with the
# number of delivery products and close the wedge. The bounded-K_a/Q story
# predicts total continuous liquidity is roughly fixed, so when products
# quadruple at the MTU15 cutover the liquidity *per product* falls ~4x. We also
# relate the daily within-hour wedge SD to per-product XBID liquidity and the
# net cross-border flow.
#
# OUT: results/regressions/bid/mtu15_critical_flat/xbid_arbitrage_capacity.csv

from pathlib import Path
import duckdb
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
TRADES = REPO / "data/processed/omie/mercado_intradiario_continuo/transacciones/trades_all.parquet"
FLOWDIR = REPO / "data/processed/entsoe/transmission"
WEDGEVOL = REPO / "data/derived/panels/wedge_volatility_panel.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/xbid_arbitrage_capacity.csv"
con = duckdb.connect()

# --- XBID daily liquidity: total MWh, distinct products, cross-border MWh ---
xbid = con.execute(f"""
  SELECT delivery_date::DATE AS d,
         SUM(quantity_mw * mtu_minutes/60.0)/1000.0           AS xbid_gwh,
         COUNT(DISTINCT delivery_start)                       AS n_products,
         SUM(CASE WHEN buyer_zone<>seller_zone AND buyer_zone<>'' AND seller_zone<>''
                  THEN quantity_mw*mtu_minutes/60.0 ELSE 0 END)/1000.0 AS xbid_xborder_gwh
  FROM '{TRADES}'
  GROUP BY 1
""").df()
xbid["d"] = pd.to_datetime(xbid["d"])
xbid["xbid_per_product_mwh"] = 1000.0 * xbid["xbid_gwh"] / xbid["n_products"]

# --- Net cross-border import to ES (GWh/day) from ENTSO-E physical flows ---
def flow(name):
    df = con.execute(f"SELECT isp_start_utc, quantity_mw, mtu_minutes FROM '{FLOWDIR}/{name}'").df()
    df["d"] = pd.to_datetime(df["isp_start_utc"]).dt.date
    df["gwh"] = df["quantity_mw"] * df["mtu_minutes"] / 60.0 / 1000.0
    return df.groupby("d")["gwh"].sum()

imp = flow("flows_physical_fr_to_es_all.parquet").add(flow("flows_physical_pt_to_es_all.parquet"), fill_value=0)
exp = flow("flows_physical_es_to_fr_all.parquet").add(flow("flows_physical_es_to_pt_all.parquet"), fill_value=0)
net = (imp - exp).rename("net_import_gwh").reset_index()
net["d"] = pd.to_datetime(net["d"])

df = xbid.merge(net, on="d", how="left").sort_values("d")
df.to_csv(OUT, index=False)

# --- Pre/post around each reform (continuous went MTU15 at ID15) ---
def window(lo, hi):
    return df[(df["d"] >= lo) & (df["d"] <= hi)]

print("=== XBID liquidity and cross-border flow, pre/post (means) ===")
for name, prelo, prehi, postlo, posthi in [
    ("ID15", "2024-12-19", "2025-03-18", "2025-03-19", "2025-04-27"),
    ("DA15", "2025-07-01", "2025-09-30", "2025-10-01", "2025-12-31")]:
    pre, post = window(prelo, prehi), window(postlo, posthi)
    print(f"\n{name}:")
    for col, lab in [("xbid_gwh", "total XBID GWh/day"),
                     ("n_products", "distinct products/day"),
                     ("xbid_per_product_mwh", "per-product MWh"),
                     ("xbid_xborder_gwh", "cross-border XBID GWh/day"),
                     ("net_import_gwh", "net ES import GWh/day")]:
        a, b = pre[col].mean(), post[col].mean()
        print(f"  {lab:28s} pre {a:8.2f}  post {b:8.2f}  ratio {b/a:5.2f}" if a else f"  {lab}: na")

# --- Wedge SD vs per-product liquidity + cross-border, around DA15 ---
import statsmodels.formula.api as smf
wv = con.execute(f"SELECT d, wedge_sd FROM '{WEDGEVOL}'").df()
wv["d"] = pd.to_datetime(wv["d"])
reg = df.merge(wv, on="d", how="inner").dropna(subset=["wedge_sd", "xbid_per_product_mwh", "net_import_gwh"])
reg = reg[(reg["d"] >= "2024-06-14") & (reg["d"] <= "2026-02-14")]
reg["post_da15"] = (reg["d"] >= "2025-10-01").astype(int)
reg["dow"] = reg["d"].dt.dayofweek.astype(str)
m = smf.ols("wedge_sd ~ xbid_per_product_mwh + net_import_gwh + post_da15 + C(dow)",
            data=reg).fit(cov_type="HAC", cov_kwds={"maxlags": 7})
print("\n=== Wedge SD ~ per-product XBID liquidity + net import + post-DA15 (HAC) ===")
for k in ["xbid_per_product_mwh", "net_import_gwh", "post_da15"]:
    print(f"  {k:24s} {m.params[k]:8.4f}  (p={m.pvalues[k]:.3f})")
print(f"\nWrote {OUT.relative_to(REPO)}")
