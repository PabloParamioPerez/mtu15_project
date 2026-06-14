# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis appendix -- price-effect robustness to a cross-border-flow
#        control. Net ES import (ENTSO-E physical flows FR+PT) is largely driven
#        by foreign prices and interconnector availability, hence predetermined
#        / exogenous to the Spanish granularity reform, and it shifts the
#        Spanish price (imports depress it, exports lift it). Adding it to the
#        headline pooled-renewable daily OLS tests whether the ID15 drop is
#        robust to cross-border conditions. Newey-West HAC lag 7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_xborder.csv

from pathlib import Path
import duckdb
import pandas as pd
import statsmodels.formula.api as smf

REPO = Path(__file__).resolve().parents[3]
FLOWDIR = REPO / "data/processed/entsoe/transmission"
PANEL = REPO / "data/derived/panels/bsts_quantities_panel.parquet"
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/ols_price_xborder.csv"
con = duckdb.connect()


def flow(name):
    df = con.execute(f"SELECT isp_start_utc, quantity_mw, mtu_minutes FROM '{FLOWDIR}/{name}'").df()
    df["d"] = pd.to_datetime(df["isp_start_utc"]).dt.date
    df["gwh"] = df["quantity_mw"] * df["mtu_minutes"] / 60.0 / 1000.0
    return df.groupby("d")["gwh"].sum()


imp = flow("flows_physical_fr_to_es_all.parquet").add(flow("flows_physical_pt_to_es_all.parquet"), fill_value=0)
exp = flow("flows_physical_es_to_fr_all.parquet").add(flow("flows_physical_es_to_pt_all.parquet"), fill_value=0)
net = (imp - exp).rename("net_import_gwh").reset_index()
net["d"] = pd.to_datetime(net["d"])

p = con.execute(f"SELECT * FROM '{PANEL}'").df()
p["d"] = pd.to_datetime(p["d"])
df = p.merge(net, on="d", how="left").sort_values("d")

SPECS = [("ID15 IDA", "ida_price_eur", "2025-03-19", "2025-04-27"),
         ("ID15 DA",  "da_price_eur",  "2025-03-19", "2025-04-27"),
         ("DA15 DA",  "da_price_eur",  "2025-10-01", "2025-12-31"),
         ("DA15 IDA", "ida_price_eur", "2025-10-01", "2025-12-31")]


def run(resp, pcut, phi, control):
    s = df[(df["d"] >= "2022-01-01") & (df["d"] <= phi)].copy()
    need = [resp, "wind_gwh", "solar_gwh", "gas_eur"] + (["net_import_gwh"] if control else [])
    s = s.dropna(subset=need)
    s["post"] = (s["d"] >= pcut).astype(int)
    s["y23"] = (s["d"].dt.year == 2023).astype(int)
    s["y24p"] = (s["d"].dt.year >= 2024).astype(int)
    s["t"] = (s["d"] - s["d"].min()).dt.days
    s["month"] = s["d"].dt.month.astype(str)
    s["dow"] = s["d"].dt.dayofweek.astype(str)
    # matches Spec 4 of ols_price_full_controls.R (2024+ pooled year-by-renewable + linear trend)
    rhs = ("post + wind_gwh + solar_gwh + gas_eur + wind_gwh:y23 + solar_gwh:y23 + "
           "wind_gwh:y24p + solar_gwh:y24p + t + C(month) + C(dow)")
    if control:
        rhs += " + net_import_gwh"
    m = smf.ols(f"{resp} ~ {rhs}", data=s).fit(cov_type="HAC", cov_kwds={"maxlags": 7})
    nic = m.params.get("net_import_gwh", float("nan"))
    return m.params["post"], m.bse["post"], m.pvalues["post"], nic


rows = []
for tag, resp, pcut, phi in SPECS:
    b0, _, p0, _ = run(resp, pcut, phi, control=False)
    b1, se1, p1, nic = run(resp, pcut, phi, control=True)
    rows.append(dict(leg=tag, eff_base=round(b0, 2), p_base=round(p0, 4),
                     eff_xborder=round(b1, 2), se_xborder=round(se1, 2),
                     p_xborder=round(p1, 4), net_import_coef=round(nic, 3)))
    print(f"{tag:10s} base {b0:7.2f} (p={p0:.3f}) | +xborder {b1:7.2f} (p={p1:.3f})  net_import_coef={nic:.3f}")
pd.DataFrame(rows).to_csv(OUT, index=False)
print(f"\nWrote {OUT.relative_to(REPO)}")
