# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis Table 7 -- an OLS counterpart to the BSTS imbalance results, so
#        the section does not rest on a single estimator. The ISP15 settlement
#        series only begins 2024-12-11, so the pre-window is inherently short
#        (a single winter for ID15); we therefore use a parsimonious spec
#        WITHOUT month fixed effects -- month FE on a ~5-month window are
#        collinear with the short post window (April appears only post) and
#        spuriously absorb the treatment. Controls: wind, solar, gas, day-of-week.
#        Newey-West HAC SE, lag 7.
#
# Outcomes (ESIOS quarter-hourly settlement, daily means):
#   bs3_eur_mwh           -- net penalty per MWh of imbalance
#   price_dev_up_eur_mwh  -- up-deviation (short) settlement price (prdvsuqh)
#   price_dev_dn_eur_mwh  -- down-deviation (long) settlement price (prdvbaqh)
#   abs_imbalance_mwh     -- aggregate imbalance volume
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_imbalance.csv

from pathlib import Path
import duckdb
import pandas as pd
import statsmodels.formula.api as smf

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/ols_imbalance.csv"
con = duckdb.connect()
imb = con.execute(f"select * from '{REPO}/data/derived/panels/bsts_imbalance_daily.parquet'").df()
pen = con.execute(f"select d, abs_imbalance_mwh, bs3_eur_mwh from '{REPO}/data/derived/panels/bsts_imbalance_penalty_daily.parquet'").df()
cov = con.execute(f"select d, wind_gwh, solar_gwh, gas_eur from '{REPO}/data/derived/panels/bsts_quantities_panel.parquet'").df()
for x in (imb, pen, cov):
    x["d"] = pd.to_datetime(x["d"])
df = imb.merge(pen, on="d", how="outer").merge(cov, on="d", how="left").sort_values("d")
df["dow"] = df["d"].dt.dayofweek.astype(str)

WIN = {"ID15": ("2024-12-01", "2025-03-19", "2025-04-27"),
       "DA15": ("2025-04-28", "2025-10-01", "2025-12-31")}
OUTCOMES = ["bs3_eur_mwh", "price_dev_up_eur_mwh", "price_dev_dn_eur_mwh", "abs_imbalance_mwh"]


def run(out, plo, pcut, phi):
    s = df[(df["d"] >= plo) & (df["d"] <= phi)].dropna(subset=[out, "wind_gwh", "solar_gwh", "gas_eur"]).copy()
    s["post"] = (s["d"] >= pcut).astype(int)
    m = smf.ols(f"{out} ~ post + wind_gwh + solar_gwh + gas_eur + C(dow)", data=s).fit(
        cov_type="HAC", cov_kwds={"maxlags": 7})
    return m.params["post"], m.bse["post"], m.pvalues["post"], int(s["post"].sum())


rows = []
for out in OUTCOMES:
    for w, (plo, pcut, phi) in WIN.items():
        b, se, p, npost = run(out, plo, pcut, phi)
        rows.append(dict(outcome=out, reform=w, eff=round(b, 2), se=round(se, 2), p=round(p, 4), n_post=npost))
        print(f"{out:24s} {w}: {b:8.2f} (se {se:.2f}, p={p:.4f})")
pd.DataFrame(rows).to_csv(OUT, index=False)
print(f"\nWrote {OUT.relative_to(REPO)}")
