# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B9 canonical attack with Fabra-Imelda-style forecast-error controls
# CLAIM: B9 progressive collapse survives the addition of demand and
#        wind+solar forecast errors as controls (canonical FI/IR convention).
"""B9 Fabra-Imelda canonical attack — adds forecast-error controls.

Per the canonical OMIE/REE literature (Fabra-Imelda 2022, Ito-Reguant 2016,
Fabra-Reguant 2014), the SUFFICIENT control for "soaking up non-strategic
reasons for differences between day-ahead and final commitments" is the
demand and wind forecast errors. F-I Eq (9) explicitly includes these
when ΔQ is the outcome.

Test 4 specs at firm-day grain, cluster SE by WEEK-OF-SAMPLE (canonical):
  Spec FI-1: q₂ ~ Big4×regime + DOW + month + WEEK-OF-SAMPLE FE
             (closest to F-I Eq 9 baseline; no q1, no year FE)
  Spec FI-2: + load forecast error
  Spec FI-3: + load forecast error + wind+solar forecast error
  Spec FI-4: + load FE + VRE FE + p_DA expected (predetermined)

If Big-4 × regime interactions remain stable (≥50% magnitude across specs),
B9 is robust to the canonical FI/IR control set.

Output:
  results/regressions/b9_canonical_fi_attack.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PIBCIE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PRICES  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
LOAD_A  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_actual_all.parquet"
LOAD_F  = PROJECT / "data" / "processed" / "entsoe" / "load" / "load_forecast_da_all.parquet"
VRE_A   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
VRE_F   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_forecast_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "b9_canonical_fi_attack.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[1/5] q2 firm-day…", flush=True)
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])

    print("[2/5] load actual + DA forecast → daily…", flush=True)
    la = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(load_mw * mtu_minutes / 60.0) / 1000.0 AS load_actual_GWh
        FROM '{LOAD_A}'
        GROUP BY 1
    """).df()
    la["date"] = pd.to_datetime(la["date"])
    lf = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(load_forecast_mw * mtu_minutes / 60.0) / 1000.0 AS load_forecast_GWh
        FROM '{LOAD_F}'
        GROUP BY 1
    """).df()
    lf["date"] = pd.to_datetime(lf["date"])
    load = la.merge(lf, on="date", how="inner")
    load["load_fe_GWh"] = load["load_actual_GWh"] - load["load_forecast_GWh"]

    print("[3/5] VRE actual + DA forecast → daily…", flush=True)
    va = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_actual_GWh
        FROM '{VRE_A}' WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    va["date"] = pd.to_datetime(va["date"])
    vf = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_forecast_GWh
        FROM '{VRE_F}' WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vf["date"] = pd.to_datetime(vf["date"])
    vre = va.merge(vf, on="date", how="inner")
    vre["vre_fe_GWh"] = vre["vre_actual_GWh"] - vre["vre_forecast_GWh"]

    print("[4/5] DA price (Spain), daily mean…", flush=True)
    pda = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, AVG(price_es_eur_mwh) AS p_da_mean
        FROM '{PRICES}'
        GROUP BY 1
    """).df()
    pda["date"] = pd.to_datetime(pda["date"])

    print("[5/5] merge + regime/calendar features…", flush=True)
    panel = (q2.merge(load[["date","load_actual_GWh","load_fe_GWh"]], on="date", how="inner")
                .merge(vre[["date","vre_actual_GWh","vre_fe_GWh"]], on="date", how="inner")
                .merge(pda, on="date", how="left"))
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["Big4"]   = panel["firm"].isin(BIG4).astype(int)
    panel["dow"]    = panel["date"].dt.dayofweek
    panel["month"]  = panel["date"].dt.month
    panel["year"]   = panel["date"].dt.year
    # Week-of-sample as YYYYWW
    iso = panel["date"].dt.isocalendar()
    panel["wos"] = iso["year"].astype(int) * 100 + iso["week"].astype(int)
    panel = panel.dropna(subset=["q2_mwh","load_actual_GWh","load_fe_GWh",
                                  "vre_actual_GWh","vre_fe_GWh","p_da_mean"])
    print(f"   firm-day panel: {len(panel):,} rows; n_wos clusters: {panel.wos.nunique()}", flush=True)

    def build(panel, ctrls):
        cols = {"const": np.ones(len(panel))}
        cols["Big4"] = panel["Big4"].values.astype(float)
        for r in REGIMES[1:]:
            cols[f"Big4×{r}"] = (panel["Big4"] * (panel["regime"] == r)).astype(float).values
        for r in REGIMES[1:]:
            cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
        for d_ in range(1, 7):
            cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
        for c in ctrls:
            cols[c] = panel[c].values
        return pd.DataFrame(cols, index=panel.index)

    cluster = panel["wos"].values  # week-of-sample, canonical

    specs = [
        ("FI-1: baseline (no FE controls)",     []),
        ("FI-2: + load FE",                      ["load_fe_GWh"]),
        ("FI-3: + load FE + VRE FE",             ["load_fe_GWh","vre_fe_GWh"]),
        ("FI-4: + load + VRE FE + p_DA + load",  ["load_fe_GWh","vre_fe_GWh","p_da_mean","load_actual_GWh"]),
    ]
    coef_dict = {}
    print(f"\n{'='*100}\nB9 CANONICAL ATTACK — Big-4 × regime, week-of-sample cluster, F-I controls\n{'='*100}\n")
    print(f"{'Term':25s}  {'FI-1 (sparse)':>14s}  {'FI-2 (+load)':>14s}  {'FI-3 (+VRE)':>14s}  {'FI-4 (+pDA+ld)':>14s}")
    print(f"{'-'*25}  {'-'*14}  {'-'*14}  {'-'*14}  {'-'*14}")
    rsq_dict = {}
    for label, ctrls in specs:
        X = build(panel, ctrls)
        y = panel["q2_mwh"].values
        m = fit_ols_cluster(y, X.values, cluster)
        coef_dict[label] = pd.Series(m.params, index=X.columns)
        rsq_dict[label] = m.rsquared

    for term in ["Big4"] + [f"Big4×{r}" for r in REGIMES[1:]]:
        line = f"{term:25s}"
        for label, _ in specs:
            v = coef_dict[label].get(term, np.nan)
            line += f"  {v:+14.2f}"
        print(line)
    print()
    for term in ["load_fe_GWh","vre_fe_GWh","p_da_mean","load_actual_GWh"]:
        line = f"{term:25s}"
        for label, _ in specs:
            v = coef_dict[label].get(term, np.nan)
            if np.isnan(v):
                line += f"  {'-':>14s}"
            else:
                line += f"  {v:+14.4f}"
        print(line)
    print()
    for label, _ in specs:
        print(f"  {label}: R² = {rsq_dict[label]:.3f}")

    df = pd.DataFrame({"term": list(coef_dict[specs[0][0]].index)})
    for label, _ in specs:
        df[f"coef_{label[:5]}"] = coef_dict[label].reindex(df["term"]).values
    df.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
