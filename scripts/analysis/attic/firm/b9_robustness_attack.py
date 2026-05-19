# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B9 robustness attack — q1 bad-control concern + year FE absorption
# CLAIM: B9 progressive Big-4 q₂_IDA collapse survives WITHOUT q₁ controls
#        (which were potentially bad controls due to Ito-Reguant identity).
"""B9 robustness attack — multi-spec robustness for the progressive collapse claim.

The B9 PDBF-augmented spec (`pdbf_disaggregated_regressions.py`) regressed
firm-ISP q₂_IDA on Big4×regime + period FE + DOW + month + year FE + VRE +
q₁_DA_day + q₁_bilateral_day, cluster SE by (date, hour). Reported Big-4 ×
regime interactions: pre-IDA baseline +47, 3-sess −21, ISP15-win −33,
DA60/ID15 −30, DA15/ID15 −26.

CONCERNS:
1. q₁_DA and q₁_bilat as controls: the Ito-Reguant identity says
   q₁_day + q₂_day ≈ Q_actual_day, so q₁_day is potentially jointly
   determined with q₂ at the day level. Including it might be a bad
   control (partials out the strategic mechanism we want to identify).
2. Year FE: regimes overlap years (2024 spans pre-IDA + 3-sess; 2025
   spans ISP15-win + DA60/ID15 + DA15/ID15). Year FE may absorb part
   of the regime variation.

Test 4 specs (firm-day grain for tractability) WITH cluster SE by date:
  Spec A: q₂_day ~ Big4×regime + cal-month + DOW + VRE  (cleanest, no q₁, no year FE)
  Spec B: + year FE                                      (test year-FE absorption)
  Spec C: + q₁_DA + q₁_bilat                             (test q₁ bad-control)
  Spec D: + year FE + q₁_DA + q₁_bilat                   (current spec, replicating)

Compare Big-4 × regime interaction coefficients across specs.
Stability ≥50% of headline magnitude across specs → B9 is robust.

Output:
  results/regressions/b9_robustness_attack.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PIBCIE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
ACTUAL  = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT     = PROJECT / "results" / "regressions" / "b9_robustness_attack.csv"

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

    # q2_IDA per firm-day from PIBCIE (simple SUM, signed natively)
    print("[1/3] q2_IDA firm-day…", flush=True)
    q2 = con.execute(f"""
        SELECT CAST(date AS DATE) AS date, COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2
    """).df()
    q2["date"] = pd.to_datetime(q2["date"])

    # q1_DA + q1_bilat per firm-day from PDBF
    print("[2/3] q1 firm-day…", flush=True)
    firms_map = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    con.register("uf", firms_map[["unit_code", "firm"]])
    q1 = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, uf.firm,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS q1_DA_GWh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS q1_bilat_GWh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        GROUP BY 1, 2
    """).df()
    q1["date"] = pd.to_datetime(q1["date"])

    # VRE generation per day
    print("[3/3] VRE…", flush=True)
    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_GWh
        FROM '{ACTUAL}' WHERE psr_type IN ('B16', 'B18', 'B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])

    # Merge
    panel = q2.merge(q1, on=["date","firm"], how="inner").merge(vre, on="date", how="left")
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["Big4"]   = panel["firm"].isin(BIG4).astype(int)
    panel["dow"]    = panel["date"].dt.dayofweek
    panel["month"]  = panel["date"].dt.month
    panel["year"]   = panel["date"].dt.year
    panel = panel.dropna(subset=["q2_mwh","q1_DA_GWh","q1_bilat_GWh","vre_GWh"])
    print(f"   firm-day panel: {len(panel):,} rows", flush=True)

    def build(panel, include_year_fe, include_q1):
        cols = {"const": np.ones(len(panel))}
        cols["Big4"] = panel["Big4"].values.astype(float)
        for r in REGIMES[1:]:
            cols[f"Big4×{r}"] = (panel["Big4"] * (panel["regime"] == r)).astype(float).values
        # regime main effects (so the panel sees regime variation in non-Big4 too)
        for r in REGIMES[1:]:
            cols[f"R_{r}"] = (panel["regime"] == r).astype(float).values
        for d_ in range(1, 7):
            cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols[f"M{m_}"] = (panel["month"] == m_).astype(float).values
        cols["vre_GWh"] = panel["vre_GWh"].values
        if include_year_fe:
            for yr in sorted(panel["year"].unique())[1:]:
                cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
        if include_q1:
            cols["q1_DA_GWh"]    = panel["q1_DA_GWh"].values
            cols["q1_bilat_GWh"] = panel["q1_bilat_GWh"].values
        return pd.DataFrame(cols, index=panel.index)

    cluster = panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values

    specs = [
        ("Spec A: clean (no year FE, no q1)",   False, False),
        ("Spec B: + year FE",                   True,  False),
        ("Spec C: + q1 controls",               False, True),
        ("Spec D: + year FE + q1 (current B9)", True,  True),
    ]
    results = []
    print(f"\n{'='*100}\nB9 ROBUSTNESS — Big-4 × regime interaction coefficients across 4 specs\n{'='*100}\n")
    print(f"{'Term':25s}  {'Spec A':>14s}  {'Spec B':>14s}  {'Spec C':>14s}  {'Spec D':>14s}")
    print(f"{'(MWh/firm-day)':25s}  {'(no FE, no q1)':>14s}  {'(year FE)':>14s}  {'(q1)':>14s}  {'(both)':>14s}")
    print(f"{'-'*25}  {'-'*14}  {'-'*14}  {'-'*14}  {'-'*14}")
    coef_dict = {}
    for label, year_fe, q1c in specs:
        X = build(panel, year_fe, q1c)
        y = panel["q2_mwh"].values
        m = fit_ols_cluster(y, X.values, cluster)
        coef_dict[label] = pd.Series(m.params, index=X.columns)
        results.append({"spec": label, "rsq": m.rsquared, "n_terms": len(X.columns)})
    for term in ["Big4"] + [f"Big4×{r}" for r in REGIMES[1:]]:
        line = f"{term:25s}"
        for label, _, _ in specs:
            v = coef_dict[label].get(term, np.nan)
            line += f"  {v:+14.2f}"
        print(line)
    print()
    # Also print q1 control coefficients for specs that have them
    for term in ["q1_DA_GWh", "q1_bilat_GWh", "vre_GWh"]:
        line = f"{term:25s}"
        for label, _, _ in specs:
            v = coef_dict[label].get(term, np.nan)
            if np.isnan(v):
                line += f"  {'-':>14s}"
            else:
                line += f"  {v:+14.4f}"
        print(line)
    print()
    for r in results:
        print(f"  {r['spec']}: R² = {r['rsq']:.3f}, n_terms = {r['n_terms']}")

    # Save
    df = pd.DataFrame({
        "term": list(coef_dict[specs[0][0]].index)
    })
    for label, _, _ in specs:
        df[f"coef_{label[:6]}"] = coef_dict[label].reindex(df["term"]).values
    df.to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
