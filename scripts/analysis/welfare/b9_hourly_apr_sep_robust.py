# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 same-cal-month robustness — Apr-Sep regression with corrected q₂
# CLAIM: Restricting B9 firm-hour regression to Apr-Sep months (the only
#        calendar window that contains both pre-IDA and DA60/ID15 observations
#        across years) preserves the Big-4 progressive-collapse trajectory.
#        The collapse is NOT a seasonal artefact.
"""B9 hourly regression with Apr-Sep restriction (same-cal-month robustness).

Specification (identical to b9_hourly_disaggregated.py except the sample):
   q2 ~ regime × Big4 + Big4 + hour FE + DOW FE + cal-month FE + year FE + daily VRE
   cluster SE by date

Sample restriction: Apr-Sep months only across all regimes.
   - pre-IDA Apr-Sep: multi-year (2018-2023, all years)
   - 3-sess Apr-Sep: Jun-Sep 2024 (3 months)
   - ISP15-win Apr-Sep: empty (Dec 2024 - Mar 2025 only) — REGIME DROPPED
   - DA60/ID15 Apr-Sep: Apr-Sep 2025
   - DA15/ID15 Apr-Sep: empty (Oct 2025 - Jan 2026) — REGIME DROPPED

So this regression covers 3 regimes only, with calendar-month and weather
variation matched. It directly addresses CLAUDE.md's same-cal-month mandate.
"""
from __future__ import annotations
from pathlib import Path
import time
import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT      = PROJECT / "data" / "derived" / "results" / "b9_hourly_apr_sep_robust.csv"

REGIMES  = ["pre-IDA", "3-sess", "DA60/ID15"]  # Apr-Sep span only
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting B9 Apr-Sep regression…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    print("[1/3] Building per-firm-hour panel (Apr-Sep months only)…", flush=True)
    ida = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS qida_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND EXTRACT('month' FROM CAST(date AS DATE)) BETWEEN 4 AND 9
        GROUP BY 1, 2, 3
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    print(f"   IDA Apr-Sep hourly: {len(ida):,} firm-hour rows", flush=True)

    print("[2/3] Building DA sell volume per firm-hour (Apr-Sep)…", flush=True)
    da = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS qda_sell_mwh
        FROM '{PDBCE}'
        WHERE EXTRACT('month' FROM CAST(date AS DATE)) BETWEEN 4 AND 9
        GROUP BY 1, 2, 3
    """).df()
    da["date"] = pd.to_datetime(da["date"])
    print(f"   DA Apr-Sep hourly: {len(da):,} firm-hour rows", flush=True)

    print("[3/3] Joining + regime + VRE control…", flush=True)
    df = ida.merge(da, on=["date", "hour", "firm"], how="left")
    df["regime"] = df["date"].apply(assign_regime)
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"]   = df["date"].dt.dayofweek
    df["is_big4"] = df["firm"].isin(BIG4)
    df["qda_sell_mwh"] = df["qda_sell_mwh"].fillna(0)

    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
          AND EXTRACT('month' FROM CAST(isp_start_utc AS DATE)) BETWEEN 4 AND 9
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")

    df = df[df["qda_sell_mwh"] > 0].copy()
    # Restrict to regimes that actually span Apr-Sep
    df = df[df["regime"].isin(REGIMES)].copy()
    print(f"   restricted panel: {len(df):,} firm-hour rows, "
          f"firms: {df.firm.nunique()}, dates: {df.date.nunique()}", flush=True)

    print(f"\n=== Raw means by regime × Big4 (Apr-Sep, mean MWh/firm-hour) ===", flush=True)
    means = (df.groupby(["regime", "is_big4"])
              .agg(mean=("qida_mwh", "mean"), count=("qida_mwh", "count"))
              .reset_index())
    means["regime"] = pd.Categorical(means["regime"], categories=REGIMES, ordered=True)
    means = means.sort_values(["regime", "is_big4"])
    print(means.to_string(index=False), flush=True)
    print()

    pv = means.pivot(index="regime", columns="is_big4", values="mean").reindex(REGIMES)
    pv.columns = ["Fringe", "Big4"]
    pv["gap"] = pv["Big4"] - pv["Fringe"]
    print("Compact Apr-Sep:", flush=True)
    print(pv.round(2).to_string(), flush=True)
    print()

    # Compute per-firm trajectories
    pf = (df[df.is_big4]
            .groupby(["firm", "regime"])
            .agg(mean=("qida_mwh", "mean"), count=("qida_mwh", "count"))
            .reset_index())
    pf["regime"] = pd.Categorical(pf["regime"], categories=REGIMES, ordered=True)
    print("Per Big-4 firm × regime (Apr-Sep, MWh/firm-hour):", flush=True)
    pf_pv = pf.pivot(index="firm", columns="regime", values="mean").reindex(BIG4).reindex(REGIMES, axis=1)
    print(pf_pv.round(1).to_string(), flush=True)
    print()

    # Regression
    print("=== Regression: q2 ~ regime × Big4 + hour FE + DOW + month FE + year FE + VRE ===", flush=True)
    print("    Cluster SE by date", flush=True)

    df_test = df.dropna(subset=["qida_mwh"]).copy()
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df_test["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values
    for h in range(2, 25):
        cols[f"H[{h}]"] = (df_test["hour"] == h).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df_test["dow"] == d_).astype(float).values
    for m in range(5, 10):
        cols[f"M[{m}]"] = (df_test["month"] == m).astype(float).values
    years = sorted(df_test["year"].unique())
    for yr in years[1:]:
        cols[f"Y[{yr}]"] = (df_test["year"] == yr).astype(float).values
    cols["vre_gwh"] = df_test["vre_gwh"].fillna(df_test["vre_gwh"].mean()).values

    X = pd.DataFrame(cols, index=df_test.index)
    y = df_test["qida_mwh"].astype(float).values
    cluster = df_test["date"].astype("category").cat.codes.values

    print(f"   Design: y={y.shape[0]:,} obs, X={X.shape[1]} columns, {len(np.unique(cluster)):,} clusters", flush=True)
    t = time.time()
    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    # Big-4 effect by regime
    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov  = model.cov_params()
    print("Big-4 effect by regime (point estimate ± SE, cluster-robust):", flush=True)
    print(f"  pre-IDA   β = {base:>+9.2f}  SE = {np.sqrt(cov[j_big4,j_big4]):>5.2f}", flush=True)
    out_rows = [{"regime": "pre-IDA", "big4_effect": base, "se": float(np.sqrt(cov[j_big4,j_big4])), "diff_vs_preida": 0.0, "diff_se": 0.0, "p": float("nan")}]
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        diff = float(model.params[j])
        diff_se = float(np.sqrt(cov[j, j]))
        b = base + diff
        var = cov[j_big4, j_big4] + cov[j, j] + 2 * cov[j_big4, j]
        se = float(np.sqrt(var))
        p = float(model.pvalues[j])
        print(f"  {r:<10}  β = {b:>+9.2f}  SE = {se:>5.2f}    diff vs pre-IDA = {diff:>+8.2f}  SE = {diff_se:>5.2f}  p = {p:.3e}", flush=True)
        out_rows.append({"regime": r, "big4_effect": b, "se": se, "diff_vs_preida": diff, "diff_se": diff_se, "p": p})
    print()

    # Joint Wald
    test_keys = [f"D[{r}]xBig4" for r in REGIMES[1:]]
    R = np.zeros((len(test_keys), len(model.params)))
    for i, k in enumerate(test_keys):
        R[i, list(X.columns).index(k)] = 1
    wald = model.wald_test(R, scalar=True)
    print(f"Joint Wald: H0 all regime × Big-4 = 0  →  F = {float(wald.statistic):.2f}, p = {float(wald.pvalue):.4e}", flush=True)
    print()

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(OUT, index=False)
    pf_pv.to_csv(OUT.with_name("b9_hourly_apr_sep_robust_perfirm.csv"))
    pv.to_csv(OUT.with_name("b9_hourly_apr_sep_robust_means.csv"))
    print(f"Wrote {OUT}", flush=True)
    print(f"Total runtime: {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
