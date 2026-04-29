# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 maximum-disaggregation: firm × ISP (15-min) regression, post-MTU15-IDA only
# CLAIM: At native firm-ISP (15-min) granularity for the post-MTU15-IDA window
#        (2025-03-19 onward), the Big-4 q₂ trajectory holds and IDA-session
#        strategic content is observable at 15-min resolution.
"""B9 native firm-ISP regression (post-MTU15-IDA only).

For post-MTU15-IDA dates (>= 2025-03-19), IDA market clears at MTU15 (15-min
ISPs).  This script runs the B9 regression at NATIVE 15-min granularity —
the maximum disaggregation possible.  Spec:

    q2_isp ~ regime × Big4 + Big4
              + period FE (1..96)
              + DOW FE
              + cal-month FE
              + year FE
              + daily VRE
    cluster SE by date

Sample: 2025-03-19 onward; three regimes (ISP15-win, DA60/ID15, DA15/ID15).
Pre-MTU15-IDA excluded by construction.

Memory budget: ~1.5M firm-ISP rows × ~50 dummy columns.  Test fits in 6GB.
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
OUT      = PROJECT / "results" / "regressions" / "b9_firm_ISP_native_post_mtu15.csv"

REGIMES  = ["ISP15-win", "DA60/ID15", "DA15/ID15"]  # post-MTU15-IDA span
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-12-01"): return "3-sess"  # shouldn't appear post 2025-03-19
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting B9 firm-ISP native regression…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # IDA: per-firm × ISP at native 15-min, post-MTU15-IDA only
    # ============================================================
    print("[1/3] Building firm-ISP IDA panel (mtu_minutes=15, date >= 2025-03-19)…", flush=True)
    ida = con.execute(f"""
        SELECT date, period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw) * 0.25 AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
          AND mtu_minutes = 15
          AND CAST(date AS DATE) >= DATE '2025-03-19'
        GROUP BY 1, 2, 3
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    print(f"   IDA firm-ISP rows: {len(ida):,}", flush=True)
    print(f"   firms: {ida.firm.nunique()}, dates: {ida.date.nunique()}, periods: {ida.period.nunique()}", flush=True)

    # ============================================================
    # DA: per-firm × ISP — cross-MTU mapping
    # Pre-MTU15-DA (date < 2025-10-01): DA at MTU60, expand to ISPs by replication
    # Post-MTU15-DA (date >= 2025-10-01): DA at MTU15 directly
    # ============================================================
    print("[2/3] Building DA sell volume aligned to ISP grid…", flush=True)
    print("       Pre-MTU15-DA (date < 2025-10-01): replicate MTU60 q_DA across 4 ISPs", flush=True)
    print("       Post-MTU15-DA (date >= 2025-10-01): native MTU15 q_DA", flush=True)
    da_pre = con.execute(f"""
        SELECT date,
               period AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS q1_mwh
        FROM '{PDBCE}'
        WHERE CAST(date AS DATE) >= DATE '2025-03-19'
          AND CAST(date AS DATE) < DATE '2025-10-01'
          AND mtu_minutes = 60
        GROUP BY 1, 2, 3
    """).df()
    da_pre["date"] = pd.to_datetime(da_pre["date"])
    # Vectorized expansion: each MTU60 hour h → ISPs 4(h-1)+1 .. 4h, q split equally
    if len(da_pre) > 0:
        da_pre["q1_mwh"] = da_pre["q1_mwh"] / 4.0
        # Replicate 4×, add an offset 0..3
        rep = da_pre.loc[da_pre.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(da_pre))
        rep["period"] = (rep["hour"].astype(int) - 1) * 4 + rep["k"] + 1
        da_pre_isp = rep[["date", "period", "firm", "q1_mwh"]].copy()
    else:
        da_pre_isp = pd.DataFrame(columns=["date", "period", "firm", "q1_mwh"])
    print(f"   DA pre-MTU15-DA expanded to ISP: {len(da_pre_isp):,} rows", flush=True)

    da_post = con.execute(f"""
        SELECT date, period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END) * 0.25 AS q1_mwh
        FROM '{PDBCE}'
        WHERE CAST(date AS DATE) >= DATE '2025-10-01'
          AND mtu_minutes = 15
        GROUP BY 1, 2, 3
    """).df()
    da_post["date"] = pd.to_datetime(da_post["date"])
    print(f"   DA post-MTU15-DA native ISP: {len(da_post):,} rows", flush=True)
    da = pd.concat([da_pre_isp, da_post], ignore_index=True)
    print(f"   Total DA ISP: {len(da):,} rows", flush=True)

    # ============================================================
    # Join + controls
    # ============================================================
    print("[3/3] Joining + controls…", flush=True)
    df = ida.merge(da, on=["date", "period", "firm"], how="left")
    df["regime"] = df["date"].apply(assign_regime)
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"]   = df["date"].dt.dayofweek
    df["is_big4"] = df["firm"].isin(BIG4)
    df["q1_mwh"] = df["q1_mwh"].fillna(0)

    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
          AND CAST(isp_start_utc AS DATE) >= DATE '2025-03-19'
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")

    # Restrict to firm-ISPs with positive DA sell
    df = df[df["q1_mwh"] > 0].copy()
    df = df[df["regime"].isin(REGIMES)].copy()
    print(f"   Final panel: {len(df):,} firm-ISP rows", flush=True)
    print(f"   firms: {df.firm.nunique()}, dates: {df.date.nunique()}, periods: {df.period.nunique()}", flush=True)
    print(f"   Big-4 share: {df.is_big4.mean()*100:.1f}%", flush=True)
    print()

    # ============================================================
    # Raw means
    # ============================================================
    print("=== Big-4 vs Fringe firm-ISP means by regime (signed q₂, MWh per firm-ISP) ===", flush=True)
    means = (df.groupby(["regime", "is_big4"])
              .agg(mean=("q2_mwh", "mean"),
                   abs_mean=("q2_mwh", lambda s: s.abs().mean()),
                   count=("q2_mwh", "count"))
              .reset_index())
    means["regime"] = pd.Categorical(means["regime"], categories=REGIMES, ordered=True)
    print(means.sort_values(["regime", "is_big4"]).to_string(index=False), flush=True)
    print()

    # Per Big-4 firm × regime
    pf = (df[df.is_big4]
            .groupby(["firm", "regime"])
            .agg(mean=("q2_mwh", "mean"), count=("q2_mwh", "count"))
            .reset_index())
    pf["regime"] = pd.Categorical(pf["regime"], categories=REGIMES, ordered=True)
    pf_pv = pf.pivot(index="firm", columns="regime", values="mean").reindex(BIG4).reindex(REGIMES, axis=1)
    print("Per-firm × regime (Big-4, mean MWh per firm-ISP):", flush=True)
    print(pf_pv.round(2).to_string(), flush=True)
    print()

    # ============================================================
    # Regression
    # ============================================================
    print("=== Regression: q2_isp ~ regime × Big4 + Big4 + period FE + DOW + month + year + VRE ===", flush=True)
    print("    Cluster SE by date", flush=True)

    df_test = df.dropna(subset=["q2_mwh"]).copy()
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df_test["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values
    # Period FE: 96 quarters → 95 dummies (omit period 1)
    print(f"   Building period FE (96 quarters)…", flush=True)
    for p in range(2, 97):
        cols[f"P[{p}]"] = (df_test["period"] == p).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df_test["dow"] == d_).astype(float).values
    months = sorted(df_test["month"].unique())
    for m in months[1:]:
        cols[f"M[{m}]"] = (df_test["month"] == m).astype(float).values
    years = sorted(df_test["year"].unique())
    for yr in years[1:]:
        cols[f"Y[{yr}]"] = (df_test["year"] == yr).astype(float).values
    cols["vre_gwh"] = df_test["vre_gwh"].fillna(df_test["vre_gwh"].mean()).values

    X = pd.DataFrame(cols, index=df_test.index)
    y = df_test["q2_mwh"].astype(float).values
    cluster = df_test["date"].astype("category").cat.codes.values

    print(f"   Design: y={y.shape[0]:,} obs, X={X.shape[1]} columns, {len(np.unique(cluster)):,} clusters", flush=True)
    print(f"   Memory estimate: {y.shape[0] * X.shape[1] * 8 / 1e9:.2f} GB for design matrix", flush=True)
    t = time.time()
    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov  = model.cov_params()
    print("Big-4 effect by regime (point estimate ± SE, cluster-robust):", flush=True)
    print(f"  ISP15-win β = {base:>+9.3f}  SE = {np.sqrt(cov[j_big4,j_big4]):>5.3f}  (baseline)", flush=True)
    out_rows = [{"regime": "ISP15-win", "big4_effect": base, "se": float(np.sqrt(cov[j_big4,j_big4])), "diff_vs_base": 0.0, "diff_se": 0.0, "p": float("nan")}]
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        diff = float(model.params[j])
        diff_se = float(np.sqrt(cov[j, j]))
        b = base + diff
        var = cov[j_big4, j_big4] + cov[j, j] + 2 * cov[j_big4, j]
        se = float(np.sqrt(var))
        p = float(model.pvalues[j])
        print(f"  {r:<10} β = {b:>+9.3f}  SE = {se:>5.3f}    diff vs ISP15-win = {diff:>+8.3f}  SE = {diff_se:>5.3f}  p = {p:.3e}", flush=True)
        out_rows.append({"regime": r, "big4_effect": b, "se": se, "diff_vs_base": diff, "diff_se": diff_se, "p": p})
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
    pf_pv.to_csv(OUT.with_name("b9_firm_ISP_perfirm.csv"))
    means.to_csv(OUT.with_name("b9_firm_ISP_means.csv"), index=False)
    print(f"\nWrote {OUT}", flush=True)
    print(f"Total runtime: {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
