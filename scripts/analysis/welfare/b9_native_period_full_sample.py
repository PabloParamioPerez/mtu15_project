# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 native-period regression (no quarter collapse)
# CLAIM: Big-4 q₂ progressive-collapse trajectory holds at NATIVE
#        per-firm-per-period granularity across all five reform regimes.
#        Pre-MTU15-IDA contributes firm-MTU60 obs (period 1..24), post-MTU15-IDA
#        contributes firm-MTU15 obs (period 1..96).  Period × MTU bucket
#        captures the natural granularity of each regime without harmonizing.
"""B9 native-period regression — no quarter collapse.

Honors the disaggregation discipline: each regime uses its native IDA
granularity.  No aggregation of MTU15 quarters into hours; no aggregation of
firm-day into firm-month.

Spec:
   q2_period ~ regime × Big4 + Big4
              + period_id FE (24 MTU60 + 96 MTU15 = 120 dummies)
              + DOW FE + cal-month FE + year FE + daily VRE
   cluster SE by date

Sample: full history (5 regimes).  Outcome unit: signed q₂ (MWh per firm-period).
For MTU60 records q₂ = sum(assigned × 1) MWh; for MTU15 records q₂ = sum(assigned × 0.25) MWh.
Both are correct MWh measures — comparable across regimes.

Memory expectation: ~1.5M firm-period rows × ~140 dummies ≈ 1.5GB design matrix.
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
OUT      = PROJECT / "data" / "derived" / "results" / "b9_native_period_full_sample.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
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
    print(f"[{time.strftime('%H:%M:%S')}] Starting B9 native-period regression…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # IDA per-firm × period at NATIVE granularity, both MTUs preserved
    # period_id = "P{mtu}_{period}" — keeps MTU60 and MTU15 periods distinct
    # ============================================================
    print("[1/3] Building firm-period IDA panel at native granularity…", flush=True)
    ida = con.execute(f"""
        SELECT date,
               period,
               mtu_minutes,
               'P' || mtu_minutes || '_' || LPAD(period::VARCHAR, 3, '0') AS period_id,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS qida_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    print(f"   IDA firm-period rows (native): {len(ida):,}", flush=True)
    print(f"   firms: {ida.firm.nunique()}, dates: {ida.date.nunique()}, "
          f"period_id values: {ida.period_id.nunique()}", flush=True)
    print(f"   Distribution by mtu_minutes: {dict(ida['mtu_minutes'].value_counts())}", flush=True)
    print()

    # ============================================================
    # DA sell volume aligned to IDA period_id.
    # For matching pre-MTU15-IDA dates: PDBCE at MTU60, period in 1..24, match directly to IDA's period_id (which is P60_*).
    # For matching post-MTU15-IDA dates pre-MTU15-DA: PDBCE still MTU60, IDA at MTU15.
    #     Replicate PDBCE row to all 4 ISPs in that hour with q_DA / 4.
    # For matching post-MTU15-DA dates: PDBCE at MTU15, period in 1..96, match directly.
    # ============================================================
    print("[2/3] Building DA sell volume aligned to IDA period grid…", flush=True)

    # DA sell volume at native MTU
    da_native = con.execute(f"""
        SELECT date,
               period,
               mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS qda_sell_mwh
        FROM '{PDBCE}'
        GROUP BY 1, 2, 3, 4
    """).df()
    da_native["date"] = pd.to_datetime(da_native["date"])
    print(f"   DA firm-period rows (native): {len(da_native):,}", flush=True)
    print(f"   DA mtu distribution: {dict(da_native['mtu_minutes'].value_counts())}", flush=True)
    print()

    # Now align DA to IDA's period_id at each (date, firm, period_id)
    # Strategy: build da_aligned that has columns matching ida's period_id grid
    #
    # Case A: ida.mtu_minutes = da_native.mtu_minutes (both 60 or both 15) → direct join on (date, firm, period)
    # Case B: ida.mtu_minutes = 15, da_native.mtu_minutes = 60 → expand DA hour h into ISPs 4(h-1)+1..4h, q/4
    # Case C: ida.mtu_minutes = 60, da_native.mtu_minutes = 15 → aggregate DA ISPs to hour
    #
    # Build the period_id from da_native to match ida's period_id format:
    # - If da_native.mtu_minutes matches ida.mtu_minutes for that date → direct
    # - Else need conversion

    # Simplification: for any (date, firm), determine ida's prevailing mtu (it's one regime).
    # Then convert DA to that mtu.

    # For each date, find IDA's mtu (post 2025-03-19 → 15; before → 60)
    print("   Aligning DA to IDA's prevailing MTU per date…", flush=True)
    ida_mtu_per_date = ida.groupby("date")["mtu_minutes"].agg(lambda s: s.mode().iat[0]).reset_index()
    ida_mtu_per_date.columns = ["date", "ida_mtu"]
    print(f"   IDA-MTU per date: {dict(ida_mtu_per_date['ida_mtu'].value_counts())}", flush=True)
    da_native = da_native.merge(ida_mtu_per_date, on="date", how="inner")  # restrict to dates IDA has

    # Case A: same mtu
    same = da_native[da_native["mtu_minutes"] == da_native["ida_mtu"]].copy()
    same["period_id_target"] = "P" + same["mtu_minutes"].astype(str) + "_" + same["period"].astype(int).astype(str).str.zfill(3)
    print(f"   Case A (same MTU): {len(same):,} rows", flush=True)

    # Case B: DA at MTU60, IDA at MTU15 → expand DA
    case_b = da_native[(da_native["mtu_minutes"] == 60) & (da_native["ida_mtu"] == 15)].copy()
    print(f"   Case B (DA60→IDA15 expand): {len(case_b):,} rows → {len(case_b)*4:,} expanded ISPs", flush=True)
    if len(case_b) > 0:
        case_b["qda_sell_mwh"] = case_b["qda_sell_mwh"] / 4.0
        rep = case_b.loc[case_b.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(case_b))
        rep["period_target"] = (rep["period"].astype(int) - 1) * 4 + rep["k"] + 1
        rep["period_id_target"] = "P15_" + rep["period_target"].astype(int).astype(str).str.zfill(3)
        case_b_exp = rep[["date", "firm", "qda_sell_mwh", "period_id_target"]].copy()
    else:
        case_b_exp = pd.DataFrame(columns=["date", "firm", "qda_sell_mwh", "period_id_target"])

    # Case C: DA at MTU15, IDA at MTU60 → shouldn't happen (IDA went MTU15 first March 2025; DA went MTU15 in Oct 2025)
    # Skip Case C.

    da_aligned = pd.concat([same[["date", "firm", "qda_sell_mwh", "period_id_target"]], case_b_exp], ignore_index=True)
    da_aligned = da_aligned.rename(columns={"period_id_target": "period_id"})
    # Re-aggregate in case of duplicates
    da_aligned = da_aligned.groupby(["date", "firm", "period_id"], as_index=False)["qda_sell_mwh"].sum()
    print(f"   DA aligned: {len(da_aligned):,} firm-period rows", flush=True)
    print()

    # ============================================================
    # Join + controls
    # ============================================================
    print("[3/3] Joining + controls…", flush=True)
    df = ida.merge(da_aligned, on=["date", "firm", "period_id"], how="left")
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
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")

    df = df[df["qda_sell_mwh"] > 0].copy()
    print(f"   Final native-period panel: {len(df):,} firm-period rows", flush=True)
    print(f"   firms: {df.firm.nunique()}, dates: {df.date.nunique()}, "
          f"period_id values: {df.period_id.nunique()}", flush=True)
    print(f"   Big-4 share: {df.is_big4.mean()*100:.1f}%", flush=True)
    print()

    # Show per-regime row counts
    print("Sample size per regime × MTU:", flush=True)
    print(df.groupby(["regime", "mtu_minutes"]).size().unstack(fill_value=0).reindex(REGIMES).to_string(), flush=True)
    print()

    # ============================================================
    # Raw means
    # ============================================================
    print("=== Big-4 vs Fringe firm-period means by regime ===", flush=True)
    print("    (mean signed q₂ MWh per firm-PERIOD; pre-MTU15-IDA = MWh/firm-hour, post = MWh/firm-ISP)", flush=True)
    means = (df.groupby(["regime", "is_big4"])
              .agg(mean=("qida_mwh", "mean"),
                   abs_mean=("qida_mwh", lambda s: s.abs().mean()),
                   count=("qida_mwh", "count"))
              .reset_index())
    means["regime"] = pd.Categorical(means["regime"], categories=REGIMES, ordered=True)
    print(means.sort_values(["regime", "is_big4"]).to_string(index=False), flush=True)
    print()

    # ============================================================
    # Regression
    # ============================================================
    print("=== Regression: q2_period ~ regime × Big4 + period_id FE + DOW + month + year + VRE ===", flush=True)
    print("    Cluster SE by date", flush=True)

    df_test = df.dropna(subset=["qida_mwh"]).copy()
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df_test["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values

    # period_id FE (drop the alphabetically first category)
    period_ids = sorted(df_test["period_id"].unique())
    print(f"   {len(period_ids)} period_id categories; dropping {period_ids[0]} as baseline", flush=True)
    for pid in period_ids[1:]:
        cols[f"PID[{pid}]"] = (df_test["period_id"] == pid).astype(float).values

    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df_test["dow"] == d_).astype(float).values
    for m in range(2, 13):
        cols[f"M[{m}]"] = (df_test["month"] == m).astype(float).values
    years = sorted(df_test["year"].unique())
    for yr in years[1:]:
        cols[f"Y[{yr}]"] = (df_test["year"] == yr).astype(float).values
    cols["vre_gwh"] = df_test["vre_gwh"].fillna(df_test["vre_gwh"].mean()).values

    X = pd.DataFrame(cols, index=df_test.index)
    y = df_test["qida_mwh"].astype(float).values
    cluster = df_test["date"].astype("category").cat.codes.values

    print(f"   Design: y={y.shape[0]:,} obs, X={X.shape[1]} columns, {len(np.unique(cluster)):,} clusters", flush=True)
    mem_gb = y.shape[0] * X.shape[1] * 8 / 1e9
    print(f"   Design matrix: {mem_gb:.2f} GB", flush=True)
    if mem_gb > 4:
        print(f"   WARNING: large design matrix; falling back to float32 to save memory", flush=True)
        Xv = X.values.astype(np.float32)
    else:
        Xv = X.values
    t = time.time()
    model = sm.OLS(y.astype(np.float64), Xv.astype(np.float64)).fit(
        cov_type="cluster", cov_kwds={"groups": cluster}
    )
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov_p  = model.cov_params()
    print("Big-4 effect by regime (point estimate ± SE, cluster-robust):", flush=True)
    print(f"  pre-IDA   β = {base:>+9.3f}  SE = {np.sqrt(cov_p[j_big4,j_big4]):>5.3f}  (baseline)", flush=True)
    out_rows = [{"regime": "pre-IDA", "big4_effect": base, "se": float(np.sqrt(cov_p[j_big4,j_big4])),
                 "diff_vs_preida": 0.0, "diff_se": 0.0, "p": float("nan")}]
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        diff = float(model.params[j])
        diff_se = float(np.sqrt(cov_p[j, j]))
        b = base + diff
        var = cov_p[j_big4, j_big4] + cov_p[j, j] + 2 * cov_p[j_big4, j]
        se = float(np.sqrt(var))
        p = float(model.pvalues[j])
        print(f"  {r:<10} β = {b:>+9.3f}  SE = {se:>5.3f}    diff vs pre-IDA = {diff:>+8.3f}  SE = {diff_se:>5.3f}  p = {p:.3e}", flush=True)
        out_rows.append({"regime": r, "big4_effect": b, "se": se,
                         "diff_vs_preida": diff, "diff_se": diff_se, "p": p})
    print()

    # Joint Wald
    test_keys = [f"D[{r}]xBig4" for r in REGIMES[1:]]
    R = np.zeros((len(test_keys), len(model.params)))
    for i, k in enumerate(test_keys):
        R[i, list(X.columns).index(k)] = 1
    wald = model.wald_test(R, scalar=True)
    print(f"Joint Wald: H0 all regime × Big-4 = 0  →  F = {float(wald.statistic):.2f}, "
          f"p = {float(wald.pvalue):.4e}", flush=True)
    print()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(OUT, index=False)
    means.to_csv(OUT.with_name("b9_native_period_means.csv"), index=False)
    print(f"Wrote {OUT}", flush=True)
    print(f"Total runtime: {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
