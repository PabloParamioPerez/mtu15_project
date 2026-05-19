# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 — uniform MTU15 grain via MTU60 replication, no quarter collapse
# CLAIM: Big-4 q₂ progressive-collapse trajectory holds when ALL observations
#        are placed on the MTU15 grid: pre-MTU15-IDA records replicated 4× per
#        hour with q₂/4 each (preserves total hourly MWh), post-MTU15-IDA at
#        native MTU15. Outcome unit is MWh per ISP, comparable across all
#        five regimes. Cluster SE at (date, hour) absorbs the within-hour
#        artificial correlation from replication.
"""B9 uniform-MTU15 regression with replication, no quarter collapse.

Honors the user's no-collapse discipline: every observation is at MTU15
grain.  Pre-MTU15-IDA q₂ values are replicated 4× per hour at q₂/4 each
(preserves total hourly energy).  Post-MTU15-IDA q₂ at native MTU15.

Outcome unit: q₂ in MWh per ISP, comparable everywhere.

Spec:
    q2_isp ~ regime × Big4 + Big4 + period FE (1..96) + DOW + month + year + VRE
    cluster SE by (date, hour) — handles the within-hour replication

Sample: full history, 5 regimes.
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
OUT      = PROJECT / "results" / "regressions" / "b9_replicated_isp_grain.csv"

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
    print(f"[{time.strftime('%H:%M:%S')}] Starting B9 replicated-ISP-grain regression…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # IDA: per-firm × period at native granularity
    # ============================================================
    print("[1/3] Aggregating IDA at native firm-period…", flush=True)
    ida_native = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """).df()
    ida_native["date"] = pd.to_datetime(ida_native["date"])
    print(f"   IDA native rows: {len(ida_native):,}", flush=True)
    print(f"   MTU dist: {dict(ida_native['mtu_minutes'].value_counts())}", flush=True)

    # ============================================================
    # Replicate MTU60 → MTU15 grid: each hour h → ISPs 4(h-1)+1..4h, qida/4
    # ============================================================
    print("   Replicating MTU60 IDA rows to MTU15 grid (q/4 each ISP)…", flush=True)
    mtu60 = ida_native[ida_native["mtu_minutes"] == 60].copy()
    mtu15 = ida_native[ida_native["mtu_minutes"] == 15].copy()
    print(f"   MTU60 rows: {len(mtu60):,}; MTU15 rows: {len(mtu15):,}", flush=True)

    if len(mtu60) > 0:
        mtu60["q2_mwh"] = mtu60["q2_mwh"] / 4.0
        mtu60["hour"] = mtu60["period"].astype(int)
        rep = mtu60.loc[mtu60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(mtu60))
        rep["period"] = (rep["hour"] - 1) * 4 + rep["k"] + 1
        rep["mtu_minutes"] = 15  # now on MTU15 grain (replicated)
        rep["was_replicated"] = True
        mtu60_exp = rep[["date", "period", "mtu_minutes", "firm", "q2_mwh", "was_replicated"]]
    else:
        mtu60_exp = pd.DataFrame()

    mtu15["was_replicated"] = False
    mtu15["hour"] = ((mtu15["period"].astype(int) - 1) // 4) + 1
    mtu15_full = mtu15[["date", "period", "mtu_minutes", "firm", "q2_mwh", "was_replicated", "hour"]]

    if len(mtu60_exp) > 0:
        mtu60_exp["hour"] = ((mtu60_exp["period"].astype(int) - 1) // 4) + 1
        ida = pd.concat([mtu60_exp, mtu15_full], ignore_index=True)
    else:
        ida = mtu15_full

    print(f"   Total uniform-MTU15-grain rows: {len(ida):,}", flush=True)
    print()

    # ============================================================
    # DA sell volume — same replication strategy for filtering
    # ============================================================
    print("[2/3] Aggregating DA sell volume + replicating MTU60 → MTU15…", flush=True)
    da_native = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS q1_mwh
        FROM '{PDBCE}'
        GROUP BY 1, 2, 3, 4
    """).df()
    da_native["date"] = pd.to_datetime(da_native["date"])
    print(f"   DA native rows: {len(da_native):,}; MTU dist: {dict(da_native['mtu_minutes'].value_counts())}", flush=True)

    da60 = da_native[da_native["mtu_minutes"] == 60].copy()
    da15 = da_native[da_native["mtu_minutes"] == 15].copy()
    if len(da60) > 0:
        da60["q1_mwh"] = da60["q1_mwh"] / 4.0
        rep = da60.loc[da60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(da60))
        rep["period"] = (rep["period"].astype(int) - 1) * 4 + rep["k"] + 1
        rep["mtu_minutes"] = 15
        da60_exp = rep[["date", "period", "firm", "q1_mwh"]]
    else:
        da60_exp = pd.DataFrame(columns=["date", "period", "firm", "q1_mwh"])
    da15_use = da15[["date", "period", "firm", "q1_mwh"]]
    da_isp = pd.concat([da60_exp, da15_use], ignore_index=True)
    da_isp = da_isp.groupby(["date", "period", "firm"], as_index=False)["q1_mwh"].sum()
    print(f"   DA uniform-ISP rows: {len(da_isp):,}", flush=True)
    print()

    # ============================================================
    # Join + controls
    # ============================================================
    print("[3/3] Joining + controls…", flush=True)
    df = ida.merge(da_isp, on=["date", "period", "firm"], how="left")
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
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")

    df = df[df["q1_mwh"] > 0].copy()
    print(f"   Final ISP-grain panel: {len(df):,} firm-ISP rows", flush=True)
    print(f"   firms: {df.firm.nunique()}, dates: {df.date.nunique()}, periods: {df.period.nunique()}", flush=True)
    print(f"   Big-4 share: {df.is_big4.mean()*100:.1f}%", flush=True)
    print(f"   Replicated rows (pre-MTU15-IDA): {df.was_replicated.sum():,} ({df.was_replicated.mean()*100:.1f}%)", flush=True)
    print()

    # Sample sizes per regime
    print("Sample size per regime (firm-ISP rows):", flush=True)
    print(df.groupby("regime").size().reindex(REGIMES).to_string(), flush=True)
    print()

    # ============================================================
    # Raw means per regime × Big4
    # ============================================================
    print("=== Big-4 vs Fringe firm-ISP means by regime (signed q₂ MWh per ISP) ===", flush=True)
    means = (df.groupby(["regime", "is_big4"])
              .agg(mean=("q2_mwh", "mean"),
                   abs_mean=("q2_mwh", lambda s: s.abs().mean()),
                   count=("q2_mwh", "count"))
              .reset_index())
    means["regime"] = pd.Categorical(means["regime"], categories=REGIMES, ordered=True)
    print(means.sort_values(["regime", "is_big4"]).to_string(index=False), flush=True)
    print()

    # Compact pivot
    pv = means.pivot(index="regime", columns="is_big4", values="mean").reindex(REGIMES)
    pv.columns = ["Fringe", "Big4"]
    pv["gap"] = pv["Big4"] - pv["Fringe"]
    print("Compact (mean signed q₂ MWh per firm-ISP):", flush=True)
    print(pv.round(3).to_string(), flush=True)
    print()

    # Per-firm × regime
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
    # Regression with cluster SE by (date, hour)
    # ============================================================
    print("=== Regression: q2_isp ~ regime × Big4 + Big4 + period FE + DOW + month + year + VRE ===", flush=True)
    print("    Cluster SE by (date, hour) — absorbs within-hour replication correlation", flush=True)

    df_test = df.dropna(subset=["q2_mwh"]).copy()
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df_test["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values
    print(f"   Building period FE (1..96)…", flush=True)
    for p in range(2, 97):
        cols[f"P[{p}]"] = (df_test["period"] == p).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df_test["dow"] == d_).astype(float).values
    for m in range(2, 13):
        cols[f"M[{m}]"] = (df_test["month"] == m).astype(float).values
    years = sorted(df_test["year"].unique())
    for yr in years[1:]:
        cols[f"Y[{yr}]"] = (df_test["year"] == yr).astype(float).values
    cols["vre_gwh"] = df_test["vre_gwh"].fillna(df_test["vre_gwh"].mean()).values

    X = pd.DataFrame(cols, index=df_test.index)
    y = df_test["q2_mwh"].astype(float).values
    cluster_str = df_test["date"].astype(str) + "_h" + df_test["hour"].astype(str)
    cluster = pd.Categorical(cluster_str).codes

    print(f"   Design: y={y.shape[0]:,} obs, X={X.shape[1]} columns, "
          f"{len(np.unique(cluster)):,} (date, hour) clusters", flush=True)
    mem_gb = y.shape[0] * X.shape[1] * 8 / 1e9
    print(f"   Design matrix: {mem_gb:.2f} GB", flush=True)
    t = time.time()
    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov_p  = model.cov_params()
    print("Big-4 effect by regime (point estimate ± SE, cluster-robust by date×hour):", flush=True)
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
    pf_pv.to_csv(OUT.with_name("b9_replicated_isp_grain_perfirm.csv"))
    means.to_csv(OUT.with_name("b9_replicated_isp_grain_means.csv"), index=False)
    print(f"Wrote {OUT}", flush=True)
    print(f"Total runtime: {(time.time()-t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
