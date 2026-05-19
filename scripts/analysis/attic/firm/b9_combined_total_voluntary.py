# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 combined IDA + continuous voluntary repositioning — strong friction test
# CLAIM: If asymmetric clocks compress strategic conduct GENUINELY (not just
#        relocate it), then total voluntary post-DA repositioning q^total =
#        q₂_IDA + q^CI should also show U-shape compression. If the
#        compression vanishes when CI is added back, substitution explains
#        the apparent IDA effect entirely. This is the falsifiable test of
#        the strong friction claim.
"""B9 combined q^total = q₂_IDA + q^CI test — does the compression survive substitution?

Builds firm-ISP-replicated grain panel with both IDA and continuous-market
voluntary repositioning, summed per firm-period. Runs the same B9 regression
on q^total.

Three possible outcomes:
1. q^total compression magnitude ≈ q₂_IDA compression → CI substitution is
   negligible; friction is real and full
2. q^total compression < q₂_IDA compression but > 0 → partial substitution;
   friction is real but mitigated by CI offset
3. q^total compression ≈ 0 → substitution explains everything; the
   "compression" was just relocation
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
PIBCICE  = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_continuo" / "programas" / "pibcice_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUTDIR   = PROJECT / "results" / "regressions" / "b9_combined_total"
OUTDIR.mkdir(parents=True, exist_ok=True)

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def replicate_to_isp_grain(df: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """Replicate MTU60 records 4× across ISPs (1..96) at value/4; keep MTU15 native."""
    mtu60 = df[df["mtu_minutes"] == 60].copy()
    mtu15 = df[df["mtu_minutes"] == 15].copy()
    if len(mtu60) > 0:
        mtu60[value_col] = mtu60[value_col] / 4.0
        mtu60["hour"] = mtu60["period"].astype(int)
        rep = mtu60.loc[mtu60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(mtu60))
        rep["period"] = (rep["hour"] - 1) * 4 + rep["k"] + 1
        rep["mtu_minutes"] = 15
        mtu60_exp = rep[["date", "period", "firm", value_col]]
    else:
        mtu60_exp = pd.DataFrame(columns=["date", "period", "firm", value_col])
    mtu15_use = mtu15[["date", "period", "firm", value_col]]
    out = pd.concat([mtu60_exp, mtu15_use], ignore_index=True)
    return out.groupby(["date", "period", "firm"], as_index=False)[value_col].sum()


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting combined q^total test…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # 1. q₂_IDA from PIBCIE (uses grupo_empresarial)
    # ============================================================
    print("[1/4] Building q₂_IDA at native firm-period…", flush=True)
    ida = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    print(f"   IDA native rows: {len(ida):,}", flush=True)

    # ============================================================
    # 2. q^CI from PIBCICE (uses grupo_short)
    # ============================================================
    print("[2/4] Building q^CI at native firm-period…", flush=True)
    ci = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_short, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS qci_mwh
        FROM '{PIBCICE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """).df()
    ci["date"] = pd.to_datetime(ci["date"])
    print(f"   CI native rows: {len(ci):,}", flush=True)
    print(f"   CI MTU dist: {dict(ci['mtu_minutes'].value_counts())}", flush=True)

    # ============================================================
    # 3. Replicate both to MTU15 grain and JOIN
    # ============================================================
    print("[3/4] Replicating MTU60 → MTU15 and combining…", flush=True)
    ida_isp = replicate_to_isp_grain(ida, "q2_mwh")
    ci_isp  = replicate_to_isp_grain(ci, "qci_mwh")
    print(f"   IDA replicated: {len(ida_isp):,}; CI replicated: {len(ci_isp):,}", flush=True)

    # Outer merge so we have all firm-period rows from either market
    df = ida_isp.merge(ci_isp, on=["date", "period", "firm"], how="outer")
    df["q2_mwh"]  = df["q2_mwh"].fillna(0)
    df["qci_mwh"] = df["qci_mwh"].fillna(0)
    df["q_total_mwh"] = df["q2_mwh"] + df["qci_mwh"]
    print(f"   merged: {len(df):,} firm-ISP rows", flush=True)

    # Add controls
    df["regime"] = df["date"].apply(assign_regime)
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"]   = df["date"].dt.dayofweek
    df["hour"]  = ((df["period"].astype(int) - 1) // 4) + 1
    df["is_big4"] = df["firm"].isin(BIG4)

    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")
    df = df[df["hour"] <= 24].copy()  # drop DST-25 records

    # Restrict to firms with active DA forward (positive cleared sells)
    da = con.execute(f"""
        SELECT date,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS q1_day_mwh
        FROM '{PDBCE}'
        GROUP BY 1, 2
    """).df()
    da["date"] = pd.to_datetime(da["date"])
    da = da[da["q1_day_mwh"] > 0].copy()
    df = df.merge(da[["date", "firm"]], on=["date", "firm"], how="inner")
    print(f"   filtered to firms with positive DA-day q₁: {len(df):,}", flush=True)
    print(f"   firms: {df.firm.nunique()}; dates: {df.date.nunique()}", flush=True)
    print(f"   Big-4 share: {df.is_big4.mean()*100:.1f}%", flush=True)

    # ============================================================
    # 4. Three trajectories: q₂_IDA, q^CI, q_total — Big-4 means by regime
    # ============================================================
    print("[4/4] Big-4 means by regime: q₂_IDA, q^CI, q_total…", flush=True)
    big4 = df[df["is_big4"]].copy()
    big4["regime"] = pd.Categorical(big4["regime"], categories=REGIMES, ordered=True)
    means = big4.groupby("regime", observed=True).agg(
        q2_ida_mean=("q2_mwh", "mean"),
        qci_mean=("qci_mwh", "mean"),
        qtotal_mean=("q_total_mwh", "mean"),
        n=("q_total_mwh", "count"),
    ).reindex(REGIMES)
    print()
    print("Big-4 mean q (MWh per firm-ISP, replicated grain):", flush=True)
    print(means.round(2).to_string(), flush=True)
    print()

    # Compression depth comparison
    means["compression_q2"]    = means["q2_ida_mean"]["pre-IDA"] - means["q2_ida_mean"]
    means["compression_qci"]   = means["qci_mean"]["pre-IDA"] - means["qci_mean"]
    means["compression_qtotal"]= means["qtotal_mean"]["pre-IDA"] - means["qtotal_mean"]
    print("Compression vs pre-IDA (MWh per firm-ISP) — positive = compressed:", flush=True)
    print(means[["compression_q2", "compression_qci", "compression_qtotal"]].round(2).to_string(), flush=True)
    print()

    means.to_csv(OUTDIR / "big4_three_trajectories.csv")

    # Per-firm trajectories
    per_firm = (big4.groupby(["firm", "regime"], observed=True)
                       .agg(q2_ida=("q2_mwh", "mean"),
                            qci=("qci_mwh", "mean"),
                            qtotal=("q_total_mwh", "mean"))
                       .reset_index())
    per_firm["regime"] = pd.Categorical(per_firm["regime"], categories=REGIMES, ordered=True)
    pv_total = per_firm.pivot(index="firm", columns="regime", values="qtotal").reindex(BIG4).reindex(REGIMES, axis=1)
    print("Per-firm q_total trajectory (MWh per firm-ISP):", flush=True)
    print(pv_total.round(1).to_string(), flush=True)
    print()
    pv_total.to_csv(OUTDIR / "big4_qtotal_perfirm_perregime.csv")

    # ============================================================
    # 5. Regression on q_total
    # ============================================================
    print("[5/4] B9 regression on q_total…", flush=True)
    df_test = df.dropna(subset=["q_total_mwh", "vre_gwh"]).copy()
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df_test["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values
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
    y = df_test["q_total_mwh"].astype(float).values
    cluster_str = df_test["date"].astype(str) + "_h" + df_test["hour"].astype(str)
    cluster = pd.Categorical(cluster_str).codes

    print(f"   Design: y={y.shape[0]:,}, X={X.shape[1]} cols, "
          f"{len(np.unique(cluster)):,} clusters", flush=True)
    mem_gb = y.shape[0] * X.shape[1] * 8 / 1e9
    print(f"   Design matrix: {mem_gb:.2f} GB", flush=True)
    t = time.time()
    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    # Extract Big-4 effect by regime
    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov_p = model.cov_params()
    print("Big-4 effect on q_total by regime (point ± SE, cluster-robust by date×hour):", flush=True)
    rows = [{"regime": "pre-IDA", "big4_effect": base, "se": float(np.sqrt(cov_p[j_big4, j_big4])),
             "diff_vs_preida": 0.0, "diff_se": 0.0, "p": float("nan")}]
    print(f"  pre-IDA   β = {base:>+9.3f}  SE = {np.sqrt(cov_p[j_big4,j_big4]):>5.3f}  (baseline)", flush=True)
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        diff = float(model.params[j])
        diff_se = float(np.sqrt(cov_p[j, j]))
        b = base + diff
        var = cov_p[j_big4, j_big4] + cov_p[j, j] + 2 * cov_p[j_big4, j]
        se = float(np.sqrt(var))
        p = float(model.pvalues[j])
        print(f"  {r:<10} β = {b:>+9.3f}  SE = {se:>5.3f}    diff vs pre-IDA = {diff:>+8.3f}  SE = {diff_se:>5.3f}  p = {p:.3e}", flush=True)
        rows.append({"regime": r, "big4_effect": b, "se": se,
                     "diff_vs_preida": diff, "diff_se": diff_se, "p": p})

    test_keys = [f"D[{r}]xBig4" for r in REGIMES[1:]]
    R = np.zeros((len(test_keys), len(model.params)))
    for i, k in enumerate(test_keys):
        R[i, list(X.columns).index(k)] = 1
    wald = model.wald_test(R, scalar=True)
    print(f"\nJoint Wald: F = {float(wald.statistic):.2f}, p = {float(wald.pvalue):.4e}", flush=True)
    print()

    pd.DataFrame(rows).to_csv(OUTDIR / "big4_qtotal_regression.csv", index=False)

    # ============================================================
    # Compare to B9 q₂_IDA-only result
    # ============================================================
    Q2_IDA_RES = PROJECT / "results" / "regressions" / "b9_replicated_isp_grain.csv"
    if Q2_IDA_RES.exists():
        ida_only = pd.read_csv(Q2_IDA_RES)
        ida_only = ida_only.set_index("regime").reindex(REGIMES)
        cmp = pd.DataFrame({
            "q₂_IDA β":      ida_only["big4_effect"].values,
            "q₂_IDA Δ":      ida_only["diff_vs_preida"].values,
            "q_total β":     [r["big4_effect"] for r in rows],
            "q_total Δ":     [r["diff_vs_preida"] for r in rows],
        }, index=REGIMES)
        cmp["substitution_share"] = (cmp["q₂_IDA Δ"] - cmp["q_total Δ"]) / cmp["q₂_IDA Δ"].replace(0, np.nan)
        print()
        print("=== STRONG-FRICTION TEST ===", flush=True)
        print("If q_total compression < q₂_IDA compression, CI substitutes for IDA.", flush=True)
        print("If q_total compression ≈ q₂_IDA compression, friction is genuine.", flush=True)
        print()
        print(cmp.round(3).to_string(), flush=True)
        cmp.to_csv(OUTDIR / "strong_friction_comparison.csv")

    print(f"\nTotal runtime: {(time.time() - t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
