# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 hour-of-day interaction — model §5.7 within-regime prediction
# CLAIM: Per the structural model (model_v2.tex §5.7), the firm-level Φ_{i,r}
#        scales with ρ_i² × E[r_i,τ²] in the local Gaussian regime. The
#        intra-hour shape r_i is peakier at evening peak hours (h17–h22)
#        and morning ramp (h7–h10) and flatter at midday/overnight. The
#        model therefore predicts that Big-4 q₂ compression is concentrated
#        at peakier hours and weaker at flat hours. This script tests it.
"""B9 hour-of-day × regime × Big-4 interaction at firm-ISP-replicated grain.

Spec:
    q₂ ~ Σ_h Σ_r β_{h,r} · 1[hour=h] · 1[regime=r] · 1[Big-4]
         + period FE + DOW FE + cal-month FE + year FE + VRE
    cluster SE by (date, hour)

Honors no-quarter-collapse discipline: pre-MTU15-IDA records replicated
4× per hour at q₂/4 each; post-MTU15-IDA records at native MTU15.

Output: per (hour, regime) Big-4 effect; the model predicts the asymmetric-
window compression is concentrated at evening peak (h17-h22) and morning
ramp (h7-h10), and weaker/absent at flat midday and overnight hours.
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
OUTDIR   = PROJECT / "data" / "derived" / "results" / "b9_hour_of_day"
OUTDIR.mkdir(parents=True, exist_ok=True)

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]

# Hour buckets per the model prediction
HOUR_BUCKETS = {
    "overnight (h1-6)":   list(range(1, 7)),
    "morning ramp (h7-10)": [7, 8, 9, 10],
    "midday (h11-16)":    [11, 12, 13, 14, 15, 16],
    "evening peak (h17-22)": [17, 18, 19, 20, 21, 22],
    "late evening (h23-24)": [23, 24],
}


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] Starting B9 hour-of-day interaction…", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET threads=4")
    con.execute("SET preserve_insertion_order=false")

    # ============================================================
    # Build firm-period IDA panel at native granularity, with hour
    # ============================================================
    print("[1/3] Building firm-period IDA panel at native granularity…", flush=True)
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

    # Replicate MTU60 → MTU15 grid
    mtu60 = ida_native[ida_native["mtu_minutes"] == 60].copy()
    mtu15 = ida_native[ida_native["mtu_minutes"] == 15].copy()
    if len(mtu60) > 0:
        mtu60["q2_mwh"] = mtu60["q2_mwh"] / 4.0
        mtu60["hour"] = mtu60["period"].astype(int)
        rep = mtu60.loc[mtu60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(mtu60))
        rep["period"] = (rep["hour"] - 1) * 4 + rep["k"] + 1
        rep["mtu_minutes"] = 15
        mtu60_exp = rep[["date", "period", "mtu_minutes", "firm", "q2_mwh", "hour"]]
    else:
        mtu60_exp = pd.DataFrame()

    mtu15["hour"] = ((mtu15["period"].astype(int) - 1) // 4) + 1
    mtu15_full = mtu15[["date", "period", "mtu_minutes", "firm", "q2_mwh", "hour"]]

    if len(mtu60_exp) > 0:
        ida = pd.concat([mtu60_exp, mtu15_full], ignore_index=True)
    else:
        ida = mtu15_full
    print(f"   uniform-MTU15-grain rows: {len(ida):,}", flush=True)

    # ============================================================
    # DA forward sell — same replication, used as filter only
    # ============================================================
    print("[2/3] Building DA q₁ for filtering…", flush=True)
    da_native = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0) AS q1_mwh
        FROM '{PDBCE}'
        GROUP BY 1, 2, 3, 4
    """).df()
    da_native["date"] = pd.to_datetime(da_native["date"])

    da60 = da_native[da_native["mtu_minutes"] == 60].copy()
    da15 = da_native[da_native["mtu_minutes"] == 15].copy()
    if len(da60) > 0:
        da60["q1_mwh"] = da60["q1_mwh"] / 4.0
        rep = da60.loc[da60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(da60))
        rep["period"] = (rep["period"].astype(int) - 1) * 4 + rep["k"] + 1
        da60_exp = rep[["date", "period", "firm", "q1_mwh"]]
    else:
        da60_exp = pd.DataFrame(columns=["date", "period", "firm", "q1_mwh"])
    da15_use = da15[["date", "period", "firm", "q1_mwh"]]
    da_isp = pd.concat([da60_exp, da15_use], ignore_index=True)
    da_isp = da_isp.groupby(["date", "period", "firm"], as_index=False)["q1_mwh"].sum()

    df = ida.merge(da_isp, on=["date", "period", "firm"], how="left")
    df["q1_mwh"] = df["q1_mwh"].fillna(0)
    df = df[df["q1_mwh"] > 0].copy()  # filter to active firm-ISPs
    df["regime"] = df["date"].apply(assign_regime)
    df["year"]  = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"]   = df["date"].dt.dayofweek
    df["is_big4"] = df["firm"].isin(BIG4)
    print(f"   final firm-ISP panel: {len(df):,}", flush=True)
    print(f"   Big-4 share: {df.is_big4.mean()*100:.1f}%", flush=True)

    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")
    print()

    # ============================================================
    # 3. Raw means by hour-of-day × regime × Big-4
    # ============================================================
    print("[3/3] Big-4 q₂ by hour-of-day × regime (mean MWh per firm-ISP)…", flush=True)
    big4 = df[df["is_big4"]].copy()
    big4["regime"] = pd.Categorical(big4["regime"], categories=REGIMES, ordered=True)
    pv = (big4.groupby(["hour", "regime"], observed=True)["q2_mwh"]
              .mean().unstack("regime").reindex(REGIMES, axis=1))
    pv.to_csv(OUTDIR / "big4_q2_by_hour_x_regime.csv")
    print("Big-4 mean q₂ by hour × regime (full 24-hour panel):", flush=True)
    print(pv.round(1).to_string(), flush=True)
    print()

    # Bucketed view per the model prediction
    rows = []
    for bucket_name, hours in HOUR_BUCKETS.items():
        sub = big4[big4["hour"].isin(hours)]
        means = (sub.groupby("regime", observed=True)["q2_mwh"]
                   .agg(["mean", "count"])
                   .reindex(REGIMES))
        for r in REGIMES:
            mn = means.loc[r, "mean"] if r in means.index else np.nan
            n  = means.loc[r, "count"] if r in means.index else 0
            rows.append({"bucket": bucket_name, "regime": r, "mean": mn, "count": n})
    bucket_df = pd.DataFrame(rows)
    bucket_pv = bucket_df.pivot(index="bucket", columns="regime", values="mean").reindex(list(HOUR_BUCKETS.keys()))[REGIMES]
    bucket_df.to_csv(OUTDIR / "big4_q2_by_hourbucket_x_regime.csv", index=False)
    print("Big-4 mean q₂ by hour BUCKET × regime:", flush=True)
    print(bucket_pv.round(1).to_string(), flush=True)
    print()

    # Compression depth (pre-IDA - ISP15-win) by bucket
    bucket_pv["compression_pre_minus_ISP15"] = bucket_pv["pre-IDA"] - bucket_pv["ISP15-win"]
    bucket_pv["recovery_DA15_minus_ISP15"] = bucket_pv["DA15/ID15"] - bucket_pv["ISP15-win"]
    print("Compression and recovery by hour bucket (MWh per firm-ISP):", flush=True)
    print(bucket_pv[["compression_pre_minus_ISP15", "recovery_DA15_minus_ISP15"]].round(1).to_string(), flush=True)
    print()

    # ============================================================
    # 4. Regression with hour-bucket × regime × Big-4 interaction
    # ============================================================
    print("[4/3] Hour-bucket × regime × Big-4 regression…", flush=True)
    # Bucket assignment
    def hour_bucket(h):
        for b, hs in HOUR_BUCKETS.items():
            if h in hs:
                return b
        return "other"
    df["bucket"] = df["hour"].astype(int).apply(hour_bucket)
    BUCKETS_ORDER = list(HOUR_BUCKETS.keys())

    # Exclude hour 25 (DST artifact, sparse cells in some regimes — causes singular
    # interactions in the regression even though it's <0.5% of observations)
    df_test = df.dropna(subset=["q2_mwh", "vre_gwh"]).copy()
    df_test = df_test[df_test["hour"] <= 24].copy()
    print(f"   excluded hour=25 DST records; remaining rows: {len(df_test):,}", flush=True)
    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"] = (df_test["regime"] == r).astype(float).values
    cols["Big4"] = df_test["is_big4"].astype(float).values
    # bucket main effects
    for b in BUCKETS_ORDER[1:]:
        cols[f"B[{b}]"] = (df_test["bucket"] == b).astype(float).values
    # bucket × Big-4
    for b in BUCKETS_ORDER:
        cols[f"B[{b}]xBig4"] = ((df_test["bucket"] == b) & df_test["is_big4"]).astype(float).values
    # regime × Big-4
    for r in REGIMES[1:]:
        cols[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    # bucket × regime × Big-4
    for b in BUCKETS_ORDER:
        for r in REGIMES[1:]:
            cols[f"B[{b}]xD[{r}]xBig4"] = (
                (df_test["bucket"] == b)
                & (df_test["regime"] == r)
                & df_test["is_big4"]).astype(float).values
    # period FE (1..96)
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

    print(f"   Design: y={y.shape[0]:,} obs, X={X.shape[1]} cols, "
          f"{len(np.unique(cluster)):,} clusters", flush=True)
    mem_gb = y.shape[0] * X.shape[1] * 8 / 1e9
    print(f"   Design matrix: {mem_gb:.2f} GB", flush=True)
    if mem_gb > 4:
        print(f"   WARNING: matrix > 4 GB, will fall back if OOM", flush=True)
    t = time.time()
    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   Fit took {time.time()-t:.1f}s; R² = {model.rsquared:.3f}", flush=True)
    print()

    # Extract triple-interaction coefficients: B[bucket]×D[regime]×Big4
    # The TOTAL Big-4 effect at (bucket, regime) is:
    #   Big4 + B[bucket]xBig4 + D[regime]xBig4 + B[bucket]xD[regime]xBig4
    # (with bucket = baseline_bucket having empty B[bucket], regime = pre-IDA having empty D[regime])
    BASE_BUCKET = BUCKETS_ORDER[0]
    print("Big-4 effect by (bucket, regime), point estimates from triple interaction:", flush=True)
    rows = []
    for b in BUCKETS_ORDER:
        for r in REGIMES:
            # Build linear combination
            terms = ["Big4"]
            if b != BASE_BUCKET:
                terms.append(f"B[{b}]xBig4")
            if r != "pre-IDA":
                terms.append(f"D[{r}]xBig4")
            if b != BASE_BUCKET and r != "pre-IDA":
                terms.append(f"B[{b}]xD[{r}]xBig4")
            est = sum(model.params[list(X.columns).index(t)] for t in terms)
            rows.append({"bucket": b, "regime": r, "big4_effect": est})
    out = pd.DataFrame(rows)
    out_pv = out.pivot(index="bucket", columns="regime", values="big4_effect").reindex(BUCKETS_ORDER)[REGIMES]
    out_pv.to_csv(OUTDIR / "big4_effect_bucket_x_regime.csv")
    print(out_pv.round(1).to_string(), flush=True)
    print()
    print("Compression by bucket (pre-IDA − ISP15-win, regression-adjusted Big-4 effect):", flush=True)
    out_pv["compression"] = out_pv["pre-IDA"] - out_pv["ISP15-win"]
    out_pv["recovery"] = out_pv["DA15/ID15"] - out_pv["ISP15-win"]
    print(out_pv[["compression", "recovery"]].round(1).to_string(), flush=True)
    print()

    print(f"Total runtime: {(time.time() - t0)/60:.1f} min", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print(f"ERROR: {type(e).__name__}: {e}", flush=True)
        print(traceback.format_exc(), flush=True)
        raise
