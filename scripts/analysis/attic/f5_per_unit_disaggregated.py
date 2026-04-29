# STATUS: DEAD-KEPT-AS-RECORD
# LAST-AUDIT: 2026-04-29
# RETRACTION-DATE: 2026-04-29
# RETRACTION-REASON: F5 (Allaz-Vila slope β=∂ΔQ_IDA/∂q_DA) is a mechanical accounting
#   identity, not strategic-conduct evidence. Q_actual ≈ q_DA + ΔQ_IDA implies
#   ∂ΔQ_IDA/∂q_DA = ∂Q_actual/∂q_DA − 1; since q_DA explains nearly all within-unit
#   variation in Q_actual, β is mechanically near −1 regardless of strategic conduct.
#   The HDFE absorption that 'restored' F5 was confirming the identity, not testing AV.
#   AV anchor for the thesis is now B9's firm-ISP cross-regime regression
#   (b9_replicated_isp_grain.py), which identifies via cross-regime variation, not
#   within-unit slope.
"""F5 Allaz-Vila at per-unit native granularity.

Native granularity discipline:
   pre-MTU15-IDA:           hourly observations (one obs per unit-hour)
   post-MTU15-IDA, pre-DA15: 15-min observations (q_DA hourly applied to ISP)
   post-MTU15-DA:           15-min observations (both DA and IDA per-15-min)

Unit FE absorbed via within-transformation (demean by unit) to keep design
matrix tractable.  Most-active dispatchable units only to bound size.

Outputs: data/derived/results/f5_per_unit_disaggregated.csv
"""
from __future__ import annotations

import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PDBC     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PIBCI    = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
REF      = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"
OUT      = PROJECT / "data" / "derived" / "results" / "f5_per_unit_disaggregated.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # Identify dispatchable units (CCGT, hydro, nuclear) that are ACTIVE
    # post-IDA (have at least 1000 q_DA>0 hours since 2024-06).  This
    # bounds the panel size.
    ref = pd.read_csv(REF, encoding='latin1')
    is_disp = ref["technology"].fillna("").str.lower().apply(
        lambda s: ("ciclo combinado" in s) or ("hidr" in s) or ("nuclear" in s))
    candidate_units = set(ref.loc[is_disp, "unit_code"].astype(str))
    print(f"Candidate dispatchable units in lista_unidades: {len(candidate_units):,}")
    con.register("disp_set", pd.DataFrame({"unit_code": list(candidate_units)}))

    # Filter to units with substantial post-IDA activity to keep the panel manageable
    print("[1/4] Filtering to post-IDA-active dispatchable units…")
    active = con.execute(f"""
        SELECT unit_code, COUNT(*) AS n_obs
        FROM '{PDBC}'
        WHERE offer_type = 1
          AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-06-14'
          AND unit_code IN (SELECT unit_code FROM disp_set)
        GROUP BY 1
        HAVING COUNT(*) >= 1000
        ORDER BY n_obs DESC
    """).df()
    keep_units = set(active["unit_code"].astype(str).tolist())
    print(f"   active dispatchable units (≥1000 post-IDA q_DA>0 hours): {len(keep_units):,}")
    con.register("keep_set", pd.DataFrame({"unit_code": list(keep_units)}))

    # Build per-unit DA cleared (offer_type=1).
    print("[2/4] Building per-unit DA panel…")
    da = con.execute(f"""
        SELECT date, period, unit_code, mtu_minutes,
               assigned_power_mw AS q_da_mw
        FROM '{PDBC}'
        WHERE offer_type = 1
          AND assigned_power_mw > 0
          AND unit_code IN (SELECT unit_code FROM keep_set)
    """).df()
    da["date"] = pd.to_datetime(da["date"])
    # DA hour = period (mtu=60) or ceil(period/4) (mtu=15)
    da["da_hour"] = np.where(da["mtu_minutes"] == 60, da["period"],
                              np.ceil(da["period"] / 4.0).astype(int))
    # Unit's hourly DA commitment (mean across the 4 ISPs if DA is 15-min)
    da_hourly = (da.groupby(["unit_code", "date", "da_hour"], as_index=False)
                   .agg(q_da_mw_hour=("q_da_mw", "mean")))
    print(f"   DA per-unit-hour panel: {len(da_hourly):,} rows; {da_hourly.unit_code.nunique()} units")

    # Build per-unit IDA signed cleared (per-period native granularity).
    print("[3/4] Building per-unit IDA panel (per-period native)…")
    ida = con.execute(f"""
        SELECT date, period, unit_code, mtu_minutes,
               SUM(CASE WHEN offer_type IN (1,3) THEN  assigned_power_mw
                        WHEN offer_type IN (8,9) THEN -assigned_power_mw
                        ELSE 0 END) * mtu_minutes / 60.0 AS dq_ida_mwh
        FROM '{PIBCI}'
        WHERE assigned_power_mw IS NOT NULL
          AND unit_code IN (SELECT unit_code FROM keep_set)
        GROUP BY 1, 2, 3, 4
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    # IDA hour = period (mtu=60) or ceil(period/4) (mtu=15)
    ida["ida_hour"] = np.where(ida["mtu_minutes"] == 60, ida["period"],
                                np.ceil(ida["period"] / 4.0).astype(int))
    print(f"   IDA per-unit-period panel: {len(ida):,} rows; "
          f"mtu mix: {ida.mtu_minutes.value_counts().to_dict()}")

    # Join: each (unit, date, ida_period) gets its containing-hour q_DA.
    print("[4/4] Joining (each ISP gets its hourly q_DA)…")
    panel = ida.merge(da_hourly,
                      left_on=["unit_code", "date", "ida_hour"],
                      right_on=["unit_code", "date", "da_hour"],
                      how="inner")
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["regime_cat"] = pd.Categorical(panel["regime"], categories=REGIMES, ordered=True)
    panel["dow"] = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"] = panel["date"].dt.year
    print(f"   joined panel: {len(panel):,} obs;  {panel.unit_code.nunique()} units; "
          f"{panel.date.nunique():,} dates")

    # daily VRE control
    print("   adding daily VRE…")
    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    panel = panel.merge(vre, on="date", how="left")
    panel["vre_gwh"] = panel["vre_gwh"].fillna(panel["vre_gwh"].mean())
    print(f"   build time {time.time()-t0:.1f}s")
    print()

    # ----------------------------------------------------------------
    # Within-transformation by unit (absorbs unit FE)
    # ----------------------------------------------------------------
    print("[demeaning by unit to absorb unit FE…]")
    # Build the regime × q_DA interaction columns first
    for r in REGIMES[1:]:
        panel[f"qda_x_{r}"] = panel["q_da_mw_hour"] * (panel["regime"] == r).astype(float)
    cols_to_demean = ["dq_ida_mwh", "q_da_mw_hour"] + [f"qda_x_{r}" for r in REGIMES[1:]] + \
                     [f"D_{r}" for r in REGIMES[1:]] + ["vre_gwh"]
    # Build regime dummies
    for r in REGIMES[1:]:
        panel[f"D_{r}"] = (panel["regime"] == r).astype(float)
    # Hour FE, DOW FE, month FE, year FE — these vary within unit so they get demeaned too
    for h in range(2, 25):
        panel[f"H_{h}"] = (panel["ida_hour"] == h).astype(float)
        cols_to_demean.append(f"H_{h}")
    for d_ in range(1, 7):
        panel[f"DOW_{d_}"] = (panel["dow"] == d_).astype(float)
        cols_to_demean.append(f"DOW_{d_}")
    for m in range(2, 13):
        panel[f"M_{m}"] = (panel["month"] == m).astype(float)
        cols_to_demean.append(f"M_{m}")
    years = sorted(panel["year"].unique())
    for yr in years[1:]:
        panel[f"Y_{yr}"] = (panel["year"] == yr).astype(float)
        cols_to_demean.append(f"Y_{yr}")

    cols_to_demean = list(dict.fromkeys(cols_to_demean))  # dedupe, preserve order
    print(f"   columns to demean: {len(cols_to_demean)}")
    # Compute unit means for each column once
    unit_means = panel.groupby("unit_code")[cols_to_demean].transform("mean")
    demeaned = panel[cols_to_demean].values.astype(np.float32) - unit_means.values.astype(np.float32)
    df_dm = pd.DataFrame(demeaned, columns=cols_to_demean, index=panel.index)
    print(f"   demeaned matrix: {df_dm.shape}; bytes = {df_dm.values.nbytes/1e9:.2f} GB")
    print()

    # OLS on demeaned (no intercept; unit FE absorbed)
    y = df_dm["dq_ida_mwh"].values
    X_cols = ["q_da_mw_hour"] + [f"qda_x_{r}" for r in REGIMES[1:]] + \
             [f"D_{r}" for r in REGIMES[1:]] + \
             [f"H_{h}" for h in range(2,25)] + \
             [f"DOW_{d_}" for d_ in range(1,7)] + \
             [f"M_{m}" for m in range(2,13)] + \
             [f"Y_{yr}" for yr in years[1:]] + ["vre_gwh"]
    X = df_dm[X_cols].values

    print(f"[fitting OLS with cluster-robust SE by date, design = {X.shape}]")
    cluster = panel["date"].astype("category").cat.codes.values
    model = sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})
    print(f"   N = {len(panel):,}; clusters = {pd.Series(cluster).nunique():,}")
    print()

    # Extract per-regime slope (β_pre = first coef; β_r = β_pre + interaction)
    j_qda = X_cols.index("q_da_mw_hour")
    base = float(model.params[j_qda])
    se_base = float(model.bse[j_qda])
    cov = model.cov_params()

    print("=" * 95)
    print("F5 PER-UNIT regression (within-unit, native quarter-hour where available)")
    print("=" * 95)
    print()
    print("   Per-regime Allaz-Vila slope β = ∂ΔQ_IDA / ∂q_DA  (within unit, cluster-robust by date)")
    print()
    print(f"   {'regime':<14}  {'β':>11}  {'SE':>9}  {'t':>7}  {'p':>8}")
    print('   ' + '-' * 55)
    rows = [{"regime": "pre-IDA", "beta": base, "se": se_base,
             "t": base/se_base if se_base > 0 else np.nan}]
    p0 = 2*(1 - sm.distributions.norm.cdf(abs(base/se_base))) if se_base > 0 else np.nan
    print(f"   {'pre-IDA':<14}  {base:>+11.5f}  {se_base:>9.5f}  {base/se_base:>+7.2f}  {p0:>8.4f}")
    for r in REGIMES[1:]:
        j = X_cols.index(f"qda_x_{r}")
        b = base + float(model.params[j])
        var = cov[j_qda, j_qda] + cov[j, j] + 2 * cov[j_qda, j]
        se = float(np.sqrt(var))
        t = b/se
        p = 2*(1 - sm.distributions.norm.cdf(abs(t)))
        rows.append({"regime": r, "beta": b, "se": se, "t": t})
        print(f"   {r:<14}  {b:>+11.5f}  {se:>9.5f}  {t:>+7.2f}  {p:>8.4f}")
    print()

    # Joint Wald: do the regime × q_DA interactions jointly differ from zero?
    R_joint = np.zeros((len(REGIMES) - 1, len(model.params)))
    for i, r in enumerate(REGIMES[1:]):
        R_joint[i, X_cols.index(f"qda_x_{r}")] = 1
    wald = model.wald_test(R_joint, scalar=True)
    print(f"   Joint Wald  H0: regime-varying Allaz-Vila slope = 0")
    print(f"     F = {float(wald.statistic):.2f},  p = {float(wald.pvalue):.4g}")
    print()

    # Pairwise: β(DA15/ID15) = β(pre-IDA)?
    j_da15 = X_cols.index("qda_x_DA15/ID15")
    R = np.zeros((1, len(model.params)))
    R[0, j_da15] = 1
    wald2 = model.wald_test(R, scalar=True)
    print(f"   Pairwise Wald  H0: β(DA15/ID15) = β(pre-IDA)")
    print(f"     F = {float(wald2.statistic):.2f},  p = {float(wald2.pvalue):.4g}")

    OUT.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT, index=False)
    print()
    print(f"   wrote {OUT}")

    print()
    print("=" * 95)
    print("Reading: F5 per-unit vs firm-hour")
    print("=" * 95)
    print()
    print("   Previous (firm-hour, ledger row):")
    print("     IB peak-hour Δβ_peak collapsed +0.0487 → +0.0026 with hour FE — OVB-driven.")
    print()
    pre = rows[0]
    da15 = next(r for r in rows if r["regime"] == "DA15/ID15")
    delta = da15["beta"] - pre["beta"]
    print(f"   This (per-UNIT, native granularity, unit FE absorbed):")
    print(f"     β(pre-IDA)    = {pre['beta']:+.5f}  (t={pre['t']:+.2f})")
    print(f"     β(DA15/ID15)  = {da15['beta']:+.5f}  (t={da15['t']:+.2f})")
    print(f"     Δβ(DA15 − pre-IDA) = {delta:+.5f}")
    print(f"     Joint Wald p = {float(wald.pvalue):.4g}")
    print()
    if float(wald.pvalue) < 0.05 and abs(delta) > 0.01:
        print("   F5 SURVIVES at per-unit native granularity.  The Allaz-Vila slope varies")
        print("   significantly across regimes after absorbing unit FE.  Disaggregation revives F5.")
    elif float(wald.pvalue) < 0.05:
        print("   Joint Wald rejects equality but the DA15-vs-pre-IDA delta is small.")
        print("   F5 partially revived; magnitudes need chapter-level discussion.")
    else:
        print("   Joint Wald fails to reject — F5 still doesn't survive at per-unit level.")
        print("   The wounding generalises beyond the firm-hour level.")


if __name__ == "__main__":
    main()
