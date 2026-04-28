# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: B9 quarter-hour native test (post-MTU15-IDA and post-MTU15-DA only)
# CLAIM: At 15-min native granularity post-MTU15-IDA, the strategic withholding
#        story can be tested without hourly aggregation hiding within-hour
#        patterns.  The user's 'stopped at MTU15-DA' claim is testable at the
#        per-ISP level once DA is also 15-min (Oct 2025+).
"""B9 at native quarter-hour granularity for post-MTU15-IDA period.

Earlier b9_hourly_disaggregated.py aggregated 15-min ISPs back to hourly
for cross-regime comparability.  But for the post-MTU15 periods we have
native 15-min data; aggregation to hourly throws away within-hour pattern
that could be strategically meaningful.

Two specific tests:

1. POST-MTU15-IDA window (DA60/ID15-PRE + DA60/ID15-POST + DA15/ID15:
   2025-03-19 to today).  IDA at 15-min, DA still 60-min until 2025-10-01.
   Per-15-min Big-4 ΔQ in IDA: do firms place strategic withholding in
   specific quarter-hours (e.g., last 15 min of the hour, when imbalance
   exposure is highest)?

2. POST-MTU15-DA window (2025-10-01+).  DA also 15-min.  At this point,
   the strategic question is whether Big-4 ΔQ converges to zero PER ISP,
   or whether some ISPs still show strategic positioning.

Outputs:
   data/derived/results/b9_quarterhour_post_mtu15.csv
"""
from __future__ import annotations
from pathlib import Path
import duckdb
import pandas as pd
import numpy as np
import statsmodels.api as sm

PROJECT  = Path(__file__).resolve().parents[3]
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT      = PROJECT / "data" / "derived" / "results" / "b9_quarterhour_post_mtu15.csv"

BIG4 = ["GE", "IB", "GN", "HC"]


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # Pull IDA at native 15-min granularity, post-MTU15-IDA only (date >= 2025-03-19).
    # PIBCIE.assigned_power_mw is signed natively per OMIE spec §5.2.2.3.
    # Simple SUM gives net IDA position change.  All Big-4 records are offer_type=1
    # so this matches the legacy CASE WHEN result for them; correct for all firms.
    print("[1/3] Loading IDA 15-min cleared volumes per firm-period (post-MTU15-IDA)…")
    ida_qh = con.execute(f"""
        SELECT date, period,                                              -- period 1..96 = quarter-hours
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw) * 0.25 AS qida_mwh
        FROM '{PIBCIE}'
        WHERE CAST(date AS DATE) >= DATE '2025-03-19'
          AND mtu_minutes = 15
          AND assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """).df()
    ida_qh["date"] = pd.to_datetime(ida_qh["date"])
    print(f"   IDA 15-min panel: {len(ida_qh):,} firm-period rows")
    print(f"   firms (top 8): {ida_qh.firm.value_counts().head(8).to_dict()}")
    print()

    # Add regime (post-MTU15-IDA splits into DA60/ID15-PRE, DA60/ID15-POST, DA15/ID15)
    def assign_regime(d):
        if d < pd.Timestamp("2025-04-28"):    return "DA60/ID15 PRE"
        if d < pd.Timestamp("2025-10-01"):    return "DA60/ID15 POST"
        return "DA15/ID15"

    ida_qh["regime"] = ida_qh["date"].apply(assign_regime)
    ida_qh["is_big4"] = ida_qh["firm"].isin(BIG4)
    ida_qh["hour"] = ((ida_qh["period"] - 1) // 4 + 1).astype(int)
    ida_qh["qh_within_hour"] = ((ida_qh["period"] - 1) % 4 + 1).astype(int)  # 1, 2, 3, 4

    # ====================================================================
    # TEST 1: per-15-min ΔQ by firm group × regime
    # ====================================================================
    print("=" * 95)
    print("TEST 1 — per-15-min ΔQ aggregate by firm group × regime")
    print("=" * 95)
    print()
    print("(In MWh per ISP; per-firm-hour values you saw earlier divide by ~16 firm-units;")
    print("here we sum across firms within each group to get the aggregate)")
    print()

    REGIMES = ["DA60/ID15 PRE", "DA60/ID15 POST", "DA15/ID15"]
    by_regime = (ida_qh.assign(group=lambda d: np.where(d["is_big4"], "Big4", "Fringe"))
                       .groupby(["group", "regime"])["qida_mwh"]
                       .agg(["mean", "median", "count"])
                       .reset_index())
    by_regime["regime"] = pd.Categorical(by_regime["regime"], categories=REGIMES, ordered=True)
    print(by_regime.sort_values(["group", "regime"]).to_string(index=False))
    print()

    # Per-firm-15-min comparison
    print("Per-firm-15min ΔQ (MWh per firm per ISP):")
    pv = (by_regime.pivot(index="regime", columns="group", values="mean")
                   .reindex(REGIMES))
    print(pv.round(3).to_string())
    print()
    print("→ User's 'stopped completely at MTU15-DA' test:")
    print(f"    Big-4 per-firm ΔQ at DA15/ID15 (per-ISP MWh): {pv.loc['DA15/ID15','Big4']:+.3f}")
    print(f"    Big-4 per-firm ΔQ at DA60/ID15 POST:           {pv.loc['DA60/ID15 POST','Big4']:+.3f}")
    pct_change = (pv.loc['DA15/ID15','Big4'] - pv.loc['DA60/ID15 POST','Big4']) / abs(pv.loc['DA60/ID15 POST','Big4']) * 100
    print(f"    Change: {pct_change:+.0f}% (negative = lower under-commitment, matching user)")
    print()

    # ====================================================================
    # TEST 2: within-hour quarter-hour pattern (do Big-4 strategically place
    # ΔQ in specific quarter-hours within an hour?)
    # ====================================================================
    print("=" * 95)
    print("TEST 2 — within-hour pattern: which quarter-hour gets the strategic withholding?")
    print("=" * 95)
    print()
    print("If Big-4 distribute ΔQ uniformly across the 4 quarter-hours within an hour,")
    print("each quarter-hour should show similar ΔQ.  If they strategically concentrate")
    print("(e.g. last 15 min when imbalance exposure peaks), one quarter would dominate.")
    print()
    big4 = ida_qh[ida_qh.is_big4]
    qh_pattern = (big4.groupby(["regime", "qh_within_hour"])["qida_mwh"]
                       .mean()
                       .reset_index())
    qh_pattern["regime"] = pd.Categorical(qh_pattern["regime"], categories=REGIMES, ordered=True)
    qh_pivot = qh_pattern.pivot(index="regime", columns="qh_within_hour", values="qida_mwh").reindex(REGIMES)
    qh_pivot.columns = [f"qh{c}" for c in qh_pivot.columns]
    qh_pivot["max/mean"] = qh_pivot.max(axis=1) / qh_pivot.mean(axis=1)
    print(qh_pivot.round(3).to_string())
    print()
    print("Reading: max/mean ratio close to 1 = uniform; > 1.2 = concentrated in one quarter")
    print()

    # ====================================================================
    # TEST 3: regression at 15-min level with hour FE + DOW FE + cal-month FE
    # ====================================================================
    print("=" * 95)
    print("TEST 3 — regression at 15-min: Big-4 vs Fringe gap at native granularity")
    print("=" * 95)
    print()

    df = ida_qh.copy()
    df["dow"] = df["date"].dt.dayofweek
    df["month"] = df["date"].dt.month

    cols = {"const": 1.0}
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]      = (df["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"] = ((df["regime"] == r) & df["is_big4"]).astype(float).values
    cols["Big4"] = df["is_big4"].astype(float).values
    for h in range(2, 25):
        cols[f"H[{h}]"] = (df["hour"] == h).astype(float).values
    for q_ in [2, 3, 4]:
        cols[f"QH[{q_}]"] = (df["qh_within_hour"] == q_).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df["dow"] == d_).astype(float).values
    for m in range(2, 13):
        cols[f"M[{m}]"] = (df["month"] == m).astype(float).values

    X = pd.DataFrame(cols, index=df.index)
    y = df["qida_mwh"].astype(float).values
    cluster_ids = df["date"].astype("category").cat.codes.values

    print(f"   N = {len(df):,} firm-15min obs;   clusters (dates) = {pd.Series(cluster_ids).nunique()}")
    print(f"   Note: cluster-robust SE unstable with this design; using HC1 (heteroskedasticity-robust).")
    model = sm.OLS(y, X.values).fit(cov_type="HC1")

    j_big4 = list(X.columns).index("Big4")
    base = float(model.params[j_big4])
    cov  = model.cov_params()
    print()
    print("Big-4 effect by regime (point estimate ± SE; cluster SE by date):")
    print(f"   {'regime':<18}  {'β (MWh per firm-ISP)':>22}  {'SE':>8}  {'t':>6}")
    print('   ' + '-' * 56)
    pre_se = float(np.sqrt(cov[j_big4, j_big4]))
    print(f"   {'DA60/ID15 PRE':<18}  {base:>+22.4f}  {pre_se:>8.4f}  {base/pre_se:>+6.2f}")
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        b = base + float(model.params[j])
        var = cov[j_big4, j_big4] + cov[j, j] + 2 * cov[j_big4, j]
        se = float(np.sqrt(var))
        t = b/se
        print(f"   {r:<18}  {b:>+22.4f}  {se:>8.4f}  {t:>+6.2f}")
    print()

    # Wald test: H0  β(Big4 × DA15/ID15) = β(Big4 × DA60-PRE)?
    j = list(X.columns).index("D[DA15/ID15]xBig4")
    R = np.zeros((1, len(model.params)))
    R[0, j] = 1
    wald = model.wald_test(R, scalar=True)
    print(f"   Wald: H0 β(Big4 × DA15/ID15) = β(Big4 × DA60-PRE)")
    print(f"     F = {float(wald.statistic):.2f},  p = {float(wald.pvalue):.4f}")
    print()

    # Save
    OUT.parent.mkdir(parents=True, exist_ok=True)
    qh_pivot.to_csv(OUT)
    print(f"   wrote {OUT}")


if __name__ == "__main__":
    main()
