# STATUS: ALIVE
# LAST-AUDIT: 2026-04-29
# FEEDS: B9 (Big-4 DA under-commitment, Ito-Reguant 2016 framework)
# CLAIM: At proper disaggregation (firm × hour), the user's narrative —
#        progressive monotonic collapse of Big-4 under-commitment across the
#        reform sequence with fringe firms unchanged — is testable.  The
#        previous firm-day analysis aggregated 24-96 hourly periods together,
#        masking the within-day strategic content.
#
# 2026-04-29 q₂-formula audit (load-bearing). Per OMIE spec v1.37 §5.2.2.3:
#   PIBCIE.assigned_power_mw is signed natively (range -99999.9 to 99999.9).
#   "Resultado incremental" = the change in net production scheduled by the
#   IDA session.  Simple SUM is the correct q₂.  Empirically, all Big-4 records
#   in PIBCIE have offer_type=1 only (zero type 8/9/10), so the legacy
#   CASE WHEN formula and simple SUM give identical Big-4 results — the
#   substantive Big-4 trajectory is unaffected.  The CASE WHEN formula was
#   WRONG for retailer/distributor firms (NA group) but their trajectory is
#   not the load-bearing claim.  See scripts/.../q2_definitions_compare.py.
"""B9 hourly disaggregated re-analysis: Ito-Reguant under-commitment test.

Theoretical framing (Ito and Reguant 2016 / Borenstein-Holland 2005):
   Dominant firms with market power have a strategic incentive to UNDER-
   commit in DA: by selling less in DA than they will physically generate,
   they reduce DA supply, raise the DA clearing price, and earn higher
   infra-marginal rents on inframarginal units.  The residual quantity is
   sold in IDA at lower clearing prices but the firm's average revenue
   rises.  Atomistic fringe firms have no such incentive (price-takers).

Empirical metric per (firm, period):
   ΔQ_period = q_IDA_signed_period
     where q_IDA = sum of cleared IDA volumes (offer_type 1 sell, 8 buy)
     positive = net-sold in IDA = under-committed in DA
     negative = net-bought in IDA = over-committed in DA

Aggregation choice:
   - Per period (hour pre-MTU15-IDA, ISP-15 post): the natural decision
     unit but the granularity changes mid-sample (60-min before
     2025-03-19, 15-min after).
   - To compare across regimes consistently, aggregate post-MTU15-IDA
     ISPs back to hourly (sum the 4 quarter-hours within each hour).
     Pre-MTU15-IDA already at hourly granularity.
   - Result: per-firm-per-hour panel covering the entire sample at a
     consistent hourly resolution.

This is the right level for the Ito-Reguant test.  The previous firm-day
analysis (under_commitment_audit.py) aggregated 24-96 hours together,
losing the within-day strategic content that drives the mechanism.

Specification:
   |ΔQ_h| (or signed ΔQ_h) ~ regime + firm + regime × firm + hour FE
                              + DOW FE + cal-month FE + year FE + daily VRE
   SEs clustered by date (≈ 3,000 clusters).

User's narrative to test:
   1. Big-4 under-commit (ΔQ > 0) pre-IDA.
   2. ISP15 (Dec 2024) cuts the under-commitment by ~half.
   3. MTU15-IDA (Mar 2025) continues the reduction.
   4. MTU15-DA (Oct 2025) eliminates Big-4 under-commitment entirely.
   5. Fringe firms show no regime effect (clean placebo).
   6. Same pattern in wind-only sub-sample (Ito-Reguant replication).
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBCE  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
ACTUAL = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
OUT    = PROJECT / "data" / "derived" / "results" / "b9_hourly_disaggregated.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def build_firm_hour_panel() -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[1/3] Building per-firm-hour IDA cleared volumes (signed)…")
    # Map any 15-min period to its hour: hour = ceil(period/4) for mtu=15, period for mtu=60.
    # Convert MWh: assigned_power_mw × hours = MW × (mtu_minutes/60).  Sum within hour.
    # PIBCIE.assigned_power_mw is signed natively per OMIE spec §5.2.2.3 (range
    # -99999.9 to 99999.9).  Simple SUM gives the firm's net IDA position change;
    # offer_type need NOT be sign-flipped (negative values within a sell offer
    # represent buy-back of an earlier scheduled sell).
    ida_hourly = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS qida_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3
    """).df()
    print(f"   IDA hourly panel: {len(ida_hourly):,} rows, "
          f"{ida_hourly.firm.nunique()} firms, "
          f"{ida_hourly.date.nunique()} dates")

    print("[2/3] Building per-firm-hour DA cleared volumes (sell side, for normalisation)…")
    da_hourly = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(CASE WHEN offer_type = 1 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0)                                    AS qda_sell_mwh,
               SUM(CASE WHEN offer_type = 8 THEN assigned_power_mw ELSE 0 END
                   * mtu_minutes / 60.0)                                    AS qda_buy_mwh
        FROM '{PDBCE}'
        GROUP BY 1, 2, 3
    """).df()
    print(f"   DA hourly panel: {len(da_hourly):,} rows")

    print("[3/3] Joining + assigning regime + adding daily VRE control…")
    df = ida_hourly.merge(da_hourly, on=["date", "hour", "firm"], how="left")
    df["date"] = pd.to_datetime(df["date"])
    df["regime"] = df["date"].apply(assign_regime)
    df["regime_cat"] = pd.Categorical(df["regime"], categories=REGIMES, ordered=False)
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["dow"] = df["date"].dt.dayofweek
    df["is_big4"] = df["firm"].isin(BIG4)
    df["qda_sell_mwh"] = df["qda_sell_mwh"].fillna(0)
    df["qda_buy_mwh"] = df["qda_buy_mwh"].fillna(0)
    # Normalised under-commitment: ΔQ / DA_sell_volume (firms with no DA sell get NaN).
    df["dq_share"] = df["qida_mwh"] / df["qda_sell_mwh"]
    df.loc[df["qda_sell_mwh"] <= 0, "dq_share"] = np.nan

    print(f"   joined panel: {len(df):,} firm-hour rows; "
          f"Big-4 share: {df.is_big4.mean()*100:.1f}%")

    # Daily VRE for control
    print("   adding daily VRE generation (B16+B18+B19, wind+solar)…")
    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    df = df.merge(vre, on="date", how="left")
    return df


def regime_means(df: pd.DataFrame, group: str, value: str) -> pd.DataFrame:
    """Per-group, per-regime mean of `value`, with sample sizes."""
    out = (df.groupby([group, "regime_cat"], observed=True)[value]
             .agg(["mean", "median", "count"])
             .reset_index()
             .sort_values([group, "regime_cat"]))
    return out


def fit_regression(df: pd.DataFrame, label: str, outcome: str) -> tuple[dict, sm.regression.linear_model.RegressionResultsWrapper]:
    """Run the augmented spec: regime × firm-group + hour FE + DOW FE + cal-month + year FE + VRE."""
    df = df.dropna(subset=[outcome]).copy()
    df["group"] = np.where(df["is_big4"], "Big4", "Fringe")

    cols = {"const": 1.0}
    # Regime × group interactions (Big4 baseline = pre-IDA-Big4; Fringe sees same baseline)
    for r in REGIMES[1:]:
        cols[f"D[{r}]"]              = (df["regime"] == r).astype(float).values
        cols[f"D[{r}]xBig4"]          = ((df["regime"] == r) & df["is_big4"]).astype(float).values
    cols["Big4"] = df["is_big4"].astype(float).values
    # Hour FE
    for h in range(2, 25):
        cols[f"H[{h}]"] = (df["hour"] == h).astype(float).values
    # DOW FE
    for d_ in range(1, 7):
        cols[f"DOW[{d_}]"] = (df["dow"] == d_).astype(float).values
    # cal-month FE
    for m in range(2, 13):
        cols[f"M[{m}]"] = (df["month"] == m).astype(float).values
    # year FE
    years = sorted(df["year"].unique())
    for yr in years[1:]:
        cols[f"Y[{yr}]"] = (df["year"] == yr).astype(float).values
    cols["vre_gwh"] = df["vre_gwh"].fillna(df["vre_gwh"].mean()).values

    X = pd.DataFrame(cols, index=df.index)
    y = df[outcome].astype(float).values
    cluster = df["date"].astype("category").cat.codes.values

    model = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster})

    out = {"label": label, "outcome": outcome, "n": len(df), "n_clusters": int(np.unique(cluster).size),
           "r2": float(model.rsquared)}
    # Big-4 effect by regime = baseline (pre-IDA-Big4) + interaction.
    j_big4 = list(X.columns).index("Big4")
    base   = float(model.params[j_big4])
    cov    = model.cov_params()
    out["pre-IDA_Big4"]    = base
    out["pre-IDA_Big4_se"] = float(np.sqrt(cov[j_big4, j_big4]))
    for r in REGIMES[1:]:
        j = list(X.columns).index(f"D[{r}]xBig4")
        b = base + float(model.params[j])
        var = cov[j_big4, j_big4] + cov[j, j] + 2 * cov[j_big4, j]
        out[f"{r}_Big4"]    = b
        out[f"{r}_Big4_se"] = float(np.sqrt(var))
    return out, model


def main() -> None:
    df = build_firm_hour_panel()
    print()

    # Restrict to firms with positive sell-side activity (relevant for the IR test)
    df_sell = df[df["qda_sell_mwh"] > 0].copy()
    print(f"Restricted to firm-hours with DA sell volume > 0: {len(df_sell):,} rows  "
          f"({len(df_sell)/len(df)*100:.0f}% of all firm-hours)")
    print()

    # ========================================================================
    # 1. Raw means per (group, regime) — the user's headline numbers
    # ========================================================================
    print("=" * 95)
    print("1. RAW MEANS — per-firm-hour ΔQ (= signed IDA cleared MWh) by group × regime")
    print("=" * 95)
    print()
    print("   Positive ΔQ = net-sold in IDA = under-committed in DA (Ito-Reguant strategic prediction)")
    print()

    df_sell["group"] = np.where(df_sell["is_big4"], "Big4", "Fringe")
    raw = (df_sell.groupby(["group", "regime_cat"], observed=True)
                  .agg(qida_mean_mwh=("qida_mwh", "mean"),
                       qida_median_mwh=("qida_mwh", "median"),
                       n_firmhours=("qida_mwh", "count"))
                  .reset_index())
    raw["regime_cat"] = pd.Categorical(raw["regime_cat"], categories=REGIMES, ordered=True)
    raw = raw.sort_values(["group", "regime_cat"])
    print(raw.to_string(index=False))
    print()

    # Pivot for compact comparison
    print("   Compact comparison (mean ΔQ MWh per firm-hour):")
    pv = raw.pivot(index="regime_cat", columns="group", values="qida_mean_mwh")
    pv["Big4 / Fringe"] = pv["Big4"] / pv["Fringe"].abs() if "Fringe" in pv.columns else np.nan
    print(pv.round(2).to_string())
    print()

    # User's specific test: progressive monotonic collapse for Big-4
    print("   Progressive Big-4 trajectory (mean ΔQ, MWh/firm-hour):")
    big4_traj = pv["Big4"].reindex(REGIMES).round(1)
    base = big4_traj.iloc[0]
    print(f"     {'pre-IDA':<10}  {big4_traj['pre-IDA']:>+8.1f}  (baseline)")
    for r in REGIMES[1:]:
        v = big4_traj[r]
        pct = (v - base) / abs(base) * 100 if base != 0 else float("nan")
        print(f"     {r:<10}  {v:>+8.1f}  ({pct:+5.0f}% vs pre-IDA)")
    print()

    # ========================================================================
    # 2. Regression with controls + cluster SE
    # ========================================================================
    print("=" * 95)
    print("2. REGRESSION — augmented spec, cluster-robust SE by date")
    print("=" * 95)
    print()
    print("   Outcome: signed ΔQ per firm-hour (MWh)")
    print("   Spec: regime × Big4 + Big4 + hour FE + DOW FE + cal-month FE + year FE + daily VRE")
    print("   SEs:  cluster-robust by date")
    print()

    rstats, model = fit_regression(df_sell, "Augmented (signed ΔQ)", "qida_mwh")
    print(f"   N = {rstats['n']:,} firm-hour obs;  clusters = {rstats['n_clusters']:,};  R² = {rstats['r2']:.3f}")
    print()
    print("   Big-4 effect by regime (point estimate ± SE):")
    for r in REGIMES:
        b = rstats[f"{r}_Big4"]
        se = rstats[f"{r}_Big4_se"]
        t = b / se if se > 0 else float("nan")
        print(f"     {r:<10}  β = {b:>+8.2f} MWh/firm-hr   SE = {se:>5.2f}   t = {t:>+5.2f}")
    print()

    # Wald test: monotonic decrease across regimes 3-sess → DA15/ID15
    cov = model.cov_params()
    X_cols = list(['const'] + [f'D[{r}]' for r in REGIMES[1:]] + [f'D[{r}]xBig4' for r in REGIMES[1:]] + ['Big4'])
    # We compare the Big-4 effect at each regime
    # Test: β_Big4_pre - β_Big4_DA15 > 0 (under-commitment higher pre than at DA15)
    # In coefficients: this is -(D[DA15]xBig4) > 0  (since pre-IDA effect = baseline 'Big4', DA15 effect = Big4 + D[DA15]xBig4)
    # Equivalently, test D[DA15]xBig4 < 0
    # Use t-test on the interaction
    print("   Hypothesis tests on Big-4 regime trajectory:")
    # Re-fit to get interaction coefficients directly
    df_test = df_sell.dropna(subset=["qida_mwh"]).copy()
    cols2 = {"const": 1.0}
    for r in REGIMES[1:]:
        cols2[f"D[{r}]"]     = (df_test["regime"] == r).astype(float).values
        cols2[f"D[{r}]xBig4"] = ((df_test["regime"] == r) & df_test["is_big4"]).astype(float).values
    cols2["Big4"] = df_test["is_big4"].astype(float).values
    for h in range(2, 25):
        cols2[f"H[{h}]"] = (df_test["hour"] == h).astype(float).values
    for d_ in range(1, 7):
        cols2[f"DOW[{d_}]"] = (df_test["dow"] == d_).astype(float).values
    for m in range(2, 13):
        cols2[f"M[{m}]"] = (df_test["month"] == m).astype(float).values
    years = sorted(df_test["year"].unique())
    for yr in years[1:]:
        cols2[f"Y[{yr}]"] = (df_test["year"] == yr).astype(float).values
    cols2["vre_gwh"] = df_test["vre_gwh"].fillna(df_test["vre_gwh"].mean()).values
    Xt = pd.DataFrame(cols2, index=df_test.index)
    yt = df_test["qida_mwh"].astype(float).values
    cluster_t = df_test["date"].astype("category").cat.codes.values
    m2 = sm.OLS(yt, Xt.values).fit(cov_type="cluster", cov_kwds={"groups": cluster_t})

    for r in REGIMES[1:]:
        j = list(Xt.columns).index(f"D[{r}]xBig4")
        b = float(m2.params[j])
        se = float(m2.bse[j])
        t = b / se
        p = float(m2.pvalues[j])
        print(f"     β(Big4 × {r:<10}) - β(Big4 × pre-IDA) = {b:>+8.2f}  SE={se:>5.2f}  "
              f"t={t:>+5.2f}  p={p:.3f}  "
              f"({'reject equality' if p < 0.05 else 'fail to reject'})")
    print()

    # Joint test: are the four regime-Big4 interactions jointly different from zero?
    R_joint = np.zeros((4, len(m2.params)))
    for i, r in enumerate(REGIMES[1:]):
        j = list(Xt.columns).index(f"D[{r}]xBig4")
        R_joint[i, j] = 1
    wald_joint = m2.wald_test(R_joint, scalar=True)
    print(f"   Joint test:  H0: all four Big4×regime interactions = 0")
    print(f"     F = {float(wald_joint.statistic):.2f},  p = {float(wald_joint.pvalue):.4g}")
    print()

    # ========================================================================
    # 3. Fringe placebo: same regression but for Fringe firms only
    # ========================================================================
    print("=" * 95)
    print("3. FRINGE PLACEBO — does the regime trajectory show in non-Big4 firms?")
    print("=" * 95)
    print()
    fringe_means = (df_sell[~df_sell.is_big4]
                     .groupby("regime_cat", observed=True)["qida_mwh"]
                     .agg(["mean", "count"])
                     .reindex(REGIMES))
    print(fringe_means.round(2).to_string())
    print()
    base_f = fringe_means["mean"].iloc[0]
    print("   Fringe trajectory (% change vs pre-IDA):")
    for r in REGIMES:
        v = fringe_means.loc[r, "mean"]
        pct = (v - base_f) / abs(base_f) * 100 if base_f else float("nan")
        print(f"     {r:<10}  {v:>+8.2f}  ({pct:+5.0f}%)")

    # ========================================================================
    # 4. Save and conclude
    # ========================================================================
    OUT.parent.mkdir(parents=True, exist_ok=True)
    pv.to_csv(OUT)
    print()
    print(f"   wrote summary table to {OUT}")
    print()
    print("=" * 95)
    print("HEADLINE SUMMARY")
    print("=" * 95)
    big4 = pv["Big4"]
    fringe = pv["Fringe"]
    print(f"   Big-4 mean ΔQ trajectory (MWh/firm-hr):  "
          + " → ".join(f"{r}: {big4[r]:+.1f}" for r in REGIMES))
    print(f"   Fringe mean ΔQ trajectory (MWh/firm-hr): "
          + " → ".join(f"{r}: {fringe[r]:+.1f}" for r in REGIMES))


if __name__ == "__main__":
    main()
