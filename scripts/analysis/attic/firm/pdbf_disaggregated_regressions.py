# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: B9 q₁_total refinement at proper grain + B11 regression-backed
# CLAIM: B9 progressive q₂_IDA collapse survives at firm-ISP grain when
#        q₁_total (auction + bilateral) is added as a control (the
#        bilateral channel does not absorb the strategic-withholding
#        signal). B11 Rule 28.8 effect is statistically significant under
#        cluster-robust regression with firm/cal-month/DOW FE.
"""Disaggregated PDBF regressions at proper grain.

Spec 1 — B9 with q₁_total control, native firm-ISP grain.
  q₂_IDA_isp ~ Big4 + regime × Big4 + period FE (1..96) + DOW + month + year +
               VRE + q₁_DA_day + q₁_bilateral_day + cluster SE by (date, hour)
  Pre-MTU15-IDA records replicated 4× per hour at q₂/4 (preserves total
  hourly energy). N ≈ 1.9M firm-ISP rows.

Spec 2 — B11 Rule 28.8 firm-day regression.
  log(bilateral_GWh+1) ~ post_2025_03_19_dummy × Big4 + firm FE +
                          cal-month FE + DOW FE + year FE
  Cluster SE by year-month. Tests whether the descriptive 11-35% Δ%
  reduction in B11 is statistically significant.

Output:
  results/regressions/pdbf_b9_q1_total_isp_grain.csv
  results/regressions/pdbf_b11_rule_28_8_regression.csv
"""
from __future__ import annotations

import time
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

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["IB", "GE", "GN", "HC"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    t0 = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] start", flush=True)

    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ============================================================
    # SPEC 1 — B9 with q₁_total control at firm-ISP grain
    # ============================================================

    # IDA q₂ at firm-period native grain
    print("[1/4] q₂_IDA panel (PIBCIE, native firm-period grain)…", flush=True)
    ida = con.execute(f"""
        SELECT date, period, mtu_minutes,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS q2_mwh
        FROM '{PIBCIE}' WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    print(f"   ida native rows: {len(ida):,}; mtu dist: {dict(ida.mtu_minutes.value_counts())}", flush=True)

    # Replicate MTU60 → MTU15 grid (q₂/4 per ISP)
    print("[2/4] Replicating MTU60 → MTU15 grid (q₂/4)…", flush=True)
    mtu60 = ida[ida.mtu_minutes == 60].copy()
    mtu15 = ida[ida.mtu_minutes == 15].copy()
    print(f"   MTU60: {len(mtu60):,}; MTU15: {len(mtu15):,}", flush=True)
    if len(mtu60) > 0:
        mtu60["q2_mwh"] /= 4.0
        mtu60["hour"] = mtu60["period"].astype(int)
        rep = mtu60.loc[mtu60.index.repeat(4)].reset_index(drop=True).copy()
        rep["k"] = np.tile(np.arange(4), len(mtu60))
        rep["period"] = (rep["hour"] - 1) * 4 + rep["k"] + 1
        rep["mtu_minutes"] = 15
        ida_replicated = pd.concat([
            rep[["date", "period", "firm", "q2_mwh"]],
            mtu15[["date", "period", "firm", "q2_mwh"]],
        ], ignore_index=True)
    else:
        ida_replicated = mtu15[["date", "period", "firm", "q2_mwh"]]
    ida_replicated["hour"] = ((ida_replicated["period"].astype(int) - 1) // 4) + 1
    print(f"   Replicated firm-ISP rows: {len(ida_replicated):,}", flush=True)

    # q₁_DA + q₁_bilateral per (firm, date) from PDBF
    print("[3/4] q₁ panel from PDBF (firm-day means)…", flush=True)
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

    # VRE generation per day (already in this project's standard control)
    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_GWh
        FROM '{ACTUAL}' WHERE psr_type IN ('B16', 'B18', 'B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])

    # Merge: ida_replicated + q1 + vre
    print("[4/4] Merging panels and running regression…", flush=True)
    panel = ida_replicated.merge(q1, on=["date", "firm"], how="inner").merge(
        vre, on="date", how="left"
    )
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["Big4"] = panel["firm"].isin(BIG4).astype(int)
    panel["dow"] = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"] = panel["date"].dt.year
    panel = panel.dropna(subset=["q2_mwh", "q1_DA_GWh", "q1_bilat_GWh", "vre_GWh"])
    print(f"   Final panel: {len(panel):,} rows", flush=True)

    # Build design matrix:
    #   q₂ ~ const + Big4 + Big4×regime + period FE (drop period 1) +
    #        DOW FE + month FE + year FE + vre + q1_DA + q1_bilat
    cols: dict[str, np.ndarray] = {"const": np.ones(len(panel))}
    cols["Big4"] = panel["Big4"].values.astype(float)
    for r in REGIMES[1:]:
        cols[f"Big4×{r}"] = (panel["Big4"] * (panel["regime"] == r)).astype(float).values
    for p_ in range(2, 97):
        cols[f"P{p_}"] = (panel["period"].astype(int) == p_).astype(float).values
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (panel["dow"] == d_).astype(float).values
    for m in range(2, 13):
        cols[f"M{m}"] = (panel["month"] == m).astype(float).values
    years = sorted(panel["year"].unique())
    for yr in years[1:]:
        cols[f"Y{yr}"] = (panel["year"] == yr).astype(float).values
    cols["vre_GWh"] = panel["vre_GWh"].values
    cols["q1_DA_GWh"]    = panel["q1_DA_GWh"].values
    cols["q1_bilat_GWh"] = panel["q1_bilat_GWh"].values

    X = pd.DataFrame(cols, index=panel.index)
    y = panel["q2_mwh"].values

    # Cluster SE by (date, hour)
    cluster_arr = (panel["date"].dt.strftime("%Y%m%d").astype(np.int64).values * 100
                   + panel["hour"].astype(np.int64).values)
    n_clusters = len(np.unique(cluster_arr))
    m = sm.OLS(y, X.values).fit(cov_type="cluster", cov_kwds={"groups": cluster_arr})

    # Extract Big-4 effects per regime
    coefs = pd.Series(m.params, index=X.columns)
    ses   = pd.Series(m.bse,    index=X.columns)
    pvals = pd.Series(m.pvalues, index=X.columns)
    print()
    print("=" * 105)
    print("SPEC 1 — B9 q₂_IDA at firm-ISP grain with q₁_total control (PDBF-augmented)")
    print("=" * 105)
    print(f"  N={len(panel):,}; n_clusters={n_clusters:,}; R²={m.rsquared:.3f}")
    print()
    print(f"  Big-4 baseline (pre-IDA): β = {coefs['Big4']:+.2f}  (SE {ses['Big4']:.2f}, p={pvals['Big4']:.2e})")
    for r in REGIMES[1:]:
        b = coefs[f"Big4×{r}"]; s = ses[f"Big4×{r}"]; pv = pvals[f"Big4×{r}"]
        net = coefs["Big4"] + b
        print(f"  Big-4 × {r}:  Δ = {b:+.2f}  (SE {s:.2f}, p={pv:.2e}); net Big-4 effect = {net:+.2f}")
    print()
    print(f"  q₁_DA_GWh:    β = {coefs['q1_DA_GWh']:+.4f}  (SE {ses['q1_DA_GWh']:.4f}, p={pvals['q1_DA_GWh']:.2e})")
    print(f"  q₁_bilat_GWh: β = {coefs['q1_bilat_GWh']:+.4f}  (SE {ses['q1_bilat_GWh']:.4f}, p={pvals['q1_bilat_GWh']:.2e})")
    print(f"  vre_GWh:      β = {coefs['vre_GWh']:+.4f}  (SE {ses['vre_GWh']:.4f}, p={pvals['vre_GWh']:.2e})")
    print()

    # Save spec 1 results
    out1 = pd.DataFrame({
        "term":  X.columns,
        "coef":  m.params,
        "se":    m.bse,
        "t":     m.tvalues,
        "p":     m.pvalues,
    })
    out1 = out1[out1.term.isin(["Big4"] + [f"Big4×{r}" for r in REGIMES[1:]] + ["q1_DA_GWh", "q1_bilat_GWh", "vre_GWh"])]
    out1.to_csv(PROJECT / "results" / "regressions" / "pdbf_b9_q1_total_isp_grain.csv", index=False)

    # ============================================================
    # SPEC 2 — B11 Rule 28.8 regression at firm-day grain
    # ============================================================
    print("=" * 105)
    print("SPEC 2 — B11 Rule 28.8 firm-day regression on log(bilateral_GWh+1)")
    print("=" * 105)
    print(f"[{time.strftime('%H:%M:%S')}] building B11 panel…", flush=True)
    b11 = q1[q1.firm.isin(BIG4)].copy()
    b11["log_bilat"] = np.log1p(b11["q1_bilat_GWh"])
    b11["post_28_8"] = (b11["date"] >= pd.Timestamp("2025-03-19")).astype(int)
    b11["dow"] = b11["date"].dt.dayofweek
    b11["month"] = b11["date"].dt.month
    b11["year"] = b11["date"].dt.year
    # Restrict to a 12-month window around 2025-03-19 (Sep 2024 – Aug 2025) for clean local estimate
    b11_local = b11[(b11.date >= pd.Timestamp("2024-09-01")) & (b11.date < pd.Timestamp("2025-09-01"))].copy()
    print(f"   B11 local panel (Sep 2024 → Aug 2025): {len(b11_local):,} firm-days", flush=True)

    cols2: dict[str, np.ndarray] = {"const": np.ones(len(b11_local))}
    # Drop GE as baseline firm
    for f in ["IB", "GN", "HC"]:
        cols2[f"firm_{f}"] = (b11_local["firm"] == f).astype(float).values
    # post×firm interactions (let GE be baseline post effect)
    cols2["post_28_8"] = b11_local["post_28_8"].values.astype(float)
    for f in ["IB", "GN", "HC"]:
        cols2[f"post_28_8 × firm_{f}"] = (b11_local["post_28_8"] * (b11_local["firm"] == f)).astype(float).values
    for d_ in range(1, 7):
        cols2[f"DOW{d_}"] = (b11_local["dow"] == d_).astype(float).values
    for m_ in range(2, 13):
        cols2[f"M{m_}"] = (b11_local["month"] == m_).astype(float).values

    X2 = pd.DataFrame(cols2, index=b11_local.index)
    y2 = b11_local["log_bilat"].values
    # Cluster SE by year-month (Cameron-Miller adjustment is borderline with n_clusters=12, but it's the right unit)
    cluster2 = b11_local["year"] * 100 + b11_local["month"]
    m2 = sm.OLS(y2, X2.values).fit(cov_type="cluster", cov_kwds={"groups": cluster2.values})

    coefs2 = pd.Series(m2.params, index=X2.columns)
    ses2   = pd.Series(m2.bse,    index=X2.columns)
    pvals2 = pd.Series(m2.pvalues, index=X2.columns)

    print(f"  N={len(b11_local):,}; n_clusters (year-month)={int(cluster2.nunique()):,}; R²={m2.rsquared:.3f}")
    print()
    print(f"  Post-2025-03-19 baseline (GE): β = {coefs2['post_28_8']:+.3f}  "
          f"(SE {ses2['post_28_8']:.3f}, p={pvals2['post_28_8']:.2e}) — log-points reduction")
    for f in ["IB", "GN", "HC"]:
        b = coefs2[f"post_28_8 × firm_{f}"]; s = ses2[f"post_28_8 × firm_{f}"]; pv = pvals2[f"post_28_8 × firm_{f}"]
        net = coefs2["post_28_8"] + b
        # convert log-points to %
        pct = (np.exp(net) - 1) * 100
        print(f"  Post × {f}: Δ = {b:+.3f}  (SE {s:.3f}, p={pv:.2e}); net post-{f} effect = {net:+.3f} ({pct:+.1f}%)")
    print()

    out2 = pd.DataFrame({
        "term":  X2.columns,
        "coef":  m2.params,
        "se":    m2.bse,
        "t":     m2.tvalues,
        "p":     m2.pvalues,
    })
    keep = ["post_28_8"] + [f"post_28_8 × firm_{f}" for f in ["IB", "GN", "HC"]]
    out2 = out2[out2.term.isin(keep)]
    out2.to_csv(PROJECT / "results" / "regressions" / "pdbf_b11_rule_28_8_regression.csv", index=False)

    # ============================================================
    # SPEC 2b — B11 single-post-dummy spec, date-clustered SE
    # ============================================================
    print()
    print("=" * 105)
    print("SPEC 2b — B11 single post-dummy spec, date-clustered SE")
    print("=" * 105)
    cols2b: dict[str, np.ndarray] = {"const": np.ones(len(b11_local))}
    for f in ["IB", "GN", "HC"]:
        cols2b[f"firm_{f}"] = (b11_local["firm"] == f).astype(float).values
    cols2b["post_28_8"] = b11_local["post_28_8"].values.astype(float)
    for d_ in range(1, 7):
        cols2b[f"DOW{d_}"] = (b11_local["dow"] == d_).astype(float).values
    for m_ in range(2, 13):
        cols2b[f"M{m_}"] = (b11_local["month"] == m_).astype(float).values

    X2b = pd.DataFrame(cols2b, index=b11_local.index)
    cluster2b = b11_local["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    m2b = sm.OLS(y2, X2b.values).fit(cov_type="cluster", cov_kwds={"groups": cluster2b})

    coefs2b = pd.Series(m2b.params, index=X2b.columns)
    ses2b   = pd.Series(m2b.bse,    index=X2b.columns)
    pvals2b = pd.Series(m2b.pvalues, index=X2b.columns)
    n_dt = len(np.unique(cluster2b))
    print(f"  N={len(b11_local):,}; n_clusters (date)={n_dt:,}; R²={m2b.rsquared:.3f}")
    pe = coefs2b["post_28_8"]; se_p = ses2b["post_28_8"]; pv_p = pvals2b["post_28_8"]
    pct_p = (np.exp(pe) - 1) * 100
    print(f"  Average post-2025-03-19 effect (across Big-4): β = {pe:+.3f} log-points "
          f"(SE {se_p:.3f}, p={pv_p:.2e}) ⇒ {pct_p:+.1f}% bilateral volume reduction")
    print()

    print(f"\nDone in {time.time()-t0:.1f}s")


if __name__ == "__main__":
    main()
