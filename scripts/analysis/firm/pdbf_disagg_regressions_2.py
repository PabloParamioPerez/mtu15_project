# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F24 formal (tech asymmetry) + F25 (reforzada reduces nuclear bilateral)
#        + B10 substitution test
# CLAIM: PDBF-channel claims F24 and B10 are formally regression-backed at
#        disaggregated grain; F23 is replaced with positive-direction F25
#        ("reforzada reduces bilateral nuclear commitment under same-cal-month
#        restriction") regression-backed with unit FE and cluster SE.
"""Three disaggregated PDBF regressions to formalise alive claims.

Spec A — F24 tech asymmetry (bilateral share by tech × regime).
  unit-day panel: bilateral_share ~ regime × tech_group + unit FE +
                  cal-month FE + DOW FE; cluster SE by date.
  Test: are CCGT bilateral shares statistically different from nuclear/hydro?

Spec B — F25 reforzada reduces bilateral nuclear (same-cal-month).
  unit-day panel for Big-4 nuclear, May-Sep months only:
  bilateral_share ~ post_blackout_2025_dummy + cal-month FE + unit FE;
  cluster SE by date. Test: did reforzada (May-Sep 2025) reduce bilateral
  share relative to historical May-Sep (2018-2024)?

Spec C — B10 substitution test.
  firm-day panel: q1_DA_GWh ~ q1_bilat_GWh + regime + firm FE + cal-month FE
  + DOW FE; cluster SE by date. Tests whether firms substitute between
  bilateral and auction (negative coefficient = substitution).

Output:
  results/regressions/pdbf_f24_tech_asymmetry.csv
  results/regressions/pdbf_f25_reforzada_reduces_nuclear.csv
  results/regressions/pdbf_b10_substitution_test.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import statsmodels.api as sm

PROJECT = Path(__file__).resolve().parents[3]
PDBF    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbf_all.parquet"
PDBCE   = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
LISTA   = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR = PROJECT / "results" / "regressions"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ============================================================
    # Common: unit-day PDBF panel (sell-side only) + tech mapping
    # ============================================================
    print("[setup] unit→firm + tech mapping…", flush=True)
    firms = con.execute(f"""
        SELECT unit_code, grupo_empresarial AS firm FROM (
          SELECT unit_code, grupo_empresarial,
                 ROW_NUMBER() OVER (PARTITION BY unit_code ORDER BY date DESC) AS rn
          FROM '{PDBCE}' WHERE grupo_empresarial IS NOT NULL) WHERE rn = 1
    """).df()
    lista = pd.read_csv(LISTA)[["unit_code", "technology"]]
    map_uf = firms.merge(lista, on="unit_code", how="left")

    def tech_group(t):
        if not isinstance(t, str): return "Other"
        tl = t.lower()
        if "gas" in tl or "ciclo" in tl: return "CCGT"
        if "nuclear" in tl: return "Nuclear"
        if "ombeo" in tl or "idráulica" in tl: return "Hydro"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code", "firm", "tech_group"]])

    # Unit-day panel of bilateral + auction sell volumes, Big-4 + dispatchable techs
    print("[panel] unit-day PDBF panel (Big-4 sell-side, CCGT/Nuclear/Hydro)…", flush=True)
    panel = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.unit_code,
               uf.firm, uf.tech_group,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group IN ('CCGT','Nuclear','Hydro')
        GROUP BY 1, 2, 3, 4
    """).df()
    panel["total_mwh"] = panel["bilateral_mwh"] + panel["auction_mwh"]
    panel = panel[panel["total_mwh"] > 0].copy()
    panel["bilat_share"] = panel["bilateral_mwh"] / panel["total_mwh"]
    panel["date"] = pd.to_datetime(panel["date"])
    panel["regime"] = panel["date"].apply(assign_regime)
    panel["dow"]   = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"]  = panel["date"].dt.year
    print(f"   unit-day panel: {len(panel):,} rows; "
          f"techs: {dict(panel.tech_group.value_counts())}", flush=True)

    # ============================================================
    # SPEC A — F24 tech asymmetry of bilateral share
    # ============================================================
    print("\n=== SPEC A — F24 tech asymmetry (bilateral share by regime × tech) ===", flush=True)
    # LHS: bilat_share at unit-day grain
    # RHS: const + tech dummies (CCGT, Nuclear; Hydro = baseline) +
    #      regime dummies (3-sess, ISP15-win, DA60/ID15, DA15/ID15;
    #                       pre-IDA = baseline) +
    #      regime × tech interactions + cal-month FE + DOW FE +
    #      unit FE (absorbed by demeaning)
    # Cluster SE by date.
    pa = panel.copy()
    # Demean by unit (within-unit transformation absorbs unit FE)
    pa["bilat_share_dm"] = pa["bilat_share"] - pa.groupby("unit_code")["bilat_share"].transform("mean")
    cols = {"const": np.ones(len(pa))}
    for t in ["CCGT", "Nuclear"]:
        cols[f"T_{t}"] = (pa["tech_group"] == t).astype(float).values
    for r in REGIMES[1:]:
        cols[f"R_{r}"] = (pa["regime"] == r).astype(float).values
    for r in REGIMES[1:]:
        for t in ["CCGT", "Nuclear"]:
            cols[f"R_{r}×T_{t}"] = (
                (pa["regime"] == r).astype(float).values * (pa["tech_group"] == t).astype(float).values
            )
    for d_ in range(1, 7):
        cols[f"DOW{d_}"] = (pa["dow"] == d_).astype(float).values
    for m_ in range(2, 13):
        cols[f"M{m_}"] = (pa["month"] == m_).astype(float).values

    XA = pd.DataFrame(cols, index=pa.index)
    yA = pa["bilat_share_dm"].values
    cluster_a = pa["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    mA = fit_ols_cluster(yA, XA.values, cluster_a)
    n_clusters_a = len(np.unique(cluster_a))
    print(f"  N={len(pa):,}; n_clusters (date)={n_clusters_a:,}; R²={mA.rsquared:.3f}")
    print()

    coefs = pd.Series(mA.params, index=XA.columns)
    ses   = pd.Series(mA.bse,    index=XA.columns)
    pvals = pd.Series(mA.pvalues, index=XA.columns)
    print("Tech baseline (Hydro = reference; pre-IDA regime baseline):")
    for t in ["CCGT", "Nuclear"]:
        print(f"  T_{t}:  β = {coefs['T_'+t]:+.3f}  (SE {ses['T_'+t]:.3f}, p={pvals['T_'+t]:.2e})")
    print("\nRegime × tech interactions (effect on Δbilateral_share vs pre-IDA Hydro baseline):")
    for r in REGIMES[1:]:
        for t in ["CCGT", "Nuclear"]:
            k = f"R_{r}×T_{t}"
            print(f"  {k}: β = {coefs[k]:+.3f}  (SE {ses[k]:.3f}, p={pvals[k]:.2e})")

    out_a = pd.DataFrame({"term": XA.columns, "coef": mA.params,
                         "se": mA.bse, "t": mA.tvalues, "p": mA.pvalues})
    keep_a = ["T_CCGT", "T_Nuclear"] + [f"R_{r}" for r in REGIMES[1:]] + \
             [f"R_{r}×T_{t}" for r in REGIMES[1:] for t in ["CCGT","Nuclear"]]
    out_a[out_a.term.isin(keep_a)].to_csv(OUT_DIR / "pdbf_f24_tech_asymmetry.csv", index=False)

    # ============================================================
    # SPEC B — F25 reforzada reduces bilateral nuclear (same-cal-month)
    # ============================================================
    print("\n=== SPEC B — F25 reforzada reduces bilateral nuclear (May-Sep, same-cal-month) ===", flush=True)
    # Restrict to nuclear units, May-Sep months, pre-IDA AND reforzada periods
    pb = panel[
        (panel.tech_group == "Nuclear")
        & panel.month.between(5, 9)
        & ((panel.date < pd.Timestamp("2024-06-14"))
           | ((panel.date >= pd.Timestamp("2025-04-28"))
              & (panel.date < pd.Timestamp("2025-10-01"))))
    ].copy()
    pb["reforzada"] = ((pb.date >= pd.Timestamp("2025-04-28"))
                      & (pb.date < pd.Timestamp("2025-10-01"))).astype(float)
    # Demean by unit
    pb["bilat_share_dm"] = pb["bilat_share"] - pb.groupby("unit_code")["bilat_share"].transform("mean")

    cols_b = {"const": np.ones(len(pb))}
    cols_b["reforzada"] = pb["reforzada"].values
    for d_ in range(1, 7):
        cols_b[f"DOW{d_}"] = (pb["dow"] == d_).astype(float).values
    for m_ in [6, 7, 8, 9]:
        cols_b[f"M{m_}"] = (pb["month"] == m_).astype(float).values

    XB = pd.DataFrame(cols_b, index=pb.index)
    yB = pb["bilat_share_dm"].values
    cluster_b = pb["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    mB = fit_ols_cluster(yB, XB.values, cluster_b)
    print(f"  N={len(pb):,} unit-days; n_clusters (date)={len(np.unique(cluster_b)):,}; R²={mB.rsquared:.3f}")
    print(f"  Pre-IDA mean nuclear bilat_share (May-Sep): {pb[pb.reforzada==0].bilat_share.mean():.3f}")
    print(f"  Reforzada mean nuclear bilat_share (May-Sep 2025): {pb[pb.reforzada==1].bilat_share.mean():.3f}")
    coefs_b = pd.Series(mB.params, index=XB.columns)
    ses_b   = pd.Series(mB.bse,    index=XB.columns)
    pvals_b = pd.Series(mB.pvalues, index=XB.columns)
    print(f"\n  reforzada β = {coefs_b['reforzada']:+.4f}  (SE {ses_b['reforzada']:.4f}, "
          f"p={pvals_b['reforzada']:.2e}) — change in bilateral share, percentage-point effect = "
          f"{coefs_b['reforzada']*100:+.2f}pp")

    out_b = pd.DataFrame({"term": XB.columns, "coef": mB.params,
                         "se": mB.bse, "t": mB.tvalues, "p": mB.pvalues})
    out_b.to_csv(OUT_DIR / "pdbf_f25_reforzada_reduces_nuclear.csv", index=False)

    # ============================================================
    # SPEC C — B10 substitution test: q₁_DA on q₁_bilateral at firm-day grain
    # ============================================================
    print("\n=== SPEC C — B10 substitution test: q₁_DA on q₁_bilateral (Big-4 firm-day) ===", flush=True)
    fd = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, uf.firm,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS q1_DA_GWh,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) / 1000.0 AS q1_bilat_GWh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
        GROUP BY 1, 2
    """).df()
    fd["date"] = pd.to_datetime(fd["date"])
    fd["regime"] = fd["date"].apply(assign_regime)
    fd["dow"]   = fd["date"].dt.dayofweek
    fd["month"] = fd["date"].dt.month
    fd["year"]  = fd["date"].dt.year
    # Demean by firm
    fd["q1_DA_dm"] = fd["q1_DA_GWh"] - fd.groupby("firm")["q1_DA_GWh"].transform("mean")

    cols_c = {"const": np.ones(len(fd))}
    cols_c["q1_bilat_GWh"] = fd["q1_bilat_GWh"].values
    for r in REGIMES[1:]:
        cols_c[f"R_{r}"] = (fd["regime"] == r).astype(float).values
    for d_ in range(1, 7):
        cols_c[f"DOW{d_}"] = (fd["dow"] == d_).astype(float).values
    for m_ in range(2, 13):
        cols_c[f"M{m_}"] = (fd["month"] == m_).astype(float).values
    years = sorted(fd["year"].unique())
    for yr in years[1:]:
        cols_c[f"Y{yr}"] = (fd["year"] == yr).astype(float).values

    XC = pd.DataFrame(cols_c, index=fd.index)
    yC = fd["q1_DA_dm"].values
    cluster_c = fd["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    mC = fit_ols_cluster(yC, XC.values, cluster_c)
    print(f"  N={len(fd):,} firm-days; n_clusters (date)={len(np.unique(cluster_c)):,}; R²={mC.rsquared:.3f}")
    coefs_c = pd.Series(mC.params, index=XC.columns)
    ses_c   = pd.Series(mC.bse,    index=XC.columns)
    pvals_c = pd.Series(mC.pvalues, index=XC.columns)
    print()
    print(f"  q1_bilat_GWh: β = {coefs_c['q1_bilat_GWh']:+.4f}  "
          f"(SE {ses_c['q1_bilat_GWh']:.4f}, p={pvals_c['q1_bilat_GWh']:.2e})")
    print("    interpretation: 1 GWh more bilateral commitment is associated with",
          f"{coefs_c['q1_bilat_GWh']:+.4f} GWh in DA-cleared sell volume (within firm)")
    if coefs_c["q1_bilat_GWh"] < 0:
        print("    → SUBSTITUTION CONFIRMED at firm-day grain")
    else:
        print("    → no substitution; firms expand both channels together")
    print()

    out_c = pd.DataFrame({"term": XC.columns, "coef": mC.params,
                         "se": mC.bse, "t": mC.tvalues, "p": mC.pvalues})
    out_c[out_c.term.isin(["q1_bilat_GWh"] + [f"R_{r}" for r in REGIMES[1:]])].to_csv(
        OUT_DIR / "pdbf_b10_substitution_test.csv", index=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
