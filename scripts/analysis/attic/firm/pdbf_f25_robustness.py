# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F25 robustness — year-by-year, hydro extension, per-unit, B10 per-tech
# CLAIM: F25 (reforzada reduces nuclear bilateral) is robust to (a) checking
#        whether the drop is reforzada-specific or a gradual cross-regime
#        decline, (b) extension to hydro, (c) per-unit decomposition.
#        B10 substitution is decomposed by tech.
"""F25 robustness diagnostics.

Test 1 — F25 year-by-year (May-Sep nuclear bilat_share by year, Big-4):
   is the 86.7% → 52.2% drop reforzada-specific (jump in 2025) or gradual
   across regimes (cumulative decline since 2024)?

Test 2 — F25 cross-tech (May-Sep, pre-IDA + reforzada same-cal restriction):
   restrict to hydro, run same regression as F25 spec B. Reforzada PO-3.2
   covers nuclear security; does it also reduce hydro bilateral?

Test 3 — F25 per-nuclear-unit (May-Sep, same-cal-month):
   regress bilat_share on reforzada × unit. Which plants drive the −34.5pp?
   Compare to the raw-means blackout-split (T1 in granular analysis) which
   showed concentration in 4 of 7 plants.

Test 4 — B10 per-tech substitution (firm-day × tech panel):
   beta(q1_bilat) per tech. Is the −0.037 GWh/GWh substitution uniform
   across CCGT/Nuclear/Hydro, or concentrated in a tech?

Output:
  results/regressions/pdbf_f25_year_by_year.csv
  results/regressions/pdbf_f25_hydro_extension.csv
  results/regressions/pdbf_f25_per_unit.csv
  results/regressions/pdbf_b10_per_tech.csv
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
    # Common: build unit-day PDBF panel + tech mapping
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

    print("[panel] unit-day PDBF panel (Big-4, CCGT/Nuclear/Hydro)…", flush=True)
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
    print(f"   unit-day panel: {len(panel):,} rows", flush=True)

    # ============================================================
    # TEST 1 — F25 year-by-year May-Sep nuclear bilat_share
    # ============================================================
    print("\n=== TEST 1 — F25 year-by-year (May-Sep nuclear, Big-4) ===", flush=True)
    nuc = panel[(panel.tech_group == "Nuclear") & panel.month.between(5, 9)].copy()
    yby = nuc.groupby("year").agg(
        n_unit_days=("bilat_share", "size"),
        mean_bilat_share=("bilat_share", "mean"),
        median_bilat_share=("bilat_share", "median"),
        n_units=("unit_code", "nunique"),
    ).reset_index()
    # Add regime annotation: 2018-2023 = pre-IDA, 2024 = mostly pre-IDA + 3-sess transition,
    # 2025 = blended (ISP15-win Mar-Apr → DA60/ID15 May-Sep) → reforzada from Apr 28
    def yr_regime(y):
        if y <= 2023: return "pre-IDA"
        if y == 2024: return "pre-IDA→3-sess"
        if y == 2025: return "DA60/ID15 (incl. reforzada May-Sep)"
        return f"{y}"
    yby["regime_context"] = yby.year.apply(yr_regime)
    print(yby.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    print()
    # Key diagnostic: compare 2024 May-Sep (pre-blackout, mostly 3-sess for some,
    # but May-Sep 2024 was actually in 3-sess regime) to 2025 May-Sep (reforzada).
    # If 2024 already low → not reforzada-specific.
    yby.to_csv(OUT_DIR / "pdbf_f25_year_by_year.csv", index=False)

    # ============================================================
    # TEST 2 — F25 cross-tech (HYDRO same-cal-month spec)
    # ============================================================
    print("\n=== TEST 2 — F25 hydro extension (May-Sep, same-cal-month spec) ===", flush=True)
    pb_hy = panel[
        (panel.tech_group == "Hydro")
        & panel.month.between(5, 9)
        & ((panel.date < pd.Timestamp("2024-06-14"))
           | ((panel.date >= pd.Timestamp("2025-04-28"))
              & (panel.date < pd.Timestamp("2025-10-01"))))
    ].copy()
    pb_hy["reforzada"] = ((pb_hy.date >= pd.Timestamp("2025-04-28"))
                         & (pb_hy.date < pd.Timestamp("2025-10-01"))).astype(float)
    pb_hy["bilat_share_dm"] = pb_hy["bilat_share"] - pb_hy.groupby("unit_code")["bilat_share"].transform("mean")

    cols_h = {"const": np.ones(len(pb_hy))}
    cols_h["reforzada"] = pb_hy["reforzada"].values
    for d_ in range(1, 7):
        cols_h[f"DOW{d_}"] = (pb_hy["dow"] == d_).astype(float).values
    for m_ in [6, 7, 8, 9]:
        cols_h[f"M{m_}"] = (pb_hy["month"] == m_).astype(float).values

    XH = pd.DataFrame(cols_h, index=pb_hy.index)
    yH = pb_hy["bilat_share_dm"].values
    cluster_h = pb_hy["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    mH = fit_ols_cluster(yH, XH.values, cluster_h)
    print(f"  N={len(pb_hy):,} hydro unit-days; n_clusters (date)={len(np.unique(cluster_h)):,}; R²={mH.rsquared:.3f}")
    print(f"  Pre-IDA mean hydro bilat_share (May-Sep): {pb_hy[pb_hy.reforzada==0].bilat_share.mean():.3f}")
    print(f"  Reforzada mean hydro bilat_share (May-Sep 2025): {pb_hy[pb_hy.reforzada==1].bilat_share.mean():.3f}")
    coefs_h = pd.Series(mH.params, index=XH.columns)
    ses_h   = pd.Series(mH.bse,    index=XH.columns)
    pvals_h = pd.Series(mH.pvalues, index=XH.columns)
    print(f"\n  reforzada β (HYDRO) = {coefs_h['reforzada']:+.4f}  (SE {ses_h['reforzada']:.4f}, "
          f"p={pvals_h['reforzada']:.2e}) — {coefs_h['reforzada']*100:+.2f}pp")

    out_h = pd.DataFrame({"term": XH.columns, "coef": mH.params,
                         "se": mH.bse, "t": mH.tvalues, "p": mH.pvalues})
    out_h.to_csv(OUT_DIR / "pdbf_f25_hydro_extension.csv", index=False)

    # ============================================================
    # TEST 3 — F25 per-nuclear-unit (May-Sep, same-cal-month)
    # ============================================================
    print("\n=== TEST 3 — F25 per-nuclear-unit (May-Sep, same-cal-month) ===", flush=True)
    pb_nuc = panel[
        (panel.tech_group == "Nuclear")
        & panel.month.between(5, 9)
        & ((panel.date < pd.Timestamp("2024-06-14"))
           | ((panel.date >= pd.Timestamp("2025-04-28"))
              & (panel.date < pd.Timestamp("2025-10-01"))))
    ].copy()
    pb_nuc["reforzada"] = ((pb_nuc.date >= pd.Timestamp("2025-04-28"))
                          & (pb_nuc.date < pd.Timestamp("2025-10-01"))).astype(float)
    # Per-unit raw means (no FE — raw effect per plant)
    perunit = pb_nuc.groupby(["unit_code", "firm"]).apply(
        lambda g: pd.Series({
            "n_pre": (g.reforzada == 0).sum(),
            "n_ref": (g.reforzada == 1).sum(),
            "mean_pre": g[g.reforzada == 0]["bilat_share"].mean(),
            "mean_ref": g[g.reforzada == 1]["bilat_share"].mean(),
            "delta_pp": (g[g.reforzada == 1]["bilat_share"].mean()
                        - g[g.reforzada == 0]["bilat_share"].mean()) * 100,
        })
    ).reset_index()
    perunit = perunit.sort_values("delta_pp").reset_index(drop=True)
    print(perunit.to_string(index=False, float_format=lambda x: f"{x:.3f}"))
    perunit.to_csv(OUT_DIR / "pdbf_f25_per_unit.csv", index=False)

    # ============================================================
    # TEST 4 — B10 per-tech substitution
    # ============================================================
    print("\n=== TEST 4 — B10 per-tech substitution (firm-day × tech) ===", flush=True)
    # Aggregate panel to firm-day-tech
    fdt = panel.groupby(["date", "firm", "tech_group"]).agg(
        bilateral_mwh=("bilateral_mwh", "sum"),
        auction_mwh=("auction_mwh", "sum"),
    ).reset_index()
    fdt["q1_DA_GWh"]    = fdt["auction_mwh"] / 1000
    fdt["q1_bilat_GWh"] = fdt["bilateral_mwh"] / 1000
    fdt["regime"] = fdt["date"].apply(assign_regime)
    fdt["dow"]    = fdt["date"].dt.dayofweek
    fdt["month"]  = fdt["date"].dt.month
    fdt["year"]   = fdt["date"].dt.year

    rows_t = []
    for tech in ["CCGT", "Nuclear", "Hydro"]:
        sub = fdt[fdt.tech_group == tech].copy()
        if len(sub) < 100:
            print(f"  {tech}: only {len(sub)} rows, skipping")
            continue
        # Demean by firm
        sub["q1_DA_dm"] = sub["q1_DA_GWh"] - sub.groupby("firm")["q1_DA_GWh"].transform("mean")
        cols_t = {"const": np.ones(len(sub))}
        cols_t["q1_bilat_GWh"] = sub["q1_bilat_GWh"].values
        for r in REGIMES[1:]:
            cols_t[f"R_{r}"] = (sub["regime"] == r).astype(float).values
        for d_ in range(1, 7):
            cols_t[f"DOW{d_}"] = (sub["dow"] == d_).astype(float).values
        for m_ in range(2, 13):
            cols_t[f"M{m_}"] = (sub["month"] == m_).astype(float).values
        years_t = sorted(sub.year.unique())
        for yr in years_t[1:]:
            cols_t[f"Y{yr}"] = (sub["year"] == yr).astype(float).values

        XT = pd.DataFrame(cols_t, index=sub.index)
        yT = sub["q1_DA_dm"].values
        cluster_t = sub["date"].dt.strftime("%Y%m%d").astype(np.int64).values
        mT = fit_ols_cluster(yT, XT.values, cluster_t)
        b   = mT.params[XT.columns.get_loc("q1_bilat_GWh")]
        se  = mT.bse[XT.columns.get_loc("q1_bilat_GWh")]
        p   = mT.pvalues[XT.columns.get_loc("q1_bilat_GWh")]
        rows_t.append({"tech": tech, "n_rows": len(sub),
                      "n_clusters": len(np.unique(cluster_t)),
                      "beta_q1_bilat": b, "se": se, "p": p, "rsq": mT.rsquared})
        print(f"  {tech} (N={len(sub):,}): β(q1_bilat) = {b:+.4f}  "
              f"(SE {se:.4f}, p={p:.2e})  R²={mT.rsquared:.3f}")

    out_t = pd.DataFrame(rows_t)
    out_t.to_csv(OUT_DIR / "pdbf_b10_per_tech.csv", index=False)

    print("\nDone.")


if __name__ == "__main__":
    main()
