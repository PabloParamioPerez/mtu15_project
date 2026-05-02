# STATUS: ALIVE
# LAST-AUDIT: 2026-05-02
# FEEDS: F25 mechanism decomposition — IDA reform vs reforzada incremental
# CLAIM: F25's −34.5pp drop is wrong-attributed: ~33pp happens at IDA
#        reform (2024-06-14), only ~2pp incremental at reforzada.
"""F25 decomposition: IDA-reform piece vs reforzada-incremental piece.

Year-by-year run showed nuclear bilat_share May-Sep 2018-2023 = 84-95%,
2024 = 54.5%, 2025 = 52.2%. The big shock is at IDA reform, not reforzada.

Two clean regressions, both same-cal-month May-Sep, unit FE (within-demean),
cluster SE by date, cal-month + DOW FE:

A) IDA-reform spec: pre-IDA May-Sep (2018-2024 strict pre 2024-06-14)
   vs 3-sess May-Sep (2024-06-14 → 2024-09-30 only).
B) Reforzada-incremental spec: 3-sess May-Sep 2024 + DA60/ID15 May-Sep 2025
   pre-blackout (May-Apr 27 2025) vs reforzada May-Sep 2025
   (Apr 28 → Sep 30 2025).

This decomposes F25's −34.5pp into an IDA-reform component + a small
reforzada-incremental component. Result drives the F25 reframing.

Output:
  results/regressions/pdbf_f25_decomposition.csv
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
OUT     = PROJECT / "results" / "regressions" / "pdbf_f25_decomposition.csv"


def fit_ols_cluster(y, X, cluster):
    return sm.OLS(y, X).fit(cov_type="cluster", cov_kwds={"groups": cluster})


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

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
        if "nuclear" in tl: return "Nuclear"
        return "Other"

    map_uf["tech_group"] = map_uf["technology"].apply(tech_group)
    con.register("uf", map_uf[["unit_code", "firm", "tech_group"]])

    print("[panel] nuclear unit-day panel (Big-4)…", flush=True)
    panel = con.execute(f"""
        SELECT CAST(p.date AS DATE) AS date, p.unit_code, uf.firm,
               SUM(CASE WHEN p.offer_type = 4 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS bilateral_mwh,
               SUM(CASE WHEN p.offer_type = 1 AND p.assigned_power_mw > 0
                        THEN p.assigned_power_mw * p.mtu_minutes / 60.0
                        ELSE 0 END) AS auction_mwh
        FROM '{PDBF}' p JOIN uf USING (unit_code)
        WHERE uf.firm IN ('IB','GE','GN','HC')
          AND uf.tech_group = 'Nuclear'
        GROUP BY 1, 2, 3
    """).df()
    panel["total_mwh"] = panel["bilateral_mwh"] + panel["auction_mwh"]
    panel = panel[panel["total_mwh"] > 0].copy()
    panel["bilat_share"] = panel["bilateral_mwh"] / panel["total_mwh"]
    panel["date"] = pd.to_datetime(panel["date"])
    panel["dow"]   = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    print(f"   nuclear unit-day panel: {len(panel):,} rows", flush=True)

    rows_out = []

    # ============================================================
    # SPEC A — IDA-reform piece
    # pre-IDA May-Sep (date < 2024-06-14, May-Sep months only)
    #   vs 3-sess May-Sep 2024 (2024-06-14 ≤ date < 2024-10-01)
    # ============================================================
    print("\n=== SPEC A — IDA reform piece (pre-IDA May-Sep vs 3-sess May-Sep 2024) ===", flush=True)
    pa = panel[
        panel.month.between(5, 9)
        & ((panel.date < pd.Timestamp("2024-06-14"))
           | ((panel.date >= pd.Timestamp("2024-06-14"))
              & (panel.date < pd.Timestamp("2024-10-01"))))
    ].copy()
    pa["post_IDA"] = (pa.date >= pd.Timestamp("2024-06-14")).astype(float)
    pa["bilat_share_dm"] = pa["bilat_share"] - pa.groupby("unit_code")["bilat_share"].transform("mean")

    cols_a = {"const": np.ones(len(pa))}
    cols_a["post_IDA"] = pa["post_IDA"].values
    for d_ in range(1, 7):
        cols_a[f"DOW{d_}"] = (pa["dow"] == d_).astype(float).values
    for m_ in [6, 7, 8, 9]:
        cols_a[f"M{m_}"] = (pa["month"] == m_).astype(float).values

    XA = pd.DataFrame(cols_a, index=pa.index)
    yA = pa["bilat_share_dm"].values
    cluster_a = pa["date"].dt.strftime("%Y%m%d").astype(np.int64).values
    mA = fit_ols_cluster(yA, XA.values, cluster_a)
    bA = mA.params[XA.columns.get_loc("post_IDA")]
    seA = mA.bse[XA.columns.get_loc("post_IDA")]
    pA = mA.pvalues[XA.columns.get_loc("post_IDA")]
    pre_mean_A = pa[pa.post_IDA == 0].bilat_share.mean()
    post_mean_A = pa[pa.post_IDA == 1].bilat_share.mean()
    print(f"  N={len(pa):,} unit-days; n_clusters={len(np.unique(cluster_a)):,}; R²={mA.rsquared:.3f}")
    print(f"  pre-IDA May-Sep mean = {pre_mean_A:.3f}; 3-sess May-Sep 2024 mean = {post_mean_A:.3f}")
    print(f"  β(post_IDA) = {bA:+.4f}  (SE {seA:.4f}, p={pA:.2e})  → {bA*100:+.2f}pp")
    rows_out.append({"spec": "A_IDA_reform_piece", "n": len(pa),
                     "n_clusters": len(np.unique(cluster_a)),
                     "pre_mean": pre_mean_A, "post_mean": post_mean_A,
                     "beta_pp": bA * 100, "se_pp": seA * 100, "p": pA,
                     "rsq": mA.rsquared})

    # ============================================================
    # SPEC B — Reforzada-incremental piece
    # 3-sess + DA60-pre-blackout May-Sep (post-2024-06-14, pre-2025-04-28)
    #   vs reforzada May-Sep 2025 (2025-04-28 → 2025-10-01)
    # Restrict to 3-sess full May-Sep 2024 (which post-IDA, pre-reforzada).
    # ============================================================
    print("\n=== SPEC B — Reforzada incremental (3-sess May-Sep 2024 vs reforzada May-Sep 2025) ===", flush=True)
    pb = panel[
        panel.month.between(5, 9)
        & (((panel.date >= pd.Timestamp("2024-06-14"))
            & (panel.date < pd.Timestamp("2025-04-28")))
           | ((panel.date >= pd.Timestamp("2025-04-28"))
              & (panel.date < pd.Timestamp("2025-10-01"))))
    ].copy()
    pb["reforzada"] = (pb.date >= pd.Timestamp("2025-04-28")).astype(float)
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
    bB = mB.params[XB.columns.get_loc("reforzada")]
    seB = mB.bse[XB.columns.get_loc("reforzada")]
    pB = mB.pvalues[XB.columns.get_loc("reforzada")]
    pre_mean_B = pb[pb.reforzada == 0].bilat_share.mean()
    post_mean_B = pb[pb.reforzada == 1].bilat_share.mean()
    print(f"  N={len(pb):,} unit-days; n_clusters={len(np.unique(cluster_b)):,}; R²={mB.rsquared:.3f}")
    print(f"  3-sess May-Sep 2024 mean = {pre_mean_B:.3f}; reforzada May-Sep 2025 mean = {post_mean_B:.3f}")
    print(f"  β(reforzada) = {bB:+.4f}  (SE {seB:.4f}, p={pB:.2e})  → {bB*100:+.2f}pp")
    rows_out.append({"spec": "B_reforzada_incremental", "n": len(pb),
                     "n_clusters": len(np.unique(cluster_b)),
                     "pre_mean": pre_mean_B, "post_mean": post_mean_B,
                     "beta_pp": bB * 100, "se_pp": seB * 100, "p": pB,
                     "rsq": mB.rsquared})

    # ============================================================
    # Summary
    # ============================================================
    print("\n" + "=" * 80)
    print("DECOMPOSITION SUMMARY (F25 −34.5pp original → IDA-reform + reforzada)")
    print("=" * 80)
    print(f"  IDA-reform piece (pre-IDA → 3-sess May-Sep): {bA*100:+.2f}pp (p={pA:.2e})")
    print(f"  Reforzada-incremental (3-sess → reforzada May-Sep): {bB*100:+.2f}pp (p={pB:.2e})")
    if abs(bB) < abs(bA) * 0.2:
        print("  → IDA-reform piece dominates; reforzada-attribution of F25 is misattributed.")
    else:
        print("  → Both pieces non-trivial; F25 is partly reforzada-driven.")

    pd.DataFrame(rows_out).to_csv(OUT, index=False)
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
