# STATUS: ALIVE
# LAST-AUDIT: 2026-05-12
# FEEDS: thesis paper.tex §A (robustness — time placebos for B1 q_2 DiD)
# CLAIM: Replicate the B1 within-day DiD on q_2 over windows that do NOT
#        cross the MTU15-DA reform (2025-10-01). If β₃ ≈ 0 in placebo
#        windows, the headline β₃ is reform-driven, not a seasonal /
#        regime-overlap artefact.
#
# Three placebo designs:
#   P1 — Within-2024: pre = Jul-Sep 2024, post = Oct-Dec 2024.
#        Both post-IDA-reform (3 sessions, 2024-06-13), both MTU60-DA,
#        both MTU60-IDA. Tests whether the Jul-Sep -> Oct-Dec seasonal
#        differential alone produces β₃ ≠ 0.
#
#   P2 — Within-2025 pre-MTU15-DA: pre = Apr-Jun 2025, post = Jul-Sep 2025.
#        Both MTU15-IDA, both MTU60-DA, both within reforzada.
#        Tests whether reforzada + seasonal drift produces β₃ ≠ 0
#        absent the DA-side reform.
#
#   P3 — One-year-shifted: pre = Oct-Dec 2023, post = Oct-Dec 2024.
#        Same calendar months as the headline, shifted back one year.
#        Confound: crosses the 2024-06-13 IDA reform (6 -> 3 sessions),
#        so β₃ here picks up that reform plus seasonal drift.
#        Less clean; report as supplementary.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
from statsmodels.api import OLS, add_constant
from statsmodels.stats.sandwich_covariance import cov_cluster
from scipy.stats import norm

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as TREATMENT_PARENTS,
    PLACEBO_PARENTS_SHORT as PLACEBO_PARENTS,
)

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

CRITICAL_HOURS = (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22)
FLAT_HOURS = (1, 2, 3)

# (placebo_id, pre_start, pre_end, post_start, post_end, note)
PLACEBOS = [
    ("P1_within2024",   "2024-07-01", "2024-10-01", "2024-10-01", "2025-01-01",
     "pre Jul-Sep 2024 vs post Oct-Dec 2024 (both MTU60-DA, MTU60-IDA, 3-session)"),
    ("P2_within2025pre","2025-04-01", "2025-07-01", "2025-07-01", "2025-10-01",
     "pre Apr-Jun 2025 vs post Jul-Sep 2025 (both MTU15-IDA, MTU60-DA, reforzada)"),
    ("P3_shifted1y",    "2023-10-01", "2024-01-01", "2024-10-01", "2025-01-01",
     "pre Oct-Dec 2023 vs post Oct-Dec 2024 (crosses 2024-06-13 IDA reform; supplementary)"),
]


def hour_class(h: int) -> str:
    if h in CRITICAL_HOURS: return "critical_canonical"
    if h in FLAT_HOURS:     return "flat_canonical"
    return "other"


def build_panel(units: pd.DataFrame, pre_start, pre_end, post_start, post_end) -> pd.DataFrame:
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code", "parent", "tech_group", "zone"]])

    rows = []
    for label, start, end in [("PRE", pre_start, pre_end), ("POST", post_start, post_end)]:
        df = con.execute(
            f"""
            WITH pibci_summed AS (
                SELECT date::DATE AS d, period,
                       ANY_VALUE(mtu_minutes) AS mtu,
                       unit_code,
                       SUM(assigned_power_mw) AS q2_mw
                FROM '{PIBCI}'
                WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
                GROUP BY 1,2,4
            ),
            with_hour AS (
                SELECT p.d, p.unit_code, p.period, p.mtu,
                       u.parent, u.tech_group, u.zone,
                       CASE WHEN mtu = 60 THEN period - 1
                            WHEN mtu = 15 THEN (period - 1) // 4
                            ELSE NULL END AS hour,
                       q2_mw * mtu / 60.0 AS q2_mwh
                FROM pibci_summed p JOIN units u USING (unit_code)
            )
            SELECT d, unit_code, parent, tech_group, zone, hour,
                   SUM(q2_mwh) AS q2_mwh_clock_hour
            FROM with_hour
            WHERE hour IS NOT NULL AND hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4,5,6
            """
        ).df()
        df["window"] = label
        df["post"] = 1 if label == "POST" else 0
        rows.append(df)
        print(f"    {label} {start}->{end}: {len(df):,} rows")

    panel = pd.concat(rows, ignore_index=True)
    panel["d"] = pd.to_datetime(panel["d"])
    panel["hour_class"] = panel["hour"].astype(int).apply(hour_class)
    panel["crit"] = (panel["hour_class"] == "critical_canonical").astype(int)
    panel["dow"] = panel["d"].dt.dayofweek
    panel["treatment_group"] = panel["parent"].apply(
        lambda p: "treatment" if p in TREATMENT_PARENTS else
                  ("placebo" if p in PLACEBO_PARENTS else "untagged")
    )
    return panel[panel["hour_class"].isin(["critical_canonical", "flat_canonical"])].copy()


def run_did(panel: pd.DataFrame, label: str) -> dict:
    if len(panel) < 30 or panel["d"].nunique() < 5:
        return {"label": label, "n": len(panel), "beta_3": np.nan, "se": np.nan, "p": np.nan,
                "n_clusters": panel["d"].nunique() if len(panel) else 0}
    df = panel.copy()
    df["crit_x_post"] = df["crit"] * df["post"]
    parent_dummies = pd.get_dummies(df["parent"], prefix="firm", drop_first=True)
    dow_dummies = pd.get_dummies(df["dow"], prefix="dow", drop_first=True)
    X = pd.concat([df[["crit", "post", "crit_x_post"]], parent_dummies, dow_dummies], axis=1).astype(float)
    X = add_constant(X, has_constant='add')
    y = df["q2_mwh_clock_hour"].astype(float).values
    cluster = df["d"].astype(str).values
    try:
        result = OLS(y, X).fit()
        cov = cov_cluster(result, cluster)
        se_cluster = np.sqrt(np.diag(cov))
        cols = list(X.columns); idx = cols.index("crit_x_post")
        b3 = result.params.iloc[idx]; se = se_cluster[idx]
        p = 2 * (1 - norm.cdf(abs(b3 / se))) if se > 0 else np.nan
        return {"label": label, "n": len(df), "n_clusters": df["d"].nunique(),
                "beta_3": b3, "se": se, "p": p,
                "y_mean_pre_flat":  float(df.loc[(df["post"]==0)&(df["crit"]==0), "q2_mwh_clock_hour"].mean()),
                "y_mean_pre_crit":  float(df.loc[(df["post"]==0)&(df["crit"]==1), "q2_mwh_clock_hour"].mean()),
                "y_mean_post_flat": float(df.loc[(df["post"]==1)&(df["crit"]==0), "q2_mwh_clock_hour"].mean()),
                "y_mean_post_crit": float(df.loc[(df["post"]==1)&(df["crit"]==1), "q2_mwh_clock_hour"].mean())}
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r: dict) -> None:
    if "error" in r:
        print(f"  {r['label']:42s}  ERROR: {r['error']}"); return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:42s}  n={r['n']:6d}  (insufficient)"); return
    sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
    print(f"  {r['label']:42s}  n={r['n']:6d}  G={r['n_clusters']:3d}  "
          f"β₃={r['beta_3']:+8.3f}  SE={r['se']:6.3f}  p={r['p']:.4f}{sig}  "
          f"means(pre-flat,post-crit)=({r['y_mean_pre_flat']:6.2f},{r['y_mean_post_crit']:6.2f})")


def main() -> None:
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short", mode="primary_owner")
    all_results = []

    for placebo_id, pre_s, pre_e, post_s, post_e, note in PLACEBOS:
        print(f"\n{'='*88}\n[{placebo_id}] {note}\n{'='*88}")
        panel = build_panel(units, pre_s, pre_e, post_s, post_e)
        print(f"  Panel rows: {len(panel):,}")

        for label, sub in [
            ("all_firms",       panel),
            ("treatment_only",  panel[panel["treatment_group"]=="treatment"]),
            ("placebo_only",    panel[panel["treatment_group"]=="placebo"]),
        ]:
            r = run_did(sub, f"{placebo_id}__{label}")
            all_results.append(r); print_result(r)

        print("  Tech-stratified (treatment group only):")
        for tech in ["CCGT", "Hydro", "Nuclear", "Wind", "Solar PV"]:
            sub = panel[(panel["treatment_group"]=="treatment") & (panel["tech_group"]==tech)]
            r = run_did(sub, f"{placebo_id}__treatment_{tech}")
            all_results.append(r); print_result(r)

    df = pd.DataFrame(all_results)
    out = OUTDIR / "B1_q2_did_time_placebos.csv"
    df.to_csv(out, index=False)
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
