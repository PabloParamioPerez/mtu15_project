# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §5 (main empirical results)
# CLAIM: Within-day DiD on q_2 (Ito-Reguant strategic IDA upward adjustment),
#        critical h{18-22} vs flat h{3-5}, pre-MTU15-DA (Oct-Dec 2024) vs
#        post-MTU15-DA (Oct-Dec 2025), pivotality-based treatment partition,
#        TECH-STRATIFIED.
#
# Mechanism: dominant firms in time-varying (critical) hours exploit higher
# bid-period granularity to extract additional rents (DA withhold + IDA sell
# adjustment). In flat hours there is no time variation, so granularity has
# no economic content. Fringe firms are not pivotal, bid at MC always.
# Empirical prediction: positive β₃ for treated dispatchable firms; ≈0 for
# placebo and for non-dispatchable techs.
#
# Outcome: q_2 in MWh per (unit, clock-hour) (= sum pibci across IDA sessions
# converted to MWh, summed over IDA periods within a clock-hour).
#
# Specification per cell:
#   q_{2,fdh} = α + β_1 crit_h + β_2 post_d + β_3 (crit × post) + γ_f + δ_DOW(d) + ε
#
# Stratifications:
#   - by tech_group  (CCGT, Hydro, Nuclear, Coal, Wind, Solar PV, others)
#   - by treatment_group (treatment_pivotal, placebo_nonpivotal, untagged)
#   - all-firms pooled
#
# Companion: critical_hours_did_thesis_robustness.py for the B5 robustness panel.

from __future__ import annotations

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd
from statsmodels.api import OLS, add_constant
from statsmodels.stats.sandwich_covariance import cov_cluster

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src"))
from mtu.classification.units import classify_units  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)

PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"

CRITICAL_HOURS = (7, 8, 16, 17, 18, 19, 20, 21, 22)  # 'joint' = supply_ramp ∪ price_peak; canonical
# (alternatives kept in robustness B5.1: price_peak h{18-22}, supply_ramp h{7,8,16-18}, demand_peak h{16-20})
FLAT_HOURS = (3, 4, 5)


def parent_of(owner: str | None) -> str:
    if not isinstance(owner, str):
        return "Other"
    o = owner.upper()
    if "IBERDROLA" in o: return "IB"
    if "ENDESA" in o: return "GE"
    if "NATURGY" in o or "GAS NATURAL" in o: return "GN"
    if "EDP ESPAÑA" in o: return "HC"
    if "EDP GEM PORTUGAL" in o: return "EDP-PT"
    if "ENGIE" in o: return "Engie"
    if "REPSOL" in o: return "Repsol"
    if "TOTALENERGIES" in o: return "TotalEnergies"
    if "MOEVE" in o or "CEPSA" in o: return "Moeve"
    return "Other"


# Treatment-effect set per `_pivotality_by_firm_critical_hours.md`:
#   - Treatment (≥10% pivotal in critical hours): IB, GE, GN, HC, EDP-PT
#   - Placebo (~0% pivotal): Repsol, Engie España (CTNU), TotalEnergies, Moeve
TREATMENT_PARENTS = {"IB", "GE", "GN", "HC", "EDP-PT"}
PLACEBO_PARENTS = {"Repsol", "Engie", "TotalEnergies", "Moeve"}


def hour_class(h: int) -> str:
    if h in CRITICAL_HOURS: return "critical_joint"
    if h in FLAT_HOURS:     return "flat_h3_5"
    return "other"


def build_panel(units: pd.DataFrame) -> pd.DataFrame:
    """Per (unit, day, clock-hour) panel of q_2 for both windows."""
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code", "parent", "tech_group", "zone"]])

    rows = []
    for label, start, end in [("PRE_2024", PRE_START, PRE_END), ("POST_2025", POST_START, POST_END)]:
        print(f"\n--- Building q_2 panel: {label} {start} to {end} ---")
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
                            WHEN mtu = 15 THEN (period - 1) / 4
                            ELSE NULL END AS hour,
                       q2_mw * mtu / 60.0 AS q2_mwh
                FROM pibci_summed p JOIN units u USING (unit_code)
            )
            SELECT d, unit_code, parent, tech_group, zone, hour,
                   SUM(q2_mwh) AS q2_mwh_clock_hour,
                   SUM(GREATEST(q2_mwh, 0)) AS q2_pos_mwh,
                   SUM(LEAST(q2_mwh, 0)) AS q2_neg_mwh
            FROM with_hour
            WHERE hour IS NOT NULL AND hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4,5,6
            """
        ).df()
        df["window"] = label
        df["post"] = 1 if label == "POST_2025" else 0
        rows.append(df)
        print(f"  {label} rows: {len(df):,}")

    panel = pd.concat(rows, ignore_index=True)
    panel["d"] = pd.to_datetime(panel["d"])
    panel["hour_class"] = panel["hour"].astype(int).apply(hour_class)
    panel["crit"] = (panel["hour_class"] == "critical_joint").astype(int)
    panel["dow"] = panel["d"].dt.dayofweek
    panel["treatment_group"] = panel["parent"].apply(
        lambda p: "treatment" if p in TREATMENT_PARENTS else
                  ("placebo" if p in PLACEBO_PARENTS else "untagged")
    )
    panel = panel[panel["hour_class"].isin(["critical_joint", "flat_h3_5"])].copy()
    return panel


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
        model = OLS(y, X)
        result = model.fit()
        cov = cov_cluster(result, cluster)
        se_cluster = np.sqrt(np.diag(cov))
        cols = list(X.columns)
        idx = cols.index("crit_x_post")
        beta_3 = result.params.iloc[idx]
        se = se_cluster[idx]
        from scipy.stats import norm
        p = 2 * (1 - norm.cdf(abs(beta_3 / se))) if se > 0 else np.nan
        return {
            "label": label,
            "n": len(df),
            "n_clusters": df["d"].nunique(),
            "beta_3": beta_3,
            "se": se,
            "p": p,
            "beta_1_crit": result.params.iloc[cols.index("crit")],
            "beta_2_post": result.params.iloc[cols.index("post")],
            "y_mean": float(y.mean()),
            "y_mean_pre_flat": float(df.loc[(df["post"]==0)&(df["crit"]==0), "q2_mwh_clock_hour"].mean()),
            "y_mean_pre_crit": float(df.loc[(df["post"]==0)&(df["crit"]==1), "q2_mwh_clock_hour"].mean()),
            "y_mean_post_flat": float(df.loc[(df["post"]==1)&(df["crit"]==0), "q2_mwh_clock_hour"].mean()),
            "y_mean_post_crit": float(df.loc[(df["post"]==1)&(df["crit"]==1), "q2_mwh_clock_hour"].mean()),
        }
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r: dict) -> None:
    if "error" in r:
        print(f"  {r['label']:32s}  ERROR: {r['error']}")
        return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:32s}  n={r['n']:6d}  (insufficient)")
        return
    sig = ""
    if r["p"] < 0.001: sig = "***"
    elif r["p"] < 0.01: sig = "**"
    elif r["p"] < 0.05: sig = "*"
    print(f"  {r['label']:32s}  n={r['n']:6d}  G={r['n_clusters']:3d}  "
          f"β₃={r['beta_3']:+8.3f}  SE={r['se']:6.3f}  p={r['p']:.4f}{sig}  "
          f"means(pre-flat,post-crit)=({r['y_mean_pre_flat']:6.2f},{r['y_mean_post_crit']:6.2f})")


def main() -> None:
    units = classify_units(
        csv_path=str(UNITS_CSV),
        keep_columns=["unit_code", "owner_agent", "tech_group", "zone"],
    )
    units["parent"] = units["owner_agent"].apply(parent_of)

    panel = build_panel(units)
    print(f"\nFull panel rows: {len(panel):,}")
    print("\nCounts by (window, hour_class, treatment_group):")
    print(panel.groupby(["window","hour_class","treatment_group"]).size().to_string())

    results = []
    print("\n=== B1 — Headline DiD on q_2 (MWh per unit-clock-hour) ===")
    print("\nAll firms pooled:")
    r = run_did(panel, "all_firms")
    results.append(r); print_result(r)

    print("\nTreatment group (pivotal in critical hours):")
    r = run_did(panel[panel["treatment_group"] == "treatment"], "treatment_only")
    results.append(r); print_result(r)

    print("\nPlacebo group (~0% pivotal):")
    r = run_did(panel[panel["treatment_group"] == "placebo"], "placebo_only")
    results.append(r); print_result(r)

    print("\n--- TECH-STRATIFIED β₃ (treatment group only) ---")
    print("Prediction: positive β₃ for dispatchable (CCGT, Hydro, Coal); ≈0 for Wind/Solar/Nuclear")
    for tech in ["CCGT", "Hydro", "Hydro_pump", "Coal", "Nuclear", "Wind", "Solar PV", "Biomass"]:
        sub = panel[(panel["treatment_group"] == "treatment") & (panel["tech_group"] == tech)]
        r = run_did(sub, f"treatment_{tech}")
        results.append(r); print_result(r)

    print("\n--- TECH-STRATIFIED β₃ (placebo group only) — should be ≈ 0 ---")
    for tech in ["CCGT", "Hydro", "Wind", "Solar PV"]:
        sub = panel[(panel["treatment_group"] == "placebo") & (panel["tech_group"] == tech)]
        r = run_did(sub, f"placebo_{tech}")
        results.append(r); print_result(r)

    print("\n--- PER-FIRM β₃ (treatment group, all techs) ---")
    for parent in ["IB", "GE", "GN", "HC", "EDP-PT"]:
        sub = panel[panel["parent"] == parent]
        r = run_did(sub, f"firm_{parent}")
        results.append(r); print_result(r)

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTDIR / "B1_q2_did.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'B1_q2_did.csv'}")
    panel.to_parquet(OUTDIR / "B1_panel.parquet", index=False)
    print(f"Saved panel: {OUTDIR / 'B1_panel.parquet'}")


if __name__ == "__main__":
    main()
