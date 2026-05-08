# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §5.3 (other outcomes for parallel comparison)
# CLAIM: Within-day DiD on DA cleared MWh per (unit, clock-hour),
#        critical h{18-22} vs flat h{3-5}, same-cal-month Oct-Dec 2024 vs
#        Oct-Dec 2025, pivotality-based partition, TECH-STRATIFIED.
#
# Mechanism prediction: dominant firms strategically withhold in DA in
# critical hours post-reform (negative β₃ on DA cleared) and recover
# the energy in IDA (positive β₃ on q_2, B1). If both hold jointly,
# the storyboard is consistent.

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
from mtu.classification.units import classify_units  # noqa: E402

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
OUTDIR.mkdir(parents=True, exist_ok=True)

PDBCE = REPO / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"

PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"
CRITICAL_HOURS = (7, 8, 16, 17, 18, 19, 20, 21, 22)  # 'joint' = supply_ramp ∪ price_peak; canonical
FLAT_HOURS = (3, 4, 5)


def parent_of(o):
    if not isinstance(o, str): return "Other"
    o = o.upper()
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


TREATMENT_PARENTS = {"IB","GE","GN","HC","EDP-PT"}
PLACEBO_PARENTS = {"Repsol","Engie","TotalEnergies","Moeve"}


def hour_class(h):
    if h in CRITICAL_HOURS: return "critical_joint"
    if h in FLAT_HOURS: return "flat_h3_5"
    return "other"


def build_panel(units):
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code","parent","tech_group","zone"]])
    rows = []
    for label, start, end in [("PRE_2024", PRE_START, PRE_END), ("POST_2025", POST_START, POST_END)]:
        print(f"--- {label}: {start} to {end} ---")
        df = con.execute(f"""
            WITH p AS (
                SELECT date::DATE AS d, period, mtu_minutes, unit_code, assigned_power_mw
                FROM '{PDBCE}'
                WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
                  AND assigned_power_mw > 0
            ),
            with_hour AS (
                SELECT p.d, p.unit_code, u.parent, u.tech_group, u.zone,
                       CASE WHEN p.mtu_minutes = 60 THEN p.period - 1
                            WHEN p.mtu_minutes = 15 THEN (p.period - 1)/4
                            ELSE NULL END AS hour,
                       p.assigned_power_mw * p.mtu_minutes / 60.0 AS mwh
                FROM p JOIN units u USING (unit_code)
            )
            SELECT d, unit_code, parent, tech_group, zone, hour,
                   SUM(mwh) AS da_mwh
            FROM with_hour
            WHERE hour BETWEEN 0 AND 23
            GROUP BY 1,2,3,4,5,6
        """).df()
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
                  ("placebo" if p in PLACEBO_PARENTS else "untagged"))
    return panel[panel["hour_class"].isin(["critical_joint","flat_h3_5"])].copy()


def run_did(panel, label):
    if len(panel) < 30 or panel["d"].nunique() < 5:
        return {"label": label, "n": len(panel), "beta_3": np.nan,
                "n_clusters": panel["d"].nunique() if len(panel) else 0}
    df = panel.copy()
    df["crit_x_post"] = df["crit"] * df["post"]
    parent_dummies = pd.get_dummies(df["parent"], prefix="firm", drop_first=True)
    dow_dummies = pd.get_dummies(df["dow"], prefix="dow", drop_first=True)
    X = pd.concat([df[["crit","post","crit_x_post"]], parent_dummies, dow_dummies], axis=1).astype(float)
    X = add_constant(X, has_constant='add')
    y = df["da_mwh"].astype(float).values
    cluster = df["d"].astype(str).values
    try:
        result = OLS(y, X).fit()
        cov = cov_cluster(result, cluster)
        se_cluster = np.sqrt(np.diag(cov))
        cols = list(X.columns); idx = cols.index("crit_x_post")
        b3 = result.params.iloc[idx]; se = se_cluster[idx]
        p = 2*(1-norm.cdf(abs(b3/se))) if se > 0 else np.nan
        return {"label": label, "n": len(df), "n_clusters": df["d"].nunique(),
                "beta_3": b3, "se": se, "p": p,
                "y_mean_pre_flat": float(df.loc[(df["post"]==0)&(df["crit"]==0),"da_mwh"].mean()),
                "y_mean_pre_crit": float(df.loc[(df["post"]==0)&(df["crit"]==1),"da_mwh"].mean()),
                "y_mean_post_flat": float(df.loc[(df["post"]==1)&(df["crit"]==0),"da_mwh"].mean()),
                "y_mean_post_crit": float(df.loc[(df["post"]==1)&(df["crit"]==1),"da_mwh"].mean())}
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r):
    if "error" in r: print(f"  {r['label']:32s}  ERROR: {r['error']}"); return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:32s}  n={r['n']:6d}  (insufficient)"); return
    sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
    print(f"  {r['label']:32s}  n={r['n']:6d}  G={r['n_clusters']:3d}  "
          f"β₃={r['beta_3']:+9.2f}  SE={r['se']:6.2f}  p={r['p']:.4f}{sig}  "
          f"means={r['y_mean_pre_flat']:6.1f}/{r['y_mean_pre_crit']:6.1f}/{r['y_mean_post_flat']:6.1f}/{r['y_mean_post_crit']:6.1f}")


def main():
    units = classify_units(csv_path=str(UNITS_CSV),
                           keep_columns=["unit_code","owner_agent","tech_group","zone"])
    units["parent"] = units["owner_agent"].apply(parent_of)
    panel = build_panel(units)
    print(f"\nPanel rows: {len(panel):,}")

    results = []
    print("\n=== B3 — DA cleared MWh DiD (per unit-clock-hour) ===")
    print("Means columns: pre-flat / pre-crit / post-flat / post-crit\n")

    print("All firms pooled:")
    r = run_did(panel, "all_firms"); results.append(r); print_result(r)

    print("\nTreatment group:")
    r = run_did(panel[panel["treatment_group"]=="treatment"], "treatment_only"); results.append(r); print_result(r)

    print("\nPlacebo group:")
    r = run_did(panel[panel["treatment_group"]=="placebo"], "placebo_only"); results.append(r); print_result(r)

    print("\n--- TECH-STRATIFIED β₃ (treatment group) ---")
    for tech in ["CCGT","Hydro","Hydro_pump","Coal","Nuclear","Wind","Solar PV","Biomass"]:
        sub = panel[(panel["treatment_group"]=="treatment") & (panel["tech_group"]==tech)]
        r = run_did(sub, f"treatment_{tech}"); results.append(r); print_result(r)

    print("\n--- PER-FIRM β₃ ---")
    for parent in ["IB","GE","GN","HC","EDP-PT"]:
        sub = panel[panel["parent"]==parent]
        r = run_did(sub, f"firm_{parent}"); results.append(r); print_result(r)

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTDIR / "B3_da_cleared_did.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'B3_da_cleared_did.csv'}")


if __name__ == "__main__":
    main()
