# STATUS: ALIVE
# LAST-AUDIT: 2026-05-08
# FEEDS: thesis paper.tex §A.1, §A.2, §A.3 (robustness appendix)
# CLAIM: B1's headline β₃ on q_2 is robust to:
#   B5.1 critical-hours definition (price_peak vs supply_ramp vs joint vs demand_peak)
#   B5.2 firm partition (pivotality-based vs administrative IB/GE/GN/HC)
#   B5.3 window (same-cal-month vs full panel)
#   B5.4 sample exclusions (drop EDP-PT, drop ABO2G, restrict fringe)
#   B5.5 reforzada window exclusion (drop Apr-Sep 2025 entirely)
#
# Uses the existing B1_panel.parquet for B5.1, B5.2, B5.4 (same window).
# Builds new panels for B5.3 (full window), B5.5 (excludes Apr-Sep 2025).

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
# Centralized firm classification: see src/mtu/classification/units.py and
# notebooks/memos/_firm_classification_audit.md for the audit trail.
from mtu.classification.units import (  # noqa: E402
    firm_unit_panel,
    TREATMENT_PARENTS_SHORT as TREATMENT_PARENTS,
    PLACEBO_PARENTS_SHORT as PLACEBO_PARENTS,
)

OUTDIR = REPO / "results" / "regressions" / "firm" / "critical_hours_thesis"
PIBCI = REPO / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
UNITS_CSV = REPO / "data" / "external" / "omie_reference" / "lista_unidades.csv"
B1_PANEL = OUTDIR / "B1_panel.parquet"

# All four critical-hours sets per `_critical_hours_calibration.md`
CRITICAL_HOUR_SETS = {
    "canonical_demand_surge_vre_transition": (5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22),  # canonical B1
    "supply_ramp": (7, 8, 16, 17, 18),                  # original σ²_within set
    "price_peak": (18, 19, 20, 21, 22),                  # top-5 by DA price
    "demand_peak": (16, 17, 18, 19, 20),                 # top-5 by raw load
    "joint": (7, 8, 16, 17, 18, 19, 20, 21, 22),         # supply_ramp ∪ price_peak
}
FLAT_HOURS = (1, 2, 3)  # truly flat (h4 already ramping in spring/summer)

# DST transition days in our 2024-2025 panel (Madrid local)
DST_DAYS = ("2024-03-31", "2024-10-27", "2025-03-30", "2025-10-26")

PRE_START, PRE_END = "2024-10-01", "2025-01-01"
POST_START, POST_END = "2025-10-01", "2026-01-01"
FULL_START, FULL_END = "2024-01-01", "2026-01-01"


ADMIN_DOMINANT_PARENTS = {"IB", "GE", "GN", "HC"}


def build_panel_full(units, start, end):
    """Build full-window q_2 panel."""
    con = duckdb.connect()
    con.execute("PRAGMA threads = 4")
    con.execute("SET memory_limit = '10GB'")
    con.register("units", units[["unit_code","parent","tech_group","zone"]])
    print(f"Building full panel {start} to {end}...")
    df = con.execute(f"""
        WITH pibci_summed AS (
            SELECT date::DATE AS d, period, ANY_VALUE(mtu_minutes) AS mtu, unit_code,
                   SUM(assigned_power_mw) AS q2_mw
            FROM '{PIBCI}'
            WHERE date::DATE >= DATE '{start}' AND date::DATE < DATE '{end}'
            GROUP BY 1,2,4
        ),
        with_hour AS (
            SELECT p.d, p.unit_code, u.parent, u.tech_group, u.zone,
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
    """).df()
    df["d"] = pd.to_datetime(df["d"])
    df["dow"] = df["d"].dt.dayofweek
    return df


def run_did(panel, label):
    if len(panel) < 30 or panel["d"].nunique() < 5:
        return {"label": label, "n": len(panel), "beta_3": np.nan,
                "n_clusters": panel["d"].nunique() if len(panel) else 0}
    df = panel.copy()
    df["crit_x_post"] = df["crit"] * df["post"]
    parent_dummies = pd.get_dummies(df["parent"], prefix="firm", drop_first=True).astype(float)
    dow_dummies = pd.get_dummies(df["dow"], prefix="dow", drop_first=True).astype(float)
    X = pd.concat([df[["crit","post","crit_x_post"]], parent_dummies, dow_dummies], axis=1).astype(float)
    X = add_constant(X, has_constant='add')
    y = df["q2_mwh_clock_hour"].astype(float).values
    cluster = df["d"].astype(str).values
    try:
        result = OLS(y, X).fit()
        cov = cov_cluster(result, cluster)
        se = np.sqrt(np.diag(cov))
        cols = list(X.columns); idx = cols.index("crit_x_post")
        b3 = result.params.iloc[idx]; s = se[idx]
        p = 2*(1-norm.cdf(abs(b3/s))) if s > 0 else np.nan
        return {"label": label, "n": len(df), "n_clusters": df["d"].nunique(),
                "beta_3": b3, "se": s, "p": p}
    except Exception as e:
        return {"label": label, "n": len(df), "error": str(e)}


def print_result(r):
    if "error" in r: print(f"  {r['label']:50s}  ERROR: {r['error']}"); return
    if pd.isna(r.get("beta_3", np.nan)):
        print(f"  {r['label']:50s}  n={r['n']:6d}  (insufficient)"); return
    sig = "***" if r["p"] < 0.001 else ("**" if r["p"] < 0.01 else ("*" if r["p"] < 0.05 else ""))
    print(f"  {r['label']:50s}  n={r['n']:6d}  G={r['n_clusters']:3d}  β₃={r['beta_3']:+8.3f}  SE={r['se']:6.3f}  p={r['p']:.4f}{sig}")


def make_did_columns(df, critical_hours, post_dummy_col=None, flat_hours=None):
    """Add crit + post dummies for a given critical-hour set."""
    if flat_hours is None:
        flat_hours = FLAT_HOURS
    df = df.copy()
    df["crit"] = df["hour"].astype(int).isin(critical_hours).astype(int)
    df["flat"] = df["hour"].astype(int).isin(flat_hours).astype(int)
    if post_dummy_col is not None and post_dummy_col in df.columns:
        df["post"] = df[post_dummy_col]
    return df[(df["crit"]==1) | (df["flat"]==1)].copy()


def main():
    # primary_owner mode deduplicates joint-owned nuclear to one row per
    # unit_code; see notebooks/memos/_firm_classification_audit.md.
    units = firm_unit_panel(csv_path=str(UNITS_CSV), scheme="short",
                             mode="primary_owner")

    # Load B1 panel for B5.1, B5.2, B5.4
    print("Loading B1 panel (same-cal-month, treatment group)...")
    b1 = pd.read_parquet(B1_PANEL)
    treat_only = b1[b1["treatment_group"]=="treatment"].copy()

    results = []

    print("\n=== B5.1 — Critical-hours definition sensitivity ===")
    print("(treatment group only; same-cal-month)\n")
    # Need to rebuild panel without hour_class restriction
    pibci_summed = treat_only[["d","unit_code","parent","tech_group","zone","hour","q2_mwh_clock_hour","post","dow"]]
    # Actually B1 panel was already restricted to crit+flat per price_peak. We need a panel that contains ALL hours.
    # Quick rebuild: same-cal-month, treatment group, all hours.
    print("Re-building treatment same-cal-month panel with all hours...")
    full_pre = build_panel_full(units[units["parent"].isin(TREATMENT_PARENTS)], PRE_START, PRE_END)
    full_post = build_panel_full(units[units["parent"].isin(TREATMENT_PARENTS)], POST_START, POST_END)
    full_pre["post"] = 0; full_post["post"] = 1
    treat_full = pd.concat([full_pre, full_post], ignore_index=True)
    print(f"Treatment full-hour panel rows: {len(treat_full):,}")

    for set_name, hours in CRITICAL_HOUR_SETS.items():
        sub = make_did_columns(treat_full, hours)
        r = run_did(sub, f"B5.1_{set_name}_h{'_'.join(map(str,hours))[:30]}")
        results.append(r); print_result(r)

    print("\n=== B5.2 — Firm partition sensitivity ===")
    print("(price_peak critical hours; same-cal-month)\n")
    sub_pivot = make_did_columns(treat_full, CRITICAL_HOUR_SETS["canonical_demand_surge_vre_transition"])  # canonical
    r = run_did(sub_pivot, "B5.2a_pivotality_treatment_set"); results.append(r); print_result(r)

    # Administrative dominant set: IB/GE/GN/HC (drop EDP-PT)
    pre_adm = build_panel_full(units[units["parent"].isin(ADMIN_DOMINANT_PARENTS)], PRE_START, PRE_END)
    post_adm = build_panel_full(units[units["parent"].isin(ADMIN_DOMINANT_PARENTS)], POST_START, POST_END)
    pre_adm["post"] = 0; post_adm["post"] = 1
    admin_full = pd.concat([pre_adm, post_adm], ignore_index=True)
    sub_adm = make_did_columns(admin_full, CRITICAL_HOUR_SETS["canonical_demand_surge_vre_transition"])  # canonical
    r = run_did(sub_adm, "B5.2b_admin_IB_GE_GN_HC"); results.append(r); print_result(r)

    print("\n=== B5.3 — Window sensitivity (same-cal-month vs full window) ===")
    print()
    # Same-cal-month already done as B5.2a. Now run full-window.
    print("Building full panel 2024-2025 (treatment group)...")
    treat_full_window = build_panel_full(units[units["parent"].isin(TREATMENT_PARENTS)], FULL_START, FULL_END)
    print(f"Full panel rows: {len(treat_full_window):,}")
    # Define post = post-MTU15-DA (Oct 1 2025 onwards)
    treat_full_window["post"] = (treat_full_window["d"] >= pd.Timestamp("2025-10-01")).astype(int)
    sub_full = make_did_columns(treat_full_window, CRITICAL_HOUR_SETS["canonical_demand_surge_vre_transition"])  # canonical
    r = run_did(sub_full, "B5.3a_full_window_2024_2025"); results.append(r); print_result(r)

    print("\n=== B5.4 — Sample exclusions ===")
    print()
    # Drop EDP-PT
    sub_no_edppt = sub_pivot[sub_pivot["parent"] != "EDP-PT"].copy()
    r = run_did(sub_no_edppt, "B5.4a_drop_EDP-PT"); results.append(r); print_result(r)
    # Drop ABO2G (it's in untagged, but might be in the broader treatment via Other-fringe-CCGT)
    sub_no_abo = sub_pivot[sub_pivot["unit_code"] != "ABO2G"].copy()
    r = run_did(sub_no_abo, "B5.4b_drop_ABO2G"); results.append(r); print_result(r)

    print("\n=== B5.5 — Drop reforzada-active months (Apr-Sep 2025) ===")
    print("(Same-cal-month already excludes these months by design — this is for the full window)\n")
    sub_no_reforzada = sub_full[~sub_full["d"].between(pd.Timestamp("2025-04-28"), pd.Timestamp("2025-09-30"))].copy()
    r = run_did(sub_no_reforzada, "B5.5_full_window_drop_Apr_Sep_2025"); results.append(r); print_result(r)

    print("\n=== B5.6 — Drop DST transition days (CET ↔ CEST clock changes) ===")
    print("(Spring-forward and fall-back days have non-standard hour counts)\n")
    dst_dates = [pd.Timestamp(d) for d in DST_DAYS]
    sub_no_dst = sub_pivot[~sub_pivot["d"].isin(dst_dates)].copy()
    r = run_did(sub_no_dst, "B5.6a_samecal_drop_DST_days"); results.append(r); print_result(r)
    sub_no_dst_full = sub_full[~sub_full["d"].isin(dst_dates)].copy()
    r = run_did(sub_no_dst_full, "B5.6b_full_window_drop_DST_days"); results.append(r); print_result(r)

    print("\n=== B5.7 — DST regime separation (CEST vs CET clock semantics differ) ===")
    print("(In CEST, clock-h17 = UTC-15; in CET, clock-h17 = UTC-16. Different solar positions.)\n")
    # CEST in our windows: Oct-26-2024 PRE-fallback = Oct-1 to Oct-25-2024; POST-fallback = Oct-27-2024 to Dec-31-2024.
    # Same for 2025: Oct-1 to Oct-25-2025 = CEST, Oct-27 to Dec-31-2025 = CET.
    cest_2024 = (pd.Timestamp("2024-10-01"), pd.Timestamp("2024-10-26"))
    cest_2025 = (pd.Timestamp("2025-10-01"), pd.Timestamp("2025-10-26"))
    is_cest = ((sub_pivot["d"] >= cest_2024[0]) & (sub_pivot["d"] < cest_2024[1])) | \
              ((sub_pivot["d"] >= cest_2025[0]) & (sub_pivot["d"] < cest_2025[1]))
    sub_cest = sub_pivot[is_cest].copy()
    sub_cet  = sub_pivot[~is_cest & ~sub_pivot["d"].isin(dst_dates)].copy()
    r = run_did(sub_cest, "B5.7a_samecal_CEST_only"); results.append(r); print_result(r)
    r = run_did(sub_cet,  "B5.7b_samecal_CET_only");  results.append(r); print_result(r)

    df_results = pd.DataFrame(results)
    df_results.to_csv(OUTDIR / "B5_robustness.csv", index=False)
    print(f"\nSaved: {OUTDIR / 'B5_robustness.csv'}")


if __name__ == "__main__":
    main()
