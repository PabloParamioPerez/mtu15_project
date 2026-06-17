# STATUS: ALIVE
# LAST-AUDIT: 2026-05-24
# FEEDS: advisor_memo.tex headline reading + robustness; the
#        descriptive_facts.tex mandatory cross-regime same-calendar check.
# CLAIM: Two complementary OVB-absorption strategies for the DA15 DiD,
#        following the recommendation in the previous chat exchange.
#
#        (a) Same-calendar-month DA15: Pre = Oct-Dec 2024, Post = Oct-Dec 2025.
#            Holds calendar (and therefore the renewable seasonal cycle, the
#            heating-cooling demand profile, and the day-length structure) fixed
#            by construction. The pre arm is post-IDA + ISP15 active but
#            pre-MTU15-DA AND pre-blackout (reforzada absent); the post arm is
#            post-MTU15-DA AND post-blackout (reforzada active). The DiD theta
#            therefore bundles MTU15-DA with reforzada -- consistent with the
#            CLAUDE.md cross-regime caveat. Useful but not clean for the
#            reforzada channel.
#
#        (b) Weather-controlled DiD on the original Jul-Sep 2025 / Oct-Dec 2025
#            windows: adds daily ENTSO-E A75 wind (B19) and solar (B16) actual
#            output (in GWh/day, demand-weighted to the regression window) as
#            additive controls. Following Brown & Reguant (2026-04 WP) we would
#            ideally use MERRA-2 weather as IV, but for our DiD identification
#            the treatment is already exogenous (reform date), so weather is a
#            CONTROL not an IV. Adding A75 actuals as covariates absorbs the
#            day-to-day renewable variation that the Fourier-SA cannot absorb
#            beyond its smooth seasonal cycle.
#
#        (c) Belt-and-braces: same-calendar + weather controls.
#
# Outcomes: DA clearing price; pump-storage cleared MW; CCGT cleared MW.
#
# OUT: results/regressions/bid/mtu15_critical_flat/same_cal_weather_did.csv

from pathlib import Path
import sys

import duckdb
import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[3]
OUT = REPO / "results/regressions/bid/mtu15_critical_flat/same_cal_weather_did.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

MPDBC = REPO / "data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet"
PDBC = REPO / "data/processed/omie/mercado_diario/programas/pdbc_all.parquet"
UNITS = REPO / "data/external/omie_reference/lista_unidades.csv"
WSACTUAL = REPO / "data/processed/entsoe/generation/wind_solar_actual_all.parquet"

CRITICAL = {5, 6, 7, 8, 16, 17, 18, 19, 20, 21, 22}
FLAT = {1, 2, 3}
DA15_REFORM = pd.Timestamp("2025-10-01")

# ============ Window definitions ============
# Baseline DA15 (used in the main memo): Pre Jul-Sep 2025, Post Oct-Dec 2025.
BASE_PRE_LO = pd.Timestamp("2025-07-01"); BASE_PRE_HI = pd.Timestamp("2025-09-30")
BASE_POST_LO = pd.Timestamp("2025-10-01"); BASE_POST_HI = pd.Timestamp("2025-12-31")

# Same-calendar-month DA15: Pre Oct-Dec 2024 (pre-MTU15-DA, pre-blackout),
# Post Oct-Dec 2025 (post-MTU15-DA, post-blackout).
SAMECAL_PRE_LO = pd.Timestamp("2024-10-01"); SAMECAL_PRE_HI = pd.Timestamp("2024-12-31")
SAMECAL_POST_LO = pd.Timestamp("2025-10-01"); SAMECAL_POST_HI = pd.Timestamp("2025-12-31")
SAMECAL_T_REF = pd.Timestamp("2025-01-01")  # treatment indicator = year >= 2025


def hour_class(h):
    if h in CRITICAL: return "Critical"
    if h in FLAT: return "Flat"
    return "Other"


def clustered_ols(y, X, cluster):
    XtX_inv = np.linalg.inv(X.T @ X)
    beta = XtX_inv @ (X.T @ y)
    e = y - X @ beta
    meat = np.zeros((X.shape[1], X.shape[1]))
    for g in np.unique(cluster):
        m = cluster == g; s = X[m].T @ e[m]; meat += np.outer(s, s)
    G = len(np.unique(cluster)); n, k = X.shape
    adj = (G / (G - 1)) * ((n - 1) / (n - k))
    V = adj * (XtX_inv @ meat @ XtX_inv)
    return beta, np.sqrt(np.diag(V))


def build_price_panel(lo, hi):
    con = duckdb.connect()
    sql = f"""SELECT CAST(date AS DATE) d, period, price_es_eur_mwh p_clear,
                     COALESCE(mtu_minutes, 60) mtu
              FROM '{MPDBC}'
              WHERE date BETWEEN '{lo.date()}' AND '{hi.date()}'
                AND price_es_eur_mwh IS NOT NULL"""
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class)
    return df


def build_tech_cleared_mw(tech_substring, lo, hi):
    units = pd.read_csv(UNITS)
    units = units[units["technology"].str.lower().str.contains(tech_substring, na=False)][
        ["unit_code"]].drop_duplicates()
    con = duckdb.connect()
    con.register("u", units)
    sql = f"""
    SELECT CAST(p.date AS DATE) d, p.period,
           SUM(CASE WHEN p.assigned_power_mw > 0 THEN p.assigned_power_mw ELSE 0 END) AS gen,
           COALESCE(p.mtu_minutes, 60) mtu
    FROM '{PDBC}' p JOIN u ON p.unit_code = u.unit_code
    WHERE p.date BETWEEN '{lo.date()}' AND '{hi.date()}'
    GROUP BY 1, p.period, mtu
    """
    df = con.execute(sql).fetchdf()
    df["d"] = pd.to_datetime(df["d"])
    df["clock_hour"] = np.where(df["mtu"] == 60, df["period"] - 1,
                                ((df["period"] - 1) // 4).astype(int))
    df["hour_class"] = df["clock_hour"].map(hour_class)
    return df


def build_daily_weather(lo, hi):
    """Daily-aggregate ENTSO-E A75 wind (B19) and solar (B16) actuals in GWh."""
    con = duckdb.connect()
    sql = f"""
    SELECT CAST(isp_start_utc AS DATE) d, psr_type,
           SUM(quantity_mw * (mtu_minutes / 60.0)) / 1000.0 AS gwh
    FROM '{WSACTUAL}'
    WHERE isp_start_utc >= '{lo.date()}' AND isp_start_utc < '{(hi + pd.Timedelta(days=1)).date()}'
      AND psr_type IN ('B16', 'B19')
    GROUP BY 1, 2
    """
    df = con.execute(sql).fetchdf()
    w = df.pivot_table(index="d", columns="psr_type", values="gwh", fill_value=0).reset_index()
    w.columns.name = None
    w = w.rename(columns={"B19": "wind_gwh", "B16": "solar_gwh"})
    w["d"] = pd.to_datetime(w["d"])
    return w[["d", "wind_gwh", "solar_gwh"]]


def run_did(panel, pre_lo, pre_hi, post_lo, post_hi, T_ref, outcome,
            extra_controls=None):
    """outcome = a + crit + theta*post*crit + (crit x weather controls) + delta_d + eps.

    NOTE: weather variables are at the DAY level only and would be perfectly
    collinear with date FE if entered as main effects. They are entered as
    crit-interactions (zero for flat hours, weather value for critical hours)
    so they vary within date and capture the (crit-flat) differential weather
    response without colliding with the date FE."""
    p = panel.copy()
    p["d"] = pd.to_datetime(p["d"])
    in_pre = (p["d"] >= pre_lo) & (p["d"] <= pre_hi)
    in_post = (p["d"] >= post_lo) & (p["d"] <= post_hi)
    p = p[(in_pre | in_post) & p["hour_class"].isin(["Critical", "Flat"])].copy()
    p = p.dropna(subset=[outcome])
    p["post"] = (p["d"] >= T_ref).astype(int)
    p["crit"] = (p["hour_class"] == "Critical").astype(int)
    p["post_crit"] = p["post"] * p["crit"]
    dd = pd.get_dummies(p["d"].astype(str), prefix="d", drop_first=True).astype(float)
    cols = [p["crit"].values.astype(float), p["post_crit"].values.astype(float)]
    if extra_controls:
        for c in extra_controls:
            if c in p.columns:
                cols.append((p["crit"].values * p[c].values).astype(float))
    X = np.column_stack([np.ones(len(p))] + cols + [dd.values])
    y = p[outcome].values.astype(float)
    beta, se = clustered_ols(y, X, p["d"].astype(str).values)
    return {"n": len(p), "DiD": beta[2], "se": se[2], "t": beta[2] / se[2]}


def main():
    rows = []

    # Build weather panel covering both the baseline and the same-cal windows.
    print("Building daily weather panel (ENTSO-E A75 wind=B19 + solar=B16)...")
    wx = build_daily_weather(SAMECAL_PRE_LO, BASE_POST_HI)
    print(f"  {len(wx):,} daily weather rows")

    # ============ (1) DA15 Clearing Price ============
    print("\n=== (1) DA15 clearing price ===")
    print("  baseline window: Jul-Sep 2025 vs Oct-Dec 2025")
    print("  same-cal window: Oct-Dec 2024 vs Oct-Dec 2025")
    # Baseline window (need only base window for baseline + weather specs)
    panel_base = build_price_panel(BASE_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    # Same-cal window
    panel_samecal = build_price_panel(SAMECAL_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    panel_samecal = panel_samecal[(panel_samecal["d"].dt.month.isin([10, 11, 12]))].copy()
    specs = [
        ("baseline (date FE)", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, None),
        ("baseline + weather x crit controls", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, ["wind_gwh", "solar_gwh"]),
        ("same-cal (Oct-Dec 24 vs 25)", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, None),
        ("same-cal + weather x crit controls", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, ["wind_gwh", "solar_gwh"]),
    ]
    for label, p, plo, phi, polo, pohi, t_ref, ctrls in specs:
        r = run_did(p, plo, phi, polo, pohi, t_ref, "p_clear", extra_controls=ctrls)
        print(f"  {label:38s}  DiD={r['DiD']:+8.2f}  se={r['se']:6.2f}  t={r['t']:+6.2f}  n={r['n']:,}")
        rows.append({"outcome": "p_clear", "spec": label, **r})

    # ============ (2) DA15 Pump-storage cleared MW ============
    print("\n=== (2) DA15 Pump-storage cleared MW ===")
    panel_base = build_tech_cleared_mw("bombeo", BASE_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    panel_samecal = build_tech_cleared_mw("bombeo", SAMECAL_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    panel_samecal = panel_samecal[panel_samecal["d"].dt.month.isin([10, 11, 12])].copy()
    specs = [
        ("baseline (date FE)", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, None),
        ("baseline + weather x crit controls", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, ["wind_gwh", "solar_gwh"]),
        ("same-cal (Oct-Dec 24 vs 25)", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, None),
        ("same-cal + weather x crit controls", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, ["wind_gwh", "solar_gwh"]),
    ]
    for label, p, plo, phi, polo, pohi, t_ref, ctrls in specs:
        r = run_did(p, plo, phi, polo, pohi, t_ref, "gen", extra_controls=ctrls)
        print(f"  {label:38s}  DiD={r['DiD']:+9.1f}  se={r['se']:7.1f}  t={r['t']:+6.2f}  n={r['n']:,}")
        rows.append({"outcome": "pump_gen_mw", "spec": label, **r})

    # ============ (3) DA15 CCGT cleared MW ============
    print("\n=== (3) DA15 CCGT cleared MW ===")
    panel_base = build_tech_cleared_mw("ciclo combinado", BASE_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    panel_samecal = build_tech_cleared_mw("ciclo combinado", SAMECAL_PRE_LO, BASE_POST_HI).merge(wx, on="d", how="left")
    panel_samecal = panel_samecal[panel_samecal["d"].dt.month.isin([10, 11, 12])].copy()
    specs = [
        ("baseline (date FE)", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, None),
        ("baseline + weather x crit controls", panel_base, BASE_PRE_LO, BASE_PRE_HI, BASE_POST_LO, BASE_POST_HI, DA15_REFORM, ["wind_gwh", "solar_gwh"]),
        ("same-cal (Oct-Dec 24 vs 25)", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, None),
        ("same-cal + weather x crit controls", panel_samecal, SAMECAL_PRE_LO, SAMECAL_PRE_HI, SAMECAL_POST_LO, SAMECAL_POST_HI, SAMECAL_T_REF, ["wind_gwh", "solar_gwh"]),
    ]
    for label, p, plo, phi, polo, pohi, t_ref, ctrls in specs:
        r = run_did(p, plo, phi, polo, pohi, t_ref, "gen", extra_controls=ctrls)
        print(f"  {label:38s}  DiD={r['DiD']:+9.1f}  se={r['se']:7.1f}  t={r['t']:+6.2f}  n={r['n']:,}")
        rows.append({"outcome": "ccgt_gen_mw", "spec": label, **r})

    pd.DataFrame(rows).to_csv(OUT, index=False)
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
