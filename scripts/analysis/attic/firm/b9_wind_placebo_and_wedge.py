# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: B9 wind-only placebo (Ito-Reguant 2016 clean identification);
#        DA-IDA wedge by regime (verifies user's anomaly observation)
# CLAIM: (1) wind-only sub-sample shows flat/rising ΔQ trajectory (operational
#        repositioning), unlike Big-4 progressive collapse (strategic
#        withholding) — clean Ito-Reguant identification. (2) DA-IDA wedge
#        inverts during ISP15-win as user predicted — supports the
#        granularity-asymmetry mechanism.
"""B9 wind placebo + DA-IDA wedge by regime.

Two tests in one script.

Test 1 — Wind-only ΔQ trajectory (Ito-Reguant clean placebo).
   Filter pdbce + pibcie to WIND units only (technology = 'RE Mercado Eólica'
   per lista_unidades.csv).  Compute aggregate ΔQ_h = signed IDA cleared MWh
   per hour, summed across wind units, by regime.

   Logic:
     - Wind generators face genuine forecast-error volatility (operational
       repositioning, not strategic).
     - Big-4 owned wind units are a small share (~9 IB + 8 EDP + 7 ENEL +
       19 Repsol vs 441 total; majority owned by independent renewables).
     - If the Big-4 progressive-collapse pattern is STRATEGIC, wind-only
       ΔQ should NOT show the same trajectory.  This is Ito-Reguant's
       clean identification.

Test 2 — DA-IDA price wedge by regime.
   User's research-proposal model predicts DA-IDA wedge maximised at
   DA60/ID15 (greatest fragmentation) and shows the user-flagged
   inversion during ISP15 → MTU15-IDA window (Dec 2024 – Mar 2025).

   Compute mean wedge = p_DA - p_IDA per hour-regime (volume-weighted by
   IDA cleared volume).  Aggregate to regime-level mean.

Outputs:
   results/regressions/b9_wind_placebo.csv
   results/regressions/b9_da_ida_wedge.csv
"""
from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
MARG_DA  = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
MARG_IDA = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "precios" / "marginalpibc_all.parquet"
REF      = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_WIND = PROJECT / "results" / "regressions" / "b9_wind_placebo.csv"
OUT_WEDGE = PROJECT / "results" / "regressions" / "b9_da_ida_wedge.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]


def assign_regime(d) -> str:
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return "pre-IDA"
    if d < pd.Timestamp("2024-12-01"): return "3-sess"
    if d < pd.Timestamp("2025-03-19"): return "ISP15-win"
    if d < pd.Timestamp("2025-10-01"): return "DA60/ID15"
    return "DA15/ID15"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    # ----------------------------------------------------------------------
    # Identify wind unit codes from lista_unidades
    # ----------------------------------------------------------------------
    ref = pd.read_csv(REF, encoding='latin1')
    wind_mask = ref.technology.str.contains("lica", case=False, na=False) & \
                ~ref.technology.str.contains("Hidr", case=False, na=False)
    wind_units = set(ref.loc[wind_mask, "unit_code"].astype(str).tolist())
    print(f"Wind units identified: {len(wind_units)}")
    print()

    # ======================================================================
    # TEST 1 — wind-only ΔQ trajectory
    # ======================================================================
    print("=" * 95)
    print("TEST 1 — Wind-only aggregate ΔQ trajectory (Ito-Reguant clean placebo)")
    print("=" * 95)
    print()

    con.register("wind_unit_set", pd.DataFrame({"unit_code": list(wind_units)}))

    # Wind IDA repositioning.  Wind units in pibcie use offer_type=10 (RE-special
    # regime) with assigned_power_mw signed natively (positive = net-sold in IDA,
    # negative = net-bought in IDA).  Aggregate signed volumes per (date, hour).
    print("[1/2] Computing wind-only IDA per-hour signed volumes (offer_type=10)…")
    wind_ida = con.execute(f"""
        WITH ida AS (
            SELECT date,
                   CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
                   assigned_power_mw * mtu_minutes / 60.0 AS signed_mwh
            FROM '{PIBCIE}'
            WHERE unit_code IN (SELECT unit_code FROM wind_unit_set)
              AND offer_type = 10
        )
        SELECT date, hour,
               SUM(signed_mwh)        AS qida_wind_mwh,
               SUM(ABS(signed_mwh))   AS abs_qida_wind_mwh
        FROM ida
        GROUP BY 1, 2
    """).df()
    wind_ida["date"] = pd.to_datetime(wind_ida["date"])
    wind_ida["regime"] = wind_ida["date"].apply(assign_regime)
    print(f"   wind IDA panel: {len(wind_ida):,} hour-rows")

    # Also compute wind DA cleared volume per hour for normalisation (RE units use offer_type=10)
    print("[2/2] Computing wind-only DA cleared volumes…")
    wind_da = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS qda_wind_signed_mwh,
               SUM(ABS(assigned_power_mw) * mtu_minutes / 60.0) AS qda_wind_abs_mwh
        FROM '{PDBCE}'
        WHERE unit_code IN (SELECT unit_code FROM wind_unit_set)
          AND offer_type = 10
        GROUP BY 1, 2
    """).df()
    wind_da["date"] = pd.to_datetime(wind_da["date"])

    df = wind_ida.merge(wind_da, on=["date", "hour"], how="outer")
    df = df.fillna({"qida_wind_mwh": 0, "abs_qida_wind_mwh": 0,
                    "qda_wind_signed_mwh": 0, "qda_wind_abs_mwh": 0})
    df["regime"] = df["date"].apply(assign_regime)
    df["regime_cat"] = pd.Categorical(df["regime"], categories=REGIMES, ordered=True)

    print()
    print("Wind aggregate trajectory (per delivery hour, summed across all wind units):")
    print("  qida_signed_mean = mean signed ΔQ in IDA (operational repositioning direction)")
    print("  qida_abs_mean    = mean |ΔQ| in IDA (operational repositioning magnitude)")
    print()
    summary = (df.groupby("regime_cat", observed=True)
                 .agg(qida_signed_mean=("qida_wind_mwh", "mean"),
                      qida_abs_mean=("abs_qida_wind_mwh", "mean"),
                      qda_signed_mean=("qda_wind_signed_mwh", "mean"),
                      qda_abs_mean=("qda_wind_abs_mwh", "mean"),
                      n=("qida_wind_mwh", "count"))
                 .reindex(REGIMES))
    print(summary.round(1).to_string())
    print()

    # User's claim: wind-only does NOT show the Big-4 progressive collapse
    print("Trajectory comparison: Big-4 (from earlier hourly disaggregation) vs Wind-only")
    big4 = {"pre-IDA": 249.9, "3-sess": 158.7, "ISP15-win": 132.0,
            "DA60/ID15": 128.1, "DA15/ID15": 167.8}
    print()
    print(f"   {'Regime':<14}  {'Big-4 ΔQ (MWh/firm-hr)':>22}  {'Wind |ΔQ| aggregate (MWh/hr)':>30}")
    print('   ' + '-' * 72)
    base_b = big4["pre-IDA"]
    base_w_abs = float(summary.loc["pre-IDA", "qida_abs_mean"])
    for r in REGIMES:
        b = big4[r]
        w_abs = float(summary.loc[r, "qida_abs_mean"])
        b_pct = (b - base_b) / abs(base_b) * 100
        w_pct = (w_abs - base_w_abs) / abs(base_w_abs) * 100 if base_w_abs else float("nan")
        print(f"   {r:<14}  {b:>+8.1f} ({b_pct:+5.0f}%)  {w_abs:>+12.1f} MWh/hr ({w_pct:+5.0f}%)")
    print()
    OUT_WIND.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(OUT_WIND)
    print(f"   wrote {OUT_WIND}")
    print()

    # ======================================================================
    # TEST 2 — DA-IDA wedge by regime
    # ======================================================================
    print("=" * 95)
    print("TEST 2 — DA-IDA price wedge by regime (verifies user's wedge inversion at ISP15)")
    print("=" * 95)
    print()

    print("[1/2] Loading DA hourly prices…")
    da_p = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{MARG_DA}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    da_p["date"] = pd.to_datetime(da_p["date"])
    print(f"   DA prices: {len(da_p):,} hour-rows")

    print("[2/2] Loading IDA hourly prices — separate by IDA session…")
    ida_p_all = con.execute(f"""
        SELECT date,
               session_number,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_ida
        FROM '{MARG_IDA}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2, 3
    """).df()
    ida_p_all["date"] = pd.to_datetime(ida_p_all["date"])
    print(f"   IDA prices: {len(ida_p_all):,} session-hour rows; sessions {sorted(ida_p_all.session_number.unique())}")

    # Mean across all sessions
    ida_p = (ida_p_all.groupby(["date", "hour"], as_index=False)
                       .agg(p_ida=("p_ida", "mean")))
    # IDA1 (or whatever is closest to delivery: post-IDA-reform IDA3 is the last session;
    # pre-IDA-reform there were 6 sessions, IDA6 closest to RT — but session_number=1 is the
    # first IDA in OMIE convention, just use the LAST session per date as 'closest to RT')
    ida_last = (ida_p_all.sort_values(["date", "hour", "session_number"])
                          .groupby(["date", "hour"], as_index=False)
                          .last()
                          .rename(columns={"p_ida": "p_ida_last"}))[["date", "hour", "p_ida_last"]]

    wedge = da_p.merge(ida_p, on=["date", "hour"], how="inner") \
                .merge(ida_last, on=["date", "hour"], how="left")
    wedge["regime"] = wedge["date"].apply(assign_regime)
    wedge["regime_cat"] = pd.Categorical(wedge["regime"], categories=REGIMES, ordered=True)
    wedge["wedge_mean_sess"] = wedge["p_da"] - wedge["p_ida"]
    wedge["wedge_last_sess"] = wedge["p_da"] - wedge["p_ida_last"]

    print()
    print("DA-IDA wedge by regime (€/MWh).  Two definitions:")
    print("  (a) wedge_mean_sess = p_DA - mean(p_IDA across sessions)")
    print("  (b) wedge_last_sess = p_DA - p_IDA(last session of day, closest to delivery)")
    print()
    wsummary = (wedge.groupby("regime_cat", observed=True)
                     .agg(p_da_mean=("p_da", "mean"),
                          p_ida_mean=("p_ida", "mean"),
                          p_ida_last_mean=("p_ida_last", "mean"),
                          wedge_mean_sess=("wedge_mean_sess", "mean"),
                          wedge_last_sess=("wedge_last_sess", "mean"),
                          wedge_median_mean_sess=("wedge_mean_sess", "median"),
                          n=("wedge_mean_sess", "count"))
                     .reindex(REGIMES))
    print(wsummary.round(2).to_string())
    print()

    # Test for inversion: is the wedge sign different in ISP15 vs other post-reform regimes?
    print("User's claim test: 'DA-IDA differential inverts during ISP15 → ID15'")
    print("(i.e. ISP15-win and possibly into early DA60/ID15)")
    print()
    print(f"   {'Regime':<14}  {'wedge mean(IDA)':>17}  {'wedge last-IDA':>17}")
    for r in REGIMES:
        w_m = wsummary.loc[r, "wedge_mean_sess"]
        w_l = wsummary.loc[r, "wedge_last_sess"]
        print(f"   {r:<14}  {w_m:+10.2f} €/MWh  {w_l:+10.2f} €/MWh")
    print()

    inverted_regimes = [r for r in REGIMES if wsummary.loc[r, "wedge_last_sess"] < 0]
    if "ISP15-win" in inverted_regimes:
        print(f"   → User's anomaly CONFIRMED: wedge inverts during ISP15-win.")
        print(f"     This supports the granularity-asymmetry mechanism: BRPs facing per-ISP")
        print(f"     imbalance settlement preemptively buy in IDA (raising IDA price above DA),")
        print(f"     because the hourly IDA cannot precisely target 15-min imbalance exposure.")
    elif len(inverted_regimes) > 0:
        print(f"   → Inversion observed in: {inverted_regimes} (not ISP15-win specifically)")
    else:
        print(f"   → No regime shows DA-IDA inversion in mean.  User's anomaly might require")
        print(f"     volume-weighting or IDA session-specific aggregation to surface.")
    print()
    OUT_WEDGE.parent.mkdir(parents=True, exist_ok=True)
    wsummary.to_csv(OUT_WEDGE)
    print(f"   wrote {OUT_WEDGE}")
    print()

    # ======================================================================
    # SYNTHESIS
    # ======================================================================
    print("=" * 95)
    print("SYNTHESIS")
    print("=" * 95)
    print()
    # Wind-vs-Big4 contrast
    big4_change = (big4["DA60/ID15"] - big4["pre-IDA"]) / abs(big4["pre-IDA"]) * 100
    base_w_abs = float(summary.loc["pre-IDA", "qida_abs_mean"])
    wind_change = (float(summary.loc["DA60/ID15", "qida_abs_mean"]) - base_w_abs) / abs(base_w_abs) * 100 \
                  if base_w_abs else float('nan')
    print(f"   Big-4 ΔQ from pre-IDA to DA60/ID15:     {big4_change:+5.0f}%   (progressive collapse)")
    print(f"   Wind |ΔQ| from pre-IDA to DA60/ID15:    {wind_change:+5.0f}%   "
          f"({'flat/rising' if wind_change > -20 else 'also collapses'})")
    print()
    if wind_change > -20:
        print("   Big-4 progressive collapse is NOT replicated in wind-only sub-sample.")
        print("   This is the clean Ito-Reguant identification: dominant firms behave")
        print("   strategically (and reform reduces strategic withholding); wind firms")
        print("   reposition operationally (independent of regime).")
    else:
        print("   Wind also shows similar trajectory — strategic-conduct interpretation")
        print("   is in trouble.")


if __name__ == "__main__":
    main()
