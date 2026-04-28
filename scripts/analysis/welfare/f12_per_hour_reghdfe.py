# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: F12 (IB pumped-storage arbitrage at hourly with year FE)
# CLAIM: F12 was wounded because annual aggregation conflated time-trend
#        (solar capacity growth) with regime effects. Per-hour panel with
#        unit FE + year FE + cal-month FE absorbed via reghdfe isolates
#        the regime-driven component on top of the trend.
"""F12 IB pumped-storage arbitrage at hourly granularity using Stata reghdfe.

The original F12 wounding (2026-04-27, `f12_seasonality_audit.py`) showed
that annual arb rate grew ~25× across 2018-2024 BEFORE any MTU15 reform
— attribution was largely solar-cannibalisation deepening the duck-curve
spread, not reform mechanics.

Per-hour panel with year FE separates "what would have happened on the
trend" from "what the reform added on top".  HDFE absorption (unit FE +
year-month FE + hour-of-day FE + DOW FE) keeps the design tractable.

Specification:
   arb_eur ~ regime + vre_gwh,
       absorb(unit_id year_month hour_of_day dow)
       cluster(date_num)

Where arb_eur per (unit, hour) is:
   - generation hour (MUEL, DUER, SIL, TAJO, TAMEGA): +q_gen × p_DA
   - pumping hour    (MUEB, DUEB, SILB, TAJB, TAMEGAB): -|q_pump| × p_DA

For the cleanest IB-strategic-pump-arb test, we restrict to MUEL/MUEB
(pure pumped-storage cycle, no river-inflow confound).
"""
from __future__ import annotations

import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PDBC     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
MARG     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "precios" / "marginalpdbc_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"

OUT_DIR  = PROJECT / "data" / "derived" / "results"
DTA_OUT  = OUT_DIR / "f12_per_hour_panel.dta"
DO_FILE  = PROJECT / "scripts" / "analysis" / "welfare" / "f12_per_hour_reghdfe.do"
RES_OUT  = OUT_DIR / "f12_per_hour_reghdfe.csv"

# IB pumped-storage units.  MUEL = generation pure-pumped; MUEB = pump.
# Keep them as the cleanest test (no river-inflow confound).
IB_PUMPSTORAGE_UNITS = {"MUEL": +1, "MUEB": -1}  # +1 generation, -1 pumping


def assign_regime_code(d):
    d = pd.Timestamp(d)
    if d < pd.Timestamp("2024-06-14"): return 1
    if d < pd.Timestamp("2024-12-01"): return 2
    if d < pd.Timestamp("2025-03-19"): return 3
    if d < pd.Timestamp("2025-10-01"): return 4
    return 5


def build_panel() -> pd.DataFrame:
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=4")

    print("[1/3] Loading MUEL+MUEB cleared programs (per-unit-period)…")
    units_str = ", ".join(f"'{u}'" for u in IB_PUMPSTORAGE_UNITS)
    da = con.execute(f"""
        SELECT date,
               period,
               unit_code,
               mtu_minutes,
               offer_type,
               assigned_power_mw
        FROM '{PDBC}'
        WHERE unit_code IN ({units_str})
          AND assigned_power_mw IS NOT NULL
    """).df()
    da["date"] = pd.to_datetime(da["date"])
    print(f"   IB pump-storage panel: {len(da):,} rows; "
          f"unit_code counts: {da.unit_code.value_counts().to_dict()}; "
          f"offer_type: {da.offer_type.value_counts().to_dict()}")

    # Convert to hour: per-period MWh = MW × mtu_min/60.  Then sum to hour.
    # Gen units (offer_type=1) and pump units (offer_type=3) — let's check the
    # actual offer types per unit.
    print(f"   per unit_code, top offer_types:")
    print(da.groupby("unit_code")["offer_type"].value_counts().head(10))

    # Determine sign: MUEL is gen (positive); MUEB is pump (negative cleared MW)
    # In OMIE, pumping is offer_type=3 with negative assigned_power_mw, OR pumping
    # may use offer_type=8 (buy) — check
    da["sign"] = da["unit_code"].map(IB_PUMPSTORAGE_UNITS)
    da["mwh_per_period"] = da["assigned_power_mw"].abs() * da["mtu_minutes"] / 60.0 * da["sign"]
    da["hour"] = np.where(da["mtu_minutes"] == 60, da["period"],
                           np.ceil(da["period"] / 4.0).astype(int))

    hourly = (da.groupby(["date", "hour", "unit_code"], as_index=False)
                .agg(net_mwh=("mwh_per_period", "sum")))
    print(f"   hourly panel: {len(hourly):,} rows")

    # Pivot to wide: gen vs pump per hour
    gen = hourly[hourly.unit_code == "MUEL"].rename(columns={"net_mwh": "gen_mwh"})[["date","hour","gen_mwh"]]
    pump = hourly[hourly.unit_code == "MUEB"].rename(columns={"net_mwh": "pump_mwh"})[["date","hour","pump_mwh"]]

    # Outer merge: every (date, hour) where either unit was active
    panel = gen.merge(pump, on=["date","hour"], how="outer").fillna(0)
    print(f"   gen+pump merged: {len(panel):,} unique (date, hour) cells")

    # Add DA price
    print("[2/3] Loading hourly DA prices…")
    p = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               AVG(price_es_eur_mwh) AS p_da
        FROM '{MARG}'
        WHERE price_es_eur_mwh IS NOT NULL
        GROUP BY 1, 2
    """).df()
    p["date"] = pd.to_datetime(p["date"])
    panel = panel.merge(p, on=["date","hour"], how="inner")

    # Compute hourly arb: gen_mwh and pump_mwh have opposite signs
    # gen_mwh > 0 → revenue at p_da; pump_mwh < 0 → cost = -pump_mwh × p_da (consumes)
    panel["arb_eur"] = (panel["gen_mwh"] + panel["pump_mwh"]) * panel["p_da"]
    print(f"   joined panel: {len(panel):,} hour-cells; "
          f"mean arb = €{panel.arb_eur.mean():.1f}/hour;  "
          f"median = €{panel.arb_eur.median():.1f}/hour")

    panel["regime"] = panel["date"].apply(assign_regime_code)
    panel["year"] = panel["date"].dt.year
    panel["month"] = panel["date"].dt.month
    panel["dow"] = panel["date"].dt.dayofweek
    panel["year_month"] = panel["year"] * 100 + panel["month"]
    panel["date_num"] = (panel["date"] - pd.Timestamp("1960-01-01")).dt.days.astype("int32")

    # Daily VRE
    print("[3/3] Adding daily VRE control…")
    vre = con.execute(f"""
        SELECT CAST(isp_start_utc AS DATE) AS date,
               SUM(quantity_mw * mtu_minutes / 60.0) / 1000.0 AS vre_gwh
        FROM '{ACTUAL}'
        WHERE psr_type IN ('B16','B18','B19')
        GROUP BY 1
    """).df()
    vre["date"] = pd.to_datetime(vre["date"])
    panel = panel.merge(vre, on="date", how="left")
    panel["vre_gwh"] = panel["vre_gwh"].fillna(panel["vre_gwh"].mean())

    print(f"   final panel: {len(panel):,} obs;  build time {time.time()-t0:.1f}s")

    return panel[["arb_eur", "regime", "year_month", "year", "month", "hour", "dow",
                  "vre_gwh", "p_da", "gen_mwh", "pump_mwh", "date_num"]]


def write_dofile() -> Path:
    do_content = f"""\
*! F12 hourly arb regression with HDFE
clear all
set more off
use "f12_panel.dta", clear

label define regime_lbl 1 "pre-IDA" 2 "3-sess" 3 "ISP15-win" 4 "DA60/ID15" 5 "DA15/ID15"
label values regime regime_lbl

di as txt _newline "=== F12 IB pumped-storage hourly arb (MUEL+MUEB) — reghdfe HDFE ==="
di as txt "Note: year_month FE would absorb the post-MTU15 regimes mechanically"
di as txt "(they coincide with calendar months).  Use YEAR FE + cal-month FE separately."

* Spec 1: sparse — regime + year FE only (year FE absorbs the long trend;
* within-year variation across regimes is the identifying variation)
di as txt _newline "--- Spec 1: sparse (year FE only) ---"
reghdfe arb_eur i.regime, ///
    absorb(year) ///
    cluster(date_num)

* Spec 2: augmented — + cal-month FE + hour FE + DOW FE + VRE
di as txt _newline "--- Spec 2: augmented (year + cal-month + hour + DOW FE + VRE) ---"
reghdfe arb_eur i.regime vre_gwh, ///
    absorb(year month hour dow) ///
    cluster(date_num)

* Save augmented spec results
preserve
clear
local regimes pre_IDA three_sess ISP15_win DA60_ID15 DA15_ID15
set obs 5
gen regime_str = ""
gen beta = .
gen se = .
local i = 1
foreach r of local regimes {{
    replace regime_str = "`r'" in `i'
    if `i' == 1 {{
        replace beta = 0 in `i'
        replace se = 0 in `i'
    }}
    else {{
        replace beta = _b[`i'.regime] in `i'
        replace se = _se[`i'.regime] in `i'
    }}
    local i = `i' + 1
}}
gen t = beta / se
list, clean noobs
export delimited "f12_results.csv", replace
restore

* Joint Wald
di as txt _newline "--- Joint Wald: H0 all regime != 0 (vs pre-IDA) ---"
test 2.regime 3.regime 4.regime 5.regime
"""
    DO_FILE.parent.mkdir(parents=True, exist_ok=True)
    DO_FILE.write_text(do_content)
    return DO_FILE


def main() -> None:
    print("=== Building F12 hourly arb panel ===")
    panel = build_panel()
    print(f"\nWriting Stata .dta to {DTA_OUT}…")
    DTA_OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_stata(DTA_OUT, write_index=False, version=118)
    print(f"   wrote {DTA_OUT.stat().st_size / 1e6:.1f} MB")

    print(f"\nWriting .do to {DO_FILE}…")
    write_dofile()

    print("\n=== Running reghdfe in Stata (via /tmp staging) ===")
    tmp = Path(tempfile.mkdtemp(prefix="f12_stata_"))
    shutil.copy(DTA_OUT, tmp / "f12_panel.dta")
    do_text = DO_FILE.read_text()
    (tmp / "f12_run.do").write_text(do_text)

    cmd = ["stata-mp", "-b", "do", "f12_run.do"]
    result = subprocess.run(cmd, cwd=str(tmp), capture_output=True, text=True)
    log_path = tmp / "f12_run.log"
    log_text = log_path.read_text() if log_path.exists() else (result.stdout + result.stderr)
    print()
    if "F12 IB pumped-storage" in log_text:
        print(log_text.split("F12 IB pumped-storage", 1)[1])
    else:
        print(log_text[-4000:])

    tmp_csv = tmp / "f12_results.csv"
    if tmp_csv.exists():
        shutil.copy(tmp_csv, RES_OUT)
        print(f"\n   wrote {RES_OUT}")


if __name__ == "__main__":
    main()
