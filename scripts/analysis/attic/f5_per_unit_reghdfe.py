# STATUS: DEAD-KEPT-AS-RECORD
# LAST-AUDIT: 2026-04-29
# RETRACTION-DATE: 2026-04-29
# RETRACTION-REASON: F5 (Allaz-Vila slope β=∂ΔQ_IDA/∂q_DA) is a mechanical accounting
#   identity, not strategic-conduct evidence. Q_actual ≈ q_DA + ΔQ_IDA implies
#   ∂ΔQ_IDA/∂q_DA = ∂Q_actual/∂q_DA − 1; since q_DA explains nearly all within-unit
#   variation in Q_actual, β is mechanically near −1 regardless of strategic conduct.
#   The HDFE absorption that 'restored' F5 was confirming the identity, not testing AV.
#   AV anchor for the thesis is now B9's firm-ISP cross-regime regression
#   (b9_replicated_isp_grain.py), which identifies via cross-regime variation, not
#   within-unit slope.
"""F5 Allaz-Vila at per-unit native granularity using Stata reghdfe.

Python builds the panel from OMIE programmes via duckdb at native
granularity (hourly pre-MTU15-IDA, 15-min after).  Exported as .dta
and analyzed with Stata's reghdfe — the gold standard for high-
dimensional fixed-effects regressions in empirical IO.

Specification (in Stata):
   reghdfe dq_ida_mwh c.q_da_mw_hour##i.regime daily_vre,        ///
       absorb(unit_code year#month hour_of_day dow)              ///
       cluster(date)

Native granularity discipline:
   pre-MTU15-IDA:           hourly observations (one obs per unit-hour)
   post-MTU15-IDA, pre-DA15: 15-min observations (q_DA hourly applied to ISP)
   post-MTU15-DA:           15-min observations (both DA and IDA per-15-min)

Output:
   data/derived/results/f5_per_unit_panel.dta       (panel for Stata)
   data/derived/results/f5_per_unit_reghdfe.csv    (Stata regression results)
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

PROJECT  = Path(__file__).resolve().parents[3]
PDBC     = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbc_all.parquet"
PIBCI    = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibci_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"
REF      = PROJECT / "data" / "external" / "omie_reference" / "lista_unidades.csv"

OUT_DIR  = PROJECT / "data" / "derived" / "results"
DTA_OUT  = OUT_DIR / "f5_per_unit_panel.dta"
DO_FILE  = PROJECT / "scripts" / "analysis" / "welfare" / "f5_per_unit_reghdfe.do"
RES_OUT  = OUT_DIR / "f5_per_unit_reghdfe.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
REGIME_CODE = {r: i for i, r in enumerate(REGIMES, start=1)}  # 1..5 numeric


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

    ref = pd.read_csv(REF, encoding='latin1')
    is_disp = ref["technology"].fillna("").str.lower().apply(
        lambda s: ("ciclo combinado" in s) or ("hidr" in s) or ("nuclear" in s))
    candidate_units = set(ref.loc[is_disp, "unit_code"].astype(str))
    print(f"Candidate dispatchable units: {len(candidate_units):,}")
    con.register("disp_set", pd.DataFrame({"unit_code": list(candidate_units)}))

    print("[1/4] Filtering to active dispatchable units (≥1000 post-IDA q_DA>0 hours)…")
    active = con.execute(f"""
        SELECT unit_code, COUNT(*) AS n_obs
        FROM '{PDBC}'
        WHERE offer_type = 1 AND assigned_power_mw > 0
          AND CAST(date AS DATE) >= DATE '2024-06-14'
          AND unit_code IN (SELECT unit_code FROM disp_set)
        GROUP BY 1 HAVING COUNT(*) >= 1000
    """).df()
    keep_units = set(active["unit_code"].astype(str).tolist())
    print(f"   active units: {len(keep_units)}")
    con.register("keep_set", pd.DataFrame({"unit_code": list(keep_units)}))

    print("[2/4] DA per-unit-hour panel…")
    da = con.execute(f"""
        SELECT date, period, unit_code, mtu_minutes, assigned_power_mw AS q_da_mw
        FROM '{PDBC}'
        WHERE offer_type = 1 AND assigned_power_mw > 0
          AND unit_code IN (SELECT unit_code FROM keep_set)
    """).df()
    da["date"] = pd.to_datetime(da["date"])
    da["da_hour"] = np.where(da["mtu_minutes"] == 60, da["period"],
                              np.ceil(da["period"] / 4.0).astype(int))
    da_hourly = (da.groupby(["unit_code", "date", "da_hour"], as_index=False)
                   .agg(q_da_mw_hour=("q_da_mw", "mean")))
    print(f"   DA panel: {len(da_hourly):,} rows")

    print("[3/4] IDA per-unit-period panel (native granularity)…")
    ida = con.execute(f"""
        SELECT date, period, unit_code, mtu_minutes,
               SUM(CASE WHEN offer_type IN (1,3) THEN  assigned_power_mw
                        WHEN offer_type IN (8,9) THEN -assigned_power_mw
                        ELSE 0 END) * mtu_minutes / 60.0 AS dq_ida_mwh
        FROM '{PIBCI}'
        WHERE assigned_power_mw IS NOT NULL
          AND unit_code IN (SELECT unit_code FROM keep_set)
        GROUP BY 1, 2, 3, 4
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    ida["ida_hour"] = np.where(ida["mtu_minutes"] == 60, ida["period"],
                                np.ceil(ida["period"] / 4.0).astype(int))
    print(f"   IDA panel: {len(ida):,} rows")

    print("[4/4] Joining and adding controls…")
    panel = ida.merge(da_hourly,
                      left_on=["unit_code", "date", "ida_hour"],
                      right_on=["unit_code", "date", "da_hour"],
                      how="inner")
    panel["regime"] = panel["date"].apply(assign_regime_code)
    panel["dow"] = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"] = panel["date"].dt.year
    panel["year_month"] = panel["year"] * 100 + panel["month"]

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
    panel = panel[panel["q_da_mw_hour"] > 0]
    print(f"   final panel: {len(panel):,} obs;  build time {time.time()-t0:.1f}s")

    # Map unit_code to integer ID (Stata-friendly)
    units = sorted(panel["unit_code"].unique())
    unit_id = {u: i for i, u in enumerate(units, start=1)}
    panel["unit_id"] = panel["unit_code"].map(unit_id).astype("int32")

    # Convert date to Stata-friendly numeric (integer days)
    panel["date_num"] = (panel["date"] - pd.Timestamp("1960-01-01")).dt.days.astype("int32")

    return panel[["dq_ida_mwh", "q_da_mw_hour", "regime", "unit_id",
                  "year_month", "ida_hour", "dow", "vre_gwh", "date_num"]].rename(
        columns={"ida_hour": "hour_of_day"}
    )


def write_dofile() -> Path:
    do_content = f"""\
*! F5 per-unit Allaz-Vila regression with HDFE absorption
*! Auto-generated by f5_per_unit_reghdfe.py
clear all
set more off
use "{DTA_OUT}", clear

label define regime_lbl 1 "pre-IDA" 2 "3-sess" 3 "ISP15-win" 4 "DA60/ID15" 5 "DA15/ID15"
label values regime regime_lbl

di as txt _newline "=== F5 per-unit Allaz-Vila with reghdfe (HDFE: unit + year-month + hour-of-day + DOW) ==="

* Spec 1: sparse FE — unit FE + year-month FE only, slope by regime
di as txt _newline "--- Spec 1: sparse (unit + year-month FE) ---"
reghdfe dq_ida_mwh c.q_da_mw_hour##i.regime, ///
    absorb(unit_id year_month) ///
    cluster(date_num) ///
    nocons

* Spec 2: augmented exogenous — add hour-of-day FE + DOW FE + VRE control
di as txt _newline "--- Spec 2: augmented (unit + year-month + hour + DOW FE + VRE) ---"
reghdfe dq_ida_mwh c.q_da_mw_hour##i.regime vre_gwh, ///
    absorb(unit_id year_month hour_of_day dow) ///
    cluster(date_num) ///
    nocons

* Save the augmented spec results to CSV via outreg2 alternative: estout
matrix b = e(b)
matrix V = e(V)
local cn : colnames b
di as txt _newline "Coefficient names:" `"`cn'"'

* Compute per-regime slope: β_pre + β_interaction_r
preserve
clear
local regimes pre_IDA three_sess ISP15_win DA60_ID15 DA15_ID15
set obs 5

gen regime_str = ""
gen beta = .
gen se = .

local i = 1
foreach r of local regimes {{
    local stata_r : word `i' of "1.regime" "2.regime" "3.regime" "4.regime" "5.regime"
    replace regime_str = "`r'" in `i'
    if `i' == 1 {{
        replace beta = _b[c.q_da_mw_hour] in `i'
        replace se   = _se[c.q_da_mw_hour] in `i'
    }}
    else {{
        lincom c.q_da_mw_hour + `i'.regime#c.q_da_mw_hour
        replace beta = r(estimate) in `i'
        replace se   = r(se) in `i'
    }}
    local i = `i' + 1
}}
gen t = beta / se
gen p = 2 * normal(-abs(t))
list, clean noobs
export delimited "{RES_OUT}", replace
restore

* Joint Wald: are interactions jointly different from zero?
di as txt _newline "--- Joint Wald: H0 all regime × q_DA interactions = 0 ---"
test 2.regime#c.q_da_mw_hour 3.regime#c.q_da_mw_hour ///
     4.regime#c.q_da_mw_hour 5.regime#c.q_da_mw_hour
"""
    DO_FILE.parent.mkdir(parents=True, exist_ok=True)
    DO_FILE.write_text(do_content)
    return DO_FILE


def main() -> None:
    print("=== Building F5 per-unit panel (Python + duckdb) ===")
    panel = build_panel()
    print(f"\nWriting Stata .dta to {DTA_OUT}…")
    DTA_OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_stata(DTA_OUT, write_index=False, version=118)
    print(f"   wrote {DTA_OUT.stat().st_size / 1e6:.1f} MB")

    print(f"\nWriting Stata .do file to {DO_FILE}…")
    write_dofile()

    print("\n=== Running reghdfe in Stata ===")
    # Stata batch mode chokes on paths with spaces.  Stage do file + dta in /tmp
    # and run from /tmp (no spaces).
    import shutil, tempfile
    tmp = Path(tempfile.mkdtemp(prefix="f5_stata_"))
    tmp_dta = tmp / "f5_panel.dta"
    tmp_do  = tmp / "f5_run.do"
    shutil.copy(DTA_OUT, tmp_dta)
    do_text = DO_FILE.read_text().replace(str(DTA_OUT), "f5_panel.dta") \
                                  .replace(str(RES_OUT), "f5_results.csv")
    tmp_do.write_text(do_text)

    cmd = ["stata-mp", "-b", "do", "f5_run.do"]
    result = subprocess.run(cmd, cwd=str(tmp), capture_output=True, text=True)
    log_path = tmp / "f5_run.log"
    if log_path.exists():
        log_text = log_path.read_text()
    else:
        log_text = result.stdout + result.stderr
    print()
    # Print the relevant portion of the log (after the F5 banner)
    if "=== F5 per-unit" in log_text:
        print(log_text.split("=== F5 per-unit", 1)[1])
    else:
        print(log_text[-4000:])
    # Copy results CSV back if produced
    tmp_csv = tmp / "f5_results.csv"
    if tmp_csv.exists():
        shutil.copy(tmp_csv, RES_OUT)
        print(f"\n   results CSV: {RES_OUT}")


if __name__ == "__main__":
    main()
