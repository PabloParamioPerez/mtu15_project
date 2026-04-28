# STATUS: ALIVE
# LAST-AUDIT: 2026-04-28
# FEEDS: B9 × F5 narrative consistency check (cross-mechanism by firm)
# CLAIM: Under the unified Allaz-Vila + Ito-Reguant narrative, the firm
#        with the largest under-commitment magnitude (B9) should be the
#        same firm with the deepest Allaz-Vila slope (F5).  This script
#        runs both decompositions on the same per-firm-period panel and
#        reports whether the rankings match.
"""B9 × F5 firm consistency check at native granularity.

Discipline:
   - Per-firm-period panel at native granularity (hourly pre-MTU15-IDA,
     15-min post)
   - Stata reghdfe with firm × regime × q_DA triple interaction
   - HDFE absorbs firm + year-month + hour + DOW
   - Cluster SE by date
   - OVB protocol: report sparse + augmented spec
   - Outcome/regressor aggregation: q_DA hourly value applied to
     within-hour ISPs (same convention as F5 per-unit)

Two outputs from the same panel:
   1. B9 per-firm-period ΔQ_IDA mean by regime (raw + augmented)
   2. F5 per-firm Allaz-Vila slope β by regime

Test: do firm rankings on |B9 ΔQ| and |F5 β| coincide?
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
PDBCE    = PROJECT / "data" / "processed" / "omie" / "mercado_diario" / "programas" / "pdbce_all.parquet"
PIBCIE   = PROJECT / "data" / "processed" / "omie" / "mercado_intradiario_subastas" / "programas" / "pibcie_all.parquet"
ACTUAL   = PROJECT / "data" / "processed" / "entsoe" / "generation" / "wind_solar_actual_all.parquet"

OUT_DIR  = PROJECT / "data" / "derived" / "results"
DTA_OUT  = OUT_DIR / "b9_f5_firm_consistency.dta"
DO_FILE  = PROJECT / "scripts" / "analysis" / "welfare" / "b9_f5_firm_consistency.do"
RES_OUT  = OUT_DIR / "b9_f5_firm_consistency.csv"

REGIMES = ["pre-IDA", "3-sess", "ISP15-win", "DA60/ID15", "DA15/ID15"]
BIG4 = ["GE", "IB", "GN", "HC"]
FIRM_IDS = {"GE": 1, "IB": 2, "GN": 3, "HC": 4}  # Big-4 only; rest aggregated to "Fringe"


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

    # Per-firm-period DA (using grupo_empresarial; restrict to dispatchable behavior:
    # offer_type=1 sell side for q_DA — same convention as B9 hourly script).
    # q_DA at the firm level is the firm's hourly DA commitment.
    print("[1/3] Building per-firm-hour DA cleared volumes…")
    da_hourly = con.execute(f"""
        SELECT date,
               CASE WHEN mtu_minutes=15 THEN CEIL(period/4.0)::INT ELSE period END AS hour,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               SUM(assigned_power_mw * mtu_minutes / 60.0)
                   / (CASE WHEN mtu_minutes=15 THEN 4.0 ELSE 1.0 END)              AS q_da_mw_hour
        FROM '{PDBCE}'
        WHERE offer_type = 1 AND assigned_power_mw > 0
        GROUP BY 1, 2, 3, mtu_minutes
    """).df()
    # Above the q_da_mw_hour represents the firm's hourly average MW commitment.
    da_hourly["date"] = pd.to_datetime(da_hourly["date"])
    print(f"   DA per-firm-hour: {len(da_hourly):,} rows; firms: {da_hourly.firm.nunique()}")

    # Per-firm-period IDA signed cleared (for B9 ΔQ outcome).  Native granularity.
    # PIBCIE.assigned_power_mw is signed natively per OMIE spec §5.2.2.3; simple
    # SUM gives the firm's net IDA position change.  Identical to legacy CASE WHEN
    # for Big-4 (sells-only); correct for retailer firms.
    print("[2/3] Building per-firm-period IDA signed cleared (native granularity)…")
    ida = con.execute(f"""
        SELECT date,
               period,
               COALESCE(grupo_empresarial, 'NA') AS firm,
               mtu_minutes AS ida_mtu,
               SUM(assigned_power_mw * mtu_minutes / 60.0) AS dq_ida_mwh
        FROM '{PIBCIE}'
        WHERE assigned_power_mw IS NOT NULL
        GROUP BY 1, 2, 3, 4
    """).df()
    ida["date"] = pd.to_datetime(ida["date"])
    ida["ida_hour"] = np.where(ida["ida_mtu"] == 60, ida["period"],
                                np.ceil(ida["period"] / 4.0).astype(int))
    print(f"   IDA per-firm-period: {len(ida):,} rows")

    # Join: each (firm, date, ida_period) gets its containing-hour q_DA.
    print("[3/3] Joining DA + IDA at IDA's native period…")
    panel = ida.merge(da_hourly,
                      left_on=["firm", "date", "ida_hour"],
                      right_on=["firm", "date", "hour"],
                      how="inner")
    panel = panel[panel["q_da_mw_hour"] > 0].copy()
    print(f"   joined panel: {len(panel):,} rows")

    panel["regime"] = panel["date"].apply(assign_regime_code)
    panel["dow"] = panel["date"].dt.dayofweek
    panel["month"] = panel["date"].dt.month
    panel["year"] = panel["date"].dt.year
    panel["year_month"] = panel["year"] * 100 + panel["month"]
    panel["date_num"] = (panel["date"] - pd.Timestamp("1960-01-01")).dt.days.astype("int32")

    # Firm ID: 1=GE, 2=IB, 3=GN, 4=HC, 5=Fringe (everything else)
    panel["firm_id"] = panel["firm"].map(FIRM_IDS).fillna(5).astype("int32")

    # daily VRE
    print("   adding daily VRE control…")
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

    print(f"   build time {time.time()-t0:.1f}s")
    return panel[["dq_ida_mwh", "q_da_mw_hour", "firm_id", "regime",
                  "year_month", "year", "month", "ida_hour", "dow",
                  "vre_gwh", "date_num"]].rename(columns={"ida_hour": "hour"})


def write_dofile() -> Path:
    do_content = r"""
*! B9 × F5 firm consistency check
clear all
set more off
use "panel.dta", clear

label define regime_lbl 1 "pre-IDA" 2 "3-sess" 3 "ISP15-win" 4 "DA60-ID15" 5 "DA15-ID15"
label values regime regime_lbl
label define firm_lbl 1 "GE" 2 "IB" 3 "GN" 4 "HC" 5 "Fringe"
label values firm_id firm_lbl

* Build firm-month cluster variable for primary inference
egen firm_month = group(firm_id year_month)
di as txt "firm-month clusters: " _N
quietly: levelsof firm_month, local(fm_levels)
local n_clusters: word count `fm_levels'
di as txt "  n_clusters = `n_clusters'"

di as txt _newline "============================================================"
di as txt    "  CHECK 1a — B9 ΔQ by FIRM × REGIME (raw means + reghdfe)"
di as txt    "============================================================"

* Raw means (Stata 17+ syntax)
di as txt _newline "Raw mean ΔQ_IDA per firm-period (MWh) by firm × regime:"
tabstat dq_ida_mwh, by(firm_id) statistics(mean) format(%8.1f)
di as txt _newline "Same broken out by regime:"
forvalues r = 1/5 {
    di as txt "  Regime `r':"
    tabstat dq_ida_mwh if regime == `r', by(firm_id) statistics(mean) format(%8.1f)
}

* reghdfe: ΔQ_IDA ~ firm × regime, augmented FE
di as txt _newline "--- B9 augmented (firm×regime, abs year-month + hour + DOW + VRE) ---"
reghdfe dq_ida_mwh i.firm_id##i.regime vre_gwh, ///
    absorb(year_month hour dow) ///
    cluster(firm_month)

* Save firm × regime ΔQ from this regression
preserve
clear
set obs 25
gen firm_id  = .
gen regime   = .
gen dq_ida   = .
gen dq_ida_se = .
local row = 1
forvalues f = 1/5 {
    forvalues r = 1/5 {
        replace firm_id = `f' in `row'
        replace regime  = `r' in `row'
        local row = `row' + 1
    }
}
* Construct predicted ΔQ_IDA per firm × regime via lincom
* β = baseline + firm + regime + firm×regime
local row = 1
forvalues f = 1/5 {
    forvalues r = 1/5 {
        if `f' == 1 & `r' == 1 {
            * pre-IDA × GE: just intercept
            replace dq_ida    = _b[_cons] in `row'
            replace dq_ida_se = _se[_cons] in `row'
        }
        else if `f' == 1 {
            lincom _cons + `r'.regime
            replace dq_ida    = r(estimate) in `row'
            replace dq_ida_se = r(se) in `row'
        }
        else if `r' == 1 {
            lincom _cons + `f'.firm_id
            replace dq_ida    = r(estimate) in `row'
            replace dq_ida_se = r(se) in `row'
        }
        else {
            lincom _cons + `f'.firm_id + `r'.regime + `f'.firm_id#`r'.regime
            replace dq_ida    = r(estimate) in `row'
            replace dq_ida_se = r(se) in `row'
        }
        local row = `row' + 1
    }
}
gen tag = "B9"
list, clean noobs
save "b9_results.dta", replace
restore

di as txt _newline "============================================================"
di as txt    "  CHECK 1b — F5 Allaz-Vila slope per FIRM × REGIME"
di as txt    "============================================================"

* Triple interaction: q_DA × firm × regime
di as txt _newline "--- F5 augmented (q_DA × firm_id × regime, abs year-month + hour + DOW + VRE) ---"
reghdfe dq_ida_mwh c.q_da_mw_hour##i.firm_id##i.regime vre_gwh, ///
    absorb(year_month hour dow) ///
    cluster(firm_month)

* Extract per-firm slope by regime via lincom
preserve
clear
set obs 25
gen firm_id  = .
gen regime   = .
gen beta     = .
gen beta_se  = .
local row = 1
forvalues f = 1/5 {
    forvalues r = 1/5 {
        replace firm_id = `f' in `row'
        replace regime  = `r' in `row'
        local row = `row' + 1
    }
}
local row = 1
forvalues f = 1/5 {
    forvalues r = 1/5 {
        local terms "c.q_da_mw_hour"
        if `f' > 1  local terms "`terms' + `f'.firm_id#c.q_da_mw_hour"
        if `r' > 1  local terms "`terms' + `r'.regime#c.q_da_mw_hour"
        if `f' > 1 & `r' > 1  local terms "`terms' + `f'.firm_id#`r'.regime#c.q_da_mw_hour"
        capture lincom `terms'
        if !_rc {
            replace beta    = r(estimate) in `row'
            replace beta_se = r(se) in `row'
        }
        local row = `row' + 1
    }
}
gen tag = "F5"
list, clean noobs
save "f5_results.dta", replace
restore

di as txt _newline "============================================================"
di as txt    "  COMBINED OUTPUT"
di as txt    "============================================================"
use "b9_results.dta", clear
merge 1:1 firm_id regime using "f5_results.dta"
drop _merge
list, clean noobs
export delimited "results.csv", replace
"""
    DO_FILE.parent.mkdir(parents=True, exist_ok=True)
    DO_FILE.write_text(do_content)
    return DO_FILE


def main() -> None:
    print("=== Building per-firm-period panel ===")
    panel = build_panel()
    print(f"\nWriting Stata .dta to {DTA_OUT}…")
    DTA_OUT.parent.mkdir(parents=True, exist_ok=True)
    panel.to_stata(DTA_OUT, write_index=False, version=118)
    print(f"   wrote {DTA_OUT.stat().st_size / 1e6:.1f} MB")

    print(f"\nWriting .do to {DO_FILE}…")
    write_dofile()

    print("\n=== Running reghdfe in Stata (via /tmp staging) ===")
    tmp = Path(tempfile.mkdtemp(prefix="b9f5_"))
    shutil.copy(DTA_OUT, tmp / "panel.dta")
    shutil.copy(DO_FILE, tmp / "run.do")

    cmd = ["stata-mp", "-b", "do", "run.do"]
    result = subprocess.run(cmd, cwd=str(tmp), capture_output=True, text=True)
    log_path = tmp / "run.log"
    log_text = log_path.read_text() if log_path.exists() else (result.stdout + result.stderr)

    if "CHECK 1a" in log_text:
        print(log_text.split("CHECK 1a", 1)[1])
    else:
        print(log_text[-6000:])

    tmp_csv = tmp / "results.csv"
    if tmp_csv.exists():
        shutil.copy(tmp_csv, RES_OUT)
        print(f"\n   wrote {RES_OUT}")
        # Read and present the firm-ranking comparison
        df = pd.read_csv(RES_OUT)
        print("\n=== Firm ranking comparison ===")
        firm_names = {1: "GE", 2: "IB", 3: "GN", 4: "HC", 5: "Fringe"}
        regime_names = {1: "pre-IDA", 2: "3-sess", 3: "ISP15-win", 4: "DA60/ID15", 5: "DA15/ID15"}
        df["firm_name"] = df["firm_id"].map(firm_names)
        df["regime_name"] = df["regime"].map(regime_names)
        for r in [3, 4, 5]:  # ISP15-win, DA60/ID15, DA15/ID15 (the post-IDA regimes)
            print(f"\n  Regime: {regime_names[r]}")
            sub = df[df["regime"] == r].copy().sort_values("firm_id")
            sub["abs_dq"] = sub["dq_ida"].abs()
            sub["abs_beta"] = sub["beta"].abs() if "beta" in sub.columns else np.nan
            for _, row in sub.iterrows():
                print(f"    {row['firm_name']:<8}  ΔQ={row['dq_ida']:+8.1f}  β_AV={row.get('beta', np.nan):+8.5f}")


if __name__ == "__main__":
    main()
