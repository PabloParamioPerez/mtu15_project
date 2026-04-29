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
"""F5 robustness pass: two-way clustering + same-cal-month sub-sample.

The original F5 spec used cluster(date_num) — handles cross-unit shocks
within a date but NOT within-unit serial correlation.  With ~3000 daily
clusters and 4.6M obs, this could understate SEs.

Two-way clustering by (unit_id, date_num):
   - 279 unit clusters (well-conditioned)
   - 2,940 date clusters
   - Cameron-Gelbach-Miller two-way variance
   - Captures both within-unit serial correlation AND cross-unit common
     shocks within a date.
   - Hypothesis: SEs may inflate by 1.5-2× vs date-only; F=49 might fall
     to F=20-30 but pattern should survive.

Same-calendar-month sub-sample (CLAUDE.md mandate for cross-regime claims):
   - Restrict to Apr-Sep months across all regimes
   - This includes pre-IDA Apr-Sep (multi-year), DA60/ID15 (Apr-Sep 2025),
     and excludes ISP15-win (Dec-Mar), DA15/ID15 (mostly Oct-Mar)
   - With this restriction, regime variation is purely calendar-aligned;
     seasonality-mix effects on regime coefficients are eliminated by
     sample selection, not absorbed by FE.

Output: results/regressions/f5_robustness_pass.csv
"""
from __future__ import annotations
import shutil
import subprocess
import tempfile
from pathlib import Path

PROJECT  = Path(__file__).resolve().parents[3]
DTA_OUT  = PROJECT / "results" / "regressions" / "f5_per_unit_panel.dta"
DO_FILE  = PROJECT / "scripts" / "analysis" / "welfare" / "f5_robustness_pass.do"
RES_OUT  = PROJECT / "results" / "regressions" / "f5_robustness_pass.csv"


def write_dofile() -> None:
    do = r"""
*! F5 robustness: two-way clustering + same-cal-month sub-sample
clear all
set more off
use "panel.dta", clear

label define regime_lbl 1 "pre-IDA" 2 "3-sess" 3 "ISP15-win" 4 "DA60-ID15" 5 "DA15-ID15"
label values regime regime_lbl

* Extract calendar month from year_month
gen month = mod(year_month, 100)

di as txt _newline "============================================================"
di as txt    "  F5 ROBUSTNESS PASS"
di as txt    "============================================================"

* ============================================================
* (A) TWO-WAY CLUSTERING by (unit_id, date_num)
* ============================================================
di as txt _newline "============================================================"
di as txt    "  (A) F5 augmented, two-way cluster(unit_id date_num)"
di as txt    "      handles within-unit serial correlation + cross-unit"
di as txt    "      common date shocks"
di as txt    "============================================================"

reghdfe dq_ida_mwh c.q_da_mw_hour##i.regime vre_gwh, ///
    absorb(unit_id year_month hour_of_day dow) ///
    cluster(unit_id date_num)

* Test joint significance of regime × q_DA interactions
di as txt _newline "Joint Wald: H0 all regime × q_DA interactions = 0"
test 2.regime#c.q_da_mw_hour 3.regime#c.q_da_mw_hour ///
     4.regime#c.q_da_mw_hour 5.regime#c.q_da_mw_hour

* Per-regime slope via lincom
di as txt _newline "Per-regime Allaz-Vila slope under two-way cluster SE:"
preserve
clear
set obs 5
gen regime_str = ""
gen beta = .
gen se = .
local regimes pre_IDA three_sess ISP15_win DA60_ID15 DA15_ID15
local i = 1
foreach r of local regimes {
    replace regime_str = "`r'" in `i'
    if `i' == 1 {
        replace beta = _b[c.q_da_mw_hour] in `i'
        replace se   = _se[c.q_da_mw_hour] in `i'
    }
    else {
        lincom c.q_da_mw_hour + `i'.regime#c.q_da_mw_hour
        replace beta = r(estimate) in `i'
        replace se   = r(se) in `i'
    }
    local i = `i' + 1
}
gen t = beta / se
gen p = 2 * normal(-abs(t))
gen spec = "twoway"
list, clean noobs
save "twoway_results.dta", replace
restore

* ============================================================
* (B) SAME-CAL-MONTH SUB-SAMPLE: Apr-Sep months only
* ============================================================
di as txt _newline "============================================================"
di as txt    "  (B) F5 same-cal-month: Apr-Sep months only"
di as txt    "      pre-IDA Apr-Sep multi-year vs DA60/ID15 Apr-Sep 2025"
di as txt    "============================================================"

preserve
keep if month >= 4 & month <= 9
di as txt "Sample size after Apr-Sep restriction: " _N

* Need at least 2 distinct regimes with data
tabulate regime
quietly tabulate regime
local n_regimes = r(r)
di as txt "Number of distinct regimes in Apr-Sep sub-sample: `n_regimes'"

if `n_regimes' >= 2 {
    reghdfe dq_ida_mwh c.q_da_mw_hour##i.regime vre_gwh, ///
        absorb(unit_id year_month hour_of_day dow) ///
        cluster(unit_id date_num)

    di as txt _newline "Joint Wald (Apr-Sep sample): regime × q_DA interactions = 0"
    capture noisily test 2.regime#c.q_da_mw_hour 3.regime#c.q_da_mw_hour ///
         4.regime#c.q_da_mw_hour 5.regime#c.q_da_mw_hour
}
else {
    di as err "ERROR: not enough regime variation in Apr-Sep sub-sample"
}

restore

* Save twoway results to CSV
use "twoway_results.dta", clear
export delimited "results.csv", replace
"""
    DO_FILE.parent.mkdir(parents=True, exist_ok=True)
    DO_FILE.write_text(do)


def main() -> None:
    if not DTA_OUT.exists():
        print(f"ERROR: panel not found at {DTA_OUT}")
        print("Run f5_per_unit_reghdfe.py first to build the panel.")
        return

    write_dofile()
    print(f"Wrote {DO_FILE}")

    print("\n=== Running reghdfe (via /tmp staging) ===")
    tmp = Path(tempfile.mkdtemp(prefix="f5_robust_"))
    shutil.copy(DTA_OUT, tmp / "panel.dta")
    shutil.copy(DO_FILE, tmp / "run.do")
    cmd = ["stata-mp", "-b", "do", "run.do"]
    result = subprocess.run(cmd, cwd=str(tmp), capture_output=True, text=True)
    log_path = tmp / "run.log"
    log_text = log_path.read_text() if log_path.exists() else (result.stdout + result.stderr)
    if "F5 ROBUSTNESS PASS" in log_text:
        print(log_text.split("F5 ROBUSTNESS PASS", 1)[1])
    else:
        print(log_text[-5000:])

    csv = tmp / "results.csv"
    if csv.exists():
        shutil.copy(csv, RES_OUT)
        print(f"\nwrote {RES_OUT}")


if __name__ == "__main__":
    main()
