
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
