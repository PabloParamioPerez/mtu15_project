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
foreach r of local regimes {
    replace regime_str = "`r'" in `i'
    if `i' == 1 {
        replace beta = 0 in `i'
        replace se = 0 in `i'
    }
    else {
        replace beta = _b[`i'.regime] in `i'
        replace se = _se[`i'.regime] in `i'
    }
    local i = `i' + 1
}
gen t = beta / se
list, clean noobs
export delimited "f12_results.csv", replace
restore

* Joint Wald
di as txt _newline "--- Joint Wald: H0 all regime != 0 (vs pre-IDA) ---"
test 2.regime 3.regime 4.regime 5.regime
