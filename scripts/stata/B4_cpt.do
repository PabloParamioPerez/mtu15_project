*============================================================================
* B4 — Conditional parallel trends on q_2 (Sant'Anna & Zhao 2020 framework)
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* Methodology reference: Sant'Anna, Lecture 5 (Emory, January 2025).
* The slides show that adding covariates X additively to a TWFE specification
* (Y = a + g·G + l·T + b·G·T + X'a + e) gives a biased ATT when treatment
* effects are heterogeneous in X. The correct approach separates
* identification (conditional PT + strong overlap) from estimation:
*   - Outcome regression: Heckman, Ichimura, Todd (1997).
*   - IPW: Abadie (2005), Sant'Anna & Zhao (2020) Hájek-stabilized.
*   - Doubly robust: Sant'Anna & Zhao (2020).
*
* Implementation: drdid (Rios-Avila Stata port).
*
* Caveat for this application: in the within-day design, "treatment"
* (critical hour) is largely deterministic given X (wind, solar, demand),
* since X has a strong hour-of-day signature. Strong-overlap fails;
* propensity-score-based estimators (IPW and DR) extrapolate or blow up.
* We report all four estimators to document this.
*============================================================================

clear all
set more off
capture log close
set linesize 200

local repo "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
local datadir "`repo'/results/regressions/firm/critical_hours_thesis/stata_panels"
local outdir  "`repo'/results/regressions/firm/critical_hours_thesis/stata_output"
local texdir  "`repo'/thesis/paper/tables"

log using "`outdir'/B4_cpt.log", replace text

*-- Build 2-period balanced panel from the raw observation-level data ------
use "`datadir'/B4_cpt_panel.dta", clear
egen cell_id = group(unit_code hour)
collapse (mean) q2_mwh wind_gw solar_gw demand_gw (first) crit parent tech_group hour, ///
    by(cell_id post)
bysort cell_id: gen n_periods = _N
keep if n_periods == 2
drop n_periods
xtset cell_id post

display _newline "Panel: " _N " obs across " _N/2 " unit-hour cells x 2 periods"

label var crit       "Critical"
label var post       "Post"
label var wind_gw    "Wind (GW)"
label var solar_gw   "Solar (GW)"
label var demand_gw  "Demand (GW)"

*-- Storage for results -----------------------------------------------------
tempname res
postfile `res' str20 method double(att se) using "`outdir'/B4_cpt_results.dta", replace

*-- Spec 1: Baseline TWFE (no covariates) ----------------------------------
display _newline "=== Spec 1: TWFE baseline (no X) ==="
reghdfe q2_mwh i.crit##i.post, absorb(parent) vce(cluster cell_id)
local b = _b[1.crit#1.post]
local s = _se[1.crit#1.post]
post `res' ("twfe_baseline") (`b') (`s')

*-- Spec 2: TWFE with X additive (BIASED per Sant'Anna 2025) ---------------
display _newline "=== Spec 2: TWFE + X additive (BIASED per slides) ==="
reghdfe q2_mwh i.crit##i.post wind_gw solar_gw demand_gw, absorb(parent) vce(cluster cell_id)
local b = _b[1.crit#1.post]
local s = _se[1.crit#1.post]
post `res' ("twfe_plus_X_biased") (`b') (`s')

*-- Spec 3: drdid -- Regression adjustment (Heckman-Ichimura-Todd 1997) ----
display _newline "=== Spec 3: drdid Outcome Regression (RA) ==="
drdid q2_mwh wind_gw solar_gw demand_gw, ivar(cell_id) time(post) tr(crit) reg
local b = _b[r1vs0.crit]
local s = _se[r1vs0.crit]
post `res' ("drdid_OR") (`b') (`s')

*-- Spec 4: drdid -- Standardized IPW (Abadie 2005, Hajek) -----------------
display _newline "=== Spec 4: drdid Standardized IPW ==="
drdid q2_mwh wind_gw solar_gw demand_gw, ivar(cell_id) time(post) tr(crit) stdipw
local b = _b[r1vs0.crit]
local s = _se[r1vs0.crit]
post `res' ("drdid_IPW") (`b') (`s')

*-- Spec 5: drdid -- Doubly Robust (Sant'Anna & Zhao 2020) -----------------
display _newline "=== Spec 5: drdid Doubly Robust (Sant'Anna & Zhao 2020) ==="
drdid q2_mwh wind_gw solar_gw demand_gw, ivar(cell_id) time(post) tr(crit) drimp
local b = _b[r1vs0.crit]
local s = _se[r1vs0.crit]
post `res' ("drdid_DR") (`b') (`s')

postclose `res'

*-- Read back results and build LaTeX table --------------------------------
use "`outdir'/B4_cpt_results.dta", clear
list, noobs sep(0)

* Pull values into scalars
forvalues r = 1/5 {
    scalar a`r' = att[`r']
    scalar s`r' = se[`r']
}

file open tex using "`texdir'/tab_B4_cpt.tex", write replace
file write tex "\begin{tabular}{l c c c c c}" _n
file write tex "\toprule" _n
file write tex " & (1) & (2) & (3) & (4) & (5) \\" _n
file write tex " & TWFE & TWFE+X & OR & IPW & DR \\" _n
file write tex " & no controls & (biased) & Heckman-IT 1997 & Abadie 2005 & Sant'Anna-Zhao 2020 \\" _n
file write tex "\midrule" _n
file write tex "ATT ($\beta_3$)"
forvalues r = 1/5 {
    file write tex " & " %9.3f (a`r')
}
file write tex " \\" _n
file write tex " "
forvalues r = 1/5 {
    file write tex " & (" %9.3f (s`r') ")"
}
file write tex " \\" _n
file write tex "\midrule" _n
file write tex "Cell-periods & 3{,}006 & 3{,}006 & 3{,}006 & 3{,}006 & 3{,}006 \\" _n
file write tex "Unit-hour cells & 1{,}503 & 1{,}503 & 1{,}503 & 1{,}503 & 1{,}503 \\" _n
file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex

display _newline "Done."
log close
exit, clear
