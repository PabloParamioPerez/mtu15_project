*============================================================================
* Appendix — DR DiD with firm-partition DiD groups
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* This is the appendix version of B4 that re-orients the DiD partition so the
* covariate conditioning has genuine support overlap.
*
* Setup:
*   Y = within-day differential q_2(critical hours) - q_2(flat hours), MWh,
*       summed over hours within each (unit, date) cell.
*   G = pivotal (1 if parent in pivotal-firm set, 0 if non-pivotal).
*   T = post (1 if Oct-Dec 2025, 0 if Oct-Dec 2024).
*   X = day-level Spain wind, solar, demand (GW) from ENTSO-E A75 + A65.
*
* Why this satisfies overlap: the firm partition is determined by ownership
* identity (Iberdrola, Endesa, Naturgy, EDP-Spain, EDP-PT vs Repsol, Engie,
* TotalEnergies, Moeve), which is independent of day-level weather. Both
* firm classes operate on all days, so P(pivotal | X) is bounded away
* from 0 and 1 across all X regions.
*
* Estimation: drdid (Rios-Avila Stata port) — outcome regression,
* standardized IPW, and improved doubly-robust (Sant'Anna & Zhao 2020).
*============================================================================

clear all
set more off
capture log close
set linesize 200

local repo "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
local datadir "`repo'/results/regressions/firm/critical_hours_thesis/stata_panels"
local outdir  "`repo'/results/regressions/firm/critical_hours_thesis/stata_output"
local texdir  "`repo'/thesis/paper/tables"

log using "`outdir'/firm_dr_did.log", replace text

*-- Load and collapse to a balanced 2-period panel --------------------------
use "`datadir'/firm_dr_did_panel.dta", clear

* Each "unit" in the drdid panel is a generation unit_code (with fixed
* pivotal status across pre and post). Y = mean over days in window.
encode unit_code, gen(unit_id)
collapse (mean) y_diff wind_gw solar_gw demand_gw (first) pivotal parent tech_group, ///
    by(unit_id post)
bysort unit_id: gen n_periods = _N
keep if n_periods == 2
drop n_periods
xtset unit_id post

display _newline "Panel: " _N " obs across " _N/2 " units x 2 periods"
tab pivotal post

label var pivotal     "Pivotal firm"
label var post        "Post (Oct-Dec 2025)"
label var y_diff      "Critical-flat q_2 differential (MWh/day)"
label var wind_gw     "Wind (GW)"
label var solar_gw    "Solar (GW)"
label var demand_gw   "Demand (GW)"

*-- Overlap check: distribution of X across pivotal/non-pivotal -------------
display _newline "=== Overlap check (X distribution by group) ==="
tabstat wind_gw solar_gw demand_gw, by(pivotal) statistics(mean sd min max)

*-- Storage for results -----------------------------------------------------
tempname res
postfile `res' str20 method double(att se n) using "`outdir'/firm_dr_did_results.dta", replace

*-- Spec 1: Baseline TWFE (no X) -------------------------------------------
display _newline "=== Spec 1: TWFE baseline (no X) ==="
reghdfe y_diff i.pivotal##i.post, noabsorb vce(cluster unit_id)
local b = _b[1.pivotal#1.post]
local s = _se[1.pivotal#1.post]
post `res' ("twfe_baseline") (`b') (`s') (`e(N)')

*-- Spec 2: TWFE + X additive (biased per slides) --------------------------
display _newline "=== Spec 2: TWFE + X additive (biased) ==="
reghdfe y_diff i.pivotal##i.post wind_gw solar_gw demand_gw, noabsorb vce(cluster unit_id)
local b = _b[1.pivotal#1.post]
local s = _se[1.pivotal#1.post]
post `res' ("twfe_plus_X") (`b') (`s') (`e(N)')

*-- Spec 3: drdid Outcome Regression (Heckman-Ichimura-Todd 1997) ----------
display _newline "=== Spec 3: drdid OR ==="
drdid y_diff wind_gw solar_gw demand_gw, ivar(unit_id) time(post) tr(pivotal) reg
local b = _b[r1vs0.pivotal]
local s = _se[r1vs0.pivotal]
post `res' ("drdid_OR") (`b') (`s') (`e(N)')

*-- Spec 4: drdid Standardized IPW (Abadie 2005) ---------------------------
display _newline "=== Spec 4: drdid IPW ==="
drdid y_diff wind_gw solar_gw demand_gw, ivar(unit_id) time(post) tr(pivotal) stdipw
local b = _b[r1vs0.pivotal]
local s = _se[r1vs0.pivotal]
post `res' ("drdid_IPW") (`b') (`s') (`e(N)')

*-- Spec 5: drdid Doubly Robust (Sant'Anna & Zhao 2020) --------------------
display _newline "=== Spec 5: drdid DR ==="
drdid y_diff wind_gw solar_gw demand_gw, ivar(unit_id) time(post) tr(pivotal) drimp
local b = _b[r1vs0.pivotal]
local s = _se[r1vs0.pivotal]
post `res' ("drdid_DR") (`b') (`s') (`e(N)')

postclose `res'

*-- Build LaTeX table -------------------------------------------------------
use "`outdir'/firm_dr_did_results.dta", clear
list, noobs sep(0)

forvalues r = 1/5 {
    scalar a`r' = att[`r']
    scalar s`r' = se[`r']
    scalar n`r' = n[`r']
}

file open tex using "`texdir'/tab_firm_dr_did.tex", write replace
file write tex "\begin{tabular}{l c c c c c}" _n
file write tex "\toprule" _n
file write tex " & (1) & (2) & (3) & (4) & (5) \\" _n
file write tex " & TWFE & TWFE+X & OR & IPW & DR \\" _n
file write tex " & no controls & (additive) & Heckman-IT 1997 & Abadie 2005 & Sant'Anna-Zhao 2020 \\" _n
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
file write tex "Unit-periods"
forvalues r = 1/5 {
    file write tex " & " %9.0fc (n`r')
}
file write tex " \\" _n
file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex

display _newline "Done."
log close
exit, clear
