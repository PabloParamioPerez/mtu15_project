*============================================================================
* Appendix — DR DiD with firm-partition DiD groups, day-level data
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* This is the appendix version of B4 that re-orients the DiD partition so the
* covariate conditioning has genuine support overlap (firm class is
* independent of weather, unlike clock-of-day).
*
* Setup:
*   Y = within-day per-hour differential of q_2 (MWh per clock-hour):
*       mean q_2 in critical hours minus mean q_2 in flat hours, computed
*       per (unit, date). Normalization by hours-per-class makes critical
*       (11 hours) and flat (3 hours) directly comparable.
*   G = pivotal (1 if parent in {IB, GE, GN, HC, EDP-PT}; 0 if in
*       {Repsol, Engie, TotalEnergies, Moeve}).
*   T = post (1 if Oct-Dec 2025, 0 if Oct-Dec 2024).
*   X = day-level Spain wind, solar, demand (GW) from ENTSO-E.
*
* Estimation: drdid (Rios-Avila Stata port) in RCS mode -- preserves all
* unit-day observations rather than collapsing to (unit, period) cells.
* Cluster-robust SE by unit_code.
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

*-- Load unit-day panel -----------------------------------------------------
use "`datadir'/firm_dr_did_panel.dta", clear
encode unit_code, gen(unit_id)
encode parent, gen(parent_id)

display _newline "Unit-day panel: " _N " obs across " ///
    `r(N)' " unit-days; " `=r(N)/2' " avg per (post, pivotal) cell"

tab post pivotal, summarize(y_diff)

label var pivotal     "Pivotal firm"
label var post        "Post (Oct-Dec 2025)"
label var y_diff      "Within-day q_2 differential (MWh per clock-hour)"
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
reghdfe y_diff i.pivotal##i.post, absorb(parent_id) vce(cluster unit_id)
local b = _b[1.pivotal#1.post]
local s = _se[1.pivotal#1.post]
post `res' ("twfe_baseline") (`b') (`s') (`e(N)')

*-- Spec 2: TWFE + X additive (biased per slides) --------------------------
display _newline "=== Spec 2: TWFE + X additive (biased) ==="
reghdfe y_diff i.pivotal##i.post wind_gw solar_gw demand_gw, absorb(parent_id) vce(cluster unit_id)
local b = _b[1.pivotal#1.post]
local s = _se[1.pivotal#1.post]
post `res' ("twfe_plus_X") (`b') (`s') (`e(N)')

*-- Spec 3: drdid OR (RCS, no ivar; cluster by unit) -----------------------
display _newline "=== Spec 3: drdid OR (RCS) ==="
drdid y_diff wind_gw solar_gw demand_gw, time(post) tr(pivotal) reg cluster(unit_id)
local b = _b[r1vs0.pivotal]
local s = _se[r1vs0.pivotal]
post `res' ("drdid_OR") (`b') (`s') (`e(N)')

*-- Spec 4: drdid IPW (RCS) ------------------------------------------------
display _newline "=== Spec 4: drdid IPW (RCS, std Hajek) ==="
drdid y_diff wind_gw solar_gw demand_gw, time(post) tr(pivotal) stdipw cluster(unit_id)
local b = _b[r1vs0.pivotal]
local s = _se[r1vs0.pivotal]
post `res' ("drdid_IPW") (`b') (`s') (`e(N)')

*-- Spec 5: drdid DR (RCS, Sant'Anna-Zhao 2020 improved) -------------------
display _newline "=== Spec 5: drdid DR (RCS, drimp) ==="
drdid y_diff wind_gw solar_gw demand_gw, time(post) tr(pivotal) drimp cluster(unit_id)
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
file write tex "Unit-days"
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
