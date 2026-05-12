*============================================================================
* Appendix — Triple-difference on q_2: crit × post × pivotal
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* Design:
*   Three binary dimensions partition the (unit, date, hour) sample:
*     crit_h      : 1 if hour in 05:00-09:00 or 16:00-23:00; 0 if flat (01:00-04:00)
*     post_d      : 1 if Oct-Dec 2025 (post MTU15-DA); 0 if Oct-Dec 2024
*     pivotal_f   : 1 if parent firm in {IB, GE, GN, HC, EDP-PT}; 0 if in
*                   {Repsol, Engie, TotalEnergies, Moeve}
*
* Regression:
*   q_{2,fudh} = a + b1·crit + b2·post + b3·piv
*              + b12·crit·post + b13·crit·piv + b23·post·piv
*              + b123·(crit·post·piv)
*              + gamma_f + delta_DOW + (theta·X_d) + eps
*
* β_{123} is the headline DDD coefficient: the additional q_2 in critical
* hours after the MTU15-DA reform for pivotal firms, over and above what the
* two-way interactions would predict. Economically: the moderating effect
* of market power on the reform's response in critical hours.
*
* SE clustered by date (192 day-clusters).
*============================================================================

clear all
set more off
capture log close
set linesize 220

local repo "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
local datadir "`repo'/results/regressions/firm/critical_hours_thesis/stata_panels"
local outdir  "`repo'/results/regressions/firm/critical_hours_thesis/stata_output"
local texdir  "`repo'/thesis/paper/tables"

log using "`outdir'/firm_ddd.log", replace text

use "`datadir'/ddd_panel.dta", clear
encode parent, gen(parent_id)
encode tech_group, gen(tech_id)

label var crit       "Critical hour"
label var post       "Post (Oct-Dec 2025)"
label var pivotal    "Pivotal firm"

local fe_opts "vce(cluster d_int)"

*-- Storage ----------------------------------------------------------------
tempname res
postfile `res' str20 spec double(b123 se123 b_piv se_piv b_npiv se_npiv n) ///
    using "`outdir'/firm_ddd_results.dta", replace

*-- Spec 1: Pure DDD, firm + DOW FE ----------------------------------------
display _newline "=== Spec 1: DDD baseline (firm + DOW FE) ==="
reghdfe q2_mwh i.crit##i.post##i.pivotal, absorb(parent_id dow) `fe_opts'
local b123  = _b[1.crit#1.post#1.pivotal]
local s123  = _se[1.crit#1.post#1.pivotal]
local bpiv  = _b[1.crit#1.post] + _b[1.crit#1.post#1.pivotal]
* Implied β3 for pivotal firms is (crit×post) + (crit×post×pivotal). Compute
* via lincom for SE.
lincom 1.crit#1.post + 1.crit#1.post#1.pivotal
local bpiv_est = r(estimate)
local bpiv_se  = r(se)
local bnpiv_est = _b[1.crit#1.post]
local bnpiv_se  = _se[1.crit#1.post]
post `res' ("DDD_baseline") (`b123') (`s123') ///
    (`bpiv_est') (`bpiv_se') (`bnpiv_est') (`bnpiv_se') (`e(N)')

*-- Spec 2: DDD + X additive (day-level covariates) ------------------------
display _newline "=== Spec 2: DDD + X additive ==="
reghdfe q2_mwh i.crit##i.post##i.pivotal wind_gw solar_gw demand_gw, ///
    absorb(parent_id dow) `fe_opts'
local b123  = _b[1.crit#1.post#1.pivotal]
local s123  = _se[1.crit#1.post#1.pivotal]
lincom 1.crit#1.post + 1.crit#1.post#1.pivotal
local bpiv_est = r(estimate)
local bpiv_se  = r(se)
local bnpiv_est = _b[1.crit#1.post]
local bnpiv_se  = _se[1.crit#1.post]
post `res' ("DDD_plus_X") (`b123') (`s123') ///
    (`bpiv_est') (`bpiv_se') (`bnpiv_est') (`bnpiv_se') (`e(N)')

*-- Spec 3: DDD + X interacted with the three DDD dimensions ---------------
display _newline "=== Spec 3: DDD + X × {crit, post, pivotal} interactions ==="
reghdfe q2_mwh i.crit##i.post##i.pivotal ///
    c.wind_gw c.solar_gw c.demand_gw ///
    c.wind_gw#i.crit c.solar_gw#i.crit c.demand_gw#i.crit ///
    c.wind_gw#i.post c.solar_gw#i.post c.demand_gw#i.post ///
    c.wind_gw#i.pivotal c.solar_gw#i.pivotal c.demand_gw#i.pivotal, ///
    absorb(parent_id dow) `fe_opts'
local b123  = _b[1.crit#1.post#1.pivotal]
local s123  = _se[1.crit#1.post#1.pivotal]
lincom 1.crit#1.post + 1.crit#1.post#1.pivotal
local bpiv_est = r(estimate)
local bpiv_se  = r(se)
local bnpiv_est = _b[1.crit#1.post]
local bnpiv_se  = _se[1.crit#1.post]
post `res' ("DDD_X_interactions") (`b123') (`s123') ///
    (`bpiv_est') (`bpiv_se') (`bnpiv_est') (`bnpiv_se') (`e(N)')

*-- Spec 4: DDD + clock-hour FE (saturated within-day) ---------------------
display _newline "=== Spec 4: DDD + clock-hour FE ==="
reghdfe q2_mwh i.crit##i.post##i.pivotal wind_gw solar_gw demand_gw, ///
    absorb(parent_id dow hour) `fe_opts'
local b123  = _b[1.crit#1.post#1.pivotal]
local s123  = _se[1.crit#1.post#1.pivotal]
lincom 1.crit#1.post + 1.crit#1.post#1.pivotal
local bpiv_est = r(estimate)
local bpiv_se  = r(se)
local bnpiv_est = _b[1.crit#1.post]
local bnpiv_se  = _se[1.crit#1.post]
post `res' ("DDD_hour_FE") (`b123') (`s123') ///
    (`bpiv_est') (`bpiv_se') (`bnpiv_est') (`bnpiv_se') (`e(N)')

postclose `res'

*-- Build LaTeX table -------------------------------------------------------
use "`outdir'/firm_ddd_results.dta", clear
list, noobs sep(0)

forvalues r = 1/4 {
    scalar b3_`r'  = b123[`r']
    scalar s3_`r'  = se123[`r']
    scalar bp_`r'  = b_piv[`r']
    scalar sp_`r'  = se_piv[`r']
    scalar bn_`r'  = b_npiv[`r']
    scalar sn_`r'  = se_npiv[`r']
    scalar n_`r'   = n[`r']
}

file open tex using "`texdir'/tab_firm_ddd.tex", write replace
file write tex "\begin{tabular}{l c c c c}" _n
file write tex "\toprule" _n
file write tex " & (1) & (2) & (3) & (4) \\" _n
file write tex " & \makecell{DDD\\baseline} & \makecell{+ X\\(additive)} & \makecell{+ X{$\times$}\{crit,\\post, piv\}} & \makecell{+ clock-hour\\FE} \\" _n
file write tex "\midrule" _n
file write tex "$\beta_{123}$ (crit{$\times$}post{$\times$}piv)"
forvalues r = 1/4 {
    file write tex " & " %9.3f (b3_`r')
}
file write tex " \\" _n
file write tex " "
forvalues r = 1/4 {
    file write tex " & (" %9.3f (s3_`r') ")"
}
file write tex " \\" _n
file write tex "\addlinespace" _n
file write tex "Implied $\beta_3$, pivotal firms"
forvalues r = 1/4 {
    file write tex " & " %9.3f (bp_`r')
}
file write tex " \\" _n
file write tex " "
forvalues r = 1/4 {
    file write tex " & (" %9.3f (sp_`r') ")"
}
file write tex " \\" _n
file write tex "\addlinespace" _n
file write tex "Implied $\beta_3$, non-pivotal firms"
forvalues r = 1/4 {
    file write tex " & " %9.3f (bn_`r')
}
file write tex " \\" _n
file write tex " "
forvalues r = 1/4 {
    file write tex " & (" %9.3f (sn_`r') ")"
}
file write tex " \\" _n
file write tex "\midrule" _n
file write tex "Observations"
forvalues r = 1/4 {
    file write tex " & " %9.0fc (n_`r')
}
file write tex " \\" _n
file write tex "\bottomrule" _n
file write tex "\end{tabular}" _n
file close tex

display _newline "Done."
log close
exit, clear
