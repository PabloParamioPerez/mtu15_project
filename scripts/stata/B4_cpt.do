*============================================================================
* B4 — Conditional parallel trends spec stack on q_2 (Equation 2 in paper)
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* Sample: pivotal-firm subset of the headline Oct-Dec 2024 vs Oct-Dec 2025
*         panel (122,495 obs).
*
* Equation 2:
*   q_{2,fdh} = alpha + b1 crit + b2 post + b3 (crit*post)
*               + gamma_f + delta_DOW + theta X_{dh} + eps_{fdh}
*
* X_{dh} sequence:
*   (1) baseline: no X
*   (2) X = {wind_z, solar_z}                       [demeaned MW]
*   (3) X = {wind_z, solar_z, crit*wind_z, crit*solar_z}
*   (4) (3) plus calendar-month fixed effects
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

use "`datadir'/B4_cpt_panel.dta", clear
encode parent, gen(parent_id)

* Generate centered VRE-by-crit interactions
gen crit_wind  = crit * wind_z
gen crit_solar = crit * solar_z

label var crit       "Critical"
label var post       "Post"
label var wind_z     "Spain wind production (MW, centered)"
label var solar_z    "Spain solar production (MW, centered)"
label var crit_wind  "Critical $\times$ wind"
label var crit_solar "Critical $\times$ solar"

local fe_opts "vce(cluster d_int)"

*-- Spec 1: baseline (firm + DOW FE)
reghdfe q2_mwh i.crit##i.post, absorb(parent_id dow) `fe_opts'
estimates store cpt1

*-- Spec 2: + wind + solar levels
reghdfe q2_mwh i.crit##i.post wind_z solar_z, absorb(parent_id dow) `fe_opts'
estimates store cpt2

*-- Spec 3: + critical-hour interactions with wind/solar
reghdfe q2_mwh i.crit##i.post wind_z solar_z crit_wind crit_solar, absorb(parent_id dow) `fe_opts'
estimates store cpt3

*-- Spec 4: + calendar-month fixed effects
reghdfe q2_mwh i.crit##i.post wind_z solar_z crit_wind crit_solar, absorb(parent_id dow month) `fe_opts'
estimates store cpt4

*-- Export
esttab cpt1 cpt2 cpt3 cpt4 ///
    using "`texdir'/tab_B4_cpt.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post wind_z solar_z crit_wind crit_solar) ///
    order(1.crit#1.post wind_z solar_z crit_wind crit_solar) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)" ///
               wind_z      "Wind (centered MW)" ///
               solar_z     "Solar (centered MW)" ///
               crit_wind   "Critical $\times$ Wind" ///
               crit_solar  "Critical $\times$ Solar") ///
    prehead("\begin{tabular}{l c c c c}" ///
            "\toprule" ///
            " & (1) & (2) & (3) & (4) \\" ///
            " & Baseline & + VRE levels & + VRE $\times$ crit & + cal-month FE \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust r2 r2_a, fmt(%9.0fc %9.0fc %9.4f %9.4f) ///
          labels("Observations" "Clusters (days)" "\$R^2\$" "Adj. \$R^2\$")) ///
    postfoot("\bottomrule" "\end{tabular}")

* Fix label-escape (esttab escapes underscore in $\beta_3$ etc.)
shell sed -i '' 's/\\beta\\_3/\\beta_3/g; s/\\beta\\_1/\\beta_1/g; s/\\beta\\_2/\\beta_2/g' "`texdir'/tab_B4_cpt.tex"

display _newline "Done. Output: `texdir'/tab_B4_cpt.tex"
log close
exit, clear
