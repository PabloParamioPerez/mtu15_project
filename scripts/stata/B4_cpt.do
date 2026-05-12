*============================================================================
* B4 — Conditional parallel trends spec stack on q_2 (Equation 2 in paper)
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* Sample: pivotal-firm subset of the headline Oct-Dec 2024 vs Oct-Dec 2025
*         panel (122,495 obs).
*
* Equation 2 (matches the workshop slide spec — wind + solar + demand +
* their crit interactions, with cal-month FE on top):
*
*   q_{2,fdh} = alpha + b1 crit_h + b2 post_d + b3 (crit_h * post_d)
*               + gamma_f + delta_DOW + theta X_{dh} + eps_{fdh}
*
* X_{dh} sequence:
*   (1) baseline: no X
*   (2) X = {wind, solar, wind*crit, solar*crit}
*   (3) X = {wind, solar, demand, wind*crit, solar*crit, demand*crit}
*   (4) (3) plus calendar-month fixed effects
*
* All controls in GW (wind/solar/demand) and demeaned so b3 reads off as
* the critical-vs-flat post-vs-pre coefficient at sample means.
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

gen crit_wind   = crit * wind_gw_z
gen crit_solar  = crit * solar_gw_z
gen crit_demand = crit * demand_gw_z

label var crit         "Critical"
label var post         "Post"
label var wind_gw_z    "Spain wind (GW, centered)"
label var solar_gw_z   "Spain solar (GW, centered)"
label var demand_gw_z  "Spain demand (GW, centered)"
label var crit_wind    "Critical $\times$ Wind"
label var crit_solar   "Critical $\times$ Solar"
label var crit_demand  "Critical $\times$ Demand"

local fe_opts "vce(cluster d_int)"

*-- Spec 1: baseline (firm + DOW FE)
reghdfe q2_mwh i.crit##i.post, absorb(parent_id dow) `fe_opts'
estimates store cpt1

*-- Spec 2: + wind/solar levels + their crit interactions
reghdfe q2_mwh i.crit##i.post wind_gw_z solar_gw_z crit_wind crit_solar, ///
    absorb(parent_id dow) `fe_opts'
estimates store cpt2

*-- Spec 3: + demand level + its crit interaction
reghdfe q2_mwh i.crit##i.post wind_gw_z solar_gw_z demand_gw_z ///
    crit_wind crit_solar crit_demand, ///
    absorb(parent_id dow) `fe_opts'
estimates store cpt3

*-- Spec 4: + calendar-month fixed effects
reghdfe q2_mwh i.crit##i.post wind_gw_z solar_gw_z demand_gw_z ///
    crit_wind crit_solar crit_demand, ///
    absorb(parent_id dow month) `fe_opts'
estimates store cpt4

*-- Export
esttab cpt1 cpt2 cpt3 cpt4 ///
    using "`texdir'/tab_B4_cpt.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post wind_gw_z solar_gw_z demand_gw_z crit_wind crit_solar crit_demand) ///
    order(1.crit#1.post wind_gw_z solar_gw_z demand_gw_z crit_wind crit_solar crit_demand) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)" ///
               wind_gw_z    "Wind (GW)" ///
               solar_gw_z   "Solar (GW)" ///
               demand_gw_z  "Demand (GW)" ///
               crit_wind    "Critical $\times$ Wind" ///
               crit_solar   "Critical $\times$ Solar" ///
               crit_demand  "Critical $\times$ Demand") ///
    prehead("\begin{tabular}{l c c c c}" ///
            "\toprule" ///
            " & (1) & (2) & (3) & (4) \\" ///
            " & Baseline & + Wind/Solar & + Demand & + cal-month FE \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust r2 r2_a, fmt(%9.0fc %9.0fc %9.4f %9.4f) ///
          labels("Observations" "Clusters (days)" "\$R^2\$" "Adj. \$R^2\$")) ///
    postfoot("\bottomrule" "\end{tabular}")

shell sed -i '' 's/\\beta\\_3/\\beta_3/g; s/\\beta\\_1/\\beta_1/g; s/\\beta\\_2/\\beta_2/g' "`texdir'/tab_B4_cpt.tex"

display _newline "Done. Output: `texdir'/tab_B4_cpt.tex"
log close
exit, clear
