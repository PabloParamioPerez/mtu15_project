*============================================================================
* B1 q_2 within-day DiD — Stata replication of the Python headline
* Author: Pablo Paramio Pérez
* Last audit: 2026-05-12
*
* Outcome: q2_mwh = strategic intraday upward sell adjustment (Ito-Reguant),
*          summed across IDA sessions, per (unit, date, clock-hour) cell.
*
* Spec (Equation 1 in the paper):
*   q_{2,fdh} = alpha + b1 crit_h + b2 post_d + b3 (crit_h * post_d)
*              + gamma_f + delta_DOW(d) + eps_{fdh}
*
* Estimator:
*   reghdfe q2_mwh i.crit##i.post, absorb(parent dow) vce(cluster d_int)
*
* Sample windows:
*   Headline: pre Oct-Dec 2024, post Oct-Dec 2025
*   P1: pre Jul-Sep 2024, post Oct-Dec 2024  (within-2024, no reform)
*   P2: pre Apr-Jun 2025, post Jul-Sep 2025  (within-2025-pre-MTU15-DA)
*   P3: pre Oct-Dec 2023, post Oct-Dec 2024  (crosses Jun-2024 IDA reform)
*============================================================================

clear all
set more off
capture log close
set linesize 200

local repo "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
local datadir "`repo'/results/regressions/firm/critical_hours_thesis/stata_panels"
local outdir  "`repo'/results/regressions/firm/critical_hours_thesis/stata_output"
local texdir  "`repo'/thesis/paper/tables"
capture mkdir "`outdir'"

log using "`outdir'/B1_q2_did.log", replace text

*----------------------------------------------------------------------------
* 1. Headline window: Oct-Dec 2024 vs Oct-Dec 2025
*----------------------------------------------------------------------------
use "`datadir'/B1_headline.dta", clear

* Encode string vars for absorption
encode parent, gen(parent_id)
encode tech_group, gen(tech_id)
encode treatment_group, gen(tgroup_id)

local fe_opts "absorb(parent_id dow) vce(cluster d_int)"

display _newline "=== B1 HEADLINE: Oct-Dec 2024 vs Oct-Dec 2025 ==="

* (1) Pooled
reghdfe q2_mwh i.crit##i.post, `fe_opts'
estimates store m1_all

* (2) Pivotal firms only
reghdfe q2_mwh i.crit##i.post if treatment_group=="treatment", `fe_opts'
estimates store m2_pivotal

* (3) Non-pivotal placebo firms
reghdfe q2_mwh i.crit##i.post if treatment_group=="placebo", `fe_opts'
estimates store m3_nonpivotal

esttab m1_all m2_pivotal m3_nonpivotal ///
    using "`texdir'/tab_B1_main.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post 1.crit 1.post) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)" ///
               1.crit "Critical ($\beta_1$)" ///
               1.post "Post ($\beta_2$)") ///
    prehead("\begin{tabular}{l c c c}" ///
            "\toprule" ///
            " & All firms & Pivotal firms & Non-pivotal firms \\" ///
            " & (1) & (2) & (3) \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust r2 r2_a, fmt(%9.0fc %9.0fc %9.4f %9.4f) ///
          labels("Observations" "Clusters (days)" "\$R^2\$" "Adj. \$R^2\$")) ///
    postfoot("\bottomrule" "\end{tabular}")

*----------------------------------------------------------------------------
* Tech-stratified for pivotal firms
*----------------------------------------------------------------------------
display _newline "=== B1 TECH-STRATIFIED (pivotal firms) ==="

local tech_models ""
foreach tech in "CCGT" "Hydro" "Hydro_pump" "Coal" "Nuclear" "Wind" "Solar PV" {
    local cleantech = subinstr("`tech'", " ", "_", .)
    capture estimates drop m_`cleantech'
    quietly count if treatment_group=="treatment" & tech_group=="`tech'"
    if r(N) > 200 {
        capture noisily reghdfe q2_mwh i.crit##i.post if treatment_group=="treatment" & tech_group=="`tech'", `fe_opts'
        if _rc == 0 {
            estimates store m_`cleantech'
            local tech_models "`tech_models' m_`cleantech'"
            display "    `tech': beta_3 = " _b[1.crit#1.post]
        }
    }
}

esttab `tech_models' ///
    using "`texdir'/tab_B1_tech_stratified.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)") ///
    prehead("\begin{tabular}{l c c c c c c c}" ///
            "\toprule" ///
            " & CCGT & Hydro & Hydro pump & Coal & Nuclear & Wind & Solar PV \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust, fmt(%9.0fc %9.0fc) labels("Obs." "Clusters")) ///
    postfoot("\bottomrule" "\end{tabular}")

*----------------------------------------------------------------------------
* Per-firm regressions
*----------------------------------------------------------------------------
display _newline "=== B1 PER-FIRM ==="

local firm_models ""
local firm_titles ""
foreach firm in "IB" "GE" "GN" "HC" "Repsol" "TotalEnergies" "Engie" "Moeve" {
    local cleanfirm = subinstr("`firm'", "-", "_", .)
    capture estimates drop f_`cleanfirm'
    quietly count if parent=="`firm'"
    if r(N) > 200 {
        capture noisily reghdfe q2_mwh i.crit##i.post if parent=="`firm'", `fe_opts'
        if _rc == 0 {
            estimates store f_`cleanfirm'
            local firm_models "`firm_models' f_`cleanfirm'"
            display "    `firm': beta_3 = " _b[1.crit#1.post]
        }
    }
}

esttab `firm_models' ///
    using "`texdir'/tab_B1_per_firm.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)") ///
    prehead("\begin{tabular}{l c c c c c c c c}" ///
            "\toprule" ///
            " & \multicolumn{4}{c}{Pivotal firms} & \multicolumn{4}{c}{Non-pivotal firms} \\" ///
            "\cmidrule(lr){2-5} \cmidrule(lr){6-9}" ///
            " & Iberdrola & Endesa & Naturgy & EDP-Spain & Repsol & TotalEnergies & Engie & Moeve \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust, fmt(%9.0fc %9.0fc) labels("Obs." "Clusters")) ///
    postfoot("\bottomrule" "\end{tabular}")

*----------------------------------------------------------------------------
* 2. Time placebos
*----------------------------------------------------------------------------

* --- P1 within-2024 ---
use "`datadir'/P1_within2024.dta", clear
encode parent, gen(parent_id)
encode treatment_group, gen(tgroup_id)
encode tech_group, gen(tech_id)

reghdfe q2_mwh i.crit##i.post, `fe_opts'
estimates store p1_all
reghdfe q2_mwh i.crit##i.post if treatment_group=="treatment", `fe_opts'
estimates store p1_piv
reghdfe q2_mwh i.crit##i.post if treatment_group=="placebo", `fe_opts'
estimates store p1_npiv

* --- P2 within-2025 ---
use "`datadir'/P2_within2025.dta", clear
encode parent, gen(parent_id)
encode treatment_group, gen(tgroup_id)
encode tech_group, gen(tech_id)

reghdfe q2_mwh i.crit##i.post, `fe_opts'
estimates store p2_all
reghdfe q2_mwh i.crit##i.post if treatment_group=="treatment", `fe_opts'
estimates store p2_piv
reghdfe q2_mwh i.crit##i.post if treatment_group=="placebo", `fe_opts'
estimates store p2_npiv

* --- P3 one-year-shifted ---
use "`datadir'/P3_shifted1y.dta", clear
encode parent, gen(parent_id)
encode treatment_group, gen(tgroup_id)
encode tech_group, gen(tech_id)

reghdfe q2_mwh i.crit##i.post, `fe_opts'
estimates store p3_all
reghdfe q2_mwh i.crit##i.post if treatment_group=="treatment", `fe_opts'
estimates store p3_piv
reghdfe q2_mwh i.crit##i.post if treatment_group=="placebo", `fe_opts'
estimates store p3_npiv

* Export placebo table (rows = sample, columns = placebo window)
esttab p1_all p1_piv p1_npiv p2_all p2_piv p2_npiv p3_all p3_piv p3_npiv ///
    using "`texdir'/tab_B1_time_placebos.tex", replace ///
    fragment se label booktabs nostar nomtitles nonum ///
    keep(1.crit#1.post) ///
    coeflabels(1.crit#1.post "Critical $\times$ Post ($\beta_3$)") ///
    prehead("\begin{tabular}{l c c c c c c c c c}" ///
            "\toprule" ///
            " & \multicolumn{3}{c}{P1: within-2024} & \multicolumn{3}{c}{P2: within-2025 pre-reform} & \multicolumn{3}{c}{P3: shifted back 1 year} \\" ///
            "\cmidrule(lr){2-4} \cmidrule(lr){5-7} \cmidrule(lr){8-10}" ///
            " & All & Pivotal & Non-piv. & All & Pivotal & Non-piv. & All & Pivotal & Non-piv. \\" ///
            "\midrule") ///
    posthead("") ///
    prefoot("\midrule") ///
    stats(N N_clust, fmt(%9.0fc %9.0fc) labels("Obs." "Clusters")) ///
    postfoot("\bottomrule" "\end{tabular}")

*----------------------------------------------------------------------------
* Post-process: esttab escapes underscore in labels (\$\beta\_3\$). Fix by
* replacing \_3$ with _3$ in the table files.
*----------------------------------------------------------------------------
foreach f in tab_B1_main tab_B1_tech_stratified tab_B1_per_firm tab_B1_time_placebos {
    shell sed -i '' 's/\\beta\\_3/\\beta_3/g; s/\\beta\\_1/\\beta_1/g; s/\\beta\\_2/\\beta_2/g' "`texdir'/`f'.tex"
}

display _newline "Done. Outputs in `texdir' (paper tables/) and `outdir' (logs)."

log close
exit, clear
