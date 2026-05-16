* ============================================================================
* 06_ddd_firm_q2.do  --  OLS-FE DDD + DDDD on firm-level q_2 outcome
* INPUT:  panels/{da15,ida15}_q2.dta
* OUTPUT: thesis/paper/tables/reform_window/tab_ddd_firm_q2_{da15,ida15}.tex
*         coefs/ddd_firm_q2_{da15,ida15}.csv
*         logs/06_ddd_firm_q2.log
* OUTCOME: q_2 = signed intraday programmed MWh per (firm, unit, date,
*          clock-hour), summed across all IDA sessions, from PIBCI. Positive
*          means the firm sold more in IDA than it had in DA.
* See 05_ddd_firm_q1.do for methodology.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/06_ddd_firm_q2.log", text replace

foreach window in da15 ida15 {

    di _n "{hline 78}"
    di "q_2 OLS-FE DDD + DDDD on `window' window"
    di "{hline 78}"

    use "${PANELS}/`window'_q2.dta", clear
    di "N=" _N
    sum q2, detail

    reghdfe q2 crit post y25 ///
                crit_post crit_y25 post_y25 ///
                crit_post_y25, ///
        absorb(firm_id unit_id clockhour dow) vce(cluster date_stata)
    estimates store DDD
    estadd local spec "DDD"
    estadd local cluster "Date"
    estadd local fe_unit "Yes"
    estadd local fe_clk  "Yes"
    estadd local fe_dow  "Yes"

    reghdfe q2 crit post y25 ///
                crit_post crit_y25 post_y25 ///
                crit_piv post_piv y25_piv ///
                crit_post_y25 crit_post_piv crit_y25_piv post_y25_piv ///
                crit_post_y25_piv, ///
        absorb(firm_id unit_id clockhour dow) vce(cluster firm_id)
    estimates store DDDD
    estadd local spec "DDDD"
    estadd local cluster "Firm"
    estadd local fe_unit "Yes"
    estadd local fe_clk  "Yes"
    estadd local fe_dow  "Yes"

    esttab DDD DDDD using "${TBLDIR}/tab_ddd_firm_q2_`window'.tex", ///
        replace booktabs ///
        label nogap ///
        nonotes ///
        cells("b(fmt(2) star) se(fmt(2) par)") ///
        starlevels(* 0.10 ** 0.05 *** 0.01) ///
        stats(N r2 spec cluster fe_unit fe_clk fe_dow, ///
              fmt(0 3 0 0 0 0 0) ///
              labels("Observations" "\$R^2\$" "Spec" "Cluster" "Unit FE" "Clock-hour FE" "DOW FE")) ///
        keep(crit_post_y25_piv crit_post_y25 ///
             crit_post_piv crit_y25_piv post_y25_piv ///
             crit_piv post_piv y25_piv ///
             crit_post crit_y25 post_y25 ///
             crit post y25) ///
        order(crit_post_y25_piv crit_post_y25 ///
              crit_post_piv crit_y25_piv post_y25_piv ///
              crit_piv post_piv y25_piv ///
              crit_post crit_y25 post_y25 ///
              crit post y25) ///
        coeflabels(crit_post_y25_piv "crit*post*y25*piv (\$\beta_{1234}\$, DDDD)" ///
                   crit_post_y25     "crit*post*y25 (\$\beta_7\$, DDD)" ///
                   crit_post_piv     "crit*post*piv" ///
                   crit_y25_piv      "crit*y25*piv" ///
                   post_y25_piv      "post*y25*piv" ///
                   crit_piv          "crit*piv" ///
                   post_piv          "post*piv" ///
                   y25_piv           "y25*piv" ///
                   crit_post         "crit*post" ///
                   crit_y25          "crit*y25" ///
                   post_y25          "post*y25" ///
                   crit              "crit" ///
                   post              "post" ///
                   y25               "y25") ///
        mtitles("DDD" "DDDD") ///
        title("DDD and DDDD on \$q_2\$ (intraday MWh/unit-hour), `window' window.")

    esttab DDD DDDD using "${COEFS}/ddd_firm_q2_`window'.csv", replace ///
        cells("b(fmt(4)) se(fmt(4)) p(fmt(4))") plain
}

log close
