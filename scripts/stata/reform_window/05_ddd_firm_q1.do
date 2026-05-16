* ============================================================================
* 05_ddd_firm_q1.do  --  OLS-FE DDD + DDDD on firm-level q_1 outcome
* INPUT:  panels/{da15,ida15}_q1.dta  (one row per firm-unit-date-clockhour)
* OUTPUT: thesis/paper/tables/reform_window/tab_ddd_firm_q1_{da15,ida15}.tex
*         coefs/ddd_firm_q1_{da15,ida15}.csv
*         logs/05_ddd_firm_q1.log
* OUTCOME: q_1 = DA-cleared MWh per (firm, unit, date, clock-hour), from PDBC.
*          Zero-filled for offline cells; aggregated to clock-hour MWh (so the
*          MTU60-vs-MTU15 period count drops out).
* MODELS:
*   (DDD)   q_1 ~ crit + post + y25 + 3 pairs + crit*post*y25 + FE
*           SE clustered at date.
*   (DDDD)  q_1 ~ crit + post + y25 + 6 pairs + 4 triples + crit*post*y25*piv + FE
*           Pivotal main effect dropped (firm-invariant, absorbed by firm FE).
*           SE clustered at firm (G ~ 9; small-G caveat noted in writeup).
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/05_ddd_firm_q1.log", text replace

foreach window in da15 ida15 {

    di _n "{hline 78}"
    di "q_1 OLS-FE DDD + DDDD on `window' window"
    di "{hline 78}"

    use "${PANELS}/`window'_q1.dta", clear
    di "N=" _N
    sum q1, detail
    tab pivotal post if y25 == 1
    tab pivotal post if y25 == 0

    * --- DDD: pooled sample, no pivotal moderator ------------------------------
    reghdfe q1 crit post y25 ///
                crit_post crit_y25 post_y25 ///
                crit_post_y25, ///
        absorb(firm_id unit_id clockhour dow) vce(cluster date_stata)
    estimates store DDD
    estadd local spec "DDD"
    estadd local cluster "Date"
    estadd local fe_unit "Yes"
    estadd local fe_clk  "Yes"
    estadd local fe_dow  "Yes"

    * --- DDDD: pivotal moderator (clusters at firm; small G caveat) ------------
    reghdfe q1 crit post y25 ///
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

    * --- Export LaTeX table ----------------------------------------------------
    esttab DDD DDDD using "${TBLDIR}/tab_ddd_firm_q1_`window'.tex", ///
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
        title("DDD and DDDD on \$q_1\$ (DA-cleared MWh/unit-hour), `window' window.")

    esttab DDD DDDD using "${COEFS}/ddd_firm_q1_`window'.csv", replace ///
        cells("b(fmt(4)) se(fmt(4)) p(fmt(4))") plain
}

log close
