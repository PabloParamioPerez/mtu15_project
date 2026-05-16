* ============================================================================
* 02_ddd_ida15.do  --  OLS-FE DDD on the IDA15 reform window for ALL outcomes
* INPUT:  panels/ida15_{da,ida}_period.dta
* OUTPUT: thesis/paper/tables/reform_window/tab_ddd_ida15_{da,ida}.tex
*         coefs/ddd_ida15_{da,ida}.csv
*         logs/02_ddd_ida15.log
* Outcomes: clearing prices (DA and IDA Session 1). Both tested in the same
*   IDA15 window: (a) the direct IDA effect; (b) spillover onto DA prices.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/02_ddd_ida15.log", text replace

local window "ida15"
foreach mkt in da ida {

    local outcome : di cond("`mkt'"=="da", "DA clearing price", "IDA Session 1 clearing price")

    di _n "{hline 78}"
    di "OLS-FE DDD on `outcome' in `window' window"
    di "{hline 78}"

    use "${PANELS}/`window'_`mkt'_period.dta", clear
    di "N=" _N
    tab crit post if y25 == 1, missing
    tab crit post if y25 == 0, missing

    reghdfe price crit post y25 crit_post crit_y25 post_y25 crit_post_y25, ///
        absorb(clockhour dow) vce(cluster date_stata)
    estimates store M1
    estadd local fe_clk "Yes"
    estadd local fe_dow "Yes"
    estadd local fe_mon "No"
    estadd local fe_date "No"
    estadd local cluster "Date"

    reghdfe price crit post y25 crit_post crit_y25 post_y25 crit_post_y25, ///
        absorb(clockhour dow month) vce(cluster date_stata)
    estimates store M2
    estadd local fe_clk "Yes"
    estadd local fe_dow "Yes"
    estadd local fe_mon "Yes"
    estadd local fe_date "No"
    estadd local cluster "Date"

    reghdfe price crit crit_post crit_y25 crit_post_y25, ///
        absorb(clockhour dow date_stata) vce(cluster date_stata)
    estimates store M3
    estadd local fe_clk "Yes"
    estadd local fe_dow "Yes"
    estadd local fe_mon "n/a"
    estadd local fe_date "Yes"
    estadd local cluster "Date"

    esttab M1 M2 M3 using "${TBLDIR}/tab_ddd_`window'_`mkt'.tex", ///
        replace booktabs ///
        label nogap ///
        nonotes ///
        cells("b(fmt(3) star) se(fmt(3) par)") ///
        starlevels(* 0.10 ** 0.05 *** 0.01) ///
        stats(N r2 fe_clk fe_dow fe_mon fe_date cluster, ///
              fmt(0 3 0 0 0 0 0) ///
              labels("Observations" "\$R^2\$" "Clock-hour FE" "DOW FE" "Month FE" "Date FE" "Cluster")) ///
        keep(crit_post_y25 crit_post crit_y25 post_y25 crit post y25) ///
        order(crit_post_y25 crit_post crit_y25 post_y25 crit post y25) ///
        coeflabels(crit_post_y25 "crit*post*y25 (\$\beta_7\$, DDD)" ///
                   crit_post "crit*post" crit_y25 "crit*y25" post_y25 "post*y25" ///
                   crit "crit" post "post" y25 "y25") ///
        mtitles("Sparse" "Month FE" "Date FE") ///
        title("OLS-FE DDD: `outcome' in IDA15 window. SE clustered at date.")

    esttab M1 M2 M3 using "${COEFS}/ddd_`window'_`mkt'.csv", replace ///
        cells("b(fmt(4)) se(fmt(4)) p(fmt(4))") ///
        keep(crit_post_y25 crit_post crit_y25 post_y25 crit post y25) ///
        plain
}

log close
