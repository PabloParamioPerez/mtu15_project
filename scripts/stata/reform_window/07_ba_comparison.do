* ============================================================================
* 07_ba_comparison.do  --  Naive before-after vs DDD: bias decomposition
* INPUT:  panels/{da15,ida15}_{da,ida}_spread.dta
* OUTPUT: thesis/paper/tables/reform_window/tab_ba_vs_ddd.tex
*         coefs/ba_vs_ddd_{window}_{market}.csv
*         logs/07_ba_comparison.log
* GOAL: Quantify how much of the apparent reform effect is seasonal bias that
*       the DDD nets out, and how much survives. For each (window, market)
*       cell we report:
*         (a) within-year naive BA on 2025 spread: spread ~ post (calendar
*             controls only, no placebo).
*         (b) within-year placebo on 2024 spread (same spec, 2024 sample).
*         (c) DDD: spread ~ post + y25 + post*y25 (+ i.dow + i.month).
*       BA - DDD = the placebo's apparent effect = the seasonal bias the DDD
*       absorbs.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/07_ba_comparison.log", text replace

* Open file for the comparison table - assembled across (window, market) loop
tempname tbl
file open `tbl' using "${TBLDIR}/tab_ba_vs_ddd.tex", write replace
file write `tbl' "\begin{table}[H]\centering\footnotesize" _n
file write `tbl' "\setlength{\tabcolsep}{4pt}" _n
file write `tbl' "\caption{Naive before-after (BA) vs DDD on the within-day (crit\$-\$flat) spread, EUR/MWh, all four (window, market) cells. BA: within-year (post \$-\$ pre) on 2025 spread, OLS with \code{i.dow + i.month} calendar controls and date-clustered SE. Placebo BA: same spec on 2024 spread. DDD: 2\$\times\$2 DiD with both years and same calendar controls (the headline OLS-FE coefficient on \$\beta_{\text{post}\times\text{y25}}\$). \emph{Seasonal bias} = BA \$-\$ DDD = what an advisor's naive within-year BA-with-controls attributes to the reform but is really seasonality (identified by the 2024 placebo). Standard errors in parentheses. Stars: \$^{*}~0.10,~^{**}~0.05,~^{***}~0.01\$.}" _n
file write `tbl' "\label{tab:ba_vs_ddd}" _n
file write `tbl' "\begin{tabular}{llcccc}" _n
file write `tbl' "\toprule" _n
file write `tbl' "Window & Outcome & BA 2025 & BA 2024 (placebo) & DDD & Seasonal bias \\" _n
file write `tbl' "       &         & (a)     & (b)               & (c) & (a) \$-\$ (c)   \\" _n
file write `tbl' "\midrule" _n

foreach window in da15 ida15 {
foreach mkt in da ida {
    local outcome : di cond("`mkt'"=="da", "DA price", "IDA-S1 price")

    di _n "{hline 78}"
    di "BA vs DDD on `outcome' (`window' window)"
    di "{hline 78}"

    use "${PANELS}/`window'_`mkt'_spread.dta", clear

    * --- BA: within-year, 2025 only --------------------------------------------
    reg spread post i.dow i.month if y25 == 1, vce(cluster date_stata)
    scalar ba25_b = _b[post]
    scalar ba25_se = _se[post]

    * --- BA: within-year, 2024 placebo only -----------------------------------
    reg spread post i.dow i.month if y25 == 0, vce(cluster date_stata)
    scalar ba24_b = _b[post]
    scalar ba24_se = _se[post]

    * --- DDD: pooled with year x post interaction -----------------------------
    capture drop post_y25
    gen post_y25 = post * y25
    reg spread post y25 post_y25 i.dow i.month, vce(cluster date_stata)
    scalar ddd_b = _b[post_y25]
    scalar ddd_se = _se[post_y25]
    drop post_y25

    * --- Seasonal bias = BA - DDD = the 2024 placebo's apparent effect --------
    scalar bias = ba25_b - ddd_b

    * --- p-values for stars ---------------------------------------------------
    foreach v in ba25 ba24 ddd {
        scalar p_`v' = 2 * (1 - normal(abs(`v'_b / `v'_se)))
    }

    * --- Star formatting ------------------------------------------------------
    local star_ba25 = cond(p_ba25 < 0.01, "***", cond(p_ba25 < 0.05, "**", cond(p_ba25 < 0.10, "*", "")))
    local star_ba24 = cond(p_ba24 < 0.01, "***", cond(p_ba24 < 0.05, "**", cond(p_ba24 < 0.10, "*", "")))
    local star_ddd  = cond(p_ddd  < 0.01, "***", cond(p_ddd  < 0.05, "**", cond(p_ddd  < 0.10, "*", "")))

    di "  BA 2025  = " %7.3f ba25_b "  (SE = " %5.3f ba25_se ", p = " %5.3f p_ba25 ")"
    di "  BA 2024  = " %7.3f ba24_b "  (SE = " %5.3f ba24_se ", p = " %5.3f p_ba24 ")"
    di "  DDD     = " %7.3f ddd_b  "  (SE = " %5.3f ddd_se  ", p = " %5.3f p_ddd  ")"
    di "  Bias    = " %7.3f bias   "  (= BA25 - DDD)"

    * --- Write row -----------------------------------------------------------
    local ba25_s : di %7.2f ba25_b
    local ba25_ses: di %5.2f ba25_se
    local ba24_s : di %7.2f ba24_b
    local ba24_ses: di %5.2f ba24_se
    local ddd_s : di %7.2f ddd_b
    local ddd_ses: di %5.2f ddd_se
    local bias_s : di %7.2f bias
    local win_lbl = cond("`window'"=="da15", "DA15", "IDA15")

    file write `tbl' "`win_lbl' & `outcome' " ///
        "& `ba25_s'`star_ba25' \scriptsize{(`ba25_ses')} " ///
        "& `ba24_s'`star_ba24' \scriptsize{(`ba24_ses')} " ///
        "& `ddd_s'`star_ddd' \scriptsize{(`ddd_ses')} " ///
        "& `bias_s' \\" _n

    * --- CSV per cell --------------------------------------------------------
    file open f using "${COEFS}/ba_vs_ddd_`window'_`mkt'.csv", write replace
    file write f "metric,b,se" _n
    file write f "ba_2025,`ba25_s',`ba25_ses'" _n
    file write f "ba_2024,`ba24_s',`ba24_ses'" _n
    file write f "ddd,`ddd_s',`ddd_ses'" _n
    file write f "bias,`bias_s'," _n
    file close f
}
}

file write `tbl' "\bottomrule" _n
file write `tbl' "\end{tabular}" _n
file write `tbl' "\end{table}" _n
file close `tbl'

log close
