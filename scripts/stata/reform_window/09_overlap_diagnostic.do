* ============================================================================
* 09_overlap_diagnostic.do  --  Propensity-score overlap diagnostic for DR-DiD
* INPUT:  panels/{da15,ida15}_{da,ida}_spread.dta
* OUTPUT: figures/working/fig_overlap_{window}_{market}.pdf  (one per cell)
*         coefs/overlap_summary.csv (per-cell pscore quantiles)
*         logs/09_overlap_diagnostic.log
* RATIONALE: DR-DiD uses inverse-probability weighting on the propensity score
*       p(X) = Pr(y25 = 1 | X). If p(X) clusters near 0 or 1 for some
*       observations, IPW weights blow up and inference is unstable. We
*       estimate p(X) by logit on i.dow + i.month, summarise its distribution
*       by treatment group, and flag any cells where p(X) extends near 0 or 1.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/09_overlap_diagnostic.log", text replace

file open f using "${COEFS}/overlap_summary.csv", write replace
file write f "window,market,N,min,p10,p50,p90,max,share_below_0p10,share_above_0p90" _n

foreach window in da15 ida15 {
foreach mkt in da ida {
    di _n "{hline 78}"
    di "Overlap diagnostic: `window' `mkt'"
    di "{hline 78}"

    use "${PANELS}/`window'_`mkt'_spread.dta", clear
    di "N=" _N

    * Estimate propensity score on the same covariate set used by DR-DiD
    capture drop pscore
    logit y25 i.dow i.month
    predict pscore, pr

    sum pscore, detail
    scalar mn = r(min)
    scalar p10 = r(p10)
    scalar p50 = r(p50)
    scalar p90 = r(p90)
    scalar mx = r(max)
    count if pscore < 0.10
    scalar n_low = r(N)
    count if pscore > 0.90
    scalar n_hi = r(N)
    scalar share_low = n_low / _N
    scalar share_hi  = n_hi / _N

    di "  N=" _N "  min=" %5.3f mn "  p10=" %5.3f p10 "  median=" %5.3f p50 ///
       "  p90=" %5.3f p90 "  max=" %5.3f mx
    di "  share with pscore < 0.10: " %5.3f share_low
    di "  share with pscore > 0.90: " %5.3f share_hi

    local mn_s : di %5.3f mn
    local p10_s : di %5.3f p10
    local p50_s : di %5.3f p50
    local p90_s : di %5.3f p90
    local mx_s : di %5.3f mx
    local sl_s : di %5.3f share_low
    local sh_s : di %5.3f share_hi
    file write f "`window',`mkt',`=_N',`mn_s',`p10_s',`p50_s',`p90_s',`mx_s',`sl_s',`sh_s'" _n

    * Side-by-side histograms by treatment group
    twoway (histogram pscore if y25 == 0, color(blue%30) bin(30)) ///
           (histogram pscore if y25 == 1, color(red%30)  bin(30)), ///
           legend(order(1 "2024 placebo" 2 "2025 treatment")) ///
           xtitle("Propensity score {it:p}(X) = Pr(y25=1 | dow, month)") ///
           ytitle("Density") ///
           title("Overlap diagnostic: `window' `mkt'") ///
           xline(0.10, lpattern(dash)) xline(0.90, lpattern(dash)) ///
           graphregion(color(white))
    graph export "${REPO}/figures/working/fig_overlap_`window'_`mkt'.pdf", replace
}
}

file close f
log close
