* ============================================================================
* 08_csdid_ida_latest.do  --  DR-DiD on IDA prices using closest-to-delivery
*                              session price (robustness to Session 1).
* INPUT:  panels/{da15,ida15}_ida_latest_spread.dta
* OUTPUT: thesis/paper/tables/reform_window/tab_csdid_ida_latest_compare.tex
*         coefs/csdid_ida_latest_{window}.csv
*         logs/08_csdid_ida_latest.log
* RATIONALE: Session 1 changed role at the MIBEL->SIDC IDA reform (2024-06-14).
*   Using the closest-to-delivery session per (date, period) is MTU- and
*   session-count-invariant and removes the "which Session 1 do you mean" gap.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/08_csdid_ida_latest.log", text replace

tempname tbl
file open `tbl' using "${TBLDIR}/tab_csdid_ida_latest_compare.tex", write replace
file write `tbl' "\begin{table}[H]\centering\footnotesize" _n
file write `tbl' "\setlength{\tabcolsep}{4pt}" _n
file write `tbl' "\caption{DR-DiD on IDA prices: Session 1 (baseline, in Table~\ref{tab:reform_ddd_summary}) vs closest-to-delivery session (robustness). For each (date, period) the closest-to-delivery price is the row with the maximum \code{session_number}: under SIDC, IDA-3 for afternoon and IDA-2 for morning; under MIBEL, Sessions 4--6 for D-day afternoons and Session 3 for morning. This robustness removes the MIBEL-to-SIDC redefinition of Session 1. Covariates: \code{i.dow + i.month}. Standard errors in parentheses. Stars: \$^{*}\$ 0.10, \$^{**}\$ 0.05, \$^{***}\$ 0.01.}" _n
file write `tbl' "\label{tab:csdid_ida_latest_compare}" _n
file write `tbl' "\begin{tabular}{lccc}" _n
file write `tbl' "\toprule" _n
file write `tbl' "Window & ATT\$_{\text{crit}}\$ & ATT\$_{\text{flat}}\$ & DDD \\" _n
file write `tbl' "\midrule" _n

foreach window in da15 ida15 {

    di _n "{hline 78}"
    di "DR-DiD on IDA latest-session price, `window' window"
    di "{hline 78}"

    use "${PANELS}/`window'_ida_latest_spread.dta", clear
    di "N=" _N
    sum price_crit price_flat, separator(0)
    capture drop pgroup ifcatt iffatt if_diff
    gen byte pgroup = post + 1

    drdid price_crit i.dow i.month, time(pgroup) tr(y25) stub(ifc)
    scalar att_crit = el(e(b), 1, 1)
    scalar se_crit  = sqrt(el(e(V), 1, 1))

    drdid price_flat i.dow i.month, time(pgroup) tr(y25) stub(iff)
    scalar att_flat = el(e(b), 1, 1)
    scalar se_flat  = sqrt(el(e(V), 1, 1))

    gen if_diff = ifcatt - iffatt
    qui sum if_diff
    scalar ddd     = att_crit - att_flat
    scalar se_ddd  = sqrt(r(Var) / r(N))
    scalar z_ddd   = ddd / se_ddd
    scalar p_ddd   = 2 * (1 - normal(abs(z_ddd)))

    di "  ATT_crit = " %7.3f att_crit "  (SE = " %5.3f se_crit ")"
    di "  ATT_flat = " %7.3f att_flat "  (SE = " %5.3f se_flat ")"
    di "  DDD     = " %7.3f ddd "  (SE = " %5.3f se_ddd ", p = " %5.3f p_ddd ")"

    local star_ddd  = cond(p_ddd  < 0.01, "***", cond(p_ddd  < 0.05, "**", cond(p_ddd  < 0.10, "*", "")))
    local att_crit_s : di %7.2f att_crit
    local se_crit_s  : di %5.2f se_crit
    local att_flat_s : di %7.2f att_flat
    local se_flat_s  : di %5.2f se_flat
    local ddd_s      : di %7.2f ddd
    local se_ddd_s   : di %5.2f se_ddd
    local win_lbl = cond("`window'"=="da15", "DA15", "IDA15")

    file write `tbl' "`win_lbl' " ///
        "& `att_crit_s' \scriptsize{(`se_crit_s')} " ///
        "& `att_flat_s' \scriptsize{(`se_flat_s')} " ///
        "& `ddd_s'`star_ddd' \scriptsize{(`se_ddd_s')} \\" _n

    file open f using "${COEFS}/csdid_ida_latest_`window'.csv", write replace
    file write f "metric,b,se,p" _n
    file write f "att_crit,`att_crit_s',`se_crit_s'," _n
    file write f "att_flat,`att_flat_s',`se_flat_s'," _n
    local p_ddd_s : di %5.3f p_ddd
    file write f "ddd,`ddd_s',`se_ddd_s',`p_ddd_s'" _n
    file close f
}

file write `tbl' "\bottomrule" _n
file write `tbl' "\end{tabular}" _n
file write `tbl' "\end{table}" _n
file close `tbl'

log close
