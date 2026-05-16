* ============================================================================
* 04_csdid_ida15.do  --  DR-DiD on the IDA15 reform window for ALL outcomes
* INPUT:  panels/ida15_{da,ida}_spread.dta
* OUTPUT: thesis/paper/tables/reform_window/tab_csdid_ida15_{da,ida}.tex
*         coefs/csdid_ida15_{da,ida}.csv
*         logs/04_csdid_ida15.log
* See 03_csdid_da15.do for methodology.
* ============================================================================

do "${REPO}/scripts/stata/reform_window/_config.do"

log using "${LOGS}/04_csdid_ida15.log", text replace

local window "ida15"
foreach mkt in da ida {

    local outcome : di cond("`mkt'"=="da", "DA clearing price", "IDA Session 1 clearing price")

    di _n "{hline 78}"
    di "DR-DiD: `outcome' in `window' window  (prices in levels, decomposed)"
    di "{hline 78}"

    use "${PANELS}/`window'_`mkt'_spread.dta", clear
    di "N=" _N
    sum price_crit price_flat, separator(0)
    capture drop pgroup ifcatt iffatt if_diff
    gen byte pgroup = post + 1
    label define pgrouplbl 1 "Pre" 2 "Post", replace
    label values pgroup pgrouplbl

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
    scalar ci_lo   = ddd - 1.96 * se_ddd
    scalar ci_hi   = ddd + 1.96 * se_ddd

    di "  ATT_crit = " %7.3f att_crit "  (SE = " %5.3f se_crit ")"
    di "  ATT_flat = " %7.3f att_flat "  (SE = " %5.3f se_flat ")"
    di "  DDD     = " %7.3f ddd "  (SE = " %5.3f se_ddd ", z=" %5.2f z_ddd ", p=" %5.3f p_ddd ")"
    di "  95% CI  = [" %7.3f ci_lo ", " %7.3f ci_hi "]"

    local att_crit_s : di %7.3f att_crit
    local se_crit_s  : di %5.3f se_crit
    local att_flat_s : di %7.3f att_flat
    local se_flat_s  : di %5.3f se_flat
    local ddd_s      : di %7.3f ddd
    local se_ddd_s   : di %5.3f se_ddd
    local p_ddd_s    : di %5.3f p_ddd
    local ci_lo_s    : di %7.3f ci_lo
    local ci_hi_s    : di %7.3f ci_hi
    local n_s        : di %5.0f r(N)

    file open f using "${TBLDIR}/tab_csdid_`window'_`mkt'.tex", write replace
    file write f "\begin{table}[H]\centering\small" _n
    file write f "\caption{DR-DiD on `outcome' in IDA15 window, prices in levels (Sant'Anna--Zhao 2020). 2$\times$2 DiD per hour-class, covariates \texttt{i.dow + i.month}; DDD is the difference of ATTs with joint SE from the influence-function difference.}\label{tab:reform_csdid_`window'_`mkt'}" _n
    file write f "\begin{tabular}{lrr}" _n
    file write f "\toprule" _n
    file write f "Coefficient & Estimate & SE \\\\" _n
    file write f "\midrule" _n
    file write f "ATT\$_{\text{crit}}\$ & `att_crit_s' & (`se_crit_s') \\\\" _n
    file write f "ATT\$_{\text{flat}}\$ & `att_flat_s' & (`se_flat_s') \\\\" _n
    file write f "DDD = ATT\$_{\text{crit}}\$ \$-\$ ATT\$_{\text{flat}}\$ & `ddd_s' & (`se_ddd_s') \\\\" _n
    file write f "\midrule" _n
    file write f "p-value (DDD)        & `p_ddd_s' & \\\\" _n
    file write f "95\% CI (DDD)        & [`ci_lo_s', `ci_hi_s'] & \\\\" _n
    file write f "N (dates)            & `n_s' & \\\\" _n
    file write f "Outcome              & `outcome' & \\\\" _n
    file write f "Window               & IDA15 (reform = 2025-03-19) & \\\\" _n
    file write f "\bottomrule" _n
    file write f "\end{tabular}" _n
    file write f "\end{table}" _n
    file close f

    file open g using "${COEFS}/csdid_`window'_`mkt'.csv", write replace
    file write g "window,market,metric,b,se,p,ci_lo,ci_hi" _n
    file write g "`window',`mkt',att_crit,`att_crit_s',`se_crit_s',,," _n
    file write g "`window',`mkt',att_flat,`att_flat_s',`se_flat_s',,," _n
    file write g "`window',`mkt',ddd,`ddd_s',`se_ddd_s',`p_ddd_s',`ci_lo_s',`ci_hi_s'" _n
    file close g
}

log close
