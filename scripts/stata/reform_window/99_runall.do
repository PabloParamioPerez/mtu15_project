* ============================================================================
* 99_runall.do  --  Orchestrator for the reform-window DDD pipeline
* Usage:
*   stata-mp -b do scripts/stata/reform_window/99_runall.do
* This file does NOT rebuild the .dta panels; run _build_panels.py first.
* ============================================================================

* Hardcode repo path here so the chain bootstraps before _config.do runs.
local REPO_DIR "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"

do "`REPO_DIR'/scripts/stata/reform_window/_config.do"

* Headline OLS-FE DDD
do "`REPO_DIR'/scripts/stata/reform_window/01_ddd_da15.do"
do "`REPO_DIR'/scripts/stata/reform_window/02_ddd_ida15.do"

* CS / DR-DiD robustness on prices
do "`REPO_DIR'/scripts/stata/reform_window/03_csdid_da15.do"
do "`REPO_DIR'/scripts/stata/reform_window/04_csdid_ida15.do"

* Firm-level outcomes: DDD + DDDD (pivotal moderator) on q_1 and q_2
do "`REPO_DIR'/scripts/stata/reform_window/05_ddd_firm_q1.do"
do "`REPO_DIR'/scripts/stata/reform_window/06_ddd_firm_q2.do"

* Naive before-after vs DDD: seasonal-bias decomposition
do "`REPO_DIR'/scripts/stata/reform_window/07_ba_comparison.do"

* IDA-price robustness: closest-to-delivery session vs Session 1
do "`REPO_DIR'/scripts/stata/reform_window/08_csdid_ida_latest.do"

* DR-DiD propensity-score overlap diagnostic
do "`REPO_DIR'/scripts/stata/reform_window/09_overlap_diagnostic.do"

* Post-process esttab output (math-mode escapes + labels)
shell uv run python "`REPO_DIR'/scripts/stata/reform_window/_postprocess_tables.py"

di "==================================================="
di "All four do-files completed. Inspect logs in ${LOGS}"
di "Tables written to ${TBLDIR}"
di "==================================================="
