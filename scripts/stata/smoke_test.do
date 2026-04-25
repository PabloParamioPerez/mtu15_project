* smoke_test.do
* Verifies the Stata/MP 17 command-line stack by opening a round-tripped
* .dta file that was converted from the project's reform_panel.parquet.
*
* Run from project root (batch mode, no interactive window):
*   /Applications/Stata/StataMP.app/Contents/MacOS/stata-mp -b -q do scripts/stata/smoke_test.do
*
* If you symlink stata-mp onto PATH (see scripts/stata/README.md), just:
*   stata-mp -b -q do scripts/stata/smoke_test.do
*
* Expects data/derived/panels/reform_panel.dta to exist. Build it first with:
*   uv run python -c "import pandas as pd; pd.read_parquet('data/derived/panels/reform_panel.parquet').to_stata('data/derived/panels/reform_panel.dta', version=118, write_index=False)"

clear all
set more off

use "data/derived/panels/reform_panel.dta", clear

display ""
display "Stata smoke test — Stata " c(version) ", `c(stata_version)' running"
display "Loaded: data/derived/panels/reform_panel.dta"
display "Observations: " c(N)
display "Variables:    " c(k)

* Descriptives matching the R smoke test.
keep if wind_tercile == "low"

tabstat dq_mwh abs_dq_mwh, statistics(mean N) by(group) format(%9.1f)

display ""
display "If this printed without errors, the Stata + .dta interop stack is working."
