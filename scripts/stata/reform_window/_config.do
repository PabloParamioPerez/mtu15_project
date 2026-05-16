* ============================================================================
* _config.do  --  Globals + paths for the reform-window DDD do-files
* Sourced by every other .do in this folder. Do not run on its own.
* LAST-AUDIT: 2026-05-16
* ============================================================================

* Repo root (one level above /scripts/) - adjust if you move this file
global REPO    "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
global PANELS  "${REPO}/results/regressions/firm/reform_window/panels"
global LOGS    "${REPO}/results/regressions/firm/reform_window/logs"
global COEFS   "${REPO}/results/regressions/firm/reform_window/coefs"
global TBLDIR  "${REPO}/thesis/paper/tables/reform_window"

* Reform-date constants (mm/dd/yyyy in Stata daily date) -----------------------
global MTU15_IDA_DATE = td(19mar2025)
global MTU15_DA_DATE  = td(01oct2025)

* DA15 sample windows  (reforzada constant ON in both halves) -----------------
global DA15_Y25_PRE_S = td(28apr2025)
global DA15_Y25_PRE_E = td(30sep2025)
global DA15_Y25_POST_S = td(01oct2025)
global DA15_Y25_POST_E = td(13feb2026)
global DA15_Y24_PRE_S = td(28apr2024)
global DA15_Y24_PRE_E = td(30sep2024)
global DA15_Y24_POST_S = td(01oct2024)
global DA15_Y24_POST_E = td(13feb2025)

* IDA15 sample windows  (reforzada constant OFF in both halves) ---------------
global IDA15_Y25_PRE_S = td(09dec2024)
global IDA15_Y25_PRE_E = td(18mar2025)
global IDA15_Y25_POST_S = td(19mar2025)
global IDA15_Y25_POST_E = td(27apr2025)
global IDA15_Y24_PRE_S = td(09dec2023)
global IDA15_Y24_PRE_E = td(18mar2024)
global IDA15_Y24_POST_S = td(19mar2024)
global IDA15_Y24_POST_E = td(27apr2024)

* Hour classes ----------------------------------------------------------------
* crit hours (5-9 + 16-22, in 1-indexed clock-hour from OMIE period numbering):
* In the panel we filter to clockhour in (5,6,7,8,16,17,18,19,20,21,22) for crit==1
* and clockhour in (1,2,3) for crit==0 (flat).  Other hours are dropped.

* Stata-side options ----------------------------------------------------------
set more off
set varabbrev off
capture log close
