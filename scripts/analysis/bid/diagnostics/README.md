# BSTS pre-window diagnostics

Three scripts that back the methodological discussion in `thesis/paper/thesis.tex` (§6.3 "Pre-window choice" and Appendix A.6 "Solar-coefficient symmetry test"). Each is self-contained and reads from `data/derived/panels/bsts_quantities_panel.parquet` and `data/derived/panels/bsts_per_session_panel.parquet`.

Run with `Rscript scripts/analysis/bid/diagnostics/<file>.R` from the repo root.

## `bsts_solar_real_vs_placebo.R`

Source for **Table A.6a** (Solar-coefficient symmetry test). Runs the pooled IDA-price BSTS under five (pre-window, arm) combinations — LONG real, LONG placebo 2024, LONG placebo 2026, SHORT real, SHORT placebo 2026 — and reports the effect, the solar covariate posterior (mean / SD / inclusion probability), the wind and gas coefficients for context, and the pre-window solar range. Also runs the same on the DA cross-market spillover.

Diagnostic finding: under the LONG pre-window, real and placebo posteriors absorb near-identical solar surges through fully-included solar coefficients of comparable magnitude — the symmetry the joint placebo-net test requires. Under the SHORT pre-window the solar coefficient's spike-and-slab inclusion probability drops to ~0.48 in the real arm; the post-window renewable surge is partly misattributed to ID15.

## `bsts_per_session_longpre.R`

Source for **Table A.6b** (per-IDA-session and pooled BSTS, ID15 IDA price). Runs the same BSTS independently on each of the three IDA-session daily price series under the LONG pre-window (2024-06-14 onwards), for the real cutover and the 2026 same-calendar placebo. Pooled IDA-price BSTS reported alongside for cross-check.

Diagnostic finding: per-session and pooled estimates agree at ~−45 EUR/MWh under the LONG specification (vs the ~−80/session reported in the earlier draft, which inherited the short post-ISP15 pre-window bias documented in Table A.6a).

## `bsts_extended_pre.R`

Robustness for the **LONG pre-window choice in §6.3**. Compares three pre-window depths — LONG (post-IDA-reform, 2024-06-14), EXTENDED (pre-IDA-reform included, 2023-06-14), MAX (full panel, 2022-01-01) — on both the IDA price (own-market) and the DA price (cross-market spillover).

Diagnostic finding: the point estimate is stable at −33 to −38 EUR/MWh across the three depths, but uncertainty (CI width) grows as the pre-window extends through more diverse market regimes (energy crisis, the 6→3 IDA session reform). The LONG window is the Goldilocks choice: long enough to identify the renewable coefficients, short enough to avoid the cross-regime variance.
