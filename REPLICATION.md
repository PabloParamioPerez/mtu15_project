# Replication map — `thesis/paper/thesis.tex`

Every numbered exhibit in the thesis and the analysis script that produces it.
Scripts live under `scripts/analysis/<topic>/`; figures are written to
`figures/thesis/` (the paper's `\graphicspath`); regression numbers are produced
as CSVs under `results/regressions/<topic>/` and typeset by hand into the inline
`tabular` blocks. The `.tex` source also carries a `% Source: …` comment above
each exhibit pointing to the same script.

## Running

- Python: `uv run python scripts/analysis/<topic>/<file>.py`
- R: `Rscript scripts/analysis/<topic>/<file>.R` (uses `arrow`, `CausalImpact`, `sandwich`, `lmtest`)
- Data lives on an external SSD symlinked at `data/raw|processed`; derived panels under `data/derived/panels/` are the direct inputs to most scripts.
- All paths in scripts are repo-relative; run from any directory.

## Figures

| Label | Figure file | Source script |
|---|---|---|
| `fig:ccgt-pt-main` | `fig_ccgt_pt_main.pdf` | `bid/fig_ccgt_pt_main.py` |
| `fig:efficiency_gains` | `efficiency_gains_timeseries.pdf` | `balancing/efficiency_gains_timeseries.py` |
| `fig:parallel-trends-sigma-p` | `fig_bid_shape_pt_*_sigma_hhi.pdf` (×4) | `bid/bid_shape_parallel_trends_fig.py` |
| `fig:parallel-trends-abgN` | `fig_bid_shape_pt_*_beta_gamma.pdf` (×4) | `bid/bid_shape_parallel_trends_fig.py` |
| `fig:ccgt-offer-type` | `fig_ccgt_offer_type.pdf` | `bid/fig_ccgt_offer_type.py` |
| `fig:bid-curves-by-tech`(`-flat`) | `fig_bid_curves_by_tech(_flat).pdf` | `bid/fig_bid_curves_by_tech.py` |
| `fig:buy-vs-sell-curves` | `fig_buy_vs_sell_curves.pdf` | `bid/fig_buy_vs_sell_curves.py` |
| `fig:ree_intervention_by_tech` | `fig_programs_by_tech_weekly.pdf` | `firm/programs_weekly_by_tech.py` |
| `fig:ree_intervention_other_tech` | `fig_programs_by_tech_other_weekly.pdf` | `firm/programs_weekly_by_tech_other.py` |
| `fig:parallel-trends-sigma-p-per-session` | `fig_parallel_trends_sigma_p_per_session.pdf` | `bid/fig_parallel_trends_sigma_p_per_session.py` |

Schematic figures drawn in-document with TikZ (no script): `fig:market-structure`,
`fig:bid_internal:timeline`, `fig:model-structure`, `fig:within-hour-state`,
`fig:ladder-quantizer`, `fig:in-band-bandwidth`.

## Tables

| Label | What it reports | Source script(s) |
|---|---|---|
| `tab:bandwidths` | in-band bandwidth δ = p90−p50 per (window, market) | values read from the MCP distribution; no standalone script |
| `tab:price-specs` | clearing-price effect by spec (OLS daily/hourly, BSTS) | `bid/ols_price_full_controls.R`, `bid/ols_price_hourly.R`, `bid/bsts_daily_year_interactions.R`, `bid/bsts_daily_quadratic.R` |
| `tab:margin-channel` | residual-demand slope change + margin index | `bid/build_per_firm_residual_demand_slope.py`, `bid/bsts_per_firm_b.R`, `bid/build_per_firm_strategic_markup.py` |
| `tab:bsts-buy-share` / `tab:wedge-sd` | DA−IDA wedge mean and within-day SD by hour class | `bid/bsts_wedge_hour_class.R` |
| `tab:spec-c` | bid-shape DiD on σ_p and HHI_tr | `bid/mtu15_critical_flat_did.py` |
| `tab:bsts-imbalance-penalty` | imbalance settlement prices and volume (OLS + BSTS) | `bid/bsts_imbalance_penalty.R`, `bid/ols_imbalance.py` |
| `tab:comparative` | theory comparative statics across the reform path | hand-built from the model (no script) |
| `tab:pretrend-placebo` | pre-only midpoint placebo for the bid-shape DiD | `bid/run_pre_only_placebo_p90.py` |
| `tab:bandwidth-robustness` | bid-shape DiD at δ = 100, 140, 200 | `bid/bandwidth_robustness_did.py` |
| `tab:bsts-per-session` | per-IDA-session price BSTS | `bid/bsts_daily_per_session.R` |
| `tab:midday-falsification` | midday-vs-critical falsification | `bid/run_spec_c_did_p90_midday.py` |
| `tab:xb-control` | cross-border-flow control on the bid-shape DiD | `bid/run_xb_control_p90.py` |
| `tab:solar-year-coefs` | per-year solar coefficient from the BSTS posterior | `bid/bsts_solar_year_coefficients.R` |
| `tab:calibrated-solar` | calibrated-solar BSTS counterfactual | `bid/bsts_calibrated_solar.R` |
| `tab:spec-c-longpre-ra` | bid-shape DiD: tight, long-pre, RA-DiD | `bid/spec_c_long_pre_ra_did.py` |
| `tab:demand-did` | demand-side (buy-curve) bid-shape DiD | `bid/slope_did_demand.py` |
| `tab:perfirm-ccgt` | per-CCGT-firm bid-shape DiD | `bid/mtu15_critical_flat_did.py` |
| `tab:residual-demand-slope` / `tab:per-firm-residual-demand` | empirical residual-demand slope b at MCP, pre vs post | `bid/build_per_firm_residual_demand_slope.py`, `bid/residual_demand_pre_vs_post.py` |
| `tab:per-session-bmt` | per-IDA-session slope b by bandwidth | `bid/build_per_session_bmt_robustness.py` |
| `tab:strategic-markup` | margin index q_strat/b per firm | `bid/build_per_firm_strategic_markup.py` |
| `tab:demand-buy-share` | continuous-market response at the three reforms | `bid/bsts_continuous.R` |
| `tab:bsts-ajuste-costs` | per-channel BSTS on REE ajuste costs | `bid/bsts_ajuste_costs.R` |
| `tab:ida-activity-fase1` | post-DA15 IDA activity rise | `bid/claim_D_ida_activity_post_da15.py` |
| `tab:per-session-ida-bidshape` | per-session IDA CCGT bid shape | `bid/claim_C_per_session_bid_shape.py` |
| `sec:5-nuclear-pdbf` | nuclear PDBF/PHF BSTS (in-text figures) | `bid/bsts_nuclear_pdbf.R` |

The critical/flat partition (§4.4, `sec:data:partition`) is built by
`balancing/partition_cv_sigma.py`; the before-after-vs-DiD diagnostic behind the
§5.2 rationale is `bid/before_after_vs_did.py`.

Labels `tab:specB-id15`, `tab:specB-da15`, `fig:bsts-id15`, `fig:bsts-da15` are
legacy `\phantomsection` cross-reference anchors, not standalone exhibits.
