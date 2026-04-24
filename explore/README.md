# `explore/` — working notebooks and narrative documents

Not thesis output. These notebooks and markdown notes carry the analysis
that feeds the thesis document (separate directory).

## Notebook stack, by role in the narrative

The three-layer empirical backbone (per `_identification_target.md` Phase D16):

| Layer | Notebook | What it carries |
|---|---|---|
| **System** | [11_outcome_audit](11_outcome_audit.ipynb) | Four concordant ENTSO-E outcomes (A87, A86, A85, A84) event-studied across reform regimes; ISP15 peak + MTU15-DA moderation pattern. |
| **Structural** | [12_structural_markup](12_structural_markup.ipynb) | Cournot-Nash Lerner indices per Big-4 firm × regime. GE 5%→35%→10%. |
| **Behavioural** | [13_bid_liquidity_revenue](13_bid_liquidity_revenue.ipynb) | IDA offer prices, XBID liquidity, firm revenue — all peak at DA60/ID15. |

Thesis-bound figure set (**provisional**):

| File | Status |
|---|---|
| [14_thesis_figures](14_thesis_figures.ipynb) | 3 approved figures + PDF/PNG exports in `../figures/`. Figures stay only if supporting narrative holds. |

Supporting / foundational evidence:

| Notebook | Role |
|---|---|
| [03_reform_narrative](03_reform_narrative.ipynb) | Descriptive bedrock: DA-IDA wedge, within-hour price dispersion, ΔQ compression in low-wind terciles, cross-regime summary. |
| [05_engineering_decomposition](05_engineering_decomposition.ipynb) | Rules out four mechanical explanations of ΔQ compression (profile-matching, ramp-lumpiness, reserve substitution, storage internalisation). Load-bearing: without this, the behavioural residual is not isolated. |
| [07_main_regression](07_main_regression.ipynb) | Formal TWFE-DiD with documented identification caveats. Parallel-trends fail, placebos fail, treatment-date sweep fails. Justifies the pivot to structural measures (nb12) and system-level evidence (nb11). |
| [08_wind_iv](08_wind_iv.ipynb) | Wind forecast-error IV, Ito-Reguant style. Narrowed to GE×CCGT signed flip; §10 placebo shows flip is not localised to ISP15. Supplementary evidence. |
| [09_bid_shape_eda](09_bid_shape_eda.ipynb) | Bid-structure EDA with systematic unit-level audit. Six aggregate findings narrowed under decomposition; five survive as within-unit or participation-shift claims (H15, H19, H20, H21, H22). |
| [10_demand_side_eda](10_demand_side_eda.ipynb) | Diagnoses the March 2025 DA buy-offer collapse as Rule 28.8 elimination (CNMC 28-Feb-2025), not MTU15-IDA. Load-bearing for interpreting firm-revenue composition shifts in nb13 §3. |

## Narrative documents (markdown)

| File | Role |
|---|---|
| [RESEARCH_LOG.md](RESEARCH_LOG.md) | Full project journal: hypotheses register, methods attempted, notebook index, findings established vs withdrawn, current state + next steps. The canonical reference when picking up the project. |
| [_identification_target.md](_identification_target.md) | Working identification narrative. Phases A1-A5 (comparisons, assumptions, data needs), Phase B (nb07 audit), Phase C (decision), Phases D1-D16 (wind-IV closure → two-layer pivot → three-layer final framing). |
| [_robustness_summary.md](_robustness_summary.md) | Results of four robustness checks on the nb12 Lerner finding: bootstrap CIs (survives), slope-window sensitivity (survives), placebo reform dates (fails for GE, passes for IB/GN/HC — reframing needed), hour-of-day profile (strengthens mechanism story). |

## Archived notebooks

See [archive/README.md](archive/README.md). Superseded at various stages:

- `01_market_statistics`, `02_bidding_behaviour` — first-pass exploration. Content folded into nb03, nb05, nb07 during consolidation.
- `04_imbalance_balancing` — ENTSO-E descriptive EDA of A85/A86/A84/A69 with cross-family scatter and forecast-revision analysis. Headline descriptive findings are formally event-studied in nb11; nb04's unique forecast-revision analysis (§7) and inconclusive H3 reserve-substitution test (§8) did not produce thesis-grade findings.
- `06_attenuation_dashboard` — descriptive CCGT conduct-gap collapse at MTU15-IDA. Finding absorbed by unit FE in nb07 §9 (no within-unit change), and covered more cleanly by nb13 §1 (IDA bid-weighted offer price event-study).

## Reproducing the analysis

Derived panels in `data/derived/` are gitignored. Rebuild with
`scripts/analysis/`:

1. `build_supply_slope_panel.py` → `supply_slope_hourly.parquet`
2. `build_firm_lerner_panel.py` → `firm_lerner_hourly.parquet` (depends on 1)
3. `build_firm_bid_revenue.py` → `firm_ida_bid_sell_panel.parquet` + `firm_revenue_panel.parquet`
4. `build_xbid_liquidity.py` → `xbid_liquidity_hourly.parquet`
5. `sync_parse_a44_fr.py` → `data/processed/entsoe/prices/fr_da_all.parquet`
6. `overnight_robustness.py` → `{bootstrap,slope_sensitivity,placebo,hour_of_day}_lerner*.parquet`

Pipeline scripts for OMIE + ENTSO-E raw data: `scripts/pipelines/`.
