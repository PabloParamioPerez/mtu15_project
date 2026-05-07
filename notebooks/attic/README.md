# Archived notebooks

Material superseded by more focused or more rigorous analyses in the active `notebooks/eda/` stack, or whose role has been absorbed by `/CLAIMS_LEDGER.md` and `notebooks/memos/_modelling_track.md`. Preserved for reference and for the viva trail; do not expect them to stay up to date with the thesis narrative.

Three earlier tracking documents (`_modelable_patterns.md`, `_open_robustness_queue.md`, `_robustness_summary.md`) were also archived here on 2026-04-25 and then deleted on 2026-05-05 during the post-pivot cleanup. Their content was fully migrated into `/CLAIMS_LEDGER.md` and `notebooks/memos/_modelling_track.md` before deletion.

## Notebooks

| File | Original role | Status |
|---|---|---|
| `01_market_statistics.ipynb` | Price spot-validation, structural statistics, within-day and monthly profiles, IDA / XBID / interconnection overview | First-pass EDA. Context only; thesis does not cite directly. |
| `02_bidding_behaviour.ipynb` | Offer-type anatomy, DA↔IDA spread, program reconciliation, bid-price anatomy, XBID book, rebid anatomy, strategic cross-market behaviour | First-pass EDA. Content folded into `03_reform_narrative`, `07_main_regression`, and `13_bid_liquidity_revenue` during consolidation. |
| `04_imbalance_balancing.ipynb` | Descriptive EDA of ENTSO-E balancing families (A85 prices, A86 volumes, A84 activated, A69 forecast, A74 revisions) + §8 H3 reserve-substitution test | Archived 2026-04-25. Headline descriptive content on A85/A86/A84/A69 is now formally event-studied in `11_outcome_audit`. The unique §7 intraday-forecast-revision analysis and §8 H3 test produced no thesis-grade finding and are preserved here. |
| `06_attenuation_dashboard.ipynb` | Descriptive CCGT conduct-gap collapse (Big-4 − Fringe) at MTU15-IDA; within-hour DA price dispersion; bid-level Ito–Reguant slope | Archived 2026-04-25. Central CCGT conduct-gap finding absorbed by unit FE in `07_main_regression` §9 (no within-unit shift; aggregate collapse is composition). Bid-level behaviour covered more cleanly by `13_bid_liquidity_revenue` §1. |
| `07_main_regression.ipynb` | Formal TWFE-DiD regressions targeting Big-4 × Post-MTU15-IDA ATT (X1 in `CLAIMS_LEDGER.md`). | Archived 2026-04-25 (Batch B). Identification fails per Phase B audit in `_identification_target.md`: parallel-trends fail (§3 event-study), analytical placebos fail (§6), randomization p=0.43 (§11), treatment-date sweep peaks 2024-07 not 2024-12 (§12). Retained as methodological documentation. |
| `08_wind_iv.ipynb` | Wind forecast-error IV (Ito–Reguant) targeting Big-4 strategic-responsiveness slope contraction (X2, X3, W2). | Archived 2026-04-25 (Batch B). Aggregate slope contraction killed by ex-nuclear robustness (D9–D10 in `_identification_target.md`). GE×CCGT signed flip killed by placebo sweep across the 3-sess + ISP15 window (D12). Nuclear pattern wounded — real but non-strategic, candidate-uncertain. |
