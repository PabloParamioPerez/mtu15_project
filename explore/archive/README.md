# Archived exploratory notebooks

Notebooks that were superseded by more focused or more rigorous analyses in the
active `explore/` stack. Preserved for reference; do not expect them to stay up
to date with the thesis narrative.

| File | Original role | Status |
|---|---|---|
| `01_market_statistics.ipynb` | Price spot-validation, structural statistics, within-day and monthly profiles, IDA / XBID / interconnection overview | First-pass EDA. Context only; thesis does not cite directly. |
| `02_bidding_behaviour.ipynb` | Offer-type anatomy, DA↔IDA spread, program reconciliation, bid-price anatomy, XBID book, rebid anatomy, strategic cross-market behaviour | First-pass EDA. Content folded into `03_reform_narrative`, `07_main_regression`, and `13_bid_liquidity_revenue` during consolidation. |
| `04_imbalance_balancing.ipynb` | Descriptive EDA of ENTSO-E balancing families (A85 prices, A86 volumes, A84 activated, A69 forecast, A74 revisions) + §8 H3 reserve-substitution test | Archived 2026-04-25. Headline descriptive content on A85/A86/A84/A69 is now formally event-studied in `11_outcome_audit`. The unique §7 intraday-forecast-revision analysis and §8 H3 test produced no thesis-grade finding and are preserved here. |
| `06_attenuation_dashboard.ipynb` | Descriptive CCGT conduct-gap collapse (Big-4 − Fringe) at MTU15-IDA; within-hour DA price dispersion; bid-level Ito–Reguant slope | Archived 2026-04-25. Central CCGT conduct-gap finding absorbed by unit FE in `07_main_regression` §9 (no within-unit shift; aggregate collapse is composition). Bid-level behaviour covered more cleanly by `13_bid_liquidity_revenue` §1. |
