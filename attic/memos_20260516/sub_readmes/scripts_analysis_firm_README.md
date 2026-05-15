# `scripts/analysis/firm/` — firm-level strategic conduct

The thesis headline lives here. After the May 5 2026 workshop pivot, the thesis identification is the **within-day critical-vs-flat-hours DiD on dominant-firm signed IDA repositioning** (B12, β₃ = +58.6 MWh/firm-hour at DA15/ID15), with the **fringe placebo** (B13, β₃ = -24.3, opposite-signed) and the **bid-shape ladder enrichment** (B14, dominant CCGT +3.09 tranches in critical hours, 4.85× amplification post-MTU15-DA) as triangulating evidence.

See `_within_market_granularity_model.md` for the theoretical anchor (within-market granularity model: σ²_within(h) is the right ranker for critical hours), `_modelling_track.md` §X for the empirical design, and `CLAIMS_LEDGER.md` rows B12-B14 for status and provenance.

## What lives here

- **`critical_hours_*`** — the headline within-day DiD work (B12, B13). Scripts produce per-regime DiD coefficients, per-tech and per-firm-tech decompositions, conditional-parallel-trends checks, baseline-window sensitivity, supply-side decomposition, ranking of critical hours.
- **`b9_*`** — B9 family (the predecessor of the within-day DiD). Includes `b9_replicated_isp_grain.py` (legacy main regression at MTU15-replicated grain), `b9_canonical_fi_attack.py` (Fabra-Imelda canonical robustness), `b9_robustness_attack.py`, `b9_firm_ISP_native_post_mtu15.py`, `b9_quarter_hour_post_mtu15.py`, `b9_hour_of_day_interaction.py`, `b9_perfirm_q2_figure.py`, etc.
- **`b11_robustness_attack.py`** — B11 (Rule 28.8 elimination) wider-window robustness.
- **`pdbf_*`** — bilateral-channel work (D6, D7, D8 alive; B10, B11 alive; F24 alive; F26 wounded). Substitution tests, blackout split, reforzada signature, contract churn, etc.
- **`f12_*`** — F12 pumped-storage arbitrage; per-hour reghdfe; seasonality audit.
- **`f15_post_blackout_ccgt_windfall.py`** — F15 post-blackout CCGT windfall.
- **`f16_ccgt_supply_slope_by_firm.py`** — F16 CCGT supply-slope by firm.
- **`q2_definitions_compare.py`** — six q₂ definitions compared (IR-cleanest selection).
- **`renewable_capture_price.py`** — wind/solar price-capture by firm.
- **`d5_sell_side_long_run.py`** — D5 long-run sell-side decomposition.
- **`marginal_price_step_concentration.py`** — distributional check.
- **`net_seller_position.py`** — net-seller position by firm.

Outputs land in `results/regressions/firm/{critical_hours,pdbf,b9,other}/`.

## What does NOT belong here

- System-level friction (S5/S6/S7/S8/B5/B6/B7) → `system/`
- Bid-shape and granularity tests (B14) → `bid/`
- RT2 / CNMC enforcement → `regulatory/`
- aFRR / mFRR per-firm decomposition → `balancing/`
- Mechanism-candidate / structural-track probes (anticipation, theta, welfare) → `modelling/`
- Retired pre-pivot work (Lerner, synthetic firm, dual-pricing, HP-sophistication, F1-F3, F5, F23, F25) → `attic/{lerner,synthetic,firm}/`
