# `scripts/analysis/system/` — system-level friction analyses

Analyses that operate on the *aggregate system layer*: BRP→TSO settlement transfers, forecast→imbalance pass-through, redispatch zone activations, Pigouvian incidence, B-claim seasonality and robustness audits.

These supply the supporting evidence for the thesis. The headline identification (within-day critical-vs-flat-hours DiD) is in `firm/`, with `firm/critical_hours/` results; the bid-shape evidence (B14) is in `bid/`. See `_within_market_granularity_model.md` and `CLAUDE.md` for the canonical project map.

## What lives here

- **`s6_*`** — S6 BRP→TSO settlement-transfer (€1.1B headline). Baseline sensitivity, monthly decomposition, OVB robustness.
- **`s7_*`** — S7 Pigouvian-incidence anchor validation.
- **`s8_*`** — S8 redispatch-zone activations, daily and renewable-controlled.
- **`b5_*`, `b6_*`** — B5/B6 seasonality + pass-through audits and robustness attacks (relocated 2026-05-04 from `modelling/`).
- **`france_da_placebo.py`** — B7 cross-country placebo (relocated from `modelling/`).
- **`a87_reserve_decomposition.py`, `esios_a87_cross.py`** — ENTSO-E A87 reserve-balance decomposition (S1 family; relocated from `modelling/`).
- **`pigouvian_clean_regression.py`, `passthrough_forecast_imbalance.py`** — modelling-style probes that ended up feeding system-layer claims (S7 and B6 respectively).
- **`b6_s6_magnitude_check.py`** — cross-channel magnitude consistency: B6 volume pass-through vs S6 transfer.
- **`rz_activation_escalation.py`** — RZ activation patterns across regimes.

Outputs land in `results/regressions/system/`.

## What does NOT belong here

- Firm-level strategic conduct → `firm/`
- Bid-shape and granularity tests → `bid/`
- RT2 / CNMC enforcement → `regulatory/`
- aFRR / mFRR / nuclear-availability → `balancing/`
- Mechanism-candidate / structural-track scripts → `modelling/`
- Retired pre-pivot work (Lerner, synthetic firm) → `attic/`
