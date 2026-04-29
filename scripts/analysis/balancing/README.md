# `scripts/analysis/balancing/` — balancing market & availability

Analyses of the *balancing* layer (aFRR, mFRR) and unit-level *availability* (nuclear, CCGT). Distinct from the DA/IDA strategic-conduct work in `firm/` because balancing markets clear in a different mechanism with different participants.

## What lives here

- **`afrr_*`** — aFRR offer depth and per-firm decomposition (anchors F19/F20 — GE captures the post-blackout aFRR up-activation windfall).
- **`mfrr_offer_depth.py`** — mFRR offer-depth characterisation.
- **`nuclear_availability_*`** — nuclear capacity factor and availability audits (v1, v2, v3 = successive refinements).
- **`nuclear_cross_subsidy.py`** — nuclear cross-subsidy measure.
- **`ccgt_availability_sweep.py`** — CCGT availability sweep across regimes.

## What does NOT belong here

- Firm-level DA/IDA strategic conduct → `firm/`
- System-level friction → `system/`
- RT2 / CNMC → `regulatory/`
- Bid-curve analyses (XBID, complexity) → `bid/`
