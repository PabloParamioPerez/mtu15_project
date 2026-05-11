# `scripts/analysis/regulatory/` — RT2 + CNMC enforcement

Analyses of REE/CNMC regulatory channels — quantification of Phase-2 technical restrictions (RT2) and replication of CNMC bid-price-wedge methodology. These operate as supporting context for the within-day DiD headline (the strategic-conduct mechanism is identified in `firm/`); regulatory channels are reported separately as parallel forces.

## What lives here

- **`rt2_post_blackout_channel.py`** — quantifies REE Phase-2 technical restrictions (RT2 = PHF − PIBCA per unit-period) post-Apr-2025 blackout. Includes the verification that the apparent OMIE Oct-2025 step-jump is a publishing-convention artefact (cross-checked against ESIOS `totalrp48preccierre`).
- **`cnmc_bid_price_wedge.py`** — replication of CNMC's 2023 SBO3 bid-price-wedge methodology applied to Big-4 CCGT 2024-26.
- **`cnmc_three_situation_replication.py`** — three-situation pivotality test (zone-pivotal vs zone-non-pivotal vs zone-irrelevant hours).
- **`cnmc_repeat_offender_concentration.py`** — concentration of CNMC sanctions across firms.

## What does NOT belong here

- Firm-level strategic conduct (B9, F-series) → `firm/`
- aFRR/mFRR/nuclear availability → `balancing/`
- System-level friction (S6/B6/etc.) → `system/`
