# `scripts/analysis/system/` — system-level friction (Acts I)

Analyses that operate on the *aggregate system layer*: BRP→TSO settlement transfers, forecast→imbalance pass-through, redispatch zone activations, Pigouvian incidence. Together these constitute Act I of the thesis friction arc.

## What lives here

- **`s6_*`** — S6 BRP→TSO settlement-transfer (€1.1B headline). Includes baseline sensitivity, monthly decomposition, OVB robustness.
- **`s7_*`** — S7 Pigouvian-incidence anchor validation.
- **`s8_*`** — S8 redispatch-zone activations, daily and renewable-controlled.
- **`b6_s6_magnitude_check.py`** — cross-channel magnitude consistency: B6 volume pass-through vs S6 transfer (volume vs price-spread decomposition).
- **`rz_activation_escalation.py`** — RZ activation patterns across regimes.

## What does NOT belong here

- Firm-level strategic conduct → `firm/`
- RT2 / CNMC enforcement → `regulatory/`
- aFRR / mFRR / nuclear-availability → `balancing/`
- Markup / Lerner work → `lerner/`
