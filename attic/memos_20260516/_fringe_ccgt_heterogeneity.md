# Fringe-CCGT heterogeneity: zone, owner, and the "fringe placebo" identification

**Created:** 2026-05-07

**Purpose.** Audit whether the "fringe" CCGT classification used in B13's
fringe-placebo coefficient (β₃ = −24.3) is a coherent strategic group, or
a heterogeneous mixture. Tests Natalia Fabra's offhand claim that "in the
fringe there was only one CCGT unit/firm."

**Window.** DA market, Oct 1 2025 – Dec 31 2025, post-MTU15-DA, parser-fixed.
21 fringe-CCGT units in OMIE register (per `firm_class` from
`src/mtu/classification/units.py`: not Big-4 ⇒ Fringe).

## Key counter-finding to Natalia's intuition

Natalia's claim is numerically incorrect: there are **12 distinct fringe-
CCGT owner-agents** in the OMIE register, not one. But her intuition has
real force — the fringe is wildly heterogeneous, and several sub-groups
behave very differently from each other.

## Zone-stratified split: Spanish vs Portuguese fringe

Five of the 21 fringe-CCGT units are Portuguese (EDP GEM PORTUGAL —
LARES1/2, RIBATE1/2/3). They behave dramatically differently from
Spanish-zone fringe:

| Group | n_units | mean n_tranches | mech_strict | mean p_max | mean p_med |
|---|---:|---:|---:|---:|---:|
| **Spanish fringe** | 16 | 2.53 | **0.80** | 384 €/MWh | 263 €/MWh |
| **Portuguese fringe (EDP-Portugal)** | 5 | **6.41** | **0.09** | 625 €/MWh | 167 €/MWh |

EDP-Portugal CCGTs post rich ladders (6.4 tranches/quarter, vs 2.5 for
Spanish-fringe) and exploit MTU15 granularity heavily (mech_strict 9% —
varying bids quarter-by-quarter on 91% of unit-days). **They look more
like Naturgy than like Spanish fringe.**

Pooling Spanish-fringe + Portuguese-fringe is therefore inappropriate
for any cross-firm strategic-conduct comparison anchored on the Spanish
day-ahead market structure. The Portuguese subset should either be
excluded or analysed separately.

## Within-Spanish-fringe heterogeneity: four strategic types

The 16 Spanish fringe-CCGTs split into recognisably different
strategic sub-types:

### Type 1 — "Engie-strategic" (~almost dominant)
- ENGIE CARTAGENA (ESCCC1/2/3): n_tr 4.5, mech 0.69, p_max 500
- ENGIE CASTELNOU (CTNU): n_tr 4.7, mech 0.22, p_max 500

4 units, ~25% of Spanish-fringe-CCGT count. Behaves more like dominant
firms than other fringe operators: high tranche counts (4.5-4.7) and
substantial granularity exploitation (mech_strict 22-69%).

### Type 2 — "Mid-strategic, ladder only"
- TOTALENERGIES (CTJON1R, CTJON3R, "represented"): n_tr 1.27, mech 0.78, p_max 500
- MOEVE GAS AND POWER (ARRU1R, ARRU2R, "represented"): n_tr 2.40, mech 1.00, p_max 535

4 units. Single-tranche or near-single, fully mechanical, but with
elevated price ceilings (500-600 €/MWh) — closer to GE's "near-cap
reserve" stance than IB's routine bidding.

### Type 3 — "Single-tranche near-mechanical, low ceiling"
- REPSOL SERVICIOS RENOVABLES (ECT3, ALG3): n_tr 3.52, mech 0.90, p_max 179
- IGNIS ENERGIA (ECT2): n_tr 1.00, mech 1.00, p_max 188
- ALPIQ (PVENT2): n_tr 1.13, mech 1.00, p_max 197
- BAHIA DE BIZKAIA (BAHIAB): n_tr 1.20, mech 0.96, p_max 373
- SERVICIOS ENERG. AE (CAMG20R): n_tr 1.46, mech 0.90, p_max 297

7 units. Routine cleared bidding at modest price ceilings (180-380 €/MWh),
no within-day strategic shape. The "purest" fringe behavior — these are
small or peripheral CCGT operators with no apparent strategic playbook.

### Type 4 — "ABO2G outlier"
- ABOÑO GENERACIONES ELECTRICAS (ABO2G): n_tr 6.0, mech 1.00, p_max 699

1 unit. Extreme behavior: 6-tranche ladder (richest in the fringe)
fully mechanical, ceiling at 699 €/MWh — almost identical to HC's pattern
(SRI4R/5R: 6 tranches, mech=1.00, p_max=699). Strongly suggests this is
operationally tied to EDP-Spain (HC) but bid through a separate agent.

## Implications for the B13 fringe-placebo identification

The β₃ = −24.3 fringe-placebo coefficient is identified against a
heterogeneous mixture that includes:

- 5 Portuguese CCGTs that behave like granularity exploiters (NOT a
  good placebo for "non-strategic" comparison).
- 4 Engie units that behave like a fifth dominant firm.
- 4 "mid-strategic" represented units (TotalEnergies, Moeve).
- 7 single-tranche near-mechanical units (Repsol/Ignis/Alpiq/etc.) —
  the cleanest "fringe" sub-group.
- 1 hidden EDP-Spain proxy (ABO2G).

The fringe-placebo coefficient is therefore mis-identified — it's not
"dominant vs non-strategic," it's a noisy weighted average of several
distinct strategic conduct types. **A robustness check that limits the
fringe to the Type 3 group (Repsol, Ignis, Alpiq, BBE, Servicios
Energéticos AE = 7 units, ~50% of Spanish fringe-CCGT count) would
give the cleanest "non-strategic" placebo.**

If that robustness check shows |β₃| larger than the original aggregate,
the dilution interpretation is confirmed; if smaller, the heterogeneity
is a feature not a bug.

## What Natalia probably meant

Two charitable interpretations of "only one CCGT in the fringe":

1. She was thinking of large/independent fringe-CCGT operators — Engie
   has 4 units but bids strategically; the rest of Spanish-fringe is
   small (1-2 units per owner), peripheral, and operationally
   non-strategic. By that count, the "real" fringe-CCGT diversity is
   indeed close to zero.

2. She might have been thinking of capacity, not count: 25% of
   Spanish-fringe-CCGT capacity is Engie's. But Engie isn't really
   fringe in conduct terms.

Either way, the implication for our work is: the "fringe" is not a
clean placebo group; it's a heterogeneous mixture that needs to be
unpacked.

## Sources

- `src/mtu/classification/units.py` — firm_class, tech_group classifier.
- `data/external/omie_reference/lista_unidades.csv` — OMIE register with owner_agent and zone.
- `results/regressions/bid/perunit_hourly_ccgt_bidshape_oct_dec_2025.csv` — per-unit bid-shape used for stratification.

## Next steps

1. Re-run the B13 fringe-placebo coefficient under three definitions:
   - Aggregate fringe (current; status quo)
   - Spanish-only fringe (drop EDP-Portugal)
   - "Pure-fringe" Type 3 only (drop Engie, TotalEnergies, Moeve, ABO2G)
   Compare β₃ across the three.

2. Consider promoting Engie to a "tier-2 strategic" group rather than
   fringe. Their conduct (4 units, n_tranches 4.5-4.7, mech 0.22-0.69)
   is qualitatively closer to dominant than to peripheral. CNMC's
   "operador principal" tier already includes Acciona and Repsol —
   adding Engie would be defensible. (Alternatively keep Engie in the
   fringe but flag it as a known dilution.)

3. Investigate ABO2G's ownership chain — if confirmed operationally
   tied to EDP-Spain, reclassify to HC.
