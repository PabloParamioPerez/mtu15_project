# Firm-classification audit (2026-05-11)

Comprehensive audit and refactor of the OMIE unit-to-firm mapping used across
the thesis. The audit identified four real bugs of varying severity, and the
fix consolidates the previously-duplicated rules into a single source-of-truth
module ([`src/mtu/classification/units.py`](../../src/mtu/classification/units.py))
covered by 36 unit tests in [`tests/test_classification.py`](../../tests/test_classification.py).

## Why we did this audit

The user pushed back ("are we sure that all of them are independent?") after
the fringe was lumped into "Other" in the firm-descriptive table. The probe
revealed multiple classification bugs that had silently propagated through the
entire pipeline. The instruction was to fix everything and document it.

## What lives in `data/external/`

**`data/external/omie_reference/lista_unidades.csv` (3,950 rows)** — the canonical
unit-level register downloaded from OMIE on 2026-04-07. Schema:

| Column | Meaning |
|---|---|
| `unit_code`     | OMIE 5–7 character code (`ABO2G`, `ALZ1`, `IBVD3`) |
| `description`   | Plant name (free text) |
| `owner_agent`   | Owning legal entity (free text) |
| `ownership_pct` | Percent owned by `owner_agent` (1..100) |
| `unit_type`     | GENERACION / COMERCIALIZADOR / GENERICA / etc. (11 values) |
| `zone`          | `ZONA ESPAÑOLA` / `ZONA PORTUGUESA` / `FRONTERA *` |
| `technology`    | Free-text Spanish tech ("Ciclo Combinado", …; 52 distinct) |

For joint-owned plants, the unit appears once per owner with the appropriate
`ownership_pct`. Only 14 units have `ownership_pct < 100` — almost entirely
Spanish nuclear.

**`data/external/omie_reference/lista_agentes.csv` (1,479 rows)** — corporate
agent register. Schema:

| Column | Meaning |
|---|---|
| `agent_code`  | 5-letter OMIE agent code (`IBGES`, `ENDG`, `REPSB`, …) |
| `description` | Agent legal name (free text) |
| `agent_type`  | `GENERACIÓN` / `GENERACIÓN RECORE` / `COMERCIALIZADOR` / `CONSUMIDOR DIRECTO` / `REPRESENTANTE` / `COMERCIALIZADOR ULTIMO RECURSO` |

The two files are **not joined by a stable ID** — only by description string,
which has versioning ("EDP ESPAÑA, S.A.U. (GENERACIÓN)" vs "EDP ESPAÑA, S.A.U.")
that breaks exact matches in four cases. The whole pipeline therefore uses
substring matching on `lista_unidades.owner_agent` rather than joining through
`lista_agentes`.

**`data/external/omie_reference/ccgt_eic_to_omie.csv`** (50 rows) — hand-built
crosswalk from ENTSO-E EIC codes to OMIE unit codes for CCGT plants. Used by
the ENTSO-E A75 generation track, not by the thesis DiD.

**`data/external/esios_reference/bsp_to_firm.csv`** (23 rows) — ESIOS 3-letter
BSP codes → firm labels for aFRR settlement data. Not used by the thesis DiD.

**`data/external/esios/*.csv`** — ESIOS taxonomy exports (BSPs, scheduling units,
physical units, cross-border auction participants). Not used by the thesis DiD.

## The bugs

### Bug 1 — Joint-owned nuclear was triple-counted

**Symptom.** OMIE registers Almaraz 1 (`ALZ1`) three times in `lista_unidades`,
once per stakeholder: IB 52.687%, GE 36.021%, GN 11.292%. The previous SQL
joined `pdbc.unit_code → units_map.unit_code` without weighting by `ownership_pct`.
PDBC has **one** row per (date, period, unit_code) with the plant's total cleared
MWh; the join therefore produced three rows per ALZ1 cleared row, each attributing
the **full** plant output to a different firm.

**Verification (Almaraz 1, 2025):**

- Real cleared (from PDBC single source): 2.041 TWh
- Previous pipeline attributed: IB 2.041 + GE 2.041 + GN 2.041 = **6.123 TWh, 3× over-count**

Same bug for Almaraz 2, Ascó 2, Trillo, Vandellós II. Cofrentes and Ascó 1 are
fully owned (IB and GE respectively) and not affected.

**Severity for the thesis.** The descriptive table over-stated all dominant
operators' cleared volume and capacity proxy. After the fix:

| Firm | Capacity (GW): before → after | DA share (%): before → after | Net position: before → after |
|---|---|---|---|
| Iberdrola | 34.10 → **31.03** | 15.42 → **9.47** | Net seller → **Mixed** |
| Endesa | 15.19 → **12.46** | 17.40 → **13.94** | Mixed → **Net buyer** |
| Naturgy | 15.04 → **12.60** | 4.73 → **2.18** | Mixed → **Net buyer** |
| EDP-Spain | 4.57 → **4.73** | 2.87 → **2.99** | Net buyer → Net buyer (now includes IBERENERGIA Trillo stake) |

For the unit-period DiD regressions (B1 q_2, B3 DA cleared, B5 robustness),
the bug tripled the Nuclear-tech-stratified subsample but had a small effect
on β₃ because nuclear runs flat with q_2 ≈ 0.

### Bug 2 — IBERENERGIA was misclassified

**Symptom.** `IBERENERGIA, S.A.` is a JV holding company that owns the
remaining 15.5% of the Trillo nuclear plant. Trillo's full ownership is:

```
TRL1   IBERDROLA ENERGÍA ESPAÑA  49.0%
TRL1   GAS NATURAL COMERC.       34.5%
TRL1   IBERENERGIA, S.A.         15.5%
TRL1   ENDESA GENERACIÓN, S.A.    1.0%
```

The 15.5% is **EDP-Spain's stake**, held through this legacy vehicle (carried
over from the old Hidroeléctrica del Cantábrico era). The previous classifier
didn't have a rule for IBERENERGIA and the broad rule "IBERDROLA" matched it
into Iberdrola — wrong, both economically (it's EDP-Spain's stake) and
mechanically (the "IBER" prefix is a coincidence).

**Fix.** Add `("IBERENERGIA", "EDP-Spain")` to the rules (before the
"IBERDROLA" rule, since order matters: first match wins). EDP-Spain now
correctly carries its Trillo stake.

### Bug 3 — REPSOL SERVICIOS RENOVABLES inflated Repsol 3×

**Symptom.** `REPSOL SERVICIOS RENOVABLES, S.A.` (agent code REPSB) is a
REPRESENTANTE-type agent that aggregates 42 third-party small wind/solar plants
for OMIE market participation. The plants are **not** Repsol-owned. The
"REPSOL" substring rule swept them into the Repsol parent firm.

**Impact (post-fix vs pre-fix):**

| Repsol | Before | After |
|---|---|---|
| Generation units | 52 | 13 |
| Capacity (GW) | 5.59 | 0.61 |
| DA share (%) | 4.97 | 1.08 |
| Net position | Mixed | Net buyer |

The new numbers are an accurate description of Repsol's actual Spanish footprint:
mostly downstream + retail, with a small refinery-cogen presence. The previous
numbers were dominated by ~42 third-party renewables that Repsol bids through
REPSB as a market intermediary.

For the headline DiD, those 42 units were polluting the Repsol *placebo*
sample with non-Repsol observations. They would still show β₃ ≈ 0 (price-takers,
not pivotal), so the placebo conclusion is unchanged in direction, but the
sample composition is now clean.

### Bug 4 — ENGIE GLOBAL MARKETS, BELGIAN BRANCH

**Symptom.** `ENGIE GLOBAL MARKETS, BELGIAN BRANCH` (FR_GS) is a Belgian
cross-border trading desk. The "ENGIE" substring rule pulled it into Engie España.

**Fix.** Added to `OWNER_EXCLUDE`. 4 units removed; capacity essentially
unchanged.

## The fix: centralized module

[`src/mtu/classification/units.py`](../../src/mtu/classification/units.py) now
exports the single source of truth:

```python
from mtu.classification.units import (
    FIRM_RULES_BROAD,      # keyword → "Iberdrola"/"Endesa"/.../"Moeve"
    FIRM_RULES_SHORT,      # keyword → "IB"/"GE"/.../"Moeve"
    OWNER_EXCLUDE,         # frozenset of exact owner_agent strings to skip
    parent_of,             # (owner_agent, scheme='short'|'broad') → parent or None
    firm_unit_panel,       # full DataFrame builder with share + mode
    TREATMENT_PARENTS_SHORT, PLACEBO_PARENTS_SHORT,
    TREATMENT_PARENTS_BROAD, PLACEBO_PARENTS_BROAD,
)
```

The two naming schemes (`'short'` and `'broad'`) use the SAME keyword rules
in the same order. They differ only in the output label.

### Two output modes for `firm_unit_panel`

```python
firm_unit_panel(scheme='short', mode='all_owners')     # multi-row per unit_code, with `share` column
firm_unit_panel(scheme='short', mode='primary_owner')  # one row per unit_code, share = 1.0
```

- **`all_owners`** is for aggregate-volume computations (cleared MWh per firm,
  capacity proxy). Downstream MUST weight by `share` to avoid the
  triple-counting bug.

- **`primary_owner`** is for unit-period regressions (the DiD on q_2, the
  bid-shape panel). Joint-owned plants are attributed in full to the
  largest-share owner so that each (date, period, unit_code) cell appears
  exactly once in the panel.

### Choice of primary owner for the joint nuclear plants

| Plant | Ownership shares | Primary owner |
|---|---|---|
| Almaraz 1 (`ALZ1`) | IB 52.7% > GE 36.0% > GN 11.3% | IB |
| Almaraz 2 (`ALZ2`) | IB 52.7% > GE 36.0% > GN 11.3% | IB |
| Ascó 1 (`ASC1`) | GE 100% | GE |
| Ascó 2 (`ASC2`) | GE 85% > IB 15% | GE |
| Cofrentes (`COF1`) | IB 100% | IB |
| Trillo (`TRL1`) | IB 49% > GN 34.5% > HC (via IBERENERGIA) 15.5% > GE 1% | IB |
| Vandellós II (`VAN2`) | GE 72% > IB 28% | GE |

## Where this fix landed

### Centralized module + tests
- [`src/mtu/classification/units.py`](../../src/mtu/classification/units.py) — new exports
- [`tests/test_classification.py`](../../tests/test_classification.py) — 36 tests, all passing

### Scripts refactored to use the central module
- [`scripts/analysis/firm/critical_hours_did_thesis.py`](../../scripts/analysis/firm/critical_hours_did_thesis.py) (B1, primary_owner)
- [`scripts/analysis/firm/critical_hours_did_thesis_robustness.py`](../../scripts/analysis/firm/critical_hours_did_thesis_robustness.py) (B5, primary_owner)
- [`scripts/analysis/firm/da_cleared_did_thesis.py`](../../scripts/analysis/firm/da_cleared_did_thesis.py) (B3, primary_owner)
- [`scripts/analysis/firm/build_thesis_table_firm_descriptive.py`](../../scripts/analysis/firm/build_thesis_table_firm_descriptive.py) (Table 1 + B.2, all_owners + share weighting)
- [`scripts/analysis/firm/operational_vs_strategic_decomposition.py`](../../scripts/analysis/firm/operational_vs_strategic_decomposition.py)
- [`scripts/analysis/firm/q2_break_id15_vs_da15.py`](../../scripts/analysis/firm/q2_break_id15_vs_da15.py)
- [`scripts/analysis/firm/parallel_trends_diagnostic.py`](../../scripts/analysis/firm/parallel_trends_diagnostic.py)
- [`scripts/analysis/bid/per_firm_hourly_ccgt_bidshape.py`](../../scripts/analysis/bid/per_firm_hourly_ccgt_bidshape.py) (Figure 3 input)
- [`scripts/analysis/bid/per_firm_pre_vs_post_mtu15da.py`](../../scripts/analysis/bid/per_firm_pre_vs_post_mtu15da.py) (Figure 4 input)
- [`scripts/analysis/bid/per_firm_competitive_zone_bidshape.py`](../../scripts/analysis/bid/per_firm_competitive_zone_bidshape.py)

### Scripts deliberately NOT refactored
- [`scripts/analysis/firm/structural_dominance_markers.py`](../../scripts/analysis/firm/structural_dominance_markers.py) — uses its own broader `parent_group()` that tracks non-thesis firms (Acciona, Axpo, Alpiq, Galp, Ignis, Enel-Green, …) for the dominance-audit memo. Its scope is wider than the thesis classifier by design.
- [`scripts/analysis/firm/critical_hours_supply_decomp.py`](../../scripts/analysis/firm/critical_hours_supply_decomp.py) — uses `pdbce.grupo_empresarial` (OMIE's own firm code) directly, not our classification.
- All `scripts/analysis/attic/*` — retired pre-pivot work.

### Outputs regenerated
- `results/regressions/firm/critical_hours_thesis/B1_q2_did.csv` (+ panel)
- `results/regressions/firm/critical_hours_thesis/B3_da_cleared_did.csv`
- `results/regressions/firm/critical_hours_thesis/B4_cpt_panel.csv`
- `results/regressions/firm/critical_hours_thesis/B5_robustness.csv`
- `results/regressions/bid/perfirm_hourly_ccgt_bidshape_oct_dec_2025.csv`
- `results/regressions/bid/perfirm_pre_vs_post_by_hour_class.csv`
- All `thesis/paper/tables/tab_*.tex`
- All `figures/thesis/fig_*.{png,pdf}` and `thesis/paper/figures/fig_*.pdf`

## Headline empirical impact

The corrected β₃ on q_2 (Ito-Reguant strategic IDA upward adjustment):

| Specification | β₃ | SE | p |
|---|---|---|---|
| Pivotal firms only (B1 headline) | **+4.508** | 0.909 | 0.0000*** |
| All firms pooled | −1.305 | 0.335 | 0.0001*** |
| Non-pivotal firms (negative control) | −4.987 | 0.333 | 0.0000*** |

Tech-stratified within pivotal firms:

| Tech | β₃ | SE | p | Predicted |
|---|---|---|---|---|
| CCGT | +10.99 | 3.17 | 0.0005*** | + |
| Hydro | −6.33 | 2.41 | 0.0085** | + (sign-mixed; investigate hydro_pump distinction) |
| Hydro_pump | +13.01 | 11.66 | 0.26 | + |
| Coal | +92.54 | 29.41 | 0.0017** | + |
| Nuclear | +68.69 | 24.17 | 0.0045** | 0 (must-run) |
| Wind | +1.27 | 1.39 | 0.36 | 0 ✓ |

CPT spec stack (B4):

| Spec | β₃ | SE | p |
|---|---|---|---|
| Baseline B1 | +4.508 | 0.909 | 0.0000*** |
| + wind/solar levels | +4.445 | 0.917 | 0.0000*** |
| + crit × renewable | +4.110 | 0.873 | 0.0000*** |
| + cal-month FE | +4.109 | 0.873 | 0.0000*** |

All four specs stable around +4.1–4.5, all p<0.001 — the OVB-discipline test passes.

B5 robustness (all p<0.001 unless noted):

| Sensitivity | β₃ |
|---|---|
| B5.1 canonical critical hours | +4.508 |
| B5.1 supply_ramp | +5.013 |
| B5.1 price_peak | +6.466 |
| B5.1 demand_peak | +6.317 |
| B5.1 joint | +5.566 |
| B5.2 pivotality vs admin set | identical (EDP-PT has no pibci) |
| B5.3 full window 2024–2025 | +1.739 (pre-MTU15-DA half already crushed by MTU15-IDA) |
| B5.4 drop EDP-PT / ABO2G | identical |
| B5.5 drop reforzada months | +3.594 |
| B5.6 drop DST transition days | +4.596 |
| B5.7a CEST only | +0.685 (p=0.50, small sample 50 days) |
| B5.7b CET only | +5.941 |

## Verification protocol going forward

Any new script that classifies firms MUST:

1. Import from `mtu.classification.units`. Never copy-paste the rules inline.
2. Decide explicitly between `'all_owners'` and `'primary_owner'` mode and
   document the choice in a one-line comment.
3. If using `'all_owners'`, weight all SUM(MWh) / SUM(MW) by `u.share` in the
   SQL JOIN.

The 36 tests in [`tests/test_classification.py`](../../tests/test_classification.py)
cover the four known-tricky cases (ALZ1 triple ownership, REPSB exclusion,
FR_GS exclusion, IBERENERGIA → EDP-Spain). Run them with:

```
uv run pytest tests/test_classification.py -v
```

before any change to the classifier rules.

## Bug 5 (follow-up, same day) — PDBC-only "DA share" understated Iberdrola

**Symptom.** After fixing bugs 1–4, the descriptive table showed Iberdrola
at 9.47 % "DA share" — much lower than the CNMC-published ~22 % generation
share. The user (correctly) flagged this as not credible.

**Diagnosis.** The metric was computed from PDBC (DA-cleared sells only).
Spot check on Almaraz 1 (2025-11-15 period 1): the unit cleared 366 MW in
the day-ahead auction (PDBC) *and* 635 MW via three bilateral contract
executions (PDBF offer_type=4). For every joint-owned nuclear and many
CCGTs, the bilateral channel — which vertically-integrated firms use to
route generation to their own retail arms — is larger than the DA-auction
channel. Restricting to PDBC therefore systematically understates the
dominant operators' total market footprint.

**Fix.** Use PDBF with `offer_type IN (1, 4)`:

```
SUM(  CASE WHEN offer_type = 1 THEN mwh  -- DA cleared
            WHEN offer_type = 4 THEN mwh  -- bilateral contract execution
            ELSE 0 END
   * u.share )                            -- share-weighted across joint owners
```

This is the metric CNMC publishes as "generation share". Numbers after the fix:

| Firm | PDBC DA-only | PDBF DA+bilateral |
|---|---|---|
| Iberdrola | 9.47 % | **24.94 %** |
| Endesa | 13.94 % | **16.39 %** |
| Naturgy | 2.18 % | **12.34 %** |
| EDP-Spain | 2.99 % | 4.14 % |
| EDP-Portugal | (PT zone) | **15.17 %** |
| Repsol | 1.08 % | 1.43 % |
| Engie España | 1.83 % | 1.73 % |
| TotalEnergies | 0.68 % | 0.63 % |
| Moeve | 0.81 % | 0.01 % |

The dominant Iberian operators jointly account for ~73 % of Iberian
day-ahead programmed sell volume — consistent with CNMC's reported market
concentration. The PDBC-only metric in the previous version of the table
under-stated this by half.

EDP-Portugal now shows 15 % instead of "(PT zone)" because PDBF includes
the Portuguese side of MIBEL (PDBC's Spanish-zone-only restriction was the
reason the column was empty before).

For the **net position** column (sell-vs-buy in the spot market), we still
use PDBC. That metric is intentionally about the firm's exposure to spot
trading; bilateral contracts are internal firm-to-counterparty transfers
that don't represent net spot-market participation.

## Bug 6 (follow-up) — Pivotality definition was the *loose* test

The pivotal_indicator panel computed two flags:
- `pivotal`: `n_below > 0 AND n_above > 0` — any wide-ladder bid that straddles clearing
- `tightly_pivotal`: highest_below tranche within 1 EUR/MWh of clearing — genuine
  price-setting

Earlier work and the original Table 2 used the *loose* `pivotal` flag, which
trivially fires for any unit with a wide-range bid. EDP-Spain showed 99.78%
critical-hour pivotality because its CCGT bids span −5 to 699 EUR/MWh — always
some tranches below and some above any plausible clearing — without ever
being economically close to setting the price.

With the strict `tightly_pivotal` flag the ranking flips: Naturgy is the most
tightly pivotal CCGT firm in critical hours (4.4%), and that is the firm with
the clean price-side quarter variation signature (23% of its critical-hour
DA CCGT cells vary in p_avg with zero qty variation). Table 2 in the paper has
been updated to use the strict measure.

## Bug 7 (the biggest) — Hour-from-period formula via `CAST AS INTEGER`

A bug found late in the session: DuckDB evaluates `(period-1)/4` as DOUBLE,
then `CAST AS INTEGER` rounds half-to-even. Period 7 maps to hour 2 instead
of hour 1; period 12 maps to hour 3 instead of hour 2. Roughly **half of
all periods are mis-assigned to the wrong clock-hour**. The correct
DuckDB syntax is `(period - 1) // 4` (integer floor division).

Affected scripts (all fixed in this session): `critical_hours_did_thesis.py`,
`critical_hours_did_thesis_robustness.py`, `da_cleared_did_thesis.py`,
`operational_vs_strategic_decomposition.py`, `q2_break_id15_vs_da15.py`,
`parallel_trends_diagnostic.py`, `bid_shape_by_tech.py`,
`bid_price_vs_qty_quarter_decomp.py`, `thesis_figures.py`,
`da_ida_wedge_did_thesis.py`, `structural_dominance_markers.py`,
`seasonal_critical_hours.py`, `pdbf_granular_analysis.py`.

A different family of scripts uses `CEIL(period/4.0)` for 1-indexed clock
hours — this is **correct** (gives 1..24 hour labels with floor semantics
on period boundaries).

### Empirical impact of the period-formula fix

Before fix → after fix, β₃ on q₂ (pivotal firms, same-cal-month):

| Spec | Pre-fix | Post-fix |
|---|---|---|
| Headline B1 | +4.508 | **+3.866** |
| CCGT-stratified | +10.99 | **+15.23** (cleaner) |
| Hydro-stratified | −6.33** | −1.96 (now NS) |
| Wind-stratified | +1.27 | −2.24 (still NS, wrong sign tiny) |
| B4 CPT spec stack | +4.1–4.5 | +3.8 stable |

Sample size drops from 311k cells to 122k after the fix — the pre-fix sample
was inflated by periods mis-attributed to critical/flat hours that didn't
actually belong there. The CCGT-stratified effect grows from +11 to +15
under the fix; nuclear weakens to p=0.08 (no longer significant); the
non-significant tech-specific effects (Hydro, Wind) become cleaner nulls.

Per-firm β₃ on DA cleared (B3), sign of "Endesa adds to DA" loss:

| Firm | Pre-fix β₃ | Post-fix β₃ |
|---|---|---|
| Iberdrola | −67.0\*\*\* | −62.4\*\*\* ✓ |
| **Endesa** | **+20.5\*\*\*** | **−2.2 (NS)** *sign-flip* |
| Naturgy | −36.4\*\*\* | −34.7\*\*\* ✓ |
| EDP-Portugal | −83.9\*\*\* | −24.2\* |

The "Endesa adds to DA in critical hours" finding was an artifact of the
period-formula bug.

## Residual limitations (documented, not fixed)

1. **Single-snapshot register.** Built from April 2026; won't reflect plant
   ownership changes mid-2024 or mid-2025. For the dominant operators we
   know of no such transfer in the analysis window.

2. **Free-text matching.** The owner_agent column is free Spanish text;
   substring rules are brittle to renamings ("GAS NATURAL FENOSA" → "NATURGY"
   in 2018, "CEPSA" → "MOEVE" in 2024). The current rules cover both legacy
   and modern names but not every conceivable variant.

3. **Code-variant mismatches.** Found one case: `ABO2` in 2025 PDBC vs `ABO2G`
   in the register (the Aboño coal-to-gas-converted plant, EDP-Spain subsidiary).
   Volume is 113k MWh (~0.03% of total), so rounding error for the thesis;
   not worth a special rule.

4. **Enel Green Power España.** Sister company of Endesa Generación under the
   Italian parent Enel S.p.A. (4.89 TWh of cleared sell volume in 2025).
   Currently classified as "Other" because it's a legally separate OMIE
   participant. Could be folded into Endesa if the thesis adopts a
   corporate-group view rather than a market-participant view. **Open
   question — not auto-fixed.**

5. **`grupo_empresarial` in PDBCE.** OMIE's own firm-grouping column in PDBCE
   covers IB/GE/GN/HC/REP only. The thesis pipeline uses it in two places
   ([`structural_dominance_markers.py`](../../scripts/analysis/firm/structural_dominance_markers.py)
   and [`critical_hours_supply_decomp.py`](../../scripts/analysis/firm/critical_hours_supply_decomp.py)). For
   non-Big-5 firms (Engie, Moeve, TotalEnergies, EDP-PT), we fall back on the
   unit_code → parent map via the broad rules.

## See also

- [`_pivotality_by_firm_critical_hours.md`](_pivotality_by_firm_critical_hours.md) — the
  treatment partition.
- [`RESEARCH_DIARY.md`](RESEARCH_DIARY.md) — dated entry for this audit.
- [`../../CLAIMS_LEDGER.md`](../../CLAIMS_LEDGER.md) — affected claims.
