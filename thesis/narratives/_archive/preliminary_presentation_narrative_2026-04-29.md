# Preliminary Presentation — narrative, slide map, figures, regressions

**Status:** working draft, 2026-04-29.
**Presentation date:** 2026-05-05.
**Goal:** clear, solid, IO-relevant narrative that anchors a follow-on
explanatory model. ~13 slides, 5–7 load-bearing figures, no thesis drafting.

---

## One-sentence thesis

> The Spanish 2024–2026 electricity-market reform sequence created a 10-month
> *asymmetric-granularity friction window* (June 2024 – October 2025) that
> produced (i) a measurable system-level BRP→TSO settlement transfer, (ii) a
> proportional 22% compression of Big-4 strategic IDA repositioning relative
> to pre-reform, and (iii) a near-full recovery of Big-4 strategic conduct once
> symmetric MTU15 was restored — superimposed by a parallel post-blackout
> regulatory cascade (REE operación reforzada via RT2) that dominates Big-4
> dispatch in absolute terms but is regulatory, not strategic.

This is a **U-shape friction story**, not a structural-change story. The IO
mechanism is sequential-markets / granularity-mismatch, not market-power
elimination.

---

## Three-act structure

### ACT I — System-level: asymmetric clock = measurable economic cost (5 slides)

**Headline:** the asymmetric-granularity window produces a BRP→TSO net
settlement transfer that wasn't there before and reverts after MTU15-DA.

| # | Slide | Claim | Regression / data anchor | Figure |
|---|---|---|---|---|
| I.1 | Reform timeline + research question | Three reform breaks generate two friction windows. | (none) | timeline schematic (need to build) |
| I.2 | Settlement-transfer concordance | Transfer aligns 1:1 with reform dates across four observable variables. | S5 four-panel concordance | `fig01` |
| I.3 | Settlement-transfer headline | Asymmetric window: +€91M/month BRP→TSO; reverts post-MTU15-DA. | S6 monthly transfer regression | `fig02` |
| I.4 | Two channels: volume + price | B6 volume passthrough explains <2% of S6; the rest is imbalance-price spread (k_hurt − p_DA) widening during asymmetric clock. | B6 per-ISP forecast→imbalance + B6×S6 magnitude check | `fig03` + spread table (need to build) |
| I.5 | France placebo | The transfer pattern does not appear in France (no asymmetric clock there). Pigouvian incidence: who pays. | B7 + S7 | `fig04` + `fig06` |

### ACT II — Firm-level: strategic IDA conduct compresses then recovers (4 slides)

**Headline:** Big-4 voluntary IDA repositioning (q₂ in Ito-Reguant notation)
shows a U-shape across regimes — progressive compression during the asymmetric
window, near-full recovery once symmetric MTU15 is restored. The reform did NOT
eliminate market power; it temporarily compressed it through friction.

| # | Slide | Claim | Regression / data anchor | Figure |
|---|---|---|---|---|
| II.1 | What is q₂? | Definition: q₂ = SUM(PIBCIE × mtu/60) per firm-period; voluntary IDA repositioning, IR-clean spot-market choice. Diff vs PHF−PDBC isolated. | OMIE spec §5.2.2.3 + `q2_definitions_compare.py` | mini-table comparing 5 q₂ definitions (need to build) |
| II.2 | Big-4 q₂ trajectory at firm-ISP grain | pre-IDA +146 → 3-sess +123 → ISP15-win +115 → DA60/ID15 +122 → DA15/ID15 +140 MWh/ISP. U-shape. | `b9_replicated_isp_grain.py` — 1.93M obs, F=477.5, p<10⁻¹⁰² | Big-4 vs Fringe gap chart (need to build) |
| II.3 | Per-firm typology | GN largest absolute, IB deepest collapse (-62%), GE most stable, HC small. | Same regression, per-firm split | `fig09` ✓ |
| II.4 | Robustness | Apr-Sep same-cal-month at same disaggregation: coefficients within 1 MWh/ISP of full sample (F=186, p=4e-41). NOT a seasonal artefact. | `b9_replicated_isp_apr_sep.py` | small comparison panel (need to build) |

### ACT III — Regulatory cascade: RT2 surge post-blackout (2 slides)

**Headline:** After the April 28 2025 blackout, REE's operación reforzada via
Phase-2 technical restrictions imposes a large mandatory dispatch increment on
Big-4 nuclear units in DA15/ID15, parallel to (and 3.4× larger than) voluntary q₂.
This is regulatory, not strategic; reported as a parallel channel.

| # | Slide | Claim | Regression / data anchor | Figure |
|---|---|---|---|---|
| III.1 | RT2 = PHF − PIBCA isolates regulatory increment | Big-4 RT2-up: ≈0 in pre-IDA → DA60/ID15; +13.6 GWh/firm-day in DA15/ID15. Concentrated in nuclear (62%), then hydro (15%), CCGT (13%). | `rt2_post_blackout_channel.py` | RT2 firm × tech bar chart (need to build) |
| III.2 | Top units + hour profile | Trillo, Almaraz I+II, Vandellós, Ascó dominate. Evening peak (h19–h22) and morning shoulder (h8–h10). GN captures ~44% of Big-4 RT2-up volume. | Same script | hour-of-day profile (need to build) |

### SYNTHESIS (2 slides)

| # | Slide | Content |
|---|---|---|
| S.1 | Three layers in one model | (a) System-level friction → BRP→TSO transfer (Act I); (b) Strategic conduct compression-then-recovery → U-shape (Act II); (c) Regulatory cascade → RT2 (Act III, parallel). All three layers can be rationalized by one parameter — the granularity-friction τ between DA market clock and ISP settlement clock. |
| S.2 | Where the explanatory model goes | Sequential-markets framework with τ. Predictions: τ↑ ⇒ system transfer ↑ (Act I), strategic q₂ compressed (Act II); τ↓ to 0 ⇒ recovery. RT2 is exogenous regulatory shock, not in the model. Next steps: write up the model, calibrate to the empirical magnitudes. |

---

## What we have vs. what's missing

### Existing figures (committed, ready to use)

- `figures/thesis/fig01_S5_four_panel_concordance.{pdf,png}` — Slide I.2
- `figures/thesis/fig02_S6_settlement_transfer_headline.{pdf,png}` — Slide I.3
- `figures/thesis/fig03_B6_passthrough_by_regime.{pdf,png}` — Slide I.4
- `figures/thesis/fig04_B7_france_placebo.{pdf,png}` — Slide I.5
- `figures/thesis/fig05_S6_blackout_robustness.{pdf,png}` — backup
- `figures/thesis/fig06_S7_pigouvian_incidence.{pdf,png}` — Slide I.5
- `figures/thesis/fig07_burden_share_regime_invariance.{pdf,png}` — backup
- `figures/thesis/fig08_model_propositions.{pdf,png}` — synthesis (probably stale, may need rework)
- `figures/thesis/fig09_B9_perfirm_q2_trajectory.{pdf,png}` — Slide II.3 ✓ (new)

### Existing regressions (committed, ready to cite)

- `b9_replicated_isp_grain.py` — Big-4 q₂ at firm-ISP-replicated grain, F=477.5, p<10⁻¹⁰²
- `b9_replicated_isp_apr_sep.py` — Apr-Sep robustness, F=186, p=4e-41
- `q2_definitions_compare.py` — 5 q₂ definitions table
- `rt2_post_blackout_channel.py` — RT2 by firm/tech/hour/unit
- (Existing) S5/S6/B6/B7/S7 scripts in `scripts/analysis/welfare/`

### Figures to BUILD before May 5

| Slide | Figure | Source data |
|---|---|---|
| I.1 | Reform-sequence timeline schematic | (compose) |
| I.4 | Imbalance-price-spread vs volume channel breakdown | `b6_s6_magnitude_check.csv` |
| II.1 | q₂ definitions table (5 measures × Big-4 mean per regime) | `q2_definitions_compare.csv` |
| II.2 | Big-4 vs Fringe q₂ gap with cluster-robust 95% CIs | `b9_replicated_isp_grain.py` output |
| II.4 | Apr-Sep vs full-sample coefficient comparison | `b9_replicated_isp_apr_sep.csv` + `b9_replicated_isp_grain.csv` |
| III.1 | RT2-up by firm × technology bar chart (DA15/ID15) | `rt2_post_blackout/02_pertechnology.csv` |
| III.2 | RT2 hour-of-day profile (DA15/ID15) | `rt2_post_blackout/03_hourofday_DA15ID15.csv` |

7 figures to build. Each is a quick matplotlib plot; total work ~2–3h.

### Regressions to verify before May 5

- B9 firm-ISP regression — re-run to confirm reproducibility (already committed; just re-run as final check) ✓
- Apr-Sep robustness — same ✓
- RT2 channel — verify Oct-2025 step-jump is a real REE behavior change, not a data-structure shift. Cross-check against ESIOS `totalrp48preccierre` if time permits.

---

## Findings deliberately sidelined for this presentation

These are alive but not part of the friction-window arc; mention only in passing
or omit:

- **F7/F8/F10/F11** — IB structural pivotality / Cournot conduct: regime-invariant,
  doesn't speak to the reform sequence. Save for the full thesis, not the
  asymmetric-friction story.
- **F14/F15/F17/F18/F19/F20/F21/F22** — post-blackout firm-specific findings,
  CNMC enforcement, cross-market specialisation: these belong in Part IV of the
  thesis; for the presentation, only RT2 (which IS reform-period) is relevant.
- **B3/B4** — earlier behavioural findings, less central than B6/B7/B9 for the
  friction arc.
- **F5/AV (RETIRED 2026-04-29)** — mechanical accounting identity, in attic.

---

## The model that the empirics motivate

The presentation should END with a one-slide model sketch that the empirical
findings will support. Direction:

**Setup.** Sequential markets with two clearings:
- t=1: DA market clears at clock granularity Δ_DA (60-min or 15-min)
- t=2: IDA market clears at clock granularity Δ_IDA (15-min)
- Settlement clock at granularity Δ_S (60-min until ISP15 reform; 15-min after)

Define the **granularity-friction parameter** τ as the mismatch between
Δ_DA, Δ_IDA, Δ_S:
- τ = 0 when all three coincide (pre-IDA: 60/60/60; DA15/ID15: 15/15/15)
- τ > 0 when they differ (e.g., DA60/ID15: 60/15/15)

**Strategic firm choice.** Big-4 firms set q₁ in DA and q₂ in IDA. Under
τ = 0 (frictionless symmetry), they play the standard Allaz-Vila /
Ito-Reguant strategic forward-undercommitment equilibrium → q₂ > 0.
Under τ > 0, the friction reduces the value of strategic forward
undercommitment (because the spot-side flexibility is harder to exploit
when settlement clock differs from market clock) → q₂ compressed.

**Comparative statics (the predictions to match):**
1. ∂(BRP→TSO transfer)/∂τ > 0 — Act I evidence.
2. ∂q₂/∂τ < 0 (in absolute value) — Act II evidence.
3. q₂ recovers when τ → 0 — Act II U-shape evidence.

**Outside the model.** The post-blackout RT2 cascade is a parallel
exogenous regulatory shock, not generated by τ. Reported separately to
avoid contamination.

This setup gives a CLEAN 3-prediction model that maps directly to three
empirical anchors. The follow-on work after the presentation is to
formalize τ, derive comparative statics, and verify the magnitudes match.

---

## Decision questions for the user

1. **Is the U-shape the right headline?** It's stronger than the legacy "MTU15
   stops market power" reading, but it requires explaining that the *recovery*
   at DA15/ID15 is the cleanest evidence (it isolates friction-only from
   structural change).
2. **How prominently should RT2 feature?** Two slides feels right for a
   parallel channel, but it's a striking finding that could draw audience
   attention away from the friction arc. Could be cut to one slide if
   tight on time.
3. **Does the model sketch belong on a slide or only in talking points?**
   Audiences sometimes appreciate seeing the model setup explicitly; sometimes
   they want only empirics. Given this is preliminary and the goal is to
   set up the model, I'd say put the τ sketch on a slide.
4. **Do we need to verify the RT2 Oct-2025 step-jump is real (not a
   data-structure shift) before May 5?** If yes, we should cross-check
   against ESIOS `totalrp48preccierre` first.
