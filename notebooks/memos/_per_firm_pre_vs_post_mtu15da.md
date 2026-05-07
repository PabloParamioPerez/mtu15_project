# Pre-vs-post MTU15-DA: are firm-specific CCGT bid ceilings reform-induced?

**Created:** 2026-05-07

**Purpose.** Determine whether the firm-specific p_max ceilings observed
post-reform (IB 345, GN 1000, HC 699, GE 2350) were created by the
MTU15-DA reform or pre-exist as firm trading-desk policy. This is now
testable for the first time because the pre-reform DA DET parser bug
was fixed on 2026-05-07.

**Comparison.** Same calendar months (Oct-Dec) across the MTU15-DA
reform: Oct-Dec 2024 (pre-reform, MTU60) vs Oct-Dec 2025 (post-reform,
MTU15). Same-cal-month controls for seasonality. CCGT sell-side bids
only.

**Source.** `scripts/analysis/bid/per_firm_pre_vs_post_mtu15da.py`,
outputs in `results/regressions/bid/perfirm_*_pre_vs_post*.csv`.

## Headline: ceilings are firm policy, not a reform response

p_max in critical hours h{18-22}, by firm:

| Firm | PRE 2024 (MTU60) | POST 2025 (MTU15) | Δ% |
|---|---:|---:|---:|
| **GN (Naturgy)** | **1000.0** | **1000.0** | **0.0%** |
| Fringe | 456.6 | 444.1 | −2.7% |
| **HC (EDP)** | 646.0 | 699.0 | +8.2% |
| **IB (Iberdrola)** | 385.8 | 344.8 | −10.6% |
| **GE (Endesa)** | **2955.4** | **2350.4** | **−20.5%** |

**Naturgy's 1000 €/MWh ceiling is rock-solid before AND after the reform.**
Same number, exactly. This is unambiguous evidence that the ceiling is
internal trading-desk policy and was NOT created by MTU15-DA.

**HC and IB ceilings are stable to ±10%** — small adjustments, but
qualitatively the same firm-level ceiling structure pre and post. HC's
small bump (646 → 699) and IB's small reduction (386 → 345) are
consistent with internal nominal-price recalibration, not strategic
response to a market design change.

**GE is the exception with a 20% reduction.** Endesa CCGTs bid at the
~3000 €/MWh near-cap level in late 2024 (mean p_max 2955, essentially
all units at the bid cap). Post-MTU15-DA, they pulled back to ~2350.
This is a real strategic change deserving investigation (see below).

## Ladder structure: increased granularity, mostly within existing ceilings

Tranches per period (=per hour pre, per quarter post), critical hours:

| Firm | PRE per hour | POST per quarter | POST per hour summed |
|---|---:|---:|---:|
| IB | 10.16 | 10.18 | 10.18 × 4 × (1−mech) ≈ 10-15 |
| GN | 12.05 | 11.06 | similar; varied by hour |
| HC | 5.96 | 6.00 | constant 6 (mech=1.00) |
| Fringe | 4.78 | 4.03 | similar |
| GE | 2.94 | 2.00 | tighter |

The post-reform period is 1/4 the duration. So a constant n_tranches
per period means 4× more tranches per HOUR if the 4 quarters carry
different bids (mech=0), or the same effective shape if mech=1.

Per `_per_firm_hourly_bidshape.md`, IB and HC have mech_strict ≈ 1.0
(uniform across 4 quarters), so their effective hourly tranches are
unchanged. GN has mech_strict drops to 0.19-0.34 at h16-17, so they
*do* increase their effective hourly tranches (~12 → ~20-24). This is
the granularity-exploitation finding restated.

## Ladder midpoint dynamics: GE and GN shifted UP, IB shifted DOWN

p_med (median tranche price), critical hours:

| Firm | PRE 2024 | POST 2025 | Direction |
|---|---:|---:|---|
| **GE** | 585 | **1015** | **UP +430** |
| GN | 177 | 222 | up +45 |
| Fringe | 172 | 204 | up +32 |
| HC | 114 | 106 | flat |
| **IB** | 159 | **130** | **DOWN −29** |

Reading these together with p_max:

- **GE moved p_med UP and p_max DOWN.** Their bidding tightened: ladder
  shifted to a higher floor (~1000) and lower ceiling (~2350). The
  ladder *compressed* into a narrower high-price band rather than
  spreading from low to cap. This is consistent with strategic
  withdrawal from "all-or-nothing" cap-bidding toward a more controlled
  high-price ladder.

- **IB moved both p_med and p_max DOWN.** Iberdrola lowered their
  entire ladder. With ceilings already low (350) and median-tranche
  even lower (130), IB is bidding to clear, not to extract scarcity
  rents. This matches the structural finding that IB has the largest
  flex-strategic share (30%) but bids it competitively.

- **GN moved p_med UP modestly with p_max fixed.** Naturgy raised the
  middle of their ladder while holding the 1000 ceiling constant.
  Strategic floor lift, not a ceiling change.

## What changed at the reform — and what didn't

**Did NOT change at MTU15-DA:**
- Firm-specific p_max ceilings (Naturgy fixed; others ±10%).
- Number of tranches per period (count is similar pre and post).
- Mechanical-repeat rate within the legacy structure (where comparable).

**DID change at MTU15-DA:**
- Effective hourly tranches via the new quarter dimension (Naturgy especially).
- GE's strategic stance: shift from cap-bidding to a tighter high-price ladder.
- Median tranche prices: IB compressed downward; GE/GN/Fringe shifted up.

## Implication for the within-market granularity model

The model in `_within_market_granularity_model.md` should explicitly
treat firm-specific ceilings as **exogenous parameters** — internal
policy constraints that bound the strategic optimization, but are not
themselves the strategic margin. The reform-driven strategic conduct
(ladder enrichment, granularity exploitation, p_med shifts) operates
*below* the firm-specific ceilings.

This is a clean modelling choice: ceilings are parameters; ladder shape
within ceilings is the equilibrium object. The B14 finding ("each firm
operates a near-rigid internal price ceiling") is now confirmed
empirically across the reform.

## What to investigate about GE's 20% ceiling reduction

GE Endesa's p_max in critical hours dropped from 2955 (Oct-Dec 2024)
to 2350 (Oct-Dec 2025). This 20% reduction is the only major
firm-specific ceiling change at the reform. Hypotheses:

1. **System bid cap reduction.** If OMIE reduced the bid cap from 3000
   to ~2400-2500 around the MTU15-DA implementation, this would
   mechanically explain GE's shift. *Need to check OMIE rule changes
   for 2025 H2.*

2. **CNMC investigation aftermath.** The CNMC opened ~100 expedientes
   (including GE) on 2026-04-23 for unauthorized production reduction
   and voltage-control violations. If parallel investigations were
   pending earlier, GE may have moderated cap-bidding to reduce
   regulatory exposure. *Timing matches: investigation period covers
   2023+; ceiling reduction observed in Oct 2025.*

3. **Internal risk-policy change.** Endesa may have updated their
   trading-desk mandate to avoid cap-bidding-related reputation risk
   after the April 28 2025 blackout (which raised regulatory scrutiny).
   *Plausible but harder to verify externally.*

The reduction does NOT appear to be reform-mechanism-driven (Naturgy's
ceiling didn't move; nothing in MTU15-DA's design forces a lower max).
It looks like a firm-specific behavioural change that coincides with
the reform window but is independent of the reform mechanism.

## Sources

- `scripts/analysis/bid/per_firm_pre_vs_post_mtu15da.py`
- `results/regressions/bid/perfirm_pre_vs_post_mtu15da_pooled.csv`
- `results/regressions/bid/perfirm_pre_vs_post_by_hour_class.csv`
- `results/regressions/bid/perfirm_pmax_critical_pre_vs_post.csv`
- `results/regressions/bid/perfirm_ntranches_critical_pre_vs_post.csv`
- `results/regressions/bid/perfirm_pmed_critical_pre_vs_post.csv`
