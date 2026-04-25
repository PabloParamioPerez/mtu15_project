# Open empirical robustness / regression / idea queue

Working list of analyses to run before any thesis writing. Created
2026-04-25. Order is suggested but not strict.

## Priority 1 — Tighten the central nb12 claim further

- [ ] **Within-tech Lerner decomposition**. Are GE's high-Lerner
      hours dominated by CCGT clearing or hydro? If CCGT, mechanism
      story (Ito-Reguant strategic withholding) is much tighter and
      links directly to nb08 §8 GE×CCGT signed flip.
- [ ] **Bootstrap CIs around price-bin-FE Spec 3 contrasts**. Currently
      report HC3 SE; bootstrap is more robust to serial correlation
      in panel.
- [ ] **France DA price placebo**. We have ENTSO-E A44 FR prices
      synced. Run the same regime-contrast spec on French DA prices
      (and a derived French Lerner-equivalent if estimable). If FR
      shows similar regime contrasts, our results are EU-wide
      confounds, not Spanish-reform effects. If FR stays flat,
      Spain reform-attributable claim strengthens.
- [ ] **Demand-side slope included in Lerner formula**. Currently
      assume |∂D/∂p|=0. Estimate demand slope from curva_pbc buy
      side and include. Should lower all Lerners but may flatten
      the regime contrast pattern.
- [ ] **Trim hours where p* < €10**. The static-FOC formula breaks
      down at very low prices; restrict sample and check robustness.

## Priority 2 — New regressions on existing data

- [ ] **Bid-shading regression**: offer_price − clearing_price as
      function of regime + firm + price-bin FE. Tests whether firms
      bid more above clearing across regimes.
- [ ] **Capacity-withholding ratio**: cleared MW / offered MW per
      (firm, regime). If firms clear less of what they offer
      post-reform, that's withholding. Cross-check Lerner.
- [ ] **Welfare proxy / producer surplus**: q × (p − MC_implied)
      per (firm, regime), where MC_implied = p × (1 − Lerner). Gives
      a euros-of-rent estimate per regime. Useful for thesis welfare
      discussion.
- [ ] **Settlement cost per unit imbalance**: A87/A86 ratio across
      regimes. €/MWh of imbalance. Tests whether ISP15 raised the
      per-MWh cost of imbalance or just the volume.
- [ ] **Within-hour DA price dispersion regression** (post-MTU15-DA
      only): explicitly test 15-min price coefficient of variation
      against pre/post.
- [ ] **Cross-regime DA-IDA wedge regression** with controls (wind
      forecast error from A75, load).

## Priority 3 — Idea exploration / open questions

- [ ] **HHI panel** across hours and regimes. How concentrated is
      DA selling? Does HHI rise at DA60/ID15 then fall at MTU15-DA?
      (Lerner formula already partly captures this; explicit HHI
      gives a more standard market-power statistic.)
- [ ] **Concentration of the Lerner peak**: which (firm, hour, day)
      cells contribute the bulk of the DA60/ID15 elevation? If a
      few specific high-Lerner hours dominate, the claim is fragile.
- [ ] **Storage entry effects on supply slope**. Spain has been
      commissioning batteries through 2025-26. Did storage entry
      flatten the supply curve at the margin and inflate Lerner?
- [ ] **Renewable curtailment correlation** with Lerner. High-
      curtailment hours (low p*, surplus supply) are precisely the
      hours where the Lerner formula blows up.
- [ ] **Cross-border flows from Spain to France** as residual
      demand modifier. Net exports in low-price hours might be
      shrinking the residual demand facing Spanish firms.
- [ ] **Test for mean reversion in DA-IDA wedge** across regimes.
      Does the wedge serial-correlation change at reform boundaries?
- [ ] **Pre-reform secular trend extrapolation**: project the
      2018-2024 Lerner trend forward; what does it predict for
      DA60/ID15 absent any reform? Compare with realised.

## Priority 4 — Build out more outcomes

- [ ] **Wholesale price level as outcome** (volatility, dispersion,
      negative-price hours).
- [ ] **Imbalance volume × price interaction**: where does the cost
      come from — high volumes in high-price periods or vice versa?
- [ ] **Auto-correlation of cleared quantities**: do firms have
      different stickiness across regimes?
- [ ] **Cross-firm correlation of bid prices**: does collusion-like
      coordination intensify or weaken across regimes?

## Output and tracking

Each completed item should land:
1. Output table/figure in `data/derived/` or `figures/` as appropriate
2. Reproducing script in `scripts/analysis/`
3. Result summary in `_robustness_summary.md` (numbered §)
4. If the result changes the headline claim, update
   `_identification_target.md` D-section accordingly

Memory: `feedback_no_thesis_drafting_yet.md` — do not propose writing
until this entire queue (or user explicitly closes it) is done.
