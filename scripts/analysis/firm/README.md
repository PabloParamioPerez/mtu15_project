# `scripts/analysis/firm/` — firm-level strategic conduct (Acts II)

Analyses that test firm-level strategic IDA repositioning, Big-4 vs Fringe gap, per-firm heterogeneity. Together these constitute Act II of the thesis friction arc.

## What lives here

- **`b9_*`** — B9 family (the headline regression family). Includes:
  - `b9_replicated_isp_grain.py` — main regression (1.93M firm-ISP, F=477) at MTU15-replicated grain
  - `b9_combined_total_voluntary.py` — q^total = q₂_IDA + q^CI strong-friction test (F=1,497)
  - `b9_replicated_isp_apr_sep.py` — same-cal-month robustness
  - `b9_hour_of_day_interaction.py` — hour-bucket compression depth (model §5.7 prediction)
  - `b9_firm_shape_rho.py` — Prop 5 cross-firm shape-exposure test
  - `b9_continuous_market_substitution.py` — q^CI substitution check
  - `b9_perfirm_q2_figure.py` — per-firm trajectory figure
  - `b9_f5_firm_consistency.py` — cross-mechanism consistency (B9 vs F5)
  - and others
- **`q2_definitions_compare.py`** — six q₂ definitions compared (IR-cleanest selection)
- **`f12_*`** — pumped-storage arbitrage and per-hour reghdfe
- **`f15_*`** — post-blackout CCGT windfall
- **`f16_*`** — CCGT supply-slope by firm
- **`renewable_capture_price.py`** — wind/solar price-capture by firm

## What does NOT belong here

- System-level friction (S5/S6/B6/B7/S7/S8) → `system/`
- RT2 / CNMC enforcement → `regulatory/`
- aFRR / mFRR per-firm decomposition → `balancing/`
- Lerner / Cournot markup work → `lerner/`
- F7/F8/F10/F11 IB pivotality (synthetic-firm method) → `synthetic/`
