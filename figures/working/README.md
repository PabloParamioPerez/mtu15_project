# `figures/working/`

Work-in-progress figures. Tex/script-referenced files live at the root so that
`\graphicspath{{../../figures/working/}}` resolves them; thematic groups of
exploratory or inspection-only figures live in subfolders.

## Subfolders

- **`bidshape/`** — recent per-curve bid-shape exploration (σ_p, N_eff levels
  and critical-flat differentials, multi-bandwidth, within-hour D_σ / D_N).
  Inspection-only figures generated to inform §4 of `advisor_memo.tex`.
- **`bid_atlas/`** — large set of DA/IDA descriptive bid-curve figures from
  `notebooks/eda/15_bid_shape_atlas.ipynb`.
- **`bid_dispersion/`** — D_w / D_p / D_q histograms and event-study figures
  from the bid-dispersion exploration.
- **`gap/`** — DA-IDA price-gap (PDBC vs PDBF) lines and deviations.
- **`q1_fase1/`** — Fase I (Q1) redispatch figures: CCGT monthly, fleet by tech,
  pre-Fase-I (PDBF) vs post-Fase-I (PHF) actuals.

Files at the root are referenced by `thesis/provisional/*.tex` or by the
critical-flat-DiD / per-curve-metrics analysis scripts. Move to `../attic/`
when retired; promote to `../thesis/` when the underlying claim has settled.
