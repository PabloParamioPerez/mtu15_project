# Presentation 2 — May 2026 preliminary results

CEMFI preliminary-results presentation, ~2026-05-05. Continuity with `presentation1/` (Feb 2026 thesis proposal).

## Contents

- **`build_figures.py`** — one-time builder. Generates the executable notebook below from a single source of truth (markdown + code cells inlined). Re-run after editing the slide arc or figure code.
- **`figures.ipynb`** — the executable notebook. Running it produces the 5 figures used in the slide deck and writes them to [`../figures/`](../figures/) (shared with the thesis chapters; same files re-used).
- **`Paramio_Pablo_slides_may2026.{tex,pdf}`** — the slide deck itself (added when written; mirror of `presentation1/Paramio_Pablo_slides.pdf` naming convention).

## Workflow

```bash
# 1. Edit the slide arc / figure code in build_figures.py
# 2. Rebuild the notebook
uv run python thesis/presentations/workshop_may_2026/build_figures.py
# 3. Execute the notebook (regenerates figures in ../figures/)
uv run jupyter nbconvert --to notebook --execute --inplace thesis/presentations/workshop_may_2026/figures.ipynb
```

## Why a separate folder

Presentation-deliverable source artefacts are kept apart from `explore/` (exploratory notebooks) and `scripts/analysis/` (per-claim analysis scripts) so that "what produces the slides" is unambiguous. `figures/thesis/` stays shared at thesis-level because the same figures appear in both the May presentation and Part I of the thesis chapters; duplicating per-deliverable would create drift.

## The 5 figures

| # | File | Lead claim | Layer / theory anchor |
|---|---|---|---|
| 1 | `fig01_S5_four_panel_concordance` | 4 ENTSO-E metrics jump concordantly at ISP15, moderate at MTU15-DA | System; §4 asymmetric-granularity friction |
| 2 | `fig02_S6_settlement_transfer_headline` | **€1,094.9M cumulative excess; bootstrap CI [-90, +73]M** | System; §4 — **headline** |
| 3 | `fig03_B6_passthrough_by_regime` | Forecast-error → imbalance R²: 0.365 (DA60/ID15 POST-blackout) → 0.028 (DA15) | System; §4 mechanism |
| 4 | `fig04_B7_france_placebo` | Spain DA volatility responds 2–3× more than France across reform dates | Cross-country control |
| 5 | `fig05_S6_blackout_robustness` | DA15 collapse holds DESPITE operación reforzada | Robustness (n=3 caveat for Oct–Dec 2025) |

## Image-dimension safety

`build_figures.py` sets `savefig.dpi=140` so the PNG outputs are ≤1890 px wide — under the 2000-px session cap that broke an earlier build (which used `savefig.dpi=200` and produced 2670-px PNGs). Vector PDFs are unaffected. See [`feedback_figure_dimensions.md`](../../.claude/projects/-Users-pabloparamio-Desktop-CEMFI-2nd-Year-Master-Thesis-mtu15-project/memory/feedback_figure_dimensions.md) for the full rule.
