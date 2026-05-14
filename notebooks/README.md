# `notebooks/` — exploratory notebooks and research memos

Not thesis output. The notebooks and markdown memos here carry the analysis that feeds the thesis paper at [`thesis/paper/paper.tex`](../thesis/paper/paper.tex). The thesis is currently in active drafting.

## Subfolder map

- **`eda/`** — numbered exploratory data-analysis notebooks (active). Cell-1 markdown STATUS block per notebook (mirrors the script header convention).
- **`memos/`** — research diary, modelling track, audits, identification target. Markdown only.
- **`attic/`** — superseded exploratory work. Do not reuse without re-validating against current data.

## Where to look first

| Document | Purpose |
|---|---|
| [`/CLAIMS_LEDGER.md`](../CLAIMS_LEDGER.md) | Single source of truth: every empirical claim with status (alive / wounded / dead) and evidence pointer. Open first. |
| [`/thesis/paper/paper.tex`](../thesis/paper/paper.tex) → [`paper.pdf`](../thesis/paper/paper.pdf) | The thesis paper itself (active drafting). |
| [`memos/_modelling_track.md`](memos/_modelling_track.md) | Economic-modelling sections: Cournot-pivotality, Allaz–Vila (REJECTED 2026-04-27), Pigouvian, asymmetric-granularity friction, bid complexification, strategic availability. |
| [`memos/_identification_target.md`](memos/_identification_target.md) | Frozen appendix-grade identification provenance (A1–C + D1–D17). Do not rewrite. |
| [`memos/_audits.md`](memos/_audits.md) | Combined audit doc (Part A coherence; Part B red-team). |
| [`memos/_critical_hours_calibration.md`](memos/_critical_hours_calibration.md) | Empirical calibration of the critical-vs-flat hour partition. |
| [`memos/_parallel_trends_diagnostic.md`](memos/_parallel_trends_diagnostic.md) | Pre-trend diagnostic for the within-day DiD. |
| [`memos/_esios_archive_catalog.md`](memos/_esios_archive_catalog.md) | ESIOS API archive triage memo (per-BSP / per-UP / per-EIC / firm chain + indicator endpoint inventory). |
| [`memos/RESEARCH_LOG.md`](memos/RESEARCH_LOG.md) | Structured front-matter: thesis question, hypotheses register, methods attempted, notebook index, current state. |
| [`memos/RESEARCH_DIARY.md`](memos/RESEARCH_DIARY.md) | Append-only chronological diary of analyses, claim changes, decisions. |
| [`/CLAUDE.md`](../CLAUDE.md) § "Claim-status discipline" | Procedure for updating these docs when a claim's status changes. |

## Notebook map (`notebooks/eda/`)

Each active notebook has a cell-1 STATUS block. Status semantics in `CLAIMS_LEDGER.md`.

| nb | Status | Role |
|---|---|---|
| 03 | ALIVE | Descriptive bedrock — DA-IDA wedge, within-hour dispersion, ΔQ compression. |
| 05 | ALIVE | Engineering decomposition — rejects H1–H4 mechanical alternatives. |
| 09 | ALIVE (descriptive) | Bid-structure EDA + unit-level audit. Anchors B8. |
| 10 | ALIVE | Rule 28.8 demand-side diagnosis (B5). |
| 11 | ALIVE | System layer headline — four-way ENTSO-E concordance. Anchors S1–S5. |
| 12 | ALIVE (caveat) | Cournot-Nash Lerner. Headline = Spec 3 matched-price contrasts (F1, F2). |
| 13 | ALIVE (caveat) | Bid prices, XBID liquidity, revenue (B2/B3/B4). |
| 14 | ALIVE (provisional) | Early thesis figures; superseded for May presentation by [`workshop_may_2026/figures.ipynb`](../thesis/presentations/workshop_may_2026/figures.ipynb). |

Attic notebooks (in `attic/`): 01, 02, 04, 06, 07, 08. See [`attic/README.md`](attic/README.md) for retention reasons.

## Presentation-deliverable notebooks live elsewhere

Notebooks that produce final slides for a workshop or defense **do not live in `notebooks/`**. They live under `thesis/presentations/<workshop>/` so presentation-deliverable source artefacts stay separate from exploratory work:

- `thesis/presentations/workshop_february_2026/` — first thesis-progress workshop
- `thesis/presentations/workshop_may_2026/figures.ipynb` — second workshop (active)

## Conventions

- Notebooks are for **exploration and visual category comparison**. Tabular / regression / diagnostic work goes into `scripts/analysis/*.py` instead.
- Research memos are markdown-only — no notebooks under `memos/`.
- Figures generated for the paper write to `figures/thesis/` (referenced by `paper.tex` via `\graphicspath`).
- Notebook outputs must stay below 2000 px per axis to avoid breaking the session.

## Live-claim cluster outside notebooks

Many alive findings since mid-April 2026 are produced by `scripts/analysis/*.py` directly (no notebook). These include the F7–F22 structural-firm cluster, S6/S7/S8 system-layer welfare scripts, and the F19/F20 aFRR per-firm decomposition. See the `Evidence script` column in `CLAIMS_LEDGER.md`.
