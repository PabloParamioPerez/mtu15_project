# `notebooks/` — working notebooks and narrative documents

Not thesis output. The notebooks and markdown memos here carry the analysis that feeds the thesis. Thesis-grade synthesis lives in [`/thesis/proposal.md`](../thesis/proposal.md).

## Subfolder map

- **`eda/`** — numbered exploratory data-analysis notebooks (active).
- **`memos/`** — research diaries, modelling track, audits, identification target. Markdown only.
- **`archive/`** — superseded exploratory work (do not reuse without re-validating against current data).

## Where to look first

- **[`/CLAIMS_LEDGER.md`](../CLAIMS_LEDGER.md)** — single source of truth: 37 alive findings (S1–S9, F1–F22, B1–B9, D1–D5), status (alive / wounded / dead), evidence script + notebook per claim. Open first.
- **[`/thesis/proposal.md`](../thesis/proposal.md)** — 5-part synthesis (system friction → IB structural → cross-market specialisation → CNMC enforcement → behavioural appendix).
- **[`memos/_modelling_track.md`](memos/_modelling_track.md)** — economic-modelling sections (§0 IB-canonical synthesis; §1 Cournot-pivotality alive for IB; §2 Allaz–Vila **REJECTED** 2026-04-27; §3 Pigouvian alive; §4 asymmetric-granularity friction alive; §5 bid complexification alive; §6 strategic availability under within-firm fleet substitution — placeholder for Part IV).
- **[`memos/_audits.md`](memos/_audits.md)** — combined audit doc (Part A coherence; Part B red-team).
- **[`memos/_identification_target.md`](memos/_identification_target.md)** — frozen appendix-grade identification provenance (A1–C + D1–D17). Do not rewrite.
- **[`memos/RESEARCH_LOG.md`](memos/RESEARCH_LOG.md)** — structured front-matter: thesis question, hypotheses register (§4), methods attempted, notebook index, current state. Updated only when hypotheses or methods change.
- **[`memos/RESEARCH_DIARY.md`](memos/RESEARCH_DIARY.md)** — append-only chronological diary of analyses, claim changes, and decisions.
- **[`/CLAUDE.md`](../CLAUDE.md)** § "Claim-status discipline" — procedure for updating these docs when a claim's status changes.

## Notebook map (notebooks/eda/)

Each active notebook has a cell-1 STATUS block. Status semantics in `CLAIMS_LEDGER.md`.

| nb | Status | Role |
|---|---|---|
| 03 | ALIVE | Descriptive bedrock — DA-IDA wedge, within-hour dispersion, ΔQ compression. |
| 05 | ALIVE | Engineering decomposition — rejects H1–H4 mechanical alternatives. |
| 09 | ALIVE (descriptive) | Bid-structure EDA + unit-level audit. Anchors B8. |
| 10 | ALIVE | Rule 28.8 demand-side diagnosis (B5). |
| 11 | ALIVE | **System layer headline.** Four-way ENTSO-E concordance. Anchors S1–S5. |
| 12 | ALIVE (caveat) | Cournot-Nash Lerner. Headline = Spec 3 matched-price contrasts (F1, F2). |
| 13 | ALIVE (caveat) | Bid prices, XBID liquidity, revenue. X4 retracted; B2/B3/B4. |
| 14 | ALIVE (provisional) | Early thesis figures (3). Superseded for May presentation by [`/thesis/presentations/workshop_may_2026/figures.ipynb`](../thesis/presentations/workshop_may_2026/figures.ipynb). |

Archived notebooks (in `archive/`): 01, 02, 04, 06, 07, 08. See [`archive/README.md`](archive/README.md) for retention reasons.

## Presentation-deliverable notebooks live elsewhere

Notebooks that produce final slides for a workshop or defense **do not live in `notebooks/`**. They live under `thesis/presentations/<workshop>/` so presentation-deliverable source artefacts stay separate from exploratory work:

- `thesis/presentations/workshop_february_2026/` — first thesis-progress workshop
- `thesis/presentations/workshop_may_2026/figures.ipynb` — second thesis-progress workshop (active)

## Live-claim cluster outside notebooks

Many alive findings since mid-April 2026 are produced by `scripts/analysis/*.py` directly (no notebook), per `memos/feedback_notebooks_vs_scripts.md`. These include the F7-F22 structural-firm cluster, S6/S7/S8 system-layer welfare scripts, and the F19/F20 aFRR per-firm decomposition. See the `Evidence script` column in `CLAIMS_LEDGER.md`.
