# `explore/` — working notebooks and narrative documents

Not thesis output. The notebooks and markdown docs here carry the analysis that feeds the thesis. Thesis-grade synthesis lives in [`thesis/proposal.md`](../thesis/proposal.md).

## Where to look first

- **[`/CLAIMS_LEDGER.md`](../CLAIMS_LEDGER.md)** — single source of truth: 37 alive findings (S1–S9, F1–F22, B1–B9, D1–D5), status (alive / wounded / dead), evidence script + notebook per claim. Open first.
- **[`/thesis/proposal.md`](../thesis/proposal.md)** — 5-part synthesis (system friction → IB structural → cross-market specialisation → CNMC enforcement → behavioural appendix). Written 2026-04-28.
- **`_modelling_track.md`** — economic-modelling sections (§0 IB-canonical synthesis; §1 Cournot-pivotality alive for IB; §2 Allaz–Vila **REJECTED** 2026-04-27; §3 Pigouvian alive; §4 asymmetric-granularity friction alive; §5 bid complexification alive; §6 strategic availability under within-firm fleet substitution — placeholder for Part IV).
- **`_audits.md`** — combined audit doc (Part A coherence check across alive claims; Part B red-team adversarial attacks with defended/pending markers). Replaces the former `_coherence_audit.md` + `_red_team_audit.md`.
- **`_identification_target.md`** — frozen appendix-grade identification provenance (A1–C + D1–D17). Do not rewrite.
- **`RESEARCH_LOG.md`** — structured front-matter: thesis question, hypotheses register (§4), methods attempted, notebook index, current state. Updated only when hypotheses or methods change.
- **`RESEARCH_DIARY.md`** — append-only chronological diary of analyses, claim changes, and decisions (split off from RESEARCH_LOG on 2026-04-28).
- **[`/CLAUDE.md`](../CLAUDE.md)** § "Claim-status discipline" — procedure for updating these docs when a claim's status changes.

## Notebook map

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
| 14 | ALIVE (provisional) | Early thesis figures (3). Superseded for May presentation by [`thesis/presentations/workshop_may_2026/figures.ipynb`](../thesis/presentations/workshop_may_2026/figures.ipynb). |

The May 2026 preliminary-results notebook (5 figures: S5, S6, B6, B7, S6 blackout-split) **lives outside `explore/`** in [`thesis/presentations/workshop_may_2026/`](../thesis/presentations/workshop_may_2026/) so that presentation-deliverable source artefacts stay separate from exploratory work.

Archived notebooks (in `archive/`): 01, 02, 04, 06, 07, 08. See `archive/README.md` for retention reasons.

## Live-claim cluster outside notebooks

Many alive findings since mid-April 2026 are produced by `scripts/analysis/*.py` directly (no notebook), per `feedback_notebooks_vs_scripts.md`. These include the F7-F22 structural-firm cluster, S6/S7/S8 system-layer welfare scripts, and the F19/F20 aFRR per-firm decomposition. See the `Evidence script` column in `CLAIMS_LEDGER.md`.
