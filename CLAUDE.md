# MTU15 Project

Master thesis data-engineering project for OMIE electricity-market data, focused on the MTU15 transition. Managed with `uv` on macOS in VSCode.

## Python / uv workflow
- Always use `uv run` to execute scripts, never `pip` or raw `python`
- Do not add dependencies without asking

## Repo path conventions
- `data/raw/{omie,esios,entsoe}` and `data/processed/{omie,esios,entsoe}` are symlinks to an external SSD
  (`/Volumes/OWC Envoy Pro FX/mtu15_data/{raw,processed}/{omie,esios,entsoe}`)
- Always use repo-relative paths; never hardcode machine-specific paths
- Symlink targets may be slow or absent; do not traverse them speculatively

## Repo layout

The project is organised by purpose at the top level. Each top-level directory has a single responsibility.

**Code:**
- `src/mtu/parsing/` — one module per data family (e.g. `pibca.py`, `pibci.py`, `marginalpdbc.py`)
- `src/mtu/transform/` — period normalization and shared transforms
- `src/mtu/validation/` — post-parse checks (`checks.py`)
- `src/mtu/ingestion/` — shared HTTP/auth/retry helpers (e.g. `entsoe_common.py`, `esios_common.py`)
- `scripts/pipelines/{omie,esios,entsoe}/` — numbered pipeline steps: `00_` download, `10_` parse, `20_` build
- `scripts/analysis/{system,firm,regulatory,balancing,bid,modelling,panels,attic}/` — empirical analyses, organised by topic (system = Acts I friction; firm = Acts II Big-4 strategic; bid = bid-shape and granularity; regulatory = RT2 + CNMC; balancing = aFRR/mFRR/nuclear-availability). The `attic/` subfolder holds retired analyses (lerner/, synthetic/, firm/-dead and other pre-pivot scripts moved 2026-05-04).
- `scripts/admin/` — one-off audit, inspect, and forensic scripts; not part of the pipeline
- `scripts/stata/` — Stata `.do` files

**Data (DATA ONLY, never analytical outputs):**
- `data/raw/{omie,esios,entsoe}/` — symlinks to external SSD; verbatim source files
- `data/processed/{omie,esios,entsoe}/` — canonical Parquet (one per family)
- `data/derived/panels/` — analysis-ready panels built from processed data
- `data/derived/attic/` — retired derived datasets
- `data/interim/`, `data/metadata/`, `data/external/` — intermediate parsing, manifests, reference tables

**Analytical outputs (code-dependent products):**
- `results/regressions/{system,firm,bid,balancing,regulatory,modelling,descriptive}/` — regression CSVs subdirectorized by topic, mirroring `scripts/analysis/`. New analyses should write to the matching topic subfolder. `firm/` is further split into `firm/{critical_hours,pdbf,b9,other}/` since it would otherwise be too dense. (Restructured 2026-05-04.)
- `results/tables/` — tables for thesis/presentation (currently empty post-2026-05-04 cleanup; the pre-pivot tables are in `results/attic/tables/`)
- `results/attic/{regressions,robustness,summaries,tables}/` — retired analytical outputs from pre-pivot framing (Lerner work, dead claims, old workshop tables)

**Figures:**
- `figures/thesis/` — final figures referenced by the thesis text (currently empty; the pre-pivot workshop figures are in `figures/attic/`)
- `figures/presentation/` — presentation-specific figures
- `figures/working/` — work-in-progress figures from analysis
- `figures/attic/` — retired figures from pre-pivot framings

**Writing:**
The thesis output is a single academic paper (`thesis/paper.tex`, sections not chapters). Drafting hasn't started yet.
- `thesis/proposal.md` — master thesis proposal
- `thesis/paper.tex` (and `paper.pdf`) — the paper itself, when drafting begins. Single LaTeX file with `\section{}` blocks (intro, data, model, results, conclusion). If sections grow large, split via `\input{}` into a `thesis/sections/` subfolder.
- `thesis/model/` — directory reserved for the new structural model when written (the pre-pivot `model.tex` was deleted 2026-05-04 since its asymmetric-granularity framing was superseded by the within-day DiD design and within-market granularity model).
- `thesis/narratives/` — presentation narratives, planning documents
- `thesis/presentations/workshop_february_2026/` — first thesis-progress presentation
- `thesis/presentations/workshop_may_2026/` — second thesis-progress presentation (the headline pivot; current reference)

**Notebooks (exploration, not thesis output):**
- `notebooks/eda/` — numbered exploratory data-analysis notebooks
- `notebooks/memos/` — research diaries, modelling track, audits (markdown only)
- `notebooks/attic/` — superseded exploratory work (renamed from `archive/` 2026-05-04 for consistency with other attics)

**External references:**
- `docs/{omie,esios,entsoe}/` — operator file specs and protocol docs
- `docs/regulation/` — BOE/CNMC/EU regulatory documents
- `docs/references/` — academic papers
- `docs/general_references/` — original research proposal, etc.
- `docs/notes/`, `docs/misc/` — codebooks, cheatsheets
- **`docs/notes/SPANISH_MARKET_STRUCTURE.md`** — full sequential-market reference (DA → IDA auctions → continuous → balancing → P48), reform timeline, file-name glossary, and project regime definitions. Read this if any market-mechanics question is unclear.

**Logs and other:**
- `logs/` — runtime logs from heavy runs
- `tests/` — pytest suite
- `attic/` — project-level retired material (theory drafts, parser backups)
- `renv/` + `.Rprofile` — R environment (not currently used, kept for future phases)

## Data families
Active families (each has a parser in `src/mtu/parsing/` and a full `00/10/20` pipeline):

| Family | Market | Description | Spec (v1.37) |
|---|---|---|---|
| `marginalpdbc` | Day-ahead | Clearing prices | §5.1.1.1 |
| `marginalpibc` | Intraday auctions | Clearing prices by session | §5.2.1.1 |
| `pdbc` | Day-ahead | Final programs by unit (auction-cleared only) | §5.1.2.1 |
| `pdbce` | Day-ahead | Final programs by firm | §5.1.2.2 |
| `pdbf` | Day-ahead | PDBC + bilateral-contract executions (offer_type=4 with `bilateral_contract_id`) | §5.1.2.3 |
| `pibca` | Intraday auctions | Accumulated programs | §5.2.2.1 |
| `pibci` | Intraday auctions | Programs by unit and session | §5.2.2.2 |
| `pibcie` | Intraday auctions | Programs by firm and session | §5.2.2.3 |
| `pibcic` | Continuous intraday | Programs by unit and round | §5.3.2.2 |
| `pibcac` | Continuous intraday | Accumulated programs | §5.3.2.1 |
| `pibcice` | Continuous intraday | Programs by firm and round | §5.3.2.3 |
| `precios_pibcic` | Continuous intraday | Aggregate prices | §5.3.1.1 |
| `precios_pibcic_ronda` | Continuous intraday | Mean price by round and period | §5.3.1.2 |
| `curva_pbc` | Day-ahead | Aggregate supply/demand curves | §5.1.3.1 |
| `curva_pibc` | Intraday auctions | Aggregate supply/demand curves | §5.2.3.1 |
| `cab` | Day-ahead | Offer headers | §5.1.4.1 |
| `det` | Day-ahead | Offer details (price/quantity tranches) | §5.1.4.2 |
| `icab` | Intraday auctions | Offer headers | §5.2.4.1 |
| `idet` | Intraday auctions | Offer details | §5.2.4.2 |
| `orders` | Continuous intraday | XBID limit orders | §5.3.3.1 |
| `trades` | Continuous intraday | XBID matched transactions | §5.3.2.7 |
| `capacidad_inter_pbc` | Day-ahead | Interconnection capacity (PBC) | §5.1.6.1 |
| `capacidad_inter_pvp` | Day-ahead | Interconnection capacity (PVP) | §5.1.6.2 |
| `omanulaintra` | Intraday | Annulled offer quantities | §5.2.6.2 |
| `osanulaintra` | Intraday | Annulled session quantities | §5.2.6.1 |
| `phf` | Intraday auctions | Final hourly program by unit and session (OS-established) | §5.2.2.4 |
| `phfc` | Continuous intraday | Final hourly program by unit and round (OS-established) | §5.3.2.4 |

**Parser sharing:** `capacidad_inter_pbc` and `capacidad_inter_pvp` share one parser module (`capacidad_inter.py`), dispatched via `file_family` argument. All other families have their own module.

Before adding or changing a parser, read at least one neighbouring family's parser first.

## Reform dates (frequently referenced)
- **2024-06-14** — IDA reform: 6 local MIBEL sessions → 3 European IDA sessions
- **2025-03-19** — MTU15 intraday: auctions + continuous switch from MTU60 to MTU15
- **2025-10-01** — MTU15 day-ahead: day-ahead market switches from MTU60 to MTU15

These dates appear as constants (`IDA_REFORM`, `INTRADAY_REFORM`, `DAY_AHEAD_REFORM`) in all notebooks and scripts.

## Exploratory notebooks
All notebooks live in `notebooks/eda/` and are for exploration only — not thesis output. Run with the `mtu15-project` kernel. Research-memo markdown (modelling track, audits, identification target, research diary) lives in `notebooks/memos/`. See `notebooks/memos/README.md` for the current notebook map and `CLAIMS_LEDGER.md` for the claim each notebook produces.

Each active notebook has a cell-1 markdown STATUS block (mirrors the script header convention). Do not duplicate analysis across notebooks; check what is already covered before adding a new section.

Presentation notebooks (e.g. `figures.ipynb` for a workshop deck) live under `thesis/presentations/<workshop>/` and are not in `notebooks/eda/`.

## External data sources

Three sources, kept strictly separate at the data layer:

- **OMIE** (primary) — Iberian wholesale market operator. Downloaded via
  `scripts/pipelines/omie/*/00_sync_*.py`. Raw at `data/raw/omie/`,
  processed at `data/processed/omie/`. Symlinked to external SSD.
- **ENTSO-E Transparency Platform** — pan-European balancing /
  generation. Downloaded via `scripts/pipelines/entsoe/*/00_sync_*.py`
  using `ENTSOE_TOKEN` from `.env`. Raw at `data/raw/entsoe/`, processed
  at `data/processed/entsoe/`. Spain control area = `10YES-REE------0`.
  See `src/mtu/ingestion/entsoe_common.py`.
- **ESIOS (REE)** — Spanish national operator data (settlement detail,
  technical-restrictions prices, aFRR offers, unit outages). Public
  archive endpoint `https://api.esios.ree.es/archives/{id}/download`
  served WITHOUT authentication. Per-subject (per-BRP) archives require
  market-participant role registration we do not have. Downloaded via
  `scripts/pipelines/esios/*/00_sync_*.py`. Raw at `data/raw/esios/`,
  processed at `data/processed/esios/`. See
  `src/mtu/ingestion/esios_common.py` and `data/raw/esios/README.md`
  for the source map and tier ordering.

**Source separation rule.** Never mix sources within a single processed
parquet without an explicit `source` column. If you find ESIOS-sourced
content under `entsoe/` (or vice versa), it is a bug — fix the path,
do not relabel.

**Source-overlap policy.** OMIE programmes (`pdbc`, `pdbce`, `pibci`,
`pibcic`, `phf`, `phfc`) cover the same conceptual ground as ESIOS
`p48cierre` / `totalp48*` / `totalpdbf` / `totalpdvp`. We use the OMIE
versions (finer granularity, longer history) and skip the ESIOS
duplicates. ENTSO-E A75 (actual generation per type) covers
ESIOS `REE_ActualGen_` / `REE_AggGenOutput`; we use ENTSO-E.

## Data layers
- **Raw** (`data/raw/`) — verbatim OMIE/ESIOS/ENTSO-E files. Never modify.
- **Processed** (`data/processed/`) — canonical Parquet tables, one per family. Preserve all raw rows and snapshot identity (`source_file`).
- **Derived** (`data/derived/panels/`) — analysis-ready panels (reconciliation, collapsed views, cross-source merges). Live in `data/derived/`, clearly marked as derived. Not substitutes for canonical tables.
- **Analytical outputs** (`results/`) are NOT data — they're code-dependent products of analysis scripts (regression coefficients, summary tables, run reports). Never put them under `data/`.

## Coding expectations
- **Conservative changes** — touch only what is needed; minimal diffs
- **Fast Idempotent scripts** — re-running any pipeline step must produce identical output, and in the fastest way.
- **Inspect before editing** — before modifying a parser or builder for one family, read the analogous file for another family first
- **Preserve structure** — match the style, naming, and conventions of neighbouring files. 
- **No destructive file operations** — no `rm -rf`, no overwriting raw data, no bulk renames without explicit request
- **Very conservative multi-file refactors** - only when it improves substantially the computation time and code

## What not to do
- Do not "fix" duplicate keys unless the economic meaning is clear and confirmed
- Do not collapse snapshot-level data into latest-state views by default
- Do not restructure the pipeline numbering scheme or folder layout
- Do not introduce new dependencies without asking

## Commands
- Lint: `uv run ruff check .`
- Test: `uv run pytest`
- Type-check: `uv run mypy src/`

## Claim-status discipline

The project tracks empirical claim status in `CLAIMS_LEDGER.md` at the repo root. Open economic-modelling questions live in `notebooks/memos/_modelling_track.md`. Identification provenance and history is frozen in `notebooks/memos/_identification_target.md` (no rewrites; appendix-grade).

**Before running any new analysis, the assistant must answer in writing:**

1. Which claim in `CLAIMS_LEDGER.md` does this strengthen (alive) or potentially kill (wounded)? **OR** which entry in `notebooks/memos/_modelling_track.md` does this advance?
2. If neither: stop. Do not run.
3. If yes: estimate runtime + writing-day impact. If total > 0.5 days, stop and ask the user.
4. Any result that changes a claim's status triggers the discipline cycle:
   1. Update the row in `CLAIMS_LEDGER.md` (status, `Date_changed`, reason). Do not delete rows.
   2. Update the producing script's STATUS header (the 4-line block at top).
   3. Update the consuming notebook's synthesis cell — strikethrough dead claims, do not delete cells.
   4. Append one dated line to `notebooks/memos/RESEARCH_DIARY.md` (or `notebooks/memos/RESEARCH_LOG.md` for hypothesis-register changes).
5. Move a script to `scripts/analysis/attic/` only if (a) status is DEAD AND (b) no live notebook imports it. Otherwise leave in place with the `DEAD-KEPT-AS-RECORD` header. Labels are reversible; moves are not in practice.

**Header convention** (one block at top of every script in `scripts/analysis/`):

```
# STATUS: ALIVE | WOUNDED | DEAD-KEPT-AS-RECORD
# LAST-AUDIT: YYYY-MM-DD
# FEEDS: <claim-IDs from CLAIMS_LEDGER, comma-separated>
# CLAIM: <one-line summary>
```

For active notebooks in `notebooks/eda/`, the same four fields appear as a markdown cell-1.

**Status meanings.** *Alive* — passed all documented robustness checks; safe to cite. *Wounded* — survives in narrowed form; cite only with caveat. *Dead* — retracted or contradicted; do not cite as positive result, may appear in identification appendix as "attempted but failed".

## Power-vs-energy discipline (added 2026-05-07)

OMIE's `quantity_mw` field is in MW (instantaneous power), regardless of MTU.
Period duration changes across the reform: 1 hour pre-MTU15, 0.25 hour post.
Summing `quantity_mw` across periods produces a 4× discrepancy that's mechanical
(period count), not physical (no extra capacity bid).

**Naming convention** for any new aggregations or derived columns:
- `*_mw`        — instantaneous power (the OMIE field; rate of energy flow)
- `*_mwh`       — energy delivered over a defined period (= MW × hours)
- `power_*`     — semantic prefix where the unit suffix is awkward
- `energy_*`    — semantic prefix for accumulated quantity over a window

**Hard rules:**
- Never compare raw `SUM(quantity_mw)` across MTU60 and MTU15 — the post-reform
  sum will look 4× larger purely from period count.
- Cross-reform energy comparisons must convert MW → MWh first (multiply by
  period length: 1.0 pre, 0.25 post), then sum.
- Per-period **means** of MW are fine WITHIN a regime (mean MW = capacity
  offered). Across regimes, still flag the period duration in any caption.
- For cross-reform comparison: report **MWh per hour-equivalent** (energy
  per clock hour), not raw period sums.
- Tranche counts, mechanical-repeat rates, and other count-based metrics are
  unaffected by the MW/MWh distinction — they're pure unit counts.

This was the implicit reason B14 used count-based metrics (n_tranches,
mech_strict) for cross-reform comparison rather than quantity sums.

## Seasonality + weather controls (mandatory for cross-regime claims)

Spanish electricity has huge seasonality (winter heating, summer AC, hydro inflows, solar daily/seasonal cycles) and weather sensitivity (wind 5×, solar 10×, temperature 30%+). The reform regimes span different calendar windows:
- pre-IDA: 78 months across all seasons
- 3-sess: Jun-Dec 2024
- ISP15-win: Dec 2024-Mar 2025 (winter)
- DA60/ID15: Apr-Sep 2025 (summer/early-fall)
- DA15/ID15: Oct 2025-Jan 2026 (fall/early-winter)

Any across-regime claim that does NOT control for seasonality is suspect by default — apparent regime effects can be calendar-mix artefacts. **B9 illustrated this** (commit a8fe1bd → c684989): the raw-means "IB compressed at DA60/ID15" was a seasonal artefact; under same-calendar comparison, IB-CCGT yield actually *rose* +0.48 above pre-IDA same-cal.

**Mandatory minimum for any cross-regime claim:**

1. **Same-calendar-month comparison.** Restrict pre-IDA to the same calendar months as the post-reform window and compare. DA60/ID15 (Apr-Sep) vs pre-IDA Apr-Sep multi-year. DA15/ID15 (Oct-Jan) vs pre-IDA Oct-Jan. ISP15-win (Dec-Mar) vs pre-IDA Dec-Mar. This absorbs seasonality via window matching.

2. **Calendar-month FE in regressions** (when sample allows). Standard spec: `Y ~ regime + cal_month_FE + other_controls + …`. If day-level outcomes are noisy (small denominators in ratios), aggregate to monthly first.

3. **Weather controls when relevant:**
   - Spanish wind+solar (B16+B18+B19 from ENTSO-E A75) for price/dispatch/imbalance outcomes
   - Reservoir filling (ENTSO-E A72) for hydro outcomes
   - Temperature / load proxy for demand-related outcomes
   - Cross-border interconnection state for price-comparison work

4. **Year FE** for long-trend processes (Spanish renewable capacity grew ~6× over 2018-2025).

**Verdict criteria:**
- Raw across-regime means alone: NOT acceptable as a primary finding. May appear as descriptive context only.
- Same-calendar-month robustness: minimum acceptable test.
- Cal-month FE + weather controls: preferred for any regression.
- If the seasonality-controlled result has the same sign and >50% magnitude of the raw result, the finding is robust. Otherwise wound or kill.

## OVB-robustness discipline for regression-based claims

Whenever a regression coefficient drives a claim, follow this protocol before promoting the claim to alive:

1. **Sparse-FE baseline**: report the simplest spec (regime/calendar FE only).
2. **Augmented exogenous spec**: add controls that are **predetermined or exogenous** to the outcome — weather/RES generation (B01+B16+B18+B19), structural calendar/hour/DOW FE, infrastructure capacity, regulatory regime indicators.
3. **Compare β across specs**: if the headline coefficient is stable in sign and ≥50% magnitude across the sparse and augmented-exogenous specs, the claim is OVB-robust. If sign flips or magnitude collapses, wound or kill the claim.
4. **Document**: include sparse-vs-augmented β and p-values in the ledger row, not just the headline coefficient.

### Good controls vs bad controls (simultaneity / mediator bias)

Critical distinction often missed: not all "controls" reduce bias. **The bad-control critique applies specifically to controls jointly determined with the OUTCOME Y, NOT with another regressor X.** A control that is correlated only with another independent variable is fine — it's just multicollinearity (affects coefficient interpretation, doesn't introduce simultaneity bias on the X→Y effect).

**Good controls** for estimating β(X → Y) — predetermined or exogenous relative to Y:
- Weather variables (wind, solar generation as input — not as bid response)
- Calendar effects (hour-of-day, day-of-week, month, year)
- Infrastructure (interconnection capacity, installed capacity)
- Reform-date indicators / regime dummies
- Variables determined in earlier markets/sessions before Y is realized (e.g. day-ahead price `p_da` is predetermined relative to intraday repositioning `ΔQ_IDA`, since DA clears before IDA bidding — `p_da` is a valid control for an IDA-side regression even if it's correlated with the DA-side regressor `q_DA`)
- Lagged values of outcome's covariates

**Bad controls** — jointly determined with Y (mediators, descendants of Y, colliders with Y):
- Equilibrium prices when outcome includes that price (e.g. controlling for `p_actual` when outcome is `mp_IB = p_actual − p_synth` — mechanical joint determination).
- Market shares determined by the firm's own offers in the same equilibrium (e.g. controlling for `IB-share` when outcome is `mp_IB`).
- Cleared quantities when outcome is a quantity-response in the same market and round.
- Imbalance volumes when outcome is imbalance-driven settlement in the same ISP.

**Key distinction (often confused):**
- A control Z is jointly determined with another regressor X but predetermined relative to Y → **multicollinearity** with X (β interpretation may shift toward "holding Z fixed") but no simultaneity bias on β(X→Y). Z is a **valid control**.
- A control Z is jointly determined with Y → **simultaneity / mediator bias** on β(X→Y). Z is a **bad control**.

**Concrete project examples:**

- **S8 (alive→wounded, 2026-04-27)**: adding renewable-capacity-growth control (good — exogenous infrastructure proxy) flipped post-IDA RZ coefficient from +120 GWh/mo (p=0.006) to −27 (p=0.61). Legitimate OVB correction; demotion was justified.

- **F11 (caveats updated 2026-04-27)**: β(|gap|) is robust at −0.04 to −0.05 across both sparse-FE and augmented-exogenous specs. The previously-claimed sign-flip in β(gap) only appeared after adding `p_actual²` — and `p_actual` IS a bad control here because `mp_IB = p_actual − p_synth` is a mechanical joint determination with the outcome. Under good-control-only specs, β(gap) is mildly positive +0.003 to +0.013 (small, consistent with weak textbook prediction).

- **F5 (further demoted 2026-04-27)**: IB peak-hour Δβ_peak collapses from +0.049 (sparse) to +0.003 under purely exogenous controls (VRE + hour FE + DOW FE) — vanishes BEFORE considering the `p_da` specs. The Spec 5-6 specs add `p_da`, which is actually a VALID control here (predetermined relative to ΔQ_IDA, since DA clears before IDA bidding); they push Δβ_peak further negative (−0.017 to −0.026). The F5 demotion holds under both pure-exogenous specs AND specs with `p_da` — even more robust than initially claimed.

**Practical signals:**
- Adding a control that is jointly determined with Y flips sign / changes magnitude → likely bad-control artifact (do not interpret as OVB correction; rely on the exogenous-controls spec).
- Adding an exogenous or predetermined-relative-to-Y control flips sign or collapses magnitude → legitimate OVB correction (wound the claim).
- A control is correlated with another regressor X but predetermined relative to Y → not a bad control; multicollinearity may shift β(X) interpretation but the underlying causal estimand is unaffected.
- R² jumps from including jointly-determined-with-Y controls are mechanically large (mediators by construction explain Y) — not a sign of correct identification.