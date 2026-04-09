# MTU15 Project: OMIE Electricity Market Data Pipeline

Master thesis project on the Spanish electricity market reform that changed the Market Time Unit (MTU) from hourly trading to 15-minute trading.

**Status date:** 9 April 2026

## Project goal

This repository builds a reproducible data pipeline for OMIE electricity-market data to study the effect of the MTU15 reform on price formation, quantities, and market outcomes in Spain.

The current focus is on collecting, parsing, validating, auditing, and consolidating OMIE market data in a way that is robust, rerunnable, and suitable for later empirical analysis.

A central design principle of the project is to distinguish clearly between:

- **canonical raw or snapshot-preserving datasets**
- **derived, collapsed, or reconciled datasets**

The pipeline should preserve information whenever repeated keys may reflect real market structure, publication revisions, or distinct market snapshots.

## Reform timeline

Main dates relevant for this project:

- **ISP / imbalance settlement:** December 2024
- **Intraday markets (auctions + continuous):** 19 March 2025
- **Day-ahead market:** 1 October 2025

## Current project scope

As of 9 April 2026, the repository contains working or partially working pipelines for the following OMIE families:

### Stable / working families
- `marginalpdbc`
- `marginalpibc`
- `pdbc`
- `precios_pibcic`
- `precios_pibcic_ronda`
- `curva_pbc` — day-ahead aggregate supply/demand curves
- `curva_pibc` — intraday auction aggregate supply/demand curves
- `omanulaintra` — intraday auction Market Operator cancelled hours
- `osanulaintra` — intraday auction System Operator cancelled hours

### Families under active analytical refinement
- `pibca`
- `pibci`

The project has moved beyond the initial price-only stage. The current repo includes stable price pipelines, quantity / program families, aggregate curve families, and cancellation families.

## Implemented pipelines and current interpretation

### 1. Day-ahead prices (`MARGINALPDBC`)

Implemented stages:

- download raw OMIE day-ahead marginal price files
- parse raw files into per-file parquet outputs
- build a consolidated `marginalpdbc_all.parquet`

Current status:

- working
- manually checked on real dates and periods
- rerun-safe
- downloader skips already downloaded raw files
- parser skips files whose output parquet already exists
- combined builder reruns deterministically and keeps the latest version when multiple OMIE file versions exist

### 2. Intraday auction prices (`MARGINALPIBC`)

Implemented stages:

- download raw OMIE intraday auction marginal price files
- parse raw files into per-file parquet outputs
- build a consolidated `marginalpibc_all.parquet`

Current status:

- working
- manually checked on real quarter-hour prices
- supports historical multi-date / midnight-spanning session files
- blank historical price fields are handled as `NaN`
- downloader skips already downloaded raw files
- parser skips files whose output parquet already exists
- combined builder reruns deterministically and keeps the latest version when multiple OMIE file versions exist

### 3. Day-ahead programs (`PDBC`)

Implemented stages:

- parse raw OMIE day-ahead program files
- build a consolidated `pdbc_all.parquet`

Current status:

- working for the currently covered historical range
- treated as a canonical row-preserving dataset
- useful as a reference point for row-identifier and offer-structure conventions

### 4. Intraday auction programs: accumulated (`PIBCA`)

Implemented stages:

- parse raw OMIE intraday auction program files into per-file parquet outputs
- build combined monthly outputs
- build canonical combined outputs preserving snapshot identity

Current interpretation:

- `PIBCA` should be treated as an **accumulated intraday snapshot** dataset
- repeated coarse keys across files can reflect **cross-snapshot revisions**, not parser failure
- the canonical build should preserve snapshot identity rather than collapsing aggressively across files

Current status:

- parser and canonical build are now structured to preserve row identity and snapshot identity
- economic interpretation has been audited, but this family should still be treated as under active refinement rather than fully “finished”

### 5. Intraday auction programs: incremental (`PIBCI`)

Implemented stages:

- parse raw OMIE intraday incremental program files into per-file parquet outputs
- build combined monthly outputs
- build canonical combined outputs preserving raw row identity

Current interpretation:

- `PIBCI` should be treated as an **incremental intraday row-level** dataset
- raw rows should be preserved in the canonical output
- any netting, collapsing, or reconciliation should be treated as **derived logic**, not as the canonical table

Current status:

- parser and canonical build now preserve within-file row identity
- row structure has been audited
- this family is usable, but the economic reconciliation layer should still be considered derived / analytical rather than part of the canonical raw build

### 6. Continuous market prices (`PRECIOS_PIBCIC`)

Implemented stages:

- parse raw OMIE continuous-market mean price files
- build a consolidated `precios_pibcic_all.parquet`

Current status:

- structurally clean
- latest-state combined table is acceptable for this family
- partial-day logic and MTU interpretation are handled explicitly

### 7. Continuous market round prices (`PRECIOS_PIBCIC_RONDA`)

Implemented stages:

- parse raw OMIE continuous-market round price files
- build a consolidated `precios_pibcic_ronda_all.parquet`

Current interpretation:

- the canonical combined object for this family is a **snapshot panel**
- collapsing on `(date, round_number, period)` is not valid as a canonical build because identical coarse keys can appear in multiple source snapshots with different values
- preserving `source_file` / snapshot identity is essential

Current status:

- parser is working
- canonical builder has been fixed to preserve the snapshot panel
- a single partial MTU15 day (`2026-03-31`) is present and appears to be a genuine format transition / partial-publication case rather than a parser artifact

## Current data coverage

### Day-ahead prices (`MARGINALPDBC`)
Current combined output:

- file: `data/processed/omie/mercado_diario/precios/marginalpdbc_all.parquet`
- date range: **2018-01-01 to 2026-03-13**

The builder correctly distinguishes:

- **MTU60** days before day-ahead reform
- **MTU15** days after day-ahead reform
- DST-compatible row counts

### Intraday auction prices (`MARGINALPIBC`)
Current combined output:

- file: `data/processed/omie/mercado_intradiario_subastas/precios/marginalpibc_all.parquet`
- date range: **2017-12-31 to 2026-02-24**

Validated split:

- **pre-reform:** MTU60 until **2025-03-18**
- **post-reform:** MTU15 from **2025-03-19**

### Processed-family coverage snapshot

Current processed coverage includes at least the following combined or monthly outputs:

- `marginalpdbc_all.parquet`
- `marginalpibc_all.parquet`
- `pdbc_all.parquet`
- `pibca_monthly/*`
- `pibci_monthly/*`
- `precios_pibcic_all.parquet`
- `precios_pibcic_ronda_all.parquet`

At the time of the latest admin audit, processed-family coverage was approximately:

- `marginalpdbc`: **2018-01-01 to 2026-03-13**
- `marginalpibc`: **2017-12-31 to 2026-02-24**
- `pdbc`: **2018-02-01 to 2023-12-31**
- `pibca`: **2019-02-28 to 2026-01-02**
- `pibci`: **2019-02-28 to 2026-01-02**
- `precios_pibcic`: **2024-08-30 to 2026-03-30**
- `precios_pibcic_ronda`: **2018-06-13 to 2026-03-31**

These ranges should be treated as pipeline audit outputs rather than immutable guarantees.

## Manual checks and audit work already performed

The project now includes not only manual checks but also explicit audit scripts for ambiguous families.

Checks already performed include:

- direct manual comparison of day-ahead and intraday prices for specific dates and quarter-hours
- confirmation that post-reform quarter-hour indexing is correct
- rerun/idempotency checks for download, parse, and build stages
- duplicate-key audits for `PIBCA`
- within-source row-structure audits for `PIBCI`
- reconciliation of net `PIBCI` against changes in `PIBCA`
- audit of continuous-market round-price snapshot overlap

Operational conclusions so far:

- **download stage:** idempotent by skipping existing raw files
- **parse stage:** idempotent by skipping existing parquet outputs
- **build stage:** rerun-safe when the economic key is correctly specified
- repeated coarse keys must not automatically be treated as bugs

## Repository structure

### Core package
- `src/mtu/`

### Parsing logic
Key parsing modules currently include:
- `src/mtu/parsing/marginalpdbc.py`
- `src/mtu/parsing/marginalpibc.py`
- `src/mtu/parsing/pdbc.py`
- `src/mtu/parsing/pibca.py`
- `src/mtu/parsing/pibci.py`
- `src/mtu/parsing/precios_pibcic.py`
- `src/mtu/parsing/precios_pibcic_ronda.py`
- `src/mtu/parsing/curva_pbc.py`
- `src/mtu/parsing/curva_pibc.py`
- `src/mtu/parsing/omanulaintra.py`
- `src/mtu/parsing/osanulaintra.py`
- `src/mtu/parsing/omie_common.py`

### Pipeline scripts
Representative pipeline scripts include:
- `scripts/pipelines/omie/00_download_marginalpdbc.py`
- `scripts/pipelines/omie/00_download_marginalpibc.py`
- `scripts/pipelines/omie/00_download_curva_pbc.py`
- `scripts/pipelines/omie/00_download_curva_pibc.py`
- `scripts/pipelines/omie/00_download_omanulaintra.py`
- `scripts/pipelines/omie/00_download_osanulaintra.py`
- `scripts/pipelines/omie/10_parse_marginalpdbc.py`
- `scripts/pipelines/omie/10_parse_marginalpibc.py`
- `scripts/pipelines/omie/10_parse_curva_pbc.py`
- `scripts/pipelines/omie/10_parse_curva_pibc.py`
- `scripts/pipelines/omie/10_parse_omanulaintra.py`
- `scripts/pipelines/omie/10_parse_osanulaintra.py`
- `scripts/pipelines/omie/20_build_marginalpdbc_all.py`
- `scripts/pipelines/omie/20_build_marginalpibc_all.py`
- `scripts/pipelines/omie/20_build_pibca_all.py`
- `scripts/pipelines/omie/20_build_pibci_all.py`
- `scripts/pipelines/omie/20_build_precios_pibcic_all.py`
- `scripts/pipelines/omie/20_build_precios_pibcic_ronda_all.py`
- `scripts/pipelines/omie/20_build_curva_pbc_all.py`
- `scripts/pipelines/omie/20_build_curva_pibc_all.py`
- `scripts/pipelines/omie/20_build_omanulaintra_all.py`
- `scripts/pipelines/omie/20_build_osanulaintra_all.py`

### Admin / audit scripts
The repo now includes reusable OMIE admin tooling such as:
- `scripts/admin/inspect_omie_raw_coverage.py`
- `scripts/admin/inspect_omie_processed_coverage.py`
- `scripts/admin/audit_duplicate_keys.py`
- `scripts/admin/audit_pibca_snapshot_model.py`
- `scripts/admin/audit_pibci_row_model.py`
- `scripts/admin/build_pibci_reconciliation_month.py`

### Data folders
- `data/raw/`
- `data/processed/`
- `data/interim/`
- `data/metadata/`

### External-storage note
Large OMIE data folders may live on external storage while remaining available under repo-relative paths through symlinks, especially for:

- `data/raw/omie`
- `data/processed/omie`

The codebase should continue to treat these as normal repo-relative paths.

## Data conventions currently handled

The current pipelines handle OMIE conventions such as:

- semicolon-separated text files
- European decimal format
- hourly period structure before reform
- quarter-hour period structure after reform
- DST-compatible counts
- historical file versioning
- midnight-spanning historical intraday auction sessions
- partial-day publications
- snapshot-preserving continuous-market round files
- row-identity preservation for ambiguous intraday program families

## Canonical vs derived outputs

This distinction is now central to the repository.

### Canonical outputs
Canonical outputs should preserve the information structure of the source material as much as possible.

Examples:
- per-file parsed parquet outputs
- `PIBCA` combined outputs preserving snapshot identity
- `PIBCI` combined outputs preserving raw row identity
- `precios_pibcic_ronda_all.parquet` as a snapshot panel

### Derived outputs
Derived outputs may collapse, net, reconcile, or summarize canonical data for specific analytical purposes.

Examples:
- monthly combined tables
- latest-version collapsed tables when economically justified
- `PIBCI` netted-within-snapshot objects
- `PIBCI` / `PIBCA` reconciliation artifacts under `data/metadata/reconciliation/`

Derived outputs should not overwrite canonical ambiguity.

## What is not yet treated as finished

The following parts should still be considered under development or not yet part of the stable core pipeline:

- broader formal validation layer in `src/mtu/validation/checks.py`
- configuration layer in `src/mtu/config.py`
- extra tests and sample fixtures not yet consolidated into the main workflow
- broader thesis datasets beyond the currently stabilized OMIE market layers
- final analytical choice for how reconciliation-filtered `PIBCI` / `PIBCA` derived datasets should be exposed
- any future derived latest-snapshot table for `precios_pibcic_ronda`

## Immediate next priorities

Natural next steps for the project are:

1. document the canonical-vs-derived data model more explicitly in docs and workflow notes
2. validate the updated `PIBCA` / `PIBCI` canonical builds on additional months
3. extend quantity / program coverage where needed for empirical analysis
4. integrate validation and tests more cleanly
5. keep the repository structure conservative and avoid unnecessary refactoring

## Reproducibility note

The repo is organized so that raw downloads, per-file parsed outputs, and consolidated outputs can be regenerated safely.

Generated metadata files such as ingestion logs, download manifests, and reconciliation outputs are useful operational artifacts but are not necessarily intended to be tracked permanently in Git.

## Current Git status of the project

As of the latest clean checkpoint (9 April 2026), the repository includes committed changes for:

- continuous-market round-price builder fix to preserve snapshot panels
- OMIE admin audit tooling and PIBCI reconciliation tooling
- preservation of `PIBCA` and `PIBCI` row identity in canonical builds
- full `curva_pbc` pipeline (day-ahead aggregate curves)
- full `curva_pibc` pipeline (intraday auction aggregate curves)
- full `omanulaintra` and `osanulaintra` pipelines (cancelled hours)

The project now covers prices, quantities/programs, aggregate curves, and cancellation data across both the day-ahead and intraday auction markets.
