# MTU15 Project

Master thesis data-engineering project for OMIE electricity-market data, focused on the MTU15 transition. Managed with `uv` on macOS in VSCode.

## Python / uv workflow
- Always use `uv run` to execute scripts, never `pip` or raw `python`
- Do not add dependencies without asking

## Repo path conventions
- `data/raw/omie` and `data/processed/omie` are symlinks to an external SSD
- Always use repo-relative paths; never hardcode machine-specific paths
- Symlink targets may be slow or absent; do not traverse them speculatively

## Repo layout
- `src/mtu/parsing/` — one module per data family (e.g. `pibca.py`, `pibci.py`, `marginalpdbc.py`)
- `src/mtu/transform/` — period normalization and shared transforms
- `src/mtu/validation/` — post-parse checks (`checks.py`)
- `scripts/pipelines/omie/` — numbered pipeline steps: `00_` download, `10_` parse, `20_` build
- `scripts/admin/` — one-off audit, inspect, and forensic scripts; not part of the pipeline

## Data families
Active families (each has a parser in `src/mtu/parsing/` and a full `00/10/20` pipeline):

| Family | Market | Description | Spec (v1.37) |
|---|---|---|---|
| `marginalpdbc` | Day-ahead | Clearing prices | §5.1.1.1 |
| `marginalpibc` | Intraday auctions | Clearing prices by session | §5.2.1.1 |
| `pdbc` | Day-ahead | Final programs by unit | §5.1.2.1 |
| `pdbce` | Day-ahead | Final programs by firm | §5.1.2.2 |
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
All notebooks live in `explore/` and are for exploration only — not thesis output. Run with the `mtu15-project` kernel.

| Notebook | Contents |
|---|---|
| `01_market_statistics.ipynb` | Price spot-validation, aggregate curve checks, structural statistics (within-day profile, intra-hour dispersion, monthly prices, IDA prices across regimes), continuous intraday volume, accumulated programs, DA offers by technology, XBID order book, interconnection capacity, XBID trades |
| `02_bidding_behaviour.ipynb` | Offer type anatomy (DA + IDA), DA↔IDA price spread and arbitrage, program reconciliation (who re-trades and how much), bid price anatomy and market power, XBID order book and iceberg orders |
| `03_reform_narrative.ipynb` | Reform effects across three regimes: DA-IDA price wedge, within-hour price dispersion (MTU15 signature), firm repositioning ΔQ (dominant vs fringe, by technology). Builds the Ito-Reguant (2016) empirical objects for the Spanish market. |

Do not duplicate analysis across notebooks. Check what is already covered before adding a new section.

## External data sources
- **OMIE** — primary source; downloaded via `00_sync_*` scripts
- **ESIOS (REE)** — secondary source for balancing/constraints data; requires `ESIOS_TOKEN` env var. Admin script: `scripts/admin/explore_esios.py`. Access not yet obtained.

## Data layers
- **Raw** — verbatim OMIE files. Never modify.
- **Processed** — canonical Parquet tables, one per family. Preserve all raw rows and snapshot identity (`source_file`).
- **Derived** — reconciliation, collapsed views. Live separately and are clearly marked as derived. Not substitutes for canonical tables.

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