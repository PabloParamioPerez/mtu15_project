# MTU15 Project

Master thesis data-engineering project for OMIE electricity-market data, focused on the MTU15 transition. Managed with `uv` on macOS in VSCode.

## Python / uv workflow
- Always use `uv run` to execute scripts, never `pip` or raw `python`
- Do not add dependencies without asking

## Repo path conventions
- `data/raw/omie` and `data/processed/omie` are symlinks to an external SSD
- Always use repo-relative paths; never hardcode machine-specific paths
- Symlink targets may be slow or absent; do not traverse them speculatively

## Data layers
- **Raw** — verbatim OMIE files. Never modify.
- **Processed** — canonical Parquet tables, one per family. Preserve all raw rows and snapshot identity (`source_file`).
- **Derived** — reconciliation, collapsed views. Live separately and are clearly marked as derived. Not substitutes for canonical tables.

## PIBCA vs PIBCI — read carefully before touching
- `PIBCA` = accumulated intraday results
- `PIBCI` = incremental intraday results; raw rows must be preserved as-is
- A derived "net PIBCI" is formed by summing within snapshot on `(snapshot_token, date, period, unit_code)`
- Reconciliation artifacts live in `data/metadata/reconciliation/`
- Do NOT simplify this by deduplicating aggressively

## precios_pibcic_ronda — read carefully before touching
- The canonical combined object is a **snapshot panel** preserving `source_file` / snapshot identity
- Collapsing on `(date, round_number, period)` was a previous bug — do not reintroduce it

## Coding expectations
- **Conservative changes** — touch only what is needed; minimal diffs
- **Idempotent scripts** — re-running any pipeline step must produce identical output
- **Inspect before editing** — before modifying a parser or builder for one family, read the analogous file for another family first
- **Preserve structure** — match the style, naming, and conventions of neighbouring files
- **No destructive file operations** — no `rm -rf`, no overwriting raw data, no bulk renames without explicit request
- **No multi-file refactors** unless specifically asked

## What not to do
- Do not "fix" duplicate keys unless the economic meaning is clear and confirmed
- Do not collapse snapshot-level data into latest-state views by default
- Do not restructure the pipeline numbering scheme or folder layout
- Do not introduce new dependencies without asking
