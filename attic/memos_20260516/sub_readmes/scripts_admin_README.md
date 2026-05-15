# `scripts/admin/` — one-off pipeline maintenance and forensic tools

Not part of the OMIE / ENTSO-E / ESIOS download-parse-build pipelines (those live in `scripts/pipelines/`). These are ad-hoc utilities run by hand when the pipelines need to be audited, reconciled, or backfilled. None of them produce derived data consumed by `scripts/analysis/` or by the notebooks.

## Coverage / inspection

| Script | What it does |
|---|---|
| `inspect_omie_raw_coverage.py` | Compact coverage report of `data/raw/omie/` by documented family. Run when adding a new family or auditing missing months. |
| `inspect_omie_processed_coverage.py` | Same, for `data/processed/omie/`. Cross-checks parser output against expected raw-file count. |
| `explore_esios.py` | Keyword search over the ESIOS archives + indicators registries. `uv run … explore_esios.py restriccion` returns matching archives. Used during ESIOS pipeline scoping. |

## Audit / forensic

| Script | What it does |
|---|---|
| `audit_duplicate_keys.py` | Detect duplicate `(date, session_number, period, unit_code)` rows in a processed parquet. Used when adjusting parsers to verify keys are unique. |
| `audit_pibca_snapshot_model.py` | Audit the PIBCA snapshot model for a given month — confirms snapshot coverage and consistency. |
| `audit_pibci_row_model.py` | Audit the PIBCI row model for a given month — confirms row counts vs reference. |

## Reconciliation / backfill

| Script | What it does |
|---|---|
| `build_pibci_reconciliation_month.py` | Reconcile per-month PIBCI snapshots against PIBCA accumulated programmes. Forensic only; not part of canonical builds. |
| `backfill_download_manifest_marginalpdbc.py` | Retroactively fill `data/metadata/download_manifest.csv` for marginalpdbc when manifest entries are missing or pre-date the manifest convention. |

## Overnight / batch

| Script | What it does |
|---|---|
| `overnight_icab_idet.sh` | Run icab + idet sync → parse → build → commit overnight. Use with `caffeinate -i -s bash scripts/admin/overnight_icab_idet.sh`. |
| `overnight_icab_idet.log` | Log output from the most recent run of the above. (Gitignored — do not commit.) |

## Conventions

- Run from the project root (`uv run scripts/admin/<script>.py …` or `bash scripts/admin/<script>.sh`).
- Output is always to stdout or to a `--output` flag; admin scripts do **not** modify processed parquets in place.
- These scripts are not on the analysis claim track — they don't appear in `CLAIMS_LEDGER.md` and don't have STATUS headers.
