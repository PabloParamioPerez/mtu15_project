# `scripts/analysis/` — multi-language analysis scripts

Scratch / standalone analysis scripts that sit outside the OMIE and ENTSO-E
pipelines. This directory hosts the cross-language work that the Python
exploratory notebooks call out to: most of it is **R**, with a **Stata**
placeholder for if/when Stata is needed.

## Toolchain

| Language | Runtime | Packages | Entry point |
|---|---|---|---|
| Python | `uv`, `pyproject.toml`, `.venv/` | `pyproject.toml`/`uv.lock` | `uv run <script>.py` |
| R | `rig` (R 4.5.3), `renv`, `renv.lock` | `renv.lock` | `Rscript <script>.R` |
| Stata | (not installed — placeholder) | — | `stata -b do <script>.do` |

### R

R is managed with [`rig`](https://github.com/r-lib/rig) (R Installation
Manager). Current default is R 4.5.3 (release). Project-level dependencies
are pinned via [`renv`](https://rstudio.github.io/renv/), analogous to
`uv` for Python. `renv` is activated automatically when R is launched
inside the project directory via the project's `.Rprofile`.

To reproduce the R environment on another machine:

```bash
# 1. Install rig (the R version manager) and R 4.5 release
brew install --cask rig
rig install release
rig default release

# 2. From the project root, restore the renv lockfile
Rscript -e 'renv::restore()'
```

User-global R packages (installed outside renv, used by VS Code):

- `languageserver` — LSP backend for the VS Code R extension.
- `httpgd` — in-editor plot device.
- `pak` — fast parallel package installer (used by rig and renv).
- `renv` — the project-level dependency manager itself.

### Python ↔ R interop

No shared runtime. The two ecosystems communicate via **Parquet files** in
`data/derived/`. Conventions:

- **Python writes** a derived panel to `data/derived/<name>.parquet` via
  `pandas.to_parquet()` or `duckdb`.
- **R reads** via `arrow::read_parquet("data/derived/<name>.parquet")`.
- **R writes** outputs back to `data/derived/<name>_r_<suffix>.parquet`
  via `arrow::write_parquet()`.
- **Python reads** R outputs via `pandas.read_parquet()`.

Everything in `data/derived/` is gitignored (large, regenerable). Persist
metadata about what the file contains in the calling script or notebook,
not in the filename alone.

### Stata ↔ Python/R interop (placeholder)

Stata natively reads/writes `.dta`. To cross languages:

- Python can read `.dta` via `pandas.read_stata()` or `pyreadstat`.
- R can read `.dta` via `haven::read_dta()`.
- Parquet → `.dta` conversion: write a small `scripts/analysis/parquet_to_dta.R`
  (or `.py`) that reads parquet and emits `.dta`. No Stata script is
  committed yet.

### Editor setup (VS Code)

See `.vscode/settings.json` for the active configuration. Recommended
extensions are in `.vscode/extensions.json` and VS Code will prompt to
install them on first open.

Key extensions:

- **R** (REditorSupport.r) — language support + session watcher + plot viewer.
- **Quarto** (quarto.quarto) — if using Quarto documents.
- **Stata Enhanced** (kylebarron.stata-enhanced) — `.do` syntax highlighting.
- **Python** + **Pylance** + **Jupyter** + **Ruff** — already in use.

The VS Code R extension uses `radian` as the REPL (installed via
`uv tool install radian`). `httpgd` is configured as the default graphics
device so plots render in a built-in VS Code webview rather than a native
quartz window.

## Scripts in this directory

- `smoke_test.R` — verifies the R + renv + arrow stack by reading a
  derived parquet and printing a summary. Run with `Rscript smoke_test.R`.
- (Future) `bsts_isp15.R` — Bayesian structural time-series counterfactual
  for ISP15 on Big-4 $\Delta Q$. See nb07 §13 for the econometric motivation.
