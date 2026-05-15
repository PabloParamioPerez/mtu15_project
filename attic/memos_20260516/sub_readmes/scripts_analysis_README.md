# `scripts/analysis/` — multi-language analysis scripts

Standalone analysis scripts that sit outside the OMIE / ESIOS / ENTSO-E pipelines. Most of the work is **Python** (`uv run`); a small **R** + **Stata** layer is kept for when those are the right tools.

For the project-level structure and policy (claim-status discipline, attic conventions, seasonality / OVB checks), see `CLAUDE.md` at the repo root.

## Toolchain

| Language | Runtime | Packages | Entry point |
|---|---|---|---|
| Python | `uv`, `pyproject.toml`, `.venv/` | `pyproject.toml`/`uv.lock` | `uv run <script>.py` |
| R | `rig` (R 4.5.3), `renv`, `renv.lock` | `renv.lock` | `Rscript <script>.R` |
| Stata | (not installed — placeholder) | — | `stata -b do <script>.do` |

### R

R is managed with [`rig`](https://github.com/r-lib/rig). Current default is R 4.5.3 (release). Project-level dependencies are pinned via [`renv`](https://rstudio.github.io/renv/), activated automatically when R is launched inside the project directory via `.Rprofile`.

To reproduce the R environment on another machine:

```bash
brew install --cask rig
rig install release
rig default release
Rscript -e 'renv::restore()'
```

### Python ↔ R interop

No shared runtime. The two ecosystems communicate via **Parquet files** in `data/derived/`. Python writes via `pandas.to_parquet()` or `duckdb`; R reads via `arrow::read_parquet()`. R writes outputs back to `data/derived/<name>_r_<suffix>.parquet`.

### Stata ↔ Python/R interop

Stata natively reads/writes `.dta`. Python reads `.dta` via `pandas.read_stata()`; R reads via `haven::read_dta()`. No Stata script is committed yet.

## Layout

```
scripts/analysis/
├── README.md                 (this file)
├── smoke_test.R              (R + renv + arrow toolchain test)
├── system/                   system-level friction (S5/S6/S7/S8/B5/B6/B7)
├── firm/                     firm-level strategic conduct (B9, B12-B14, F12/F15/F16, pdbf)
├── bid/                      bid-shape and granularity tests (B1, B2, B8, B14)
├── balancing/                aFRR / mFRR / nuclear-availability
├── regulatory/               RT2 + CNMC enforcement
├── modelling/                mechanism-candidate / structural-track scripts
├── panels/                   long-lived panel + price-series builders
└── attic/                    retired pre-pivot work (lerner/, synthetic/, firm/-dead)
```

The structure mirrors `results/regressions/` so that script `scripts/analysis/<topic>/<x>.py` writes to `results/regressions/<topic>/`. (`firm/` results are further split into `firm/{critical_hours,pdbf,b9,other}/`.)

## What lives where

Each topic subfolder has its own `README.md` listing its scripts. Quick map:

- **`system/`** — S6 settlement transfer, S7 Pigouvian, S8 redispatch zone, B5/B6 seasonality + pass-through, B7 France placebo, A87 reserve decomposition.
- **`firm/`** — the **headline** (within-day critical-vs-flat-hours DiD, B12). Per-firm B9 family, B11 robustness, pdbf bilateral channel (D6/D7/D8/B10/F24/F26), F12 pumped-storage, F15 post-blackout windfall, q₂ definitions.
- **`bid/`** — bid-shape / granularity (B14 critical_vs_flat_bidshape — the bid-shape evidence supporting B12), bid-function shape (B2), bid complexity (B8), CCGT extensive-margin and tranche-count diagnostics.
- **`balancing/`** — aFRR offer depth, mFRR depth, nuclear availability v3, nuclear cross-subsidy.
- **`regulatory/`** — RT2 post-blackout channel, CNMC bid-price-wedge replication, repeat-offender concentration.
- **`modelling/`** — mechanism-candidate probes (anticipation_test, theta_calibration, welfare_proxy, asymmetric_granularity_welfare, da_ida_wedge_structure, forecast_bias_curtailment, survivor_skeptic).
- **`panels/`** — `build_firm_*_panel.py`, `build_supply_slope_panel.py`, `build_xbid_liquidity.py`, etc. — panel builders feeding everything else.
- **`attic/`** — pre-pivot work kept as historical record per the DEAD-KEPT-AS-RECORD discipline. Lerner index work, synthetic-firm Ciarreta–Espinosa pipeline, dead F1/F2/F3 HP-sophistication scripts, dual-pricing scripts, F25 pdbf scripts, etc.

Every script in active topic folders has a 4-line `STATUS:` header identifying the `CLAIMS_LEDGER.md` rows it feeds. Move a script to `attic/` only if its claim is DEAD AND no live notebook imports it (per `CLAUDE.md` § "Claim-status discipline").

## Editor setup (VS Code)

See `.vscode/settings.json` and `.vscode/extensions.json`. Key extensions: **Python** + **Pylance** + **Jupyter** + **Ruff** (Python); **R** (REditorSupport.r) using `radian` as the REPL with `httpgd` as the default plot device; **Stata Enhanced** for `.do` syntax.
