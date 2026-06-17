# Replication package --- MTU15/ISP15 granularity reforms

Master's thesis, **Pablo Paramio Pérez**, CEMFI (2026): a bid-level study of the
2024–2025 fifteen-minute granularity reforms in the Spanish wholesale electricity
market (ID15 intraday, 2025-03-19; DA15 day-ahead, 2025-10-01; ISP15 settlement,
2024-12-11), separated from the post-blackout "reinforced-operation" regime.

- **Paper:** `thesis/paper/thesis.tex` (+ compiled `thesis.pdf`); the submission copy
  is `Paramio_Pablo_Thesis.pdf` at the repo root.
- **This package:** `Paramio_Pablo_Replication.zip` — a self-contained snapshot of the
  repository below (source, scripts, compiled paper, empty data scaffold, lockfiles).
- **Code repository:** <https://github.com/PabloParamioPerez/mtu15_project>
- **Driver:** `replication/run_replication.py` reproduces every reported number and
  figure **section by section** (Q1 prices, the margin channel, the within-hour price
  gap, bid-shape DiD, imbalance, migration, robustness); the headline values are then
  entered into `thesis.tex` by hand (the paper has no auto-generated tables). The
  exhibit→script map is `REPLICATION.md`; every live script under
  `scripts/analysis/` (outside `attic/`) is one the driver runs.

---

## 1. What the driver produces

`run_replication.py` runs four stages and writes to `results/regressions/` (regression
CSVs) and `figures/` (PDF/PNG figures). It does **not** edit the LaTeX; copy the
numbers into the paper manually.

| Stage | Does | Skipped when… |
|------|------|----------------|
| 0 Data download | OMIE (public) + ESIOS / ENTSO-E (need API keys) | processed parquet already present |
| 1 Parse + build | raw files → `data/processed/*.parquet` | processed parquet already present |
| 2 Derived panels | processed → `data/derived/panels/*.parquet` | — |
| 3 Analyses | panels → regression CSVs + figures | — |

**The data stages auto-skip when processed data is already on disk**, so re-running
is cheap and idempotent. They run only when the processed tables are missing.

## 2. Requirements

- **Python** via [`uv`](https://docs.astral.sh/uv/) (the repo pins versions in
  `pyproject.toml` / `uv.lock`):
  ```bash
  git clone https://github.com/PabloParamioPerez/mtu15_project.git
  cd mtu15_project
  uv sync            # creates the environment from the lockfile
  ```
  Always run scripts with `uv run python ...` (never a bare `python`).

- **R** (only for the BSTS / OLS-HAC steps). Install these packages once:
  ```r
  install.packages(c("arrow", "CausalImpact", "lmtest", "sandwich", "bsts"))
  ```
  The driver calls R scripts with `Rscript`; ensure it is on your `PATH`.

- **Storage.** `data/raw` and `data/processed` are symlinks to an external SSD in the
  author's setup. For replication, either recreate those folders locally or repoint the
  symlinks; a full raw mirror is large (hundreds of GB of OMIE/ESIOS/ENTSO-E files).
  Most users should obtain the **processed parquet** and run only stages 2–3.

## 3. Credentials (only for a from-scratch download)

OMIE is public. ESIOS and ENTSO-E need free API tokens. Provide them via a `.env` at
the repo root — **never commit it**:

```bash
cp replication/.env.example .env   # then edit .env and fill in your own tokens
```

Keys: `ENTSOE_TOKEN`, `TP_USERNAME`, `TP_PASSWORD`, `ESIOS_TOKEN`. If the processed
data is already present these are never read.

## 4. Running

```bash
# Print the full plan without running anything:
uv run python replication/run_replication.py --list

# Full run (auto-skips download/parse if processed data is present):
uv run python replication/run_replication.py

# If you already have the processed parquet, go straight to panels + analyses:
uv run python replication/run_replication.py --from 2

# Re-run a single stage:
uv run python replication/run_replication.py --only 3
```

A failed step is reported and the driver continues; re-run that script directly to
debug. Stage 3 prints results to `results/regressions/...` and figures to `figures/`.

## 5. Repository layout (what matters for replication)

```
src/mtu/                     parsers, transforms, validation, ingestion helpers
scripts/pipelines/{omie,esios,entsoe,external}/   00_ download, 10_ parse, 20_ build
scripts/analysis/{bid,system,firm,balancing,...}  empirical analyses (stage 3)
scripts/analysis/panels/     derived-panel builders (stage 2)
data/processed/              canonical parquet (one per family)
data/derived/panels/         analysis-ready panels
results/regressions/         regression CSVs consumed by the paper
figures/thesis/              final figures referenced by thesis.tex
thesis/paper/                thesis.tex, thesis.pdf, references.bib
```

## 6. Notes / caveats

- **Power vs. energy.** OMIE `quantity_mw` is instantaneous power (a rate), independent
  of the 60→15-minute period length. Cross-reform quantity comparisons convert to MWh
  first; per-period slopes/rates are granularity-neutral and are *not* summed across the
  four quarters of a clock-hour. (See `CLAUDE.md` for the full discipline.)
- **Reform vs. reinforced operation.** An April 2025 blackout triggered a separate
  cost regime; the analysis windows are constructed to keep it separable, and the driver
  reproduces those windows exactly as coded.
- **Determinism.** BSTS draws use the seeds set inside each R script; OLS/DiD are
  deterministic. Minor last-digit differences in BSTS posterior summaries across machines
  are expected and do not affect the reported signs/magnitudes.
