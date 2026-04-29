# `results/` — analytical outputs

Code-dependent products of analysis scripts. **NOT data.** The contents of `results/` are regenerated when scripts change — do not treat them as canonical, do not edit them by hand, and do not put raw or processed datasets here.

## Subfolders

- **`regressions/`** — CSV outputs of regression scripts. One file or sub-directory per analysis script. The naming convention is `<topic>_<spec>.csv` (e.g. `b9_replicated_isp_grain.csv`) or a sub-directory grouping multiple files from one analysis run (e.g. `b9_continuous_market/`).
- **`robustness/`** — robustness-check tables that are kept separate from the headline regression for clarity. Used as defensive material in Q&A.
- **`summaries/`** — human-readable run summaries (e.g. `HEAVY_RUN_SUMMARY.md`). Markdown reports written when a batch of analyses finishes; useful for quick orientation.
- **`tables/`** — tables formatted for the thesis or presentation deck (LaTeX, CSV, Markdown).
- **`attic/`** — retired analytical outputs from claims that were dead-listed. Kept for transparency.

## Conventions

- Each regression script writes its output to a path constructed from `PROJECT / "results" / "regressions" / "<name>.csv"`. Do not bypass this convention.
- If your script produces multiple related CSVs (e.g. point estimates + per-firm matrix + monthly aggregate), put them in a sub-directory under `regressions/` rather than spreading them across the flat directory.
- Most CSVs are gitignored (the directory `results/regressions/` is large and changes frequently). Markdown summaries and tables that the thesis cites should be force-added (`git add -f`) so they remain reproducible from the repo alone.
- For human-readable orientation when starting work: read the latest `summaries/HEAVY_RUN_SUMMARY.md` (or equivalent) before reading individual CSVs.
