# `notebooks/` — exploratory notebooks

Not thesis output. The notebooks here are for **visual exploration and category-by-category comparison** that feeds the paper at [`thesis/paper/paper.tex`](../thesis/paper/paper.tex). Tabular / regression / diagnostic work lives in `scripts/analysis/*.py`.

## Subfolders

- **`eda/`** — numbered exploratory notebooks (active). Each has a cell-1 markdown header noting status + role.
- **`memos/`** — a single reference: [`_esios_archive_catalog.md`](memos/_esios_archive_catalog.md) (the ESIOS API archive triage memo / data dictionary). Older memos / research diary / claims ledger / narratives are in [`../attic/memos_20260516/`](../attic/memos_20260516/) for historical reference.
- **`attic/`** — superseded notebooks (do not reuse without re-validating against current data).

## Active notebooks

Open any notebook and read its cell-1 header for status + role. Run with the Python interpreter in `.venv/`.

Heavy outputs (cells, embedded figures) must stay below ~2000 px per axis to avoid breaking the kernel.
