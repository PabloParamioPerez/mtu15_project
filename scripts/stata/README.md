# `scripts/stata/` — Stata placeholder

Stata is **not currently installed** on this machine. This directory is a
placeholder for if/when we need Stata for parts of the thesis analysis
(typical use cases: legacy `.dta` datasets, specific Stata commands like
`xtabond2` for dynamic panel GMM, or collaborators who work in Stata).

## If Stata becomes needed

1. Install Stata (via the CEMFI license, typically). Verify with:
   ```bash
   which stata stata-se stata-mp
   ```
2. Put `.do` scripts here.
3. Data interchange convention (parquet is the lingua franca):
   ```bash
   # Python → .dta
   uv run python -c "import pandas as pd; pd.read_parquet('data/derived/X.parquet').to_stata('data/derived/X.dta', version=118)"
   # R → .dta
   Rscript -e "haven::write_dta(arrow::read_parquet('data/derived/X.parquet'), 'data/derived/X.dta')"
   # Stata → parquet (use the `parquet` extension by Mauricio Caceres)
   # stata -b do scripts/stata/to_parquet.do
   ```
4. Batch-run `.do` files from the command line:
   ```bash
   stata-mp -b do scripts/stata/<script>.do
   ```
5. Add the [Stata Enhanced](https://marketplace.visualstudio.com/items?itemName=kylebarron.stata-enhanced)
   VS Code extension for syntax highlighting (already listed in
   `.vscode/extensions.json`).

## Not supported yet

- No Stata-side lockfile equivalent of `renv.lock` / `uv.lock`. Pin Stata
  version and community packages (via `ado.pkg`) manually in a
  `stata_setup.do` prelude script if that becomes necessary.
