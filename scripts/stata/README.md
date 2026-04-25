# `scripts/stata/` — Stata integration

**Stata/MP 17** is installed at
`/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp`. The CLI works
in batch mode; the `.app` bundle stays available for interactive use too.

## Running a `.do` file from the command line

Absolute path (works out of the box, no PATH changes required):

```bash
/Applications/Stata/StataMP.app/Contents/MacOS/stata-mp -b -q do scripts/stata/smoke_test.do
```

If you'd prefer `stata-mp` on `PATH` for convenience, one-time setup:

```bash
# Create a symlink in a directory that's already on $PATH (needs sudo
# if writing to /opt/homebrew/bin; or use ~/.local/bin if preferred).
sudo ln -s /Applications/Stata/StataMP.app/Contents/MacOS/stata-mp /opt/homebrew/bin/stata-mp
sudo ln -s /Applications/Stata/StataMP.app/Contents/MacOS/stata-mp /opt/homebrew/bin/stata

# Verify
which stata-mp
stata-mp -b -q 'display c(version)'
```

After that, `stata-mp -b -q do scripts/stata/smoke_test.do` from the
project root works directly.

## Data interchange (parquet ↔ .dta)

Parquet is the project's lingua franca. Stata doesn't read parquet
natively, so we convert on demand:

```bash
# Python: parquet → .dta  (requires pyreadstat, in pyproject.toml)
uv run python -c "
import pandas as pd
df = pd.read_parquet('data/derived/panels/reform_panel.parquet')
df.to_stata('data/derived/panels/reform_panel.dta', version=118, write_index=False)
"

# R: parquet → .dta  (uses haven, pinned in renv.lock)
Rscript -e "
haven::write_dta(arrow::read_parquet('data/derived/panels/reform_panel.parquet'),
                 'data/derived/panels/reform_panel.dta')
"

# Stata → parquet  (via Mauricio Caceres' `parquet` extension)
# Install once in Stata:   net install parquet, from("https://raw.githubusercontent.com/mcaceresb/stata-parquet/master/src")
# Then from a .do file:    parquet save "data/derived/X.parquet", by(unit_code)
```

Both `pyreadstat` and `haven` are pinned in the project's dependency
files; no extra install needed once `uv sync` and `renv::restore()` have
run.

## Files in this directory

- `smoke_test.do` — reads `data/derived/panels/reform_panel.dta` (after running
  the Python conversion command above) and prints a Big-4/Fringe summary.
  Mirrors what `scripts/analysis/smoke_test.R` does for R.

## Editor setup

VS Code extension `kylebarron.stata-enhanced` is listed in
`.vscode/extensions.json` for syntax highlighting of `.do`/`.ado` files.
The extension does not provide a REPL or code-runner — invoke Stata via
the terminal.

## Not supported

- **No Stata-side lockfile** equivalent of `renv.lock` / `uv.lock`. Stata
  community packages installed via `ssc install` or `net install` live in
  `~/ado/` and are not reproducible across machines without a prelude
  script. If the thesis ends up depending on specific Stata packages,
  write a `stata_setup.do` that installs them and document the versions
  in this README.
