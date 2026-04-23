#!/usr/bin/env Rscript
# smoke_test.R
# Verifies the R + renv + arrow stack by loading a derived panel and
# printing a short summary. Intended to confirm the R environment is
# configured correctly, not to produce any output.
#
# Run with:
#   Rscript scripts/analysis/smoke_test.R
# or from inside R:
#   source("scripts/analysis/smoke_test.R")

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
})

PANEL_PATH <- "data/derived/reform_panel.parquet"

if (!file.exists(PANEL_PATH)) {
  stop(sprintf(
    "Derived panel %s not found. Run nb07 §1 in Python to materialise it first.",
    PANEL_PATH
  ))
}

panel <- read_parquet(PANEL_PATH)

cat(sprintf("Smoke test — R %s, arrow %s\n",
            paste(R.version[c("major", "minor")], collapse = "."),
            packageVersion("arrow")))
cat(sprintf("Loaded %s (%d rows, %d cols)\n",
            PANEL_PATH, nrow(panel), ncol(panel)))
cat("Columns:\n  ", paste(colnames(panel), collapse = ", "), "\n", sep = "")
cat(sprintf("Date range: %s to %s\n",
            as.character(min(panel$date)), as.character(max(panel$date))))

summary_tab <- panel |>
  filter(wind_tercile == "low") |>
  group_by(group) |>
  summarise(
    n_unit_days     = n(),
    mean_dq_mwh     = mean(dq_mwh, na.rm = TRUE),
    mean_abs_dq_mwh = mean(abs_dq_mwh, na.rm = TRUE),
    .groups = "drop"
  )

cat("\nLow-wind Big-4 vs Fringe summary:\n")
print(summary_tab)

cat("\nIf this printed without errors, the R + renv + arrow stack is working.\n")
