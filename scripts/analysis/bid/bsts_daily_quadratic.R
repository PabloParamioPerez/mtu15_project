# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis Table 3 (price effect by spec) -- BSTS analogue of the
#        quadratic-renewable spec, so the quadratic row appears in the BSTS
#        block too. Same structure as bsts_daily_year_interactions.R but adds
#        wind^2 and solar^2 (and their year interactions) to the spike-and-slab
#        regressors. Real cells only (4 legs); placebos already in the headline
#        BSTS run.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_quadratic.csv

suppressPackageStartupMessages({library(arrow); library(CausalImpact); library(zoo)})

args <- commandArgs(trailingOnly = FALSE); sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else "scripts/analysis/bid/bsts_daily_quadratic.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel$wind_gwh2  <- panel$wind_gwh^2
panel$solar_gwh2 <- panel$solar_gwh^2
BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur", "wind_gwh2", "solar_gwh2")

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  new_cols <- c()
  for (y in yrs[-1]) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]   <- sub$wind_gwh   * is_y
    sub[[sprintf("solar_x_%d", y)]]  <- sub$solar_gwh  * is_y
    sub[[sprintf("wind2_x_%d", y)]]  <- sub$wind_gwh2  * is_y
    sub[[sprintf("solar2_x_%d", y)]] <- sub$solar_gwh2 * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y),
                  sprintf("wind2_x_%d", y), sprintf("solar2_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) { cat(sprintf("  %s: skip\n", tag)); return(NULL) }
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_ts <- zoo(as.matrix(sub[, c(response, cov_set)]), order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                      model.args = list(niter = 10000, nseasons = 7,
                                        season.duration = 1, prior.level.sd = 0.005))
  s <- imp$summary
  cat(sprintf("  %-14s eff=%+8.3f  CI=[%+8.3f,%+8.3f]  p=%.3f\n", tag,
              s["Average", "AbsEffect"], s["Average", "AbsEffect.lower"],
              s["Average", "AbsEffect.upper"], s$p[1]))
  data.frame(eff = s["Average", "AbsEffect"], lo = s["Average", "AbsEffect.lower"],
             hi = s["Average", "AbsEffect.upper"], p = s$p[1], stringsAsFactors = FALSE)
}

specs <- list(
  list("ID15", "ida", "ida_price_eur", "2022-01-01", "2025-03-19", "2025-04-27"),
  list("ID15", "da",  "da_price_eur",  "2022-01-01", "2025-03-19", "2025-04-27"),
  list("DA15", "da",  "da_price_eur",  "2022-01-01", "2025-10-01", "2025-11-09"),
  list("DA15", "ida", "ida_price_eur", "2022-01-01", "2025-10-01", "2025-11-09"))

rows <- list()
for (s in specs) {
  r <- run_bsts(s[[3]], s[[4]], s[[5]], s[[6]], sprintf("%s %s", s[[1]], s[[2]]))
  if (!is.null(r)) { r$reform <- s[[1]]; r$market <- s[[2]]; rows[[length(rows) + 1]] <- r }
}
out <- do.call(rbind, rows)
write.csv(out, file.path(out_dir, "bsts_daily_quadratic.csv"), row.names = FALSE)
cat("\nWrote bsts_daily_quadratic.csv\n")
