# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: robustness for Spec A BSTS prices. Re-runs the long-pre BSTS
#        with renewables-by-year interactions added to the covariate set
#        so the BSTS structural regression can absorb a year-varying
#        renewable-coefficient (renewable penetration grew ~6x over
#        2018-2025; a flat wind/solar coefficient mis-fits cross-year
#        merit-order dynamics in long-window training).
#
# Builds wind_gwh:year_dummy and solar_gwh:year_dummy interactions for
# each calendar year in the pre+post span and passes them to CausalImpact
# alongside the headline (wind_gwh, solar_gwh, gas_eur). Spike-and-slab
# decides which interactions to retain.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_year_interactions.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_year_interactions.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) {
    return(list(df = sub, cols = c()))
  }
  yrs_kept <- yrs[-1]
  new_cols <- c()
  for (y in yrs_kept) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(panel, response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  %s: skip (n=%d)\n", tag, nrow(sub))); return(NULL)
  }
  yr <- add_year_interactions(sub)
  sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 10000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  s <- imp$summary
  cat(sprintf("  %-26s  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  p=%.3f  n_pre=%d  n_post=%d  n_cov=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover), length(cov_set)))
  list(eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], p = s$p[1],
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover),
       n_cov = length(cov_set))
}

rows <- list()
add <- function(reform, outcome, tech, r) {
  if (is.null(r)) return()
  rows[[length(rows)+1]] <<- data.frame(
    reform=reform, outcome=outcome, tech=tech,
    eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
    n_pre=r$n_pre, n_post=r$n_post, n_cov=r$n_cov,
    stringsAsFactors = FALSE)
}

# Long pre-windows from 2022-01-01: ensures multi-year span so year
# interactions identify a year-varying renewable coefficient.

# ID15: long pre + 40d post (pre-blackout)
cat("=== ID15 long pre with year interactions ===\n")
r <- run_bsts(panel, "ida_price_eur", "2022-01-01", "2025-03-19", "2025-04-27",
              "ID15 IDA price (long pre + year int.)")
add("ID15", "price", "ida", r)
r <- run_bsts(panel, "da_price_eur",  "2022-01-01", "2025-03-19", "2025-04-27",
              "ID15 DA price  (long pre + year int.)")
add("ID15", "price", "da", r)

# DA15: long pre + 40d post (reforzada is mixed in the pre-window, so this
# specification is the joint MTU15-DA + reforzada effect, comparable to the
# beta specification of advisor_memo.tex)
cat("\n=== DA15 long pre with year interactions ===\n")
r <- run_bsts(panel, "da_price_eur",  "2022-01-01", "2025-10-01", "2025-11-09",
              "DA15 DA price  (long pre + year int.)")
add("DA15", "price", "da", r)
r <- run_bsts(panel, "ida_price_eur", "2022-01-01", "2025-10-01", "2025-11-09",
              "DA15 IDA price (long pre + year int.)")
add("DA15", "price", "ida", r)

# Placebo 2024 (same calendar): no MTU15 in either window
cat("\n=== Placebo 2024 with year interactions ===\n")
r <- run_bsts(panel, "ida_price_eur", "2022-01-01", "2024-03-19", "2024-04-27",
              "PLB-ID IDA price (long pre + year int.)")
add("PLB_ID15", "price", "ida", r)
r <- run_bsts(panel, "da_price_eur",  "2022-01-01", "2024-10-01", "2024-11-09",
              "PLB-DA DA price  (long pre + year int.)")
add("PLB_DA15", "price", "da", r)

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_daily_year_interactions.csv"),
           row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_daily_year_interactions.csv\n", nrow(out_df)))
