# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: diagnostic for the year-interaction Spec A. For one representative
#        BSTS run (ID15 IDA price, long pre + year interactions), extract
#        the posterior mean and inclusion probability of each
#        solar:year_dummy and wind:year_dummy regressor. Report a table
#        showing how the renewable coefficient evolves across years.
#        Confirms that the year interactions are doing work (high inclusion
#        probability, monotone or non-monotone coefficient shifts).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_solar_year_coefs.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo); library(bsts)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_solar_year_coefficients.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

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

extract_year_coefs <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  bs <- imp$model$bsts.model
  # bsts coefficient posterior is a matrix: iterations × predictors (excluding intercept)
  coefs <- bs$coefficients
  # inclusion probability = fraction of iterations where each coef != 0
  inc_prob <- apply(coefs != 0, 2, mean)
  # posterior mean conditional on inclusion
  post_mean_cond <- sapply(seq_len(ncol(coefs)), function(j) {
    nonzero <- coefs[, j][coefs[, j] != 0]
    if (length(nonzero) == 0) NA else mean(nonzero)
  })
  # unconditional posterior mean (over all draws, including zeros)
  post_mean_uncond <- apply(coefs, 2, mean)
  # Predictor names
  pnames <- colnames(coefs)
  cat(sprintf("\n=== %s ===\n", tag))
  cat(sprintf("Pre-window: %s to %s (n=%d)\n", ps, cutover - 1, sum(sub$d < cutover)))
  cat(sprintf("Covariates: %d\n\n", length(pnames)))
  cat(sprintf("%-22s %10s %16s %16s\n", "predictor", "inc.prob.", "cond. mean", "uncond. mean"))
  cat(sprintf("%-22s %10s %16s %16s\n", "---------", "---------", "----------", "------------"))
  for (j in seq_along(pnames)) {
    cat(sprintf("%-22s %10.3f %+16.5f %+16.5f\n",
                pnames[j], inc_prob[j], post_mean_cond[j], post_mean_uncond[j]))
  }
  data.frame(
    tag = tag, predictor = pnames,
    inc_prob = inc_prob, mean_cond = post_mean_cond,
    mean_uncond = post_mean_uncond,
    stringsAsFactors = FALSE
  )
}

rows <- list()
# ID15 spec: pre 2022-01 -> 2025-03-18 (2025 = winter only)
rows[[1]] <- extract_year_coefs("ida_price_eur", "2022-01-01", "2025-03-19", "2025-04-27",
                                  "ID15_IDA_price")
rows[[2]] <- extract_year_coefs("da_price_eur",  "2022-01-01", "2025-03-19", "2025-04-27",
                                  "ID15_DA_price")
# DA15 spec: pre 2022-01 -> 2025-09-30 (2025 = Jan-Sep, includes summer)
rows[[3]] <- extract_year_coefs("ida_price_eur", "2022-01-01", "2025-10-01", "2025-11-09",
                                  "DA15_IDA_price")
rows[[4]] <- extract_year_coefs("da_price_eur",  "2022-01-01", "2025-10-01", "2025-11-09",
                                  "DA15_DA_price")

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_solar_year_coefs.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_solar_year_coefs.csv\n", nrow(out_df)))
