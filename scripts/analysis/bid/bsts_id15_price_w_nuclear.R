# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4(ii) -- ID15 price BSTS
#        with nuclear cleared MWh added as covariate. Tests whether the
#        residual -24.7 EUR/MWh DA placebo-net price drop is explained by
#        the spring-2025 vs spring-2024 nuclear refueling-cycle differential
#        (about +25 GWh/day more nuclear in 2025 spring).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_id15_price_w_nuclear.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_id15_price_w_nuclear.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/bsts_daily_panel_w_nuclear.parquet")
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVAR_SPECS <- list(
  base       = c("wind_gwh", "solar_gwh", "gas_eur"),
  with_nuc   = c("wind_gwh", "solar_gwh", "gas_eur", "nuclear_gwh")
)

OUTCOMES <- c("da_price_eur", "ida_price_eur")

CFGS <- list(
  list("ID15", "real",    "2024-06-14", "2025-03-18", "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", "2023-06-14", "2024-03-18", "2024-03-19", "2024-04-27")
)


run_one <- function(response, pre_lo, pre_hi, post_lo, post_hi, covars) {
  ps <- as.Date(pre_lo); pe <- as.Date(post_hi)
  cutover <- as.Date(post_lo)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, covars)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, covars)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       p   = s$p[1],
       n_pre  = sum(sub$d < cutover),
       n_post = sum(sub$d >= cutover))
}


panel <- read_parquet(panel_fp)
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]
cat(sprintf("Panel: %d days, %s to %s\n", nrow(panel),
            min(panel$d), max(panel$d)))

rows <- list()
for (spec_name in names(COVAR_SPECS)) {
  covars <- COVAR_SPECS[[spec_name]]
  cat(sprintf("\n##### Covariate spec: %s (%s) #####\n",
              spec_name, paste(covars, collapse=", ")))
  for (cfg in CFGS) {
    reform <- cfg[[1]]; side <- cfg[[2]]
    pre_lo <- cfg[[3]]; pre_hi <- cfg[[4]]
    post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
    cat(sprintf("\n=== %s %s [%s] ===\n", reform, side, spec_name))
    for (outcome in OUTCOMES) {
      r <- run_one(outcome, pre_lo, pre_hi, post_lo, post_hi, covars)
      if (is.null(r)) next
      cat(sprintf("  %-15s eff=%+7.2f  [%+6.2f, %+6.2f]  p=%5.3f  n=%d/%d\n",
                  outcome, r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
      rows[[length(rows)+1]] <- data.frame(
        spec=spec_name, reform=reform, side=side, outcome=outcome,
        eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
        n_pre=r$n_pre, n_post=r$n_post,
        stringsAsFactors=FALSE)
    }
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_id15_price_w_nuclear.csv"),
           row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_id15_price_w_nuclear.csv\n", nrow(out_df)))
