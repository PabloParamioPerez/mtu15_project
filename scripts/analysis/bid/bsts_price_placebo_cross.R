# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- BSTS placebo for the two
#        cross-market price cells that the headline BSTS script never ran:
#        ID15 DA cross (response da_price_eur, cutover 2024-03-19)
#        DA15 IDA cross (response ida_price_eur, cutover 2024-10-01)
#
# Same spec as bsts_daily_year_interactions.R: per-year wind/solar dummies,
# spike-and-slab, niter=10000, prior.level.sd=0.005.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_price_placebo_cross.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  yrs_kept <- yrs[-1]; new_cols <- c()
  for (y in yrs_kept) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 10000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  s <- imp$summary
  cat(sprintf("  %-40s eff=%+8.2f  CI=[%+7.2f,%+7.2f] p=%.3f n_pre=%d n_post=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  data.frame(tag=tag, eff=s["Average","AbsEffect"],
             lo=s["Average","AbsEffect.lower"], hi=s["Average","AbsEffect.upper"],
             p=s$p[1], n_pre=sum(sub$d < cutover), n_post=sum(sub$d >= cutover),
             stringsAsFactors=FALSE)
}

rows <- list()
cat("=== ID15 DA cross placebo (cutover 2024-03-19, 40d post) ===\n")
rows[[1]] <- run_bsts("da_price_eur", "2022-01-01", "2024-03-19", "2024-04-27",
                       "PLB ID15 DA cross")
cat("\n=== DA15 IDA cross placebo (cutover 2024-10-01, 92d post) ===\n")
rows[[2]] <- run_bsts("ida_price_eur", "2022-01-01", "2024-10-01", "2024-12-31",
                       "PLB DA15 IDA cross")
out <- do.call(rbind, rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_price_placebo_cross.csv"),
          row.names=FALSE)
cat("\nWrote bsts_price_placebo_cross.csv\n")
