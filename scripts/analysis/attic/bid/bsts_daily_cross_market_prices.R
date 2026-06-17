# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex Table 3 (Spec A) -- fill the
#        cross-market price cells so the table reports BOTH the DA and the
#        IDA clearing price under each reform. Same windows and covariates
#        as bsts_daily_longpre.R.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_cross_market_prices.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_cross_market_prices.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

run_one <- function(response, pre_start, post_start, post_end) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
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
       n_pre = sum(sub$d < cutover),
       n_post = sum(sub$d >= cutover))
}

# Windows match bsts_daily_longpre.R
CFGS <- list(
  list("ID15", "real",    "da_price_eur",  "2024-06-14", "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", "da_price_eur",  "2023-06-14", "2024-03-19", "2024-04-27"),
  list("DA15", "real",    "ida_price_eur", "2025-04-28", "2025-10-01", "2025-11-09"),
  list("DA15", "placebo", "ida_price_eur", "2024-04-28", "2024-10-01", "2024-11-09")
)

rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  resp <- cfg[[3]]; pre_lo <- cfg[[4]]; post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  r <- run_one(resp, pre_lo, post_lo, post_hi)
  if (is.null(r)) next
  cat(sprintf("%s %-8s %-14s  eff=%+7.2f  [%+7.2f,%+7.2f]  p=%.3f  n=%d/%d\n",
              reform, side, resp, r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
  rows[[length(rows)+1]] <- data.frame(
    reform=reform, side=side, response=resp,
    eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
    n_pre=r$n_pre, n_post=r$n_post, stringsAsFactors=FALSE)
}

out_df <- do.call(rbind, rows)
write.csv(out_df,
  file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_daily_cross_market_prices.csv"),
  row.names = FALSE)
cat(sprintf("\nWrote %d rows\n", nrow(out_df)))
