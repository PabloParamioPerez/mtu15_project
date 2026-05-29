# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4 (cross-market cleared MW
#        BSTS, both markets x both reforms x all techs).
#
# Purpose: extend the Spec A BSTS to BOTH markets for both reforms, so we can
# see the cross-market substitution in CLEARED quantities (not just bids).
# The existing memo §4.A has CCGT DA15 cleared scale-up but does not show
# the IDA-side mirror; likewise for ID15.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_cross_market_q.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_cross_market_quantities.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet")
out_dir  <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
TECHS  <- c("ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind")
MARKETS <- c("da", "ida")

# (reform, side, pre_lo, pre_hi, post_lo, post_hi)
CFGS <- list(
  list("ID15", "real",    "2024-06-14", "2025-03-18", "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", "2023-06-14", "2024-03-18", "2024-03-19", "2024-04-27"),
  list("DA15", "real",    "2025-04-28", "2025-09-30", "2025-10-01", "2025-12-31"),
  list("DA15", "placebo", "2024-04-28", "2024-09-30", "2024-10-01", "2024-12-31")
)


run_bsts <- function(panel, response, pre_lo, pre_hi, post_lo, post_hi) {
  ps <- as.Date(pre_lo); pe <- as.Date(post_hi)
  cutover <- as.Date(post_lo)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  pre_period  <- c(ps, cutover - 1)
  post_period <- c(cutover, pe)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, pre_period, post_period,
                  model.args = list(niter = 1500, nseasons = 7,
                                     season.duration = 1)),
    error = function(e) NULL)
  if (is.null(imp)) return(NULL)
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
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_lo <- cfg[[3]]; pre_hi <- cfg[[4]]
  post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  cat(sprintf("\n=== %s %s: post %s -> %s ===\n", reform, side, post_lo, post_hi))
  for (tech in TECHS) {
    for (market in MARKETS) {
      col <- sprintf("q_%s_gwh_%s", tech, market)
      if (!col %in% names(panel)) next
      r <- run_bsts(panel, col, pre_lo, pre_hi, post_lo, post_hi)
      if (is.null(r)) next
      cat(sprintf("  %-12s %-3s eff=%+8.2f  [%+7.2f, %+7.2f]  p=%5.3f\n",
                  tech, market, r$eff, r$lo, r$hi, r$p))
      rows[[length(rows)+1]] <- data.frame(
        reform=reform, side=side, tech=tech, market=market,
        eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
        n_pre=r$n_pre, n_post=r$n_post,
        stringsAsFactors=FALSE)
    }
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_cross_market_q.csv"), row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_cross_market_q.csv\n", nrow(out_df)))
