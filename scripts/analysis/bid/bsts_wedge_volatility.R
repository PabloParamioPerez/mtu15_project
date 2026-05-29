# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4(ii) -- BSTS on wedge
#        VOLATILITY (within-day SD across clock-hours of the DA - IDA
#        price wedge), reforzada-constant pre/post-windows per reform.
#        Level wedge BSTS is null; volatility is the natural follow-up
#        check for the granularity-asymmetry mechanism.
#
# Also runs BSTS on |wedge| daily mean and on the daily wedge p90 - p10
# spread as secondary volatility measures.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_wedge_volatility.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_wedge_volatility.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/wedge_volatility_panel.parquet")
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
OUTCOMES <- c("wedge_sd", "wedge_abs", "wedge_iqr",
              "wedge_sd_critical", "wedge_sd_midday", "wedge_sd_flat")
CFGS <- list(
  # ID15 (intraday MTU60 -> MTU15, 2025-03-19): asymmetric granularity break
  list("ID15",  "real",    "2024-06-14", "2025-03-18", "2025-03-19", "2025-04-27"),
  list("ID15",  "placebo", "2023-06-14", "2024-03-18", "2024-03-19", "2024-04-27"),
  # DA15 (day-ahead MTU60 -> MTU15, 2025-10-01): granularity becomes symmetric
  list("DA15",  "real",    "2025-04-28", "2025-09-30", "2025-10-01", "2025-12-31"),
  list("DA15",  "placebo", "2024-04-28", "2024-09-30", "2024-10-01", "2024-12-31"),
  # ISP15 (REE settlement period 60 -> 15, 2024-12-11): NOT a granularity
  # break at OMIE (OMIE contract grid still 60-min); pre = post-IDA-reform
  # regime, post stops at ID15
  list("ISP15", "real",    "2024-06-14", "2024-12-10", "2024-12-11", "2025-03-18"),
  list("ISP15", "placebo", "2023-06-14", "2023-12-10", "2023-12-11", "2024-03-18")
)


run_one <- function(response, pre_lo, pre_hi, post_lo, post_hi) {
  ps <- as.Date(pre_lo); pe <- as.Date(post_hi)
  cutover <- as.Date(post_lo)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                  model.args = list(niter = 2000, nseasons = 7,
                                     season.duration = 1)),
    error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       rel = s["Average","RelEffect"],
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
  cat(sprintf("\n=== %s %s ===\n", reform, side))
  for (outcome in OUTCOMES) {
    r <- run_one(outcome, pre_lo, pre_hi, post_lo, post_hi)
    if (is.null(r)) next
    cat(sprintf("  %-12s eff=%+6.2f  [%+6.2f, %+6.2f]  rel=%+5.1f%%  p=%5.3f  n=%d/%d\n",
                outcome, r$eff, r$lo, r$hi, 100*r$rel, r$p,
                r$n_pre, r$n_post))
    rows[[length(rows)+1]] <- data.frame(
      reform=reform, side=side, outcome=outcome,
      eff=r$eff, lo=r$lo, hi=r$hi, rel=r$rel, p=r$p,
      n_pre=r$n_pre, n_post=r$n_post,
      stringsAsFactors=FALSE)
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_wedge_volatility.csv"),
           row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_wedge_volatility.csv\n", nrow(out_df)))
