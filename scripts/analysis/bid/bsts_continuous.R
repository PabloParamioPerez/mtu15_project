# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 6.B (added in this session)
#        -- BSTS counterfactual on the daily continuous-market panel for the
#        three structural reform cutovers:
#          ISP15 (2024-12-11): settlement period 60->15 min
#          ID15  (2025-03-19): intraday auctions + continuous market -> MTU15
#          DA15  (2025-10-01): day-ahead -> MTU15
#
# Outcomes: n_trades, gwh, vw_price. Covariates: wind_gwh, solar_gwh, gas_eur.
# Real-vs-placebo (same calendar window 2024 with fake cutover).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_continuous.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_continuous.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/continuous_daily_panel.parquet")
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
OUTS   <- c("n_trades", "gwh", "vw_price")

CFGS <- list(
  # ISP15: pre = post-IDA-reform regime; post = pre-ID15 (~3 months)
  list("ISP15", "real",    "2024-06-14", "2024-12-10", "2024-12-11", "2025-03-18"),
  list("ISP15", "placebo", "2023-06-14", "2023-12-10", "2023-12-11", "2024-03-18"),
  # ID15: pre = post-ISP15; post = pre-blackout (40 days)
  list("ID15",  "real",    "2024-12-11", "2025-03-18", "2025-03-19", "2025-04-27"),
  list("ID15",  "placebo", "2023-12-11", "2024-03-18", "2024-03-19", "2024-04-27"),
  # DA15: pre = post-blackout / pre-DA15; post = first ~3 months
  list("DA15",  "real",    "2025-04-28", "2025-09-30", "2025-10-01", "2025-12-31"),
  list("DA15",  "placebo", "2024-04-28", "2024-09-30", "2024-10-01", "2024-12-31")
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
                  model.args = list(niter = 2000, nseasons = 7,
                                     season.duration = 1)),
    error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       eff_rel = s["Average","RelEffect"],
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
  cat(sprintf("\n=== %s %s: pre %s -> %s | post %s -> %s ===\n",
              reform, side, pre_lo, pre_hi, post_lo, post_hi))
  for (outcome in OUTS) {
    r <- run_bsts(panel, outcome, pre_lo, pre_hi, post_lo, post_hi)
    if (is.null(r)) { cat(sprintf("  %-9s: NA\n", outcome)); next }
    cat(sprintf("  %-9s eff=%+10.3f  [%+9.3f, %+9.3f]  rel=%+7.1f%%  p=%5.3f  n=%d/%d\n",
                outcome, r$eff, r$lo, r$hi, 100*r$eff_rel, r$p, r$n_pre, r$n_post))
    rows[[length(rows)+1]] <- data.frame(
      reform=reform, side=side, outcome=outcome,
      eff=r$eff, lo=r$lo, hi=r$hi, eff_rel=r$eff_rel,
      p=r$p, n_pre=r$n_pre, n_post=r$n_post,
      stringsAsFactors=FALSE)
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_continuous.csv"), row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_continuous.csv\n", nrow(out_df)))
