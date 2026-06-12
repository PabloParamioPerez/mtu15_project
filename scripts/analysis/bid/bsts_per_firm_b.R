# STATUS: ALIVE
# LAST-AUDIT: 2026-06-12
# FEEDS: BSTS-daily companion to the per-firm residual-demand-slope OLS
#        (ols_per_firm_b_residual.R), completing the confirmation battery
#        (OLS hourly rich no-trend + BSTS daily) for the b_f finding.
#
# Outcome: daily mean log(b_f) per (focal_firm, market), from the
# Ito-Reguant per-firm residual-demand recovery at the clearing price.
# CausalImpact with (wind, solar, gas) regressors and 7-day seasonality,
# same convention as bsts_da_ida_wedge.R.
#
# Windows (reforzada-constant, per project convention):
#   ID15 real:    pre 2024-06-14 -> 2025-03-18, post 2025-03-19 -> 2025-04-27
#   DA15 real:    pre 2025-04-28 -> 2025-09-30, post 2025-10-01 -> 2025-12-31
#   DA15 placebo: pre 2024-06-14 -> 2024-09-30, post 2024-10-01 -> 2024-12-31
#     (short pre, ~3.5 months; flagged in output. ID15 placebo impossible:
#      the per-firm panel starts at the European-IDA reform 2024-06-14.)
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_per_firm_b.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_per_firm_b.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
slope_fp <- file.path(repo, "data/derived/panels/per_firm_residual_demand_slope.parquet")
covar_fp <- file.path(repo, "data/derived/panels/bsts_daily_panel.parquet")
out_dir  <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
FIRMS  <- c("IB", "GE", "GN", "HC")

CFGS <- list(
  list("ID15", "real",    "2024-06-14", "2025-03-18", "2025-03-19", "2025-04-27"),
  list("DA15", "real",    "2025-04-28", "2025-09-30", "2025-10-01", "2025-12-31"),
  list("DA15", "placebo", "2024-06-14", "2024-09-30", "2024-10-01", "2024-12-31")
)

slope <- read_parquet(slope_fp)
slope$d <- as.Date(slope$d)
slope <- slope[slope$b_residual_mw_per_eur > 0, ]
slope$logb <- log(slope$b_residual_mw_per_eur)

covar <- read_parquet(covar_fp)
covar$d <- as.Date(covar$d)
covar <- covar[, c("d", COVARS)]

run_bsts <- function(daily, pre_lo, pre_hi, post_lo, post_hi) {
  ps <- as.Date(pre_lo); pe <- as.Date(post_hi); cutover <- as.Date(post_lo)
  sub <- daily[daily$d >= ps & daily$d <= pe, ]
  sub <- sub[complete.cases(sub[, c("logb", COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_ts <- zoo(as.matrix(sub[, c("logb", COVARS)]), order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                 model.args = list(niter = 2000, nseasons = 7,
                                   season.duration = 1)),
    error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average", "AbsEffect"],
       lo  = s["Average", "AbsEffect.lower"],
       hi  = s["Average", "AbsEffect.upper"],
       p   = s$p[1],
       n_pre  = sum(sub$d < cutover),
       n_post = sum(sub$d >= cutover))
}

rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_lo <- cfg[[3]]; pre_hi <- cfg[[4]]
  post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  cat(sprintf("\n=== %s %s ===\n", reform, side))
  for (mkt in c("DA", "IDA")) {
    for (firm in FIRMS) {
      sl <- slope[slope$market == mkt & slope$focal_firm == firm, ]
      if (nrow(sl) == 0) next
      daily <- aggregate(logb ~ d, data = sl, FUN = mean)
      daily <- merge(daily, covar, by = "d")
      daily <- daily[order(daily$d), ]
      r <- run_bsts(daily, pre_lo, pre_hi, post_lo, post_hi)
      if (is.null(r)) next
      cat(sprintf("  %-4s %-4s eff=%+6.3f  [%+6.3f, %+6.3f]  p=%5.3f  n=%d/%d\n",
                  mkt, firm, r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
      rows[[length(rows) + 1]] <- data.frame(
        reform = reform, side = side, market = mkt, firm = firm,
        eff_logb = r$eff, lo = r$lo, hi = r$hi, p = r$p,
        n_pre = r$n_pre, n_post = r$n_post,
        stringsAsFactors = FALSE)
    }
  }
}

out_df <- do.call(rbind, rows)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(out_df, file.path(out_dir, "bsts_per_firm_b.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_per_firm_b.csv\n", nrow(out_df)))
