# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4(ii) -- BSTS on the daily
#        DA - IDA price spread (the wedge) at each reform, with 2024
#        same-calendar placebos.
#
# Why: the §4.A Spec A table reports DA price and IDA price separately as
# BSTS effects. The DA-IDA wedge is the natural cross-market mechanism
# object -- it identifies the granularity-asymmetry channel that opens
# under ID15 (DA60 vs ID15) and should close under DA15 (DA15 vs ID15).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_da_ida_wedge.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_da_ida_wedge.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/bsts_daily_panel.parquet")
out_dir  <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

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
                  model.args = list(niter = 2000, nseasons = 7,
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

# Derive the DA-IDA spread
panel$wedge <- panel$da_price_eur - panel$ida_price_eur
cat(sprintf("Panel: %d days, %s to %s. wedge range %.2f to %.2f\n",
            nrow(panel), min(panel$d), max(panel$d),
            min(panel$wedge, na.rm=TRUE), max(panel$wedge, na.rm=TRUE)))

OUTCOMES <- c("da_price_eur", "ida_price_eur", "wedge")
rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_lo <- cfg[[3]]; pre_hi <- cfg[[4]]
  post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  cat(sprintf("\n=== %s %s ===\n", reform, side))
  for (outcome in OUTCOMES) {
    r <- run_bsts(panel, outcome, pre_lo, pre_hi, post_lo, post_hi)
    if (is.null(r)) next
    cat(sprintf("  %-13s eff=%+7.2f  [%+6.2f, %+6.2f]  p=%5.3f  n=%d/%d\n",
                outcome, r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
    rows[[length(rows)+1]] <- data.frame(
      reform=reform, side=side, outcome=outcome,
      eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
      n_pre=r$n_pre, n_post=r$n_post,
      stringsAsFactors=FALSE)
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_da_ida_wedge.csv"), row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_da_ida_wedge.csv\n", nrow(out_df)))
