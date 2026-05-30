# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: advisor_memo.tex sec 4.B Spec B BSTS results under the
#        window-and-market-specific p90 bandwidth (h \in {45,46,49,50,58,62}).
#        Uses the 4 panels built by build_bsts_hour_class_p90.py, one per
#        (reform, real-or-placebo) cell.
#
# Loops over (tech, market, hour_class, outcome) inside each window's panel.
# 144 BSTS runs total. Saves to bsts_hour_class_p90.csv.

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_hour_class_p90.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_dir <- file.path(repo, "data/derived/panels/bsts_hour_class_p90")
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
TECHS  <- c("ccgt", "hydro", "hydro_pump")
MARKETS <- c("da", "ida")
HCS    <- c("morning_ramp", "midday", "evening_ramp", "flat")
OUTS   <- c("p", "mwh")

# (reform, real_or_placebo, panel_filename, cutover_date, post_end)
CFGS <- list(
  list("ID15", "real",    "bsts_hour_class_ID15_real_hDA50_hIDA62.parquet", "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", "bsts_hour_class_ID15_placebo_hDA45_hIDA46.parquet", "2024-03-19", "2024-04-27"),
  list("DA15", "real",    "bsts_hour_class_DA15_real_hDA50_hIDA58.parquet",  "2025-10-01", "2025-11-09"),
  list("DA15", "placebo", "bsts_hour_class_DA15_placebo_hDA45_hIDA49.parquet", "2024-10-01", "2024-11-09")
)


run_bsts <- function(panel, response, post_start, post_end, covars, tag) {
  ps <- min(panel$d); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, covars)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, covars)])
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
  list(eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], p = s$p[1],
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover))
}


rows <- list()
add <- function(reform, rop, outcome, tech, market, hour_class, r) {
  if (is.null(r)) return()
  rows[[length(rows)+1]] <<- data.frame(
    reform = reform, side = rop, outcome = outcome, tech = tech,
    market = market, hour_class = hour_class,
    eff = r$eff, lo = r$lo, hi = r$hi, p = r$p,
    n_pre = r$n_pre, n_post = r$n_post,
    stringsAsFactors = FALSE)
}


for (cfg in CFGS) {
  reform   <- cfg[[1]]; side <- cfg[[2]]; fname <- cfg[[3]]
  cutover  <- cfg[[4]]; post_end <- cfg[[5]]
  cat(sprintf("\n=== %s %s (%s) cutover=%s ===\n", reform, side, fname, cutover))
  panel <- read_parquet(file.path(panel_dir, fname))
  panel$d <- as.Date(panel$d)
  panel <- panel[order(panel$d), ]
  for (outcome in OUTS) {
    for (tech in TECHS) {
      for (market in MARKETS) {
        for (hc in HCS) {
          col <- sprintf("%s_%s_%s_%s", outcome, tech, market, hc)
          if (!col %in% names(panel)) next
          r <- run_bsts(panel, col, cutover, post_end, COVARS,
                         sprintf("%s %s", reform, col))
          add(reform, side, outcome, tech, market, hc, r)
        }
      }
    }
  }
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_hour_class_p90.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_hour_class_p90.csv\n", nrow(out_df)))
