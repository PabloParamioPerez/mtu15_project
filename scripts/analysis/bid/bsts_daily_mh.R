# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 5(c) window-length robustness. Headline BSTS
#        results (regime-constant long pre) are produced by bsts_daily_longpre.R;
#        this script is the Markle-Huss 80-day-pre baseline for comparison.
#
# Bayesian Structural Time Series (BSTS / CausalImpact) following the
# Markle-Huss et al. (2017, Energy Economics) setup for EPEX 15-min:
#   - Daily granularity.
#   - Local linear trend with stochastic slope (default priors).
#   - Weekly seasonality (nseasons = 7, season.duration = 1).
#   - Wind, solar and gas as covariates (M-H used wind+solar only for Germany;
#     we add gas because Spain has meaningfully different gas exposure).
#   - 80-day pre + 40-day post window (matched to their setup).
#   - niter = 2000 MCMC iterations.
#
# Runs for prices and per-tech auction-cleared GWh (CCGT, Hydro, Wind,
# Solar, Nuclear), for ID15 and DA15 alpha and the 2024 placebo.
#
# OUT:
#   results/regressions/bid/mtu15_critical_flat/bsts_daily_mh.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_mh.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]
cat("Daily panel:", nrow(panel), "days, range",
    as.character(min(panel$d)), "to", as.character(max(panel$d)), "\n\n")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
# Note: M-H 2017 used wind+solar only for EPEX. We add gas because Spain has
# meaningfully different gas exposure than Germany did in the M-H window;
# robustness check after peer-review flagged the IDA price effect as exposed.
SAVE_POINTWISE <- c("ID15 IDA price", "DA15 q_ccgt_da", "DA15 DA price",
                     "PLB-ID IDA price", "PLB-DA q_ccgt_da", "PLB-DA DA price")


run_bsts <- function(panel, response, pre_start, pre_end, post_start,
                      post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  needed <- c(response, COVARS)
  sub <- sub[complete.cases(sub[, needed]), ]
  if (nrow(sub) < 30) {
    cat(sprintf("  %s: only %d days -- skip\n", tag, nrow(sub))); return(NULL)
  }
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  pre_period <- c(as.Date(pre_start), cutover - 1)
  post_period <- c(cutover, pe)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, pre_period, post_period,
                 model.args = list(niter = 2000, nseasons = 7,
                                    season.duration = 1)),
    error = function(e) {
      cat(sprintf("  %s ERROR: %s\n", tag, conditionMessage(e))); NULL
    }
  )
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  eff <- s["Average", "AbsEffect"]
  lo  <- s["Average", "AbsEffect.lower"]
  hi  <- s["Average", "AbsEffect.upper"]
  rel <- s["Average", "RelEffect"]
  pv  <- s$p[1]
  cat(sprintf("  %-26s  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  rel=%+5.2f  p=%.3f  n_pre=%d  n_post=%d\n",
              tag, eff, lo, hi, rel, pv,
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  # Save pointwise series for selected outcomes (into pointwise/ subdir)
  if (any(sapply(SAVE_POINTWISE, function(s) grepl(s, tag, fixed = TRUE)))) {
    pw <- as.data.frame(imp$series)
    pw$date <- as.Date(rownames(pw))
    pw_dir <- file.path(out_dir, "pointwise")
    dir.create(pw_dir, recursive = TRUE, showWarnings = FALSE)
    pw_path <- file.path(pw_dir,
                          sprintf("bsts_daily_pointwise_%s.csv",
                                   gsub(" ", "_", tag)))
    write.csv(pw, pw_path, row.names = FALSE)
  }
  list(eff = eff, lo = lo, hi = hi, rel = rel, p = pv,
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover))
}


results <- list()
add <- function(reform, outcome, tech, r) {
  if (is.null(r)) return()
  results[[length(results) + 1]] <<- data.frame(
    reform = reform, outcome = outcome, tech = tech,
    eff = r$eff, lo = r$lo, hi = r$hi, rel = r$rel, p = r$p,
    n_pre = r$n_pre, n_post = r$n_post,
    stringsAsFactors = FALSE
  )
}


# ============================================================
# ID15 (pre 2024-12-29 -> 2025-03-18; post 2025-03-19 -> 2025-04-27)
# ============================================================
cat("=== ID15 (80d pre + 40d post; pre-blackout post) ===\n")
ID_PRE <- "2024-12-29"; ID_REFORM <- "2025-03-19"; ID_POST_END <- "2025-04-27"

r <- run_bsts(panel, "ida_price_eur", ID_PRE, NULL, ID_REFORM, ID_POST_END,
              "ID15 IDA price")
add("ID15", "price", NA, r)
for (tech in c("ccgt", "hydro", "wind", "solar", "nuclear")) {
  col <- sprintf("q_%s_gwh_ida", tech)
  r <- run_bsts(panel, col, ID_PRE, NULL, ID_REFORM, ID_POST_END,
                sprintf("ID15 q_%s_ida", tech))
  add("ID15", "cleared_gwh", tech, r)
}


# ============================================================
# DA15 alpha (pre 2025-07-13 -> 2025-09-30; post 2025-10-01 -> 2025-11-09)
# 80-day pre window starts well after reforzada onset (2025-04-28),
# holding the regulatory regime constant.
# ============================================================
cat("\n=== DA15 alpha (80d pre + 40d post; reforzada-constant) ===\n")
DA_PRE <- "2025-07-13"; DA_REFORM <- "2025-10-01"; DA_POST_END <- "2025-11-09"

r <- run_bsts(panel, "da_price_eur", DA_PRE, NULL, DA_REFORM, DA_POST_END,
              "DA15 DA price")
add("DA15", "price", NA, r)
for (tech in c("ccgt", "hydro", "wind", "solar", "nuclear")) {
  col <- sprintf("q_%s_gwh_da", tech)
  r <- run_bsts(panel, col, DA_PRE, NULL, DA_REFORM, DA_POST_END,
                sprintf("DA15 q_%s_da", tech))
  add("DA15", "cleared_gwh", tech, r)
}


# ============================================================
# Placebo for DA15 -- same calendar 2024 (pre 2024-07-13 -> 2024-09-30;
# fake post 2024-10-01 -> 2024-11-09).
# Pre-reform, pre-blackout, pre-reforzada.
# ============================================================
cat("\n=== Placebo DA15-2024 (same calendar as DA15) ===\n")
PLB_DA_PRE <- "2024-07-13"; PLB_DA_REFORM <- "2024-10-01"; PLB_DA_POST_END <- "2024-11-09"

r <- run_bsts(panel, "da_price_eur", PLB_DA_PRE, NULL, PLB_DA_REFORM, PLB_DA_POST_END,
              "PLB-DA DA price")
add("PLB_DA15", "price", NA, r)
for (tech in c("ccgt", "hydro", "wind", "solar", "nuclear")) {
  col <- sprintf("q_%s_gwh_da", tech)
  r <- run_bsts(panel, col, PLB_DA_PRE, NULL, PLB_DA_REFORM, PLB_DA_POST_END,
                sprintf("PLB-DA q_%s_da", tech))
  add("PLB_DA15", "cleared_gwh", tech, r)
}


# ============================================================
# Placebo for ID15 -- same calendar 2024 (pre 2023-12-29 -> 2024-03-18;
# fake post 2024-03-19 -> 2024-04-27). Pre-reform.
# ============================================================
cat("\n=== Placebo ID15-2024 (same calendar as ID15) ===\n")
PLB_ID_PRE <- "2023-12-29"; PLB_ID_REFORM <- "2024-03-19"; PLB_ID_POST_END <- "2024-04-27"

r <- run_bsts(panel, "ida_price_eur", PLB_ID_PRE, NULL, PLB_ID_REFORM, PLB_ID_POST_END,
              "PLB-ID IDA price")
add("PLB_ID15", "price", NA, r)
for (tech in c("ccgt", "hydro", "wind", "solar", "nuclear")) {
  col <- sprintf("q_%s_gwh_ida", tech)
  r <- run_bsts(panel, col, PLB_ID_PRE, NULL, PLB_ID_REFORM, PLB_ID_POST_END,
                sprintf("PLB-ID q_%s_ida", tech))
  add("PLB_ID15", "cleared_gwh", tech, r)
}


# ============================================================
# Save
# ============================================================
out_df <- do.call(rbind, results)
write.csv(out_df, file.path(out_dir, "bsts_daily_mh.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_daily_mh.csv\n", nrow(out_df)))
cat("Done.\n")
