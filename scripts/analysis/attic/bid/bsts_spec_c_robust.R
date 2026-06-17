# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex Spec C robustness -- BSTS on the daily
#        critical-flat sigma_p differential per (tech, market).
#        Uses the same headline Spec A methodology (long pre-window from
#        2022-01-01, year-by-year wind/solar interactions, 7-day cycle,
#        same-calendar 2024 placebo).
#
#        This is the time-series analogue of the within-day DiD on Spec C:
#        the outcome is the daily critical-flat differential of mean
#        sigma_p across in-band curves, and BSTS gives the counterfactual
#        for that differential absent the reform.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_spec_c.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
DA  <- file.path(repo, "data/derived/panels/per_curve_metrics_da_full.parquet")
IDA <- file.path(repo, "data/derived/panels/per_curve_metrics_ida.parquet")
RENEW <- file.path(repo, "data/derived/panels/bsts_daily_panel.parquet")
OUT <- file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_spec_c.csv")

CRIT <- c(5,6,7,8,16,17,18,19,20,21,22)
FLAT <- c(1,2,3)

LONG_PRE <- "2022-01-01"
CONFIGS <- list(
  list("DA15_DA",  "CCGT",        DA,  "2025-10-01", "2025-12-31"),
  list("DA15_DA",  "Hydro",       DA,  "2025-10-01", "2025-12-31"),
  list("DA15_DA",  "Hydro_pump",  DA,  "2025-10-01", "2025-12-31"),
  list("ID15_DA",  "CCGT",        DA,  "2025-03-19", "2025-04-27"),
  list("ID15_IDA", "Hydro",       IDA, "2025-03-19", "2025-04-27"),
  list("ID15_IDA", "Hydro_pump",  IDA, "2025-03-19", "2025-04-27")
)

build_daily_diff <- function(panel_fp, tech_name) {
  p <- read_parquet(panel_fp)
  p$d <- as.Date(p$d)
  p$hc <- ifelse(p$clock_hour %in% CRIT, "critical",
            ifelse(p$clock_hour %in% FLAT, "flat", NA))
  p <- p[!is.na(p$hc) & p$tech == tech_name, c("d","hc","sigma_p")]
  # Daily mean per hour-class
  agg <- aggregate(sigma_p ~ d + hc, data = p, FUN = mean)
  # Pivot
  crit <- agg[agg$hc == "critical", c("d","sigma_p")]; names(crit)[2] <- "crit"
  flat <- agg[agg$hc == "flat",     c("d","sigma_p")]; names(flat)[2] <- "flat"
  m <- merge(crit, flat, by = "d")
  m$diff <- m$crit - m$flat
  m
}

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  new_cols <- c()
  for (y in yrs[-1]) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d",  y)]] <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(panel_fp, tech_name, post_lo, post_hi, side = "real") {
  d <- build_daily_diff(panel_fp, tech_name)
  if (nrow(d) < 60) return(NULL)
  r <- read_parquet(RENEW); r$d <- as.Date(r$d)
  m <- merge(d, r[, c("d","wind_gwh","solar_gwh","gas_eur")], by = "d", all.x = TRUE)
  m <- m[complete.cases(m), ]

  if (side == "placebo") {
    # Shift cutover by one year, end window before real reform
    p_lo <- as.Date(post_lo) - 365
    p_hi <- as.Date(post_hi) - 365
    pre_end <- p_lo - 1
  } else {
    p_lo <- as.Date(post_lo)
    p_hi <- as.Date(post_hi)
    pre_end <- p_lo - 1
  }
  m <- m[m$d >= as.Date(LONG_PRE) & m$d <= p_hi, ]
  m <- m[order(m$d), ]
  if (nrow(m[m$d < p_lo, ]) < 60 || nrow(m[m$d >= p_lo, ]) < 7) return(NULL)

  yr <- add_year_interactions(m); m <- yr$df
  cov_set <- c("wind_gwh","solar_gwh","gas_eur", yr$cols)
  ts <- zoo(as.matrix(m[, c("diff", cov_set)]), order.by = m$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(ts, c(as.Date(LONG_PRE), pre_end), c(p_lo, p_hi),
                  model.args = list(niter = 2000, nseasons = 7,
                                     season.duration = 1,
                                     prior.level.sd = 0.005)),
    error = function(e) { cat(sprintf("  ERROR %s\n", conditionMessage(e))); NULL },
    warning = function(w) tryCatch(suppressWarnings(
      CausalImpact(ts, c(as.Date(LONG_PRE), pre_end), c(p_lo, p_hi),
                    model.args = list(niter = 2000, nseasons = 7,
                                       season.duration = 1,
                                       prior.level.sd = 0.005))),
      error = function(e) NULL)
  )
  if (is.null(imp) || is.null(imp$summary)) return(NULL)
  s <- imp$summary
  list(eff = as.numeric(s["Average","AbsEffect"]),
       lo  = as.numeric(s["Average","AbsEffect.lower"]),
       hi  = as.numeric(s["Average","AbsEffect.upper"]),
       p   = as.numeric(s$p[1]),
       n_pre = sum(m$d < p_lo), n_post = sum(m$d >= p_lo))
}

rows <- list()
for (cfg in CONFIGS) {
  rm <- cfg[[1]]; tech <- cfg[[2]]; panel <- cfg[[3]]
  p_lo <- cfg[[4]]; p_hi <- cfg[[5]]
  for (side in c("real", "placebo")) {
    cat(sprintf("=== %s %s [%s] ===\n", rm, tech, side))
    r <- run_bsts(panel, tech, p_lo, p_hi, side)
    if (is.null(r)) {
      rows[[length(rows)+1]] <- data.frame(
        cell = sprintf("%s %s", rm, tech), side = side,
        eff = NA, lo = NA, hi = NA, p = NA, n_pre = 0, n_post = 0,
        stringsAsFactors = FALSE)
      next
    }
    cat(sprintf("  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  p=%.4f  n=%d/%d\n",
                r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
    rows[[length(rows)+1]] <- data.frame(
      cell = sprintf("%s %s", rm, tech), side = side,
      eff = r$eff, lo = r$lo, hi = r$hi, p = r$p,
      n_pre = r$n_pre, n_post = r$n_post, stringsAsFactors = FALSE)
  }
}
out <- do.call(rbind, rows)
dir.create(dirname(OUT), recursive = TRUE, showWarnings = FALSE)
write.csv(out, OUT, row.names = FALSE)
cat(sprintf("\nWrote %s with %d rows.\n", OUT, nrow(out)))

cat("\n=== BSTS Spec C summary (effect on daily crit-flat sigma_p differential) ===\n")
for (cell in unique(out$cell)) {
  real <- out[out$cell == cell & out$side == "real", "eff"]
  plac <- out[out$cell == cell & out$side == "placebo", "eff"]
  real_lo <- out[out$cell == cell & out$side == "real", "lo"]
  real_hi <- out[out$cell == cell & out$side == "real", "hi"]
  real_p <- out[out$cell == cell & out$side == "real", "p"]
  if (length(real) == 0) next
  star <- ifelse(!is.na(real_p) && real_p < 0.01, "***",
            ifelse(!is.na(real_p) && real_p < 0.05, "**",
              ifelse(!is.na(real_p) && real_p < 0.10, "*", "")))
  cat(sprintf("  %-30s  real=%+8.3f%-3s [%+7.3f,%+7.3f]  placebo=%+8.3f  net=%+8.3f\n",
              cell, real, star, real_lo, real_hi,
              ifelse(is.na(plac), 0, plac),
              ifelse(is.na(plac), real, real - plac)))
}
