# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: BSTS on bid-curve intercept alpha with the RESTRICTED (post-reforzada-
#        onset, pre-MTU15-DA) pre-window for DA15. The long-pre Spec A (see
#        bsts_alpha_spec_a.R) suffers from huge daily-mean alpha variability
#        over 2022-2025. The restricted-pre version uses only 2025-04-28 ->
#        2025-09-30 as pre, which keeps the reforzada-regime constant and gives
#        a cleaner alpha baseline.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_alpha_restricted.csv

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(bsts); library(CausalImpact);
  library(readr); library(zoo)
})

REPO <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
PANEL <- file.path(REPO, "data/derived/panels/alpha_daily_panel.parquet")
OUT_DIR <- file.path(REPO, "results/regressions/bid/mtu15_critical_flat")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Restricted-pre DA15 only (reforzada-constant)
DA15 <- list(
  market = "da",
  pre_lo = as.Date("2025-04-28"), pre_hi = as.Date("2025-09-30"),
  post_lo = as.Date("2025-10-01"), post_hi = as.Date("2026-01-31"),
  placebo_pre_lo = as.Date("2024-04-28"), placebo_pre_hi = as.Date("2024-09-30"),
  placebo_post_lo = as.Date("2024-10-01"), placebo_post_hi = as.Date("2025-01-31")
)
TECHS <- c("CCGT", "Hydro", "Hydro_pump")

interp_na <- function(x) {
  if (all(is.na(x))) return(x)
  ix <- which(!is.na(x))
  if (length(ix) < 2) { x[is.na(x)] <- mean(x, na.rm = TRUE); return(x) }
  approx(ix, x[ix], xout = seq_along(x), rule = 2)$y
}

run_one <- function(panel, tech, pre_lo, post_lo, post_hi) {
  d <- panel %>% filter(tech == !!tech, market == DA15$market)
  d <- d %>% filter(d >= pre_lo & d <= post_hi) %>% arrange(d)
  if (nrow(d) < 60) return(NULL)
  d_grid <- tibble(d = seq(pre_lo, post_hi, by = "day"))
  d <- d_grid %>% left_join(d, by = "d")
  for (cv in c("wind_gwh", "solar_gwh", "gas_eur")) d[[cv]] <- interp_na(d[[cv]])
  d$alpha_mean <- interp_na(d$alpha_mean)
  pre_period  <- c(1L, as.integer(sum(d$d < post_lo)))
  post_period <- c(pre_period[2] + 1L, nrow(d))
  if (post_period[2] < post_period[1]) return(NULL)
  zoo_data <- zoo::zoo(cbind(y = d$alpha_mean,
                              as.matrix(d[, c("wind_gwh","solar_gwh","gas_eur")])),
                       order.by = d$d)
  ci <- tryCatch(
    CausalImpact(zoo_data,
                 pre.period = c(d$d[pre_period[1]], d$d[pre_period[2]]),
                 post.period = c(d$d[post_period[1]], d$d[post_period[2]]),
                 model.args = list(niter = 2000, prior.level.sd = 0.01,
                                   nseasons = 7)),
    error = function(e) { cat("  ERROR:", conditionMessage(e), "\n"); NULL })
  if (is.null(ci)) return(NULL)
  s <- ci$summary["Average", ]
  list(point_effect = s$AbsEffect, ci_lo = s$AbsEffect.lower,
       ci_hi = s$AbsEffect.upper, p_post = s$p,
       n_pre = pre_period[2], n_post = post_period[2] - post_period[1] + 1)
}

main <- function() {
  panel <- arrow::read_parquet(PANEL); panel$d <- as.Date(panel$d)
  rows <- list()
  for (tech in TECHS) {
    for (kind in c("real", "placebo")) {
      if (kind == "real") {
        pre_lo <- DA15$pre_lo; post_lo <- DA15$post_lo; post_hi <- DA15$post_hi
      } else {
        pre_lo <- DA15$placebo_pre_lo; post_lo <- DA15$placebo_post_lo
        post_hi <- DA15$placebo_post_hi
      }
      cat(sprintf("[DA15|%s|%s] running\n", tech, kind))
      r <- tryCatch(run_one(panel, tech, pre_lo, post_lo, post_hi),
                    error = function(e) { cat("  ERR:", conditionMessage(e), "\n"); NULL })
      if (is.null(r)) next
      rows[[length(rows) + 1]] <- c(list(tech = tech, kind = kind), r)
    }
  }
  out <- bind_rows(lapply(rows, as_tibble))
  fout <- file.path(OUT_DIR, "bsts_alpha_restricted.csv")
  write_csv(out, fout)
  cat("\nWrote", fout, "with", nrow(out), "rows\n"); print(out, n = 50)
}

main()
