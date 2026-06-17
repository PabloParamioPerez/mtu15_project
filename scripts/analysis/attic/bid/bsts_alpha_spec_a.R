# STATUS: ALIVE
# LAST-AUDIT: 2026-06-05
# FEEDS: Long-pre + year-by-year renewable interactions BSTS on the
#        per-curve intercept alpha (level of the in-band bid curve at Q=0).
#        Spec A mirrors the price/cleared-MWh BSTS already in use.
#
# IN:  data/derived/panels/alpha_daily_panel.parquet
#       columns: d, tech, market, alpha_mean, n_curves, wind_gwh, solar_gwh, gas_eur
# OUT: results/regressions/bid/mtu15_critical_flat/
#        bsts_alpha_spec_a.csv
#       columns: reform, market, tech, post_lo, post_hi,
#                point_effect, ci_lo, ci_hi, p_post, n_pre, n_post

suppressPackageStartupMessages({
  library(arrow);    library(dplyr);  library(tidyr);
  library(bsts);     library(CausalImpact); library(readr);
  library(zoo)
})

REPO <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
PANEL <- file.path(REPO, "data/derived/panels/alpha_daily_panel.parquet")
OUT_DIR <- file.path(REPO, "results/regressions/bid/mtu15_critical_flat")
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

WINDOWS <- list(
  ID15_real = list(market = "ida", pre_lo = as.Date("2022-01-01"),
                   post_lo = as.Date("2025-03-19"), post_hi = as.Date("2025-04-27"),
                   placebo_post_lo = as.Date("2024-03-19"),
                   placebo_post_hi = as.Date("2024-04-27")),
  DA15_real = list(market = "da",  pre_lo = as.Date("2022-01-01"),
                   post_lo = as.Date("2025-10-01"), post_hi = as.Date("2026-01-31"),
                   placebo_post_lo = as.Date("2024-10-01"),
                   placebo_post_hi = as.Date("2025-01-31"))
)

TECHS <- c("CCGT", "Hydro", "Hydro_pump")
NITER <- 2000
PRIOR_LEVEL_SD <- 0.005

interp_na <- function(x) {
  if (all(is.na(x))) return(x)
  ix <- which(!is.na(x))
  if (length(ix) < 2) {
    x[is.na(x)] <- mean(x, na.rm = TRUE); return(x)
  }
  approx(ix, x[ix], xout = seq_along(x), rule = 2)$y
}

run_one <- function(panel, reform_key, tech, post_lo, post_hi) {
  W <- WINDOWS[[reform_key]]
  # Use the FULL pre-window for real, regardless of post-window
  pre_lo <- W$pre_lo
  d <- panel %>% filter(tech == !!tech, market == W$market)
  d <- d %>% filter(d >= pre_lo & d <= post_hi) %>% arrange(d)
  if (nrow(d) < 100) return(NULL)
  d_grid <- tibble(d = seq(pre_lo, post_hi, by = "day"))
  d <- d_grid %>% left_join(d, by = "d")
  d$year <- as.integer(format(d$d, "%Y"))
  for (cv in c("wind_gwh", "solar_gwh", "gas_eur")) {
    d[[cv]] <- interp_na(d[[cv]])
  }
  # interpolate response NAs (cannot pass NA to CausalImpact)
  d$alpha_mean <- interp_na(d$alpha_mean)
  yrs <- unique(d$year); yrs <- yrs[yrs > min(yrs)]
  for (y in yrs) {
    d[[paste0("wind_y", y)]]  <- d$wind_gwh  * (d$year == y)
    d[[paste0("solar_y", y)]] <- d$solar_gwh * (d$year == y)
  }
  pre_period  <- as.integer(c(1, sum(d$d < post_lo)))
  post_period <- as.integer(c(pre_period[2] + 1, nrow(d)))
  if (post_period[2] < post_period[1]) return(NULL)
  cov_cols <- c("wind_gwh", "solar_gwh", "gas_eur",
                grep("^wind_y|^solar_y", names(d), value = TRUE))
  X <- as.matrix(d[, cov_cols])
  # use CausalImpact's positional pre/post indices on a complete y
  zoo_data <- zoo::zoo(cbind(y = d$alpha_mean, X), order.by = d$d)
  ci <- tryCatch(
    CausalImpact(zoo_data, pre.period = c(d$d[pre_period[1]], d$d[pre_period[2]]),
                 post.period = c(d$d[post_period[1]], d$d[post_period[2]]),
                 model.args = list(niter = NITER,
                                   prior.level.sd = PRIOR_LEVEL_SD,
                                   nseasons = 7)),
    error = function(e) { cat("    inner ERROR:", conditionMessage(e), "\n"); NULL })
  if (is.null(ci)) return(NULL)
  s <- ci$summary["Average", ]
  list(point_effect = s$AbsEffect, ci_lo = s$AbsEffect.lower,
       ci_hi = s$AbsEffect.upper, p_post = s$p,
       n_pre = pre_period[2], n_post = post_period[2] - post_period[1] + 1)
}

main <- function() {
  panel <- arrow::read_parquet(PANEL)
  panel$d <- as.Date(panel$d)
  rows <- list()
  for (rk in names(WINDOWS)) {
    W <- WINDOWS[[rk]]
    for (tech in TECHS) {
      for (kind in c("real", "placebo")) {
        post_lo <- if (kind == "real") W$post_lo else W$placebo_post_lo
        post_hi <- if (kind == "real") W$post_hi else W$placebo_post_hi
        cat(sprintf("[%s|%s|%s] running\n", rk, tech, kind))
        r <- tryCatch(run_one(panel, rk, tech, post_lo, post_hi),
                      error = function(e) {
                        cat("  ERROR:", conditionMessage(e), "\n"); NULL })
        if (is.null(r)) next
        rows[[length(rows) + 1]] <- c(
          list(reform = sub("_real", "", rk), market = W$market, tech = tech,
               kind = kind,
               post_lo = as.character(post_lo), post_hi = as.character(post_hi)),
          r)
      }
    }
  }
  out <- bind_rows(lapply(rows, as_tibble))
  fout <- file.path(OUT_DIR, "bsts_alpha_spec_a.csv")
  write_csv(out, fout)
  cat("\nWrote", fout, "with", nrow(out), "rows\n")
  print(out, n = 50)
}

main()
