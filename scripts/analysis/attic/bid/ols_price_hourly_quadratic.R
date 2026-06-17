# STATUS: ALIVE
# LAST-AUDIT: 2026-06-10
# Quadratic-renewable robustness on the hourly ID15 (and DA15) price OLS,
# WITHOUT linear time trend. Adds wind^2 and solar^2 to capture convexity
# in the price-depressing effect of renewables (a sunny GWh suppresses
# price more when solar penetration is already high --- merit-order curvature).
#
# Spec ladder:
#   A: base, linear renewables, NO year-x-renew
#   B: + year-x-renew (2024+25 pooled) on linear terms only
#   C: A + wind^2 + solar^2 (no year-x-renew)
#   D: B + wind^2 + solar^2 + year-x-renew on both linear and quadratic terms
#
# No linear time trend in any spec.
# Newey-West HAC SE lag=24*7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_hourly_quadratic.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000
panel$wind_gwh2  <- panel$wind_gwh^2
panel$solar_gwh2 <- panel$solar_gwh^2
panel$wind_gwh3  <- panel$wind_gwh^3
panel$solar_gwh3 <- panel$solar_gwh^3

prep <- function(sub, cutover) {
  sub$year   <- as.integer(format(sub$d, "%Y"))
  sub$month  <- factor(format(sub$d, "%m"))
  sub$dow    <- factor(format(sub$d, "%u"))
  sub$hour_f <- factor(sub$hour)
  sub$post   <- as.integer(sub$d >= cutover)
  sub$y2024p <- as.integer(sub$year >= 2024)
  sub
}

run <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub <- prep(sub, cutover)
  cat(sprintf("\n=== %s (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-60s eff=%+7.2f  SE=%5.2f  p=%.3f\n",
                spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3],
               n=nobs(m), stringsAsFactors=FALSE)
  }
  rows <- list()
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + wind_gwh2 + solar_gwh2 +
                       gas_eur +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       wind_gwh2:y2024p + solar_gwh2:y2024p +
                       hour_f + month + dow, data=sub),
                    "D  quadratic + year-x-renew on linear & quadratic, NO trend")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + wind_gwh2 + solar_gwh2 +
                       wind_gwh3 + solar_gwh3 + gas_eur +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       wind_gwh2:y2024p + solar_gwh2:y2024p +
                       wind_gwh3:y2024p + solar_gwh3:y2024p +
                       hour_f + month + dow, data=sub),
                    "E  cubic + year-x-renew on linear, quadratic & cubic, NO trend")
  do.call(rbind, rows)
}

specs <- list(
  list(tag="ID15 IDA hourly", response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 DA  hourly", response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="DA15 DA  hourly", response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA hourly", response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31")
)

out <- do.call(rbind, lapply(specs, function(s)
  run(s$response, s$pre_lo, s$post_lo, s$post_hi, s$tag)))
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_hourly_quadratic.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_hourly_quadratic.csv\n")
