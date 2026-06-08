# STATUS: ALIVE
# LAST-AUDIT: 2026-06-08
# FEEDS: thesis/paper/thesis.tex --- OLS price effect at hourly granularity.
#        Hourly DA & IDA prices, hour-of-day FE + month FE + DOW FE +
#        year-by-renewable + linear trend. Newey-West HAC SE lag=24*7.
#
# Two strategies:
#   1. Pooled hourly with hour FE — one regression per (reform, market).
#   2. Per-hour separate (Reguant-style) — 24 regressions per (reform, market).
#
# RAM-safe: hourly panel is 38k rows × 20 cols; pooled regression is small.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_hourly.csv
#      results/regressions/bid/mtu15_critical_flat/ols_price_per_hour.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]

# Convert MWh to GWh for the renewable controls (more interpretable scale)
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

prep <- function(sub, cutover) {
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$hour_f<- factor(sub$hour)
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2025 <- as.integer(sub$year == 2025)
  sub$y2024p<- as.integer(sub$year >= 2024)
  sub
}

run_pooled <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub <- prep(sub, cutover)
  n_post <- sum(sub$post)
  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n",
                spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3],
               n=nobs(m), stringsAsFactors=FALSE)
  }
  cat(sprintf("\n=== %s pooled hourly (n=%d, n_post=%d) ===\n", tag, nrow(sub), n_post))
  rows <- list()
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "Spec 1h: base + hour-of-day + month + DOW")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       t + hour_f + month + dow, data=sub),
                    "Spec 2h: + linear time trend")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + hour_f + month + dow, data=sub),
                    "Spec 3h: + year-by-renew (per year)")
  rows[[4]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + hour_f + month + dow, data=sub),
                    "Spec 4h: + year-by-renew (2024+ pooled)")
  do.call(rbind, rows)
}

run_per_hour <- function(response, pre_start, post_start, post_end, tag,
                          year_struct = "pooled") {
  # year_struct: "pooled" = Spec 4 (2024+25 pooled), "peryear" = Spec 3 (separate)
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub <- prep(sub, cutover)
  cat(sprintf("\n=== %s per-hour (24 separate regressions, %s year-struct) ===\n", tag, year_struct))
  rows <- list()
  for (h in 0:23) {
    sub_h <- sub[sub$hour == h, ]
    if (nrow(sub_h) < 30) next
    if (year_struct == "pooled") {
      m <- lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
              wind_gwh:y2023 + solar_gwh:y2023 +
              wind_gwh:y2024p + solar_gwh:y2024p +
              t + month + dow, data=sub_h)
    } else {
      m <- lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
              wind_gwh:y2023 + solar_gwh:y2023 +
              wind_gwh:y2024 + solar_gwh:y2024 +
              wind_gwh:y2025 + solar_gwh:y2025 +
              t + month + dow, data=sub_h)
    }
    s <- coeftest(m, vcov=NeweyWest(m, lag=7, prewhite=FALSE))
    if (!"post" %in% rownames(s)) next
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  hour=%02d  eff=%+7.2f  SE=%5.2f  p=%.3f\n", h, c1[1], c1[2], c1[3]))
    rows[[length(rows)+1]] <- data.frame(
      tag=tag, year_struct=year_struct, hour=h, eff=c1[1], se=c1[2], p=c1[3],
      n=nobs(m), stringsAsFactors=FALSE)
  }
  do.call(rbind, rows)
}

# ============================================================================
# Pooled hourly regressions
# ============================================================================
pooled_specs <- list(
  list(tag="ID15 IDA hourly", response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 DA hourly",  response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="DA15 DA hourly",  response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA hourly", response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31")
)
pooled_rows <- do.call(rbind, lapply(pooled_specs, function(s)
  run_pooled(s$response, s$pre_lo, s$post_lo, s$post_hi, s$tag)))
write.csv(pooled_rows, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_hourly.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_hourly.csv\n")

# ============================================================================
# Per-hour regressions (Reguant-style, headline year-struct per reform)
# ============================================================================
ph_specs <- list(
  list(tag="ID15 IDA per-hour", response="ida_price_eur", year_struct="pooled",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 DA per-hour",  response="da_price_eur",  year_struct="pooled",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="DA15 DA per-hour",  response="da_price_eur",  year_struct="peryear",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA per-hour", response="ida_price_eur", year_struct="peryear",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31")
)
ph_rows <- do.call(rbind, lapply(ph_specs, function(s)
  run_per_hour(s$response, s$pre_lo, s$post_lo, s$post_hi, s$tag, s$year_struct)))
write.csv(ph_rows, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_per_hour.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_per_hour.csv\n")
