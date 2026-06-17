# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- ID15 OLS hourly price effect
#        at progressively longer post-windows. Mirror of ols_id15_longpost.R
#        but with hour-of-day FE, on the hourly panel.
#
# Specs: base / per-year / 2024+25 pooled. Newey-West HAC lag=24*7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_id15_longpost_hourly.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

run_ols <- function(response, pre_start, post_start, post_end, tag, window_lbl) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
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
  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, window=window_lbl, spec=spec_name,
               eff=c1[1], se=c1[2], p=c1[3], n=nobs(m), n_post=sum(sub$post),
               stringsAsFactors=FALSE)
  }
  cat(sprintf("\n=== %s @ %s ===\n", tag, window_lbl))
  rows <- list()
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- base (hour + month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (2024+25 pooled)")
  do.call(rbind, rows)
}

windows <- list(
  list(lbl="40d",  post_hi="2025-04-27"),
  list(lbl="90d",  post_hi="2025-06-16"),
  list(lbl="180d", post_hi="2025-09-14"),
  list(lbl="196d", post_hi="2025-09-30")
)
specs <- list(
  list(tag="ID15 IDA",       response="ida_price_eur"),
  list(tag="ID15 DA cross",  response="da_price_eur")
)
all_rows <- list()
for (s in specs) for (w in windows) {
  all_rows[[length(all_rows)+1]] <-
    run_ols(s$response, "2022-01-01", "2025-03-19", w$post_hi, s$tag, w$lbl)
}
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo,
  "results/regressions/bid/mtu15_critical_flat/ols_id15_longpost_hourly.csv"),
  row.names=FALSE)
cat("\nWrote ols_id15_longpost_hourly.csv\n")
