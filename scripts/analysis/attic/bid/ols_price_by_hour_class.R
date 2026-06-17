# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- ID15 + DA15 price effect on
#        DA and IDA, broken out by hour class:
#          morning_ramp = 5-8     evening_ramp = 16-22
#          midday       = 11-14   flat         = 1-3
#        Headline OLS hourly + year-by-renewable (2024+25 pooled) spec.
#        Newey-West HAC lag=24*7.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_by_hour_class.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

HCLASS <- list(
  morning_ramp = c(5, 6, 7, 8),
  evening_ramp = c(16, 17, 18, 19, 20, 21, 22),
  midday       = c(11, 12, 13, 14),
  flat         = c(1, 2, 3)
)

run_ols <- function(response, pre_start, post_start, post_end, hour_set, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe & panel$hour %in% hour_set, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$hour_f<- factor(sub$hour)
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024p<- as.integer(sub$year >= 2024)
  m <- lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
           wind_gwh:y2023 + solar_gwh:y2023 +
           wind_gwh:y2024p + solar_gwh:y2024p +
           t + hour_f + month + dow, data=sub)
  s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
  c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
  cat(sprintf("  %-30s eff=%+7.2f  SE=%5.2f  p=%.3f  n=%d\n",
              tag, c1[1], c1[2], c1[3], nobs(m)))
  data.frame(tag=tag, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
             stringsAsFactors=FALSE)
}

REFORMS <- list(
  list(name="ID15 IDA", resp="ida_price_eur", pre="2022-01-01", post="2025-03-19", end="2025-04-27"),
  list(name="ID15 DA",  resp="da_price_eur",  pre="2022-01-01", post="2025-03-19", end="2025-04-27"),
  list(name="DA15 DA",  resp="da_price_eur",  pre="2022-01-01", post="2025-10-01", end="2025-12-31"),
  list(name="DA15 IDA", resp="ida_price_eur", pre="2022-01-01", post="2025-10-01", end="2025-12-31")
)

all_rows <- list()
for (r in REFORMS) {
  cat(sprintf("\n=== %s by hour class ===\n", r$name))
  for (hc in names(HCLASS)) {
    tag <- sprintf("%s --- %s", r$name, hc)
    all_rows[[length(all_rows)+1]] <-
      run_ols(r$resp, r$pre, r$post, r$end, HCLASS[[hc]], tag)
  }
}
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_by_hour_class.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_by_hour_class.csv\n")
