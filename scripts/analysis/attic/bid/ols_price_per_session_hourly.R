# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- ID15 IDA price effect decomposed
#        by IDA session (S1/S2/S3) at HOURLY frequency, using the headline
#        OLS hourly + year-by-renewable (2024+25 pooled) spec. Newey-West HAC
#        lag=24*7.
#
# Outcomes: ida_price_eur_s{1,2,3} at ID15 (cutover 2025-03-19, 40d post).
# Pre-window: 2024-06-14 onward (post-European-IDA reform 3-session regime).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_per_session_hourly.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_per_session_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

run_ols <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$hour_f<- factor(sub$hour)
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2025p<- as.integer(sub$year >= 2025)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
               stringsAsFactors=FALSE)
  }

  cat(sprintf("\n=== %s hourly (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  m <- lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
           wind_gwh:y2025p + solar_gwh:y2025p +
           t + hour_f + month + dow, data=sub)
  fmt(m, "OLS hourly --- + year-by-renew (2024+25 pooled)")
}

specs <- list(
  list(tag="ID15 IDA S1", resp="ida_price_eur_s1", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  list(tag="ID15 IDA S2", resp="ida_price_eur_s2", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  list(tag="ID15 IDA S3", resp="ida_price_eur_s3", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  list(tag="DA15 IDA S1", resp="ida_price_eur_s1", pre="2024-06-14", post="2025-10-01", end="2025-12-31"),
  list(tag="DA15 IDA S2", resp="ida_price_eur_s2", pre="2024-06-14", post="2025-10-01", end="2025-12-31"),
  list(tag="DA15 IDA S3", resp="ida_price_eur_s3", pre="2024-06-14", post="2025-10-01", end="2025-12-31")
)
all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <-
  run_ols(s$resp, s$pre, s$post, s$end, s$tag)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_per_session_hourly.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_per_session_hourly.csv\n")
