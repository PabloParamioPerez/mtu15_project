# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- per-session DA-IDA wedge OLS
#        at HOURLY frequency. Mirrors the headline 3-spec ladder (base /
#        per-year / 2024+25 pooled). Newey-West HAC lag=24*7.
#
# Outcomes: wedge_sX_h = da_price_eur - ida_price_eur_sX, hourly, X in {1,2,3}
# Pre-window: 2024-06-14 onward (post-European-IDA reform 3-session regime).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_wedge_per_session_hourly.csv

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
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2025 <- as.integer(sub$year == 2025)
  sub$y2025p<- as.integer(sub$year >= 2025)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
               stringsAsFactors=FALSE)
  }

  rows <- list()
  cat(sprintf("\n=== %s hourly (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- base (hour + month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2025p + solar_gwh:y2025p +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (2024+25 pooled)")
  rows[[4]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- year-by-renew (per year), NO trend")
  rows[[5]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2025p + solar_gwh:y2025p +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- year-by-renew (2024+25 pooled), NO trend")
  do.call(rbind, rows)
}

specs <- list(
  # ID15: pre 2024-06-14 -> 2025-03-18; post 40d to 2025-04-27
  list(tag="ID15 wedge_s1_h", resp="wedge_s1_h", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  list(tag="ID15 wedge_s2_h", resp="wedge_s2_h", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  list(tag="ID15 wedge_s3_h", resp="wedge_s3_h", pre="2024-06-14", post="2025-03-19", end="2025-04-27"),
  # DA15: pre 2024-06-14 -> 2025-09-30; post 92d to 2025-12-31
  list(tag="DA15 wedge_s1_h", resp="wedge_s1_h", pre="2024-06-14", post="2025-10-01", end="2025-12-31"),
  list(tag="DA15 wedge_s2_h", resp="wedge_s2_h", pre="2024-06-14", post="2025-10-01", end="2025-12-31"),
  list(tag="DA15 wedge_s3_h", resp="wedge_s3_h", pre="2024-06-14", post="2025-10-01", end="2025-12-31")
)

all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <- run_ols(s$resp, s$pre, s$post, s$end, s$tag)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_wedge_per_session_hourly.csv"),
          row.names=FALSE)
cat("\nWrote ols_wedge_per_session_hourly.csv\n")
