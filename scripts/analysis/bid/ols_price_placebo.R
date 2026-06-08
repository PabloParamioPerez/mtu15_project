# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- Same OLS battery as the
#        headline prices table but with placebo cutovers (one year earlier),
#        matching post-window lengths.
#
# ID15 placebo: cutover 2024-03-19, post 2024-04-27 (40 d)
# DA15 placebo: cutover 2024-10-01, post 2024-12-31 (92 d)
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_placebo.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel_q  <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel_q$d <- as.Date(panel_q$d); panel_q <- panel_q[order(panel_q$d), ]
panel_h  <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel_h$d <- as.Date(panel_h$d); panel_h <- panel_h[order(panel_h$d, panel_h$hour), ]
panel_h$wind_gwh  <- panel_h$wind_mwh  / 1000
panel_h$solar_gwh <- panel_h$solar_mwh / 1000

run_ols_daily <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel_q[panel_q$d >= pre_lo & panel_q$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2024p<- as.integer(sub$year >= 2024)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
               stringsAsFactors=FALSE)
  }

  rows <- list()
  cat(sprintf("\n=== %s daily (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       month + dow, data=sub), "OLS daily --- base (month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       t + month + dow, data=sub),
                    "OLS daily --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + month + dow, data=sub),
                    "OLS daily --- + year-by-renew (2024+25 pooled)")
  do.call(rbind, rows)
}

run_ols_hourly <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel_h[panel_h$d >= pre_lo & panel_h$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$hour_f<- factor(sub$hour)
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2024p<- as.integer(sub$year >= 2024)
  cat(sprintf("\n=== %s hourly (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n", spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3], n=nobs(m),
               stringsAsFactors=FALSE)
  }
  rows <- list()
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- base (hour + month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (2024+25 pooled)")
  do.call(rbind, rows)
}

specs <- list(
  list(tag="ID15 IDA (plb)",       resp="ida_price_eur", pre="2022-01-01", post="2024-03-19", end="2024-04-27"),
  list(tag="ID15 DA cross (plb)",  resp="da_price_eur",  pre="2022-01-01", post="2024-03-19", end="2024-04-27"),
  list(tag="DA15 DA (plb)",         resp="da_price_eur",  pre="2022-01-01", post="2024-10-01", end="2024-12-31"),
  list(tag="DA15 IDA cross (plb)", resp="ida_price_eur", pre="2022-01-01", post="2024-10-01", end="2024-12-31")
)

all_rows <- list()
for (s in specs) {
  all_rows[[length(all_rows)+1]] <- run_ols_daily(s$resp, s$pre, s$post, s$end, s$tag)
  all_rows[[length(all_rows)+1]] <- run_ols_hourly(s$resp, s$pre, s$post, s$end, s$tag)
}
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_placebo.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_placebo.csv\n")
