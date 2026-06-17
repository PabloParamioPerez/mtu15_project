# STATUS: ALIVE
# LAST-AUDIT: 2026-06-08
# FEEDS: thesis/presentations/.../slides.tex --- ID15 OLS price effect at
#        progressively longer post-windows (40d, 90d, 180d, 196d).
#        Mirrors bsts_id15_price_longpost.R.
# Specs: base / per-year year-by-renew / 2024+25 pooled year-by-renew.
# Newey-West HAC lag=7. Same long pre (2022-01-01) as headline OLS.
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_id15_longpost.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

run_ols <- function(response, pre_start, post_start, post_end, tag, window_lbl) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$post  <- as.integer(sub$d >= cutover)
  sub$t     <- as.integer(sub$d - min(sub$d))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2025 <- as.integer(sub$year == 2025)
  sub$y2024p<- as.integer(sub$year >= 2024)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f  n_post=%d\n",
                spec_name, c1[1], c1[2], c1[3], sum(sub$post)))
    data.frame(tag=tag, window=window_lbl, spec=spec_name,
               eff=c1[1], se=c1[2], p=c1[3], n=nobs(m), n_post=sum(sub$post),
               stringsAsFactors=FALSE)
  }
  cat(sprintf("\n=== %s @ %s ===\n", tag, window_lbl))
  rows <- list()
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       month + dow, data=sub), "base (month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + month + dow, data=sub),
                    "+ year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + month + dow, data=sub),
                    "+ year-by-renew (2024+25 pooled)")
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
for (s in specs) {
  for (w in windows) {
    all_rows[[length(all_rows)+1]] <-
      run_ols(s$response, "2022-01-01", "2025-03-19", w$post_hi, s$tag, w$lbl)
  }
}
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo,
  "results/regressions/bid/mtu15_critical_flat/ols_id15_longpost.csv"),
  row.names=FALSE)
cat("\nWrote ols_id15_longpost.csv\n")
