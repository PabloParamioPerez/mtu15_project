# STATUS: ALIVE
# LAST-AUDIT: 2026-06-08
# FEEDS: thesis/paper/thesis.tex --- OLS robustness for BSTS price effects.
#        Daily price ~ POST + base controls + seasonal FE + year-by-renewable.
#        Newey-West HAC SE for daily autocorrelation.
#        Per-IDA-session (s1, s2, s3) prices added 2026-06-08.
#
# Headline year-by-renewable spec differs by reform:
#   - ID15 headline: Spec 4 (2024+2025 pooled — 2025 pre is Jan-Mar winter only).
#   - DA15 headline: Spec 3 (2024 and 2025 separate — DA15 pre extends to Sep 2025).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_full_controls.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel_q  <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel_q$d  <- as.Date(panel_q$d);  panel_q  <- panel_q[order(panel_q$d), ]
panel_ps <- read_parquet(file.path(repo, "data/derived/panels/bsts_per_session_panel.parquet"))
panel_ps$d <- as.Date(panel_ps$d); panel_ps <- panel_ps[order(panel_ps$d), ]

run_ols <- function(panel, response, pre_start, post_start, post_end, tag) {
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
    cat(sprintf("  %-50s eff=%+7.2f  SE=%5.2f  p=%.3f\n",
                spec_name, c1[1], c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name, eff=c1[1], se=c1[2], p=c1[3],
               n=nobs(m), stringsAsFactors=FALSE)
  }

  rows <- list()
  cat(sprintf("\n=== %s (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       month + dow, data=sub),
                    "Spec 1: base + month FE + DOW FE")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       t + month + dow, data=sub),
                    "Spec 2: + linear time trend")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + month + dow, data=sub),
                    "Spec 3: + year-by-renewable (per year)")
  rows[[4]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + month + dow, data=sub),
                    "Spec 4: + year-by-renewable (2024+ pooled)")
  rows[[5]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2023 + solar_gwh:y2023 +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + I(t^2) + month + dow, data=sub),
                    "Spec 5: + quadratic time trend")
  do.call(rbind, rows)
}

specs <- list(
  # Headline aggregate prices (quantities panel)
  list(tag="ID15 IDA",            panel=panel_q,  response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 DA cross",       panel=panel_q,  response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="DA15 DA",              panel=panel_q,  response="da_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA cross",      panel=panel_q,  response="ida_price_eur",
       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31"),
  # Per-IDA-session prices (per-session panel; pre starts at European IDA reform)
  list(tag="ID15 IDA-S1",         panel=panel_ps, response="ida_price_eur_s1",
       pre_lo="2024-06-14", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 IDA-S2",         panel=panel_ps, response="ida_price_eur_s2",
       pre_lo="2024-06-14", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="ID15 IDA-S3",         panel=panel_ps, response="ida_price_eur_s3",
       pre_lo="2024-06-14", post_lo="2025-03-19", post_hi="2025-04-27"),
  list(tag="DA15 IDA-S1 cross",   panel=panel_ps, response="ida_price_eur_s1",
       pre_lo="2024-06-14", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA-S2 cross",   panel=panel_ps, response="ida_price_eur_s2",
       pre_lo="2024-06-14", post_lo="2025-10-01", post_hi="2025-12-31"),
  list(tag="DA15 IDA-S3 cross",   panel=panel_ps, response="ida_price_eur_s3",
       pre_lo="2024-06-14", post_lo="2025-10-01", post_hi="2025-12-31")
)
all_rows <- do.call(rbind, lapply(specs, function(s)
  run_ols(s$panel, s$response, s$pre_lo, s$post_lo, s$post_hi, s$tag)))
write.csv(all_rows, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_full_controls.csv"),
          row.names=FALSE)
cat("\nWrote ols_price_full_controls.csv\n")
