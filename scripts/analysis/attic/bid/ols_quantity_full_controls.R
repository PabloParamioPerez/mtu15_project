# STATUS: ALIVE
# LAST-AUDIT: 2026-06-08
# FEEDS: thesis/paper/thesis.tex --- OLS quantity effects (PDBC = DA cleared,
#        PIBCI = IDA cleared) at ID15 and DA15. Daily per-tech GWh from the
#        quantities panel. Same five-spec ladder as ols_price_full_controls.R.
#
# Headline year-by-renewable spec:
#   - ID15 headline = Spec 4 (2024+2025 pooled).
#   - DA15 headline = Spec 3 (per-year separate).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_quantity_full_controls.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

run_ols <- function(response, pre_start, post_start, post_end, tag) {
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

TECHS <- c("ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind")
specs <- list()
i <- 1
for (mkt in c("da", "ida")) {
  for (tech in TECHS) {
    resp <- sprintf("q_%s_gwh_%s", tech, mkt)
    specs[[i]] <- list(tag=sprintf("ID15 %s (%s)", toupper(mkt), tech), response=resp,
                       pre_lo="2022-01-01", post_lo="2025-03-19", post_hi="2025-04-27")
    i <- i + 1
    specs[[i]] <- list(tag=sprintf("DA15 %s (%s)", toupper(mkt), tech), response=resp,
                       pre_lo="2022-01-01", post_lo="2025-10-01", post_hi="2025-12-31")
    i <- i + 1
  }
}
all_rows <- do.call(rbind, lapply(specs, function(s)
  run_ols(s$response, s$pre_lo, s$post_lo, s$post_hi, s$tag)))
write.csv(all_rows, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_quantity_full_controls.csv"),
          row.names=FALSE)
cat("\nWrote ols_quantity_full_controls.csv\n")
