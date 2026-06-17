# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: thesis/presentations/.../slides.tex --- HOURLY OLS quantity effects
#        (PDBC = DA cleared, PIBCI = IDA cleared) at ID15 and DA15. Per-tech
#        hourly MWh; coefficient reported in MWh per hour AND converted to
#        GWh/day-equivalent (×24/1000) for direct comparison with the daily
#        spec. Newey-West HAC lag = 24×7.
#
# Headline year-by-renewable spec:
#   - ID15 headline = +year-by-renewable (2024+25 pooled).
#   - DA15 headline = +year-by-renewable (per year).
#
# Hourly panel covers ccgt / hydro / nuclear / solar / wind (no hydro_pump).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_quantity_full_controls_hourly.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_hourly_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d, panel$hour), ]
panel$wind_gwh  <- panel$wind_mwh  / 1000
panel$solar_gwh <- panel$solar_mwh / 1000

run_ols <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub$year   <- as.integer(format(sub$d, "%Y"))
  sub$month  <- factor(format(sub$d, "%m"))
  sub$dow    <- factor(format(sub$d, "%u"))
  sub$hour_f <- factor(sub$hour)
  sub$post   <- as.integer(sub$d >= cutover)
  sub$t      <- as.integer(sub$d - min(sub$d))
  sub$y2023  <- as.integer(sub$year == 2023)
  sub$y2024  <- as.integer(sub$year == 2024)
  sub$y2025  <- as.integer(sub$year == 2025)
  sub$y2024p <- as.integer(sub$year >= 2024)
  sub$y2025p <- as.integer(sub$year >= 2025)

  fmt <- function(m, spec_name) {
    s <- coeftest(m, vcov=NeweyWest(m, lag=24*7, prewhite=FALSE))
    c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
    # eff in MWh/h; convert to GWh/day-equivalent
    eff_gwh_d <- c1[1] * 24 / 1000
    cat(sprintf("  %-50s eff=%+8.1f MWh/h (=%+5.2f GWh/d-eq)  SE=%5.1f  p=%.3f\n",
                spec_name, c1[1], eff_gwh_d, c1[2], c1[3]))
    data.frame(tag=tag, spec=spec_name,
               eff_mwh_h=c1[1], eff_gwh_d_eq=eff_gwh_d,
               se=c1[2], p=c1[3], n=nobs(m), stringsAsFactors=FALSE)
  }

  rows <- list()
  cat(sprintf("\n=== %s hourly (n=%d, n_post=%d) ===\n", tag, nrow(sub), sum(sub$post)))
  rows[[1]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       hour_f + month + dow, data=sub),
                    "OLS hourly --- base (hour + month + DOW)")
  rows[[2]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2024 + solar_gwh:y2024 +
                       wind_gwh:y2025 + solar_gwh:y2025 +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (per year)")
  rows[[3]] <- fmt(lm(get(response) ~ post + wind_gwh + solar_gwh + gas_eur +
                       wind_gwh:y2024p + solar_gwh:y2024p +
                       t + hour_f + month + dow, data=sub),
                    "OLS hourly --- + year-by-renew (2024+25 pooled)")
  do.call(rbind, rows)
}

TECHS <- c("ccgt", "hydro", "nuclear", "solar", "wind")
specs <- list()
i <- 1
for (tech in TECHS) {
  # ID15 IDA own-venue (granular leg at IDA reform)
  specs[[i]] <- list(tag=sprintf("ID15 IDA (%s)", tech),
                      resp=sprintf("q_%s_mwh_ida", tech),
                      pre="2022-01-01", post="2025-03-19", end="2025-04-27"); i <- i+1
  # ID15 DA cross-market
  specs[[i]] <- list(tag=sprintf("ID15 DA (%s)", tech),
                      resp=sprintf("q_%s_mwh_da", tech),
                      pre="2022-01-01", post="2025-03-19", end="2025-04-27"); i <- i+1
  # DA15 DA own-venue (granular leg at DA reform)
  specs[[i]] <- list(tag=sprintf("DA15 DA (%s)", tech),
                      resp=sprintf("q_%s_mwh_da", tech),
                      pre="2022-01-01", post="2025-10-01", end="2025-12-31"); i <- i+1
  # DA15 IDA cross-market
  specs[[i]] <- list(tag=sprintf("DA15 IDA (%s)", tech),
                      resp=sprintf("q_%s_mwh_ida", tech),
                      pre="2022-01-01", post="2025-10-01", end="2025-12-31"); i <- i+1
}

all_rows <- list()
for (s in specs) all_rows[[length(all_rows)+1]] <- run_ols(s$resp, s$pre, s$post, s$end, s$tag)
out <- do.call(rbind, all_rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_quantity_full_controls_hourly.csv"),
          row.names=FALSE)
cat("\nWrote ols_quantity_full_controls_hourly.csv\n")
