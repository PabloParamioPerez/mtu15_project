# STATUS: ALIVE
# LAST-AUDIT: 2026-06-15
# FEEDS: thesis Table 3 (price effect by spec) -- daily-average analogue of the
#        hourly quadratic-renewable spec (ols_price_hourly_quadratic.R), so the
#        quadratic-renewable row appears in the OLS-daily block too, not only
#        hourly. Spec: post + wind + solar + wind^2 + solar^2 + gas +
#        year(>=2024)-by-renewable on linear & quadratic terms + month + DOW FE.
#        No linear time trend. Newey-West HAC SE lag = 7 (daily).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_price_daily_quadratic.csv

suppressPackageStartupMessages({library(arrow); library(sandwich); library(lmtest)})

args <- commandArgs(trailingOnly = FALSE); sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else "scripts/analysis/bid/ols_price_daily_quadratic.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
panel$wind_gwh2  <- panel$wind_gwh^2
panel$solar_gwh2 <- panel$solar_gwh^2

prep <- function(sub, cutover) {
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$post  <- as.integer(sub$d >= cutover)
  sub$y2024p <- as.integer(sub$year >= 2024)
  sub
}

run <- function(response, pre_start, post_start, post_end, tag) {
  pre_lo <- as.Date(pre_start); cutover <- as.Date(post_start); pe <- as.Date(post_end)
  sub <- panel[panel$d >= pre_lo & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, "wind_gwh", "solar_gwh", "gas_eur")]), ]
  sub <- prep(sub, cutover)
  m <- lm(get(response) ~ post + wind_gwh + solar_gwh + wind_gwh2 + solar_gwh2 + gas_eur +
            wind_gwh:y2024p + solar_gwh:y2024p + wind_gwh2:y2024p + solar_gwh2:y2024p +
            month + dow, data = sub)
  s <- coeftest(m, vcov = NeweyWest(m, lag = 7, prewhite = FALSE))
  c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
  cat(sprintf("%-18s eff=%+7.2f  SE=%5.2f  p=%.4f  n=%d\n", tag, c1[1], c1[2], c1[3], nobs(m)))
  data.frame(tag = tag, spec = "D: quadratic renewable x year, daily, no trend",
             eff = c1[1], se = c1[2], p = c1[3], n = nobs(m), stringsAsFactors = FALSE)
}

SPECS <- list(
  list("ID15 IDA daily", "ida_price_eur", "2022-01-01", "2025-03-19", "2025-04-27"),
  list("ID15 DA daily",  "da_price_eur",  "2022-01-01", "2025-03-19", "2025-04-27"),
  list("DA15 DA daily",  "da_price_eur",  "2022-01-01", "2025-10-01", "2025-12-31"),
  list("DA15 IDA daily", "ida_price_eur", "2022-01-01", "2025-10-01", "2025-12-31"))

out <- do.call(rbind, lapply(SPECS, function(s) run(s[[2]], s[[3]], s[[4]], s[[5]], s[[1]])))
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_price_daily_quadratic.csv"),
          row.names = FALSE)
cat("\nWrote ols_price_daily_quadratic.csv\n")
