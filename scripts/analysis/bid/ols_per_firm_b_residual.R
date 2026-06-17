# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: merged "market power and imbalance" slide of the June 2026 deck.
#
# Per-(reform x market x firm) before/after regression on log(b_residual)
# using the SAME headline controls as the price spec --- but NOT DiD,
# just a single-difference with seasonality + weather absorbed.
#
# Spec (single-difference, per cell):
#   log b_{f,t}  =  theta * POST  +  month FE + DOW FE + gas
#                  + wind + solar + (year x renewable interactions)
#                  + epsilon
# NO linear time trend.
#
# Headline year-by-renewable:
#   ID15: 2024+25 pooled (analogous to the price-side headline)
#   DA15: per-year separate
#
# Newey-West HAC lag = 7 (the per-firm b is aggregated to a daily mean).
#
# OUT: results/regressions/bid/mtu15_critical_flat/ols_per_firm_b_residual.csv

suppressPackageStartupMessages({ library(arrow); library(lmtest); library(sandwich) })
.cmdargs <- commandArgs(trailingOnly = FALSE)
.thisfile <- sub("^--file=", "", .cmdargs[grep("^--file=", .cmdargs)])
repo <- normalizePath(file.path(dirname(.thisfile), "..", "..", ".."))
# Per-firm residual demand slope panel (one row per d, period, market, focal_firm)
b_panel <- read_parquet(file.path(repo, "data/derived/panels/per_firm_residual_demand_slope.parquet"))
b_panel$d <- as.Date(b_panel$d)
# Map period -> clock_hour. The window label tells us the granularity
# (pre = 60-min, period = clock_hour + 1; post = 15-min, period = 4*clock_hour + q).
is_pre_60 <- grepl("^pre_", b_panel$window)
b_panel$clock_hour <- ifelse(is_pre_60,
                              b_panel$period - 1,
                              as.integer((b_panel$period - 1) %/% 4))
# Aggregate to hourly mean per (firm, market, d, clock_hour)
b_hourly <- aggregate(b_residual_mw_per_eur ~ d + clock_hour + focal_firm + market,
                       data = b_panel, FUN = function(x) mean(x, na.rm = TRUE))
names(b_hourly)[names(b_hourly) == "b_residual_mw_per_eur"] <- "b"
b_hourly$log_b <- log(pmax(b_hourly$b, 1e-6))

# Daily covariates (same as the price-side panel)
covars <- read_parquet(file.path(repo, "data/derived/panels/bsts_daily_panel.parquet"))
covars$d <- as.Date(covars$d)
covars <- covars[, c("d", "wind_gwh", "solar_gwh", "gas_eur")]

WINDOWS <- list(
  list(reform="ID15", post="2025-03-19", end="2025-04-27",
       pre_start="2022-01-01", spec="pooled_24_25"),
  list(reform="DA15", post="2025-10-01", end="2025-12-31",
       pre_start="2022-01-01", spec="per_year")
)
FIRMS  <- c("IB", "GE", "GN", "HC")
MARKETS <- c("DA", "IDA")

run_cell <- function(reform, market, firm, post_start, pre_start, post_end, spec) {
  cutover <- as.Date(post_start); pe <- as.Date(post_end); ps <- as.Date(pre_start)
  sub <- merge(b_hourly[b_hourly$focal_firm == firm & b_hourly$market == market, ],
                covars, by = "d", all.x = TRUE)
  sub <- sub[sub$d >= ps & sub$d <= pe, ]
  sub <- sub[complete.cases(sub[, c("log_b", "wind_gwh", "solar_gwh", "gas_eur")]), ]
  if (nrow(sub) < 60 || sum(sub$d >= cutover) < 10) return(NULL)
  sub$post  <- as.integer(sub$d >= cutover)
  sub$month <- factor(format(sub$d, "%m"))
  sub$dow   <- factor(format(sub$d, "%u"))
  sub$hour_f<- factor(sub$clock_hour)
  sub$year  <- as.integer(format(sub$d, "%Y"))
  sub$y2023 <- as.integer(sub$year == 2023)
  sub$y2024 <- as.integer(sub$year == 2024)
  sub$y2025 <- as.integer(sub$year == 2025)
  sub$y2024p<- as.integer(sub$year >= 2024)

  if (spec == "pooled_24_25") {
    f <- as.formula(paste("log_b ~ post + wind_gwh + solar_gwh + gas_eur +",
      "wind_gwh:y2024p + solar_gwh:y2024p + hour_f + month + dow"))
  } else {
    f <- as.formula(paste("log_b ~ post + wind_gwh + solar_gwh + gas_eur +",
      "wind_gwh:y2023 + solar_gwh:y2023 +",
      "wind_gwh:y2024 + solar_gwh:y2024 +",
      "wind_gwh:y2025 + solar_gwh:y2025 + hour_f + month + dow"))
  }
  m <- lm(f, data = sub)
  s <- coeftest(m, vcov = NeweyWest(m, lag = 24*7, prewhite = FALSE))
  c1 <- s["post", c("Estimate", "Std. Error", "Pr(>|t|)")]
  # log -> approximate %-change at 100 * (exp(theta) - 1)
  pct <- 100 * (exp(c1[1]) - 1)
  cat(sprintf("  %-6s %-3s %-3s eff_log=%+6.3f (%%=%+7.1f)  SE=%.3f  p=%.3f  n=%d  n_post=%d\n",
              reform, market, firm, c1[1], pct, c1[2], c1[3], nobs(m), sum(sub$post)))
  data.frame(reform=reform, market=market, firm=firm,
             theta_log=c1[1], pct=pct, se=c1[2], p=c1[3], n=nobs(m),
             stringsAsFactors=FALSE)
}

rows <- list()
for (w in WINDOWS) {
  for (mkt in MARKETS) {
    for (f in FIRMS) {
      r <- run_cell(w$reform, mkt, f, w$post, w$pre_start, w$end, w$spec)
      if (!is.null(r)) rows[[length(rows)+1]] <- r
    }
  }
}
out <- do.call(rbind, rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/ols_per_firm_b_residual.csv"),
          row.names=FALSE)
cat("\nWrote ols_per_firm_b_residual.csv\n")
