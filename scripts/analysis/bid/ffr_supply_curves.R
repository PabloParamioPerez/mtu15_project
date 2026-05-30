# STATUS: ALIVE
# LAST-AUDIT: 2026-05-30
# FEEDS: results/regressions/bid/ffr_supply_curves/  (curve-level
#        predicted-vs-actual per (reform, market, session, hour-class))
#
# Functional Factor Regression (Otto & Winter, 2025; ffr R package) on
# aggregate sell-side supply curves. For each (reform, market, session,
# hour-class):
#   - Y_t : 201-point supply curve at hour-class on date t
#   - Functional regressors: hourly wind curve (24 vals), hourly solar
#     curve (24 vals).
#   - Scalar regressor: daily gas price (EUR).
#   - Fit on pre-reform window. Predict on post-reform window.
#   - Save predicted vs actual curves for downstream visualisation.
#
# Hour-classes follow the memo: critical={5..8,16..22}, midday={11..14},
# flat={1,2,3}. Aggregation across hours within class is by simple mean
# of the cumulative-MW supply curves.
#
# This is a STANDALONE analysis; results are NOT wired into the memo.

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(tidyr); library(ffr)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/ffr_supply_curves.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

supply_path  <- file.path(repo, "data/derived/panels/supply_curves_panel.parquet")
weather_path <- file.path(repo, "data/derived/panels/hourly_weather_panel.parquet")
out_dir      <- file.path(repo, "results/regressions/bid/ffr_supply_curves")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# Configuration --------------------------------------------------------------
GRID_PRICES <- 0:200          # 201 grid points, matches build_supply_curve_panel.py
HOURS <- list(
  critical = c(5L, 6L, 7L, 8L, 16L, 17L, 18L, 19L, 20L, 21L, 22L),
  midday   = c(11L, 12L, 13L, 14L),
  flat     = c(1L, 2L, 3L)
)
WINDOWS <- list(
  list(reform = "ID15", side = "real",    pre = c("2024-06-14", "2025-03-18"),
       post = c("2025-03-19", "2025-04-27")),
  list(reform = "ID15", side = "placebo", pre = c("2023-06-14", "2024-03-18"),
       post = c("2024-03-19", "2024-04-27")),
  list(reform = "DA15", side = "real",    pre = c("2025-04-28", "2025-09-30"),
       post = c("2025-10-01", "2025-11-09")),
  list(reform = "DA15", side = "placebo", pre = c("2024-04-28", "2024-09-30"),
       post = c("2024-10-01", "2024-11-09"))
)

# Per-(reform, market) which sessions to run
MARKET_SESSIONS <- list(
  list(market = "DA",  session = NA_integer_),
  list(market = "IDA", session = 1L),
  list(market = "IDA", session = 2L),
  list(market = "IDA", session = 3L)
)

# Loaders --------------------------------------------------------------------
load_supply <- function() {
  s <- read_parquet(supply_path)
  s$d <- as.Date(s$d)
  s
}
load_weather <- function() {
  w <- read_parquet(weather_path)
  w$d <- as.Date(w$d)
  # Solar at night: ENTSO-E omits zeros -> NA. Fill with 0.
  solar_cols <- grep("^solar_h", names(w), value = TRUE)
  w[solar_cols] <- lapply(w[solar_cols], function(x) ifelse(is.na(x), 0, x))
  w
}

aggregate_hour_class <- function(supply, market_, session_, hours_) {
  # Filter to (market, session, hours in class); average curves across hours
  # within each date.
  if (is.na(session_)) {
    sub <- supply[supply$market == market_ & is.na(supply$session)
                  & supply$clock_hour %in% hours_, ]
  } else {
    sub <- supply[supply$market == market_ & !is.na(supply$session)
                  & supply$session == session_
                  & supply$clock_hour %in% hours_, ]
  }
  grid_cols <- grep("^Q_", names(sub), value = TRUE)
  agg <- sub |>
    group_by(d) |>
    summarise(across(all_of(grid_cols), mean, na.rm = TRUE),
              n_hours = n(), .groups = "drop")
  # Require at least 75% of class hours present
  min_h <- ceiling(0.75 * length(hours_))
  agg <- agg[agg$n_hours >= min_h, ]
  agg$n_hours <- NULL
  agg
}

build_data_list <- function(supply_agg, weather, dates) {
  # Restrict and align by date; build matrices.
  s <- supply_agg[supply_agg$d %in% dates, ]
  w <- weather[weather$d %in% dates, ]
  common <- intersect(s$d, w$d)
  s <- s[s$d %in% common, ]; s <- s[order(s$d), ]
  w <- w[w$d %in% common, ]; w <- w[order(w$d), ]
  # Drop rows with any NA in regressors or response
  grid_cols  <- grep("^Q_", names(s), value = TRUE)
  wind_cols  <- grep("^wind_h",  names(w), value = TRUE)
  solar_cols <- grep("^solar_h", names(w), value = TRUE)
  complete_y <- complete.cases(s[, grid_cols])
  complete_x <- complete.cases(w[, c(wind_cols, solar_cols, "gas_eur")])
  keep <- complete_y & complete_x
  s <- s[keep, ]; w <- w[keep, ]
  list(
    dates  = s$d,
    Supply = as.matrix(s[, grid_cols]),
    Wind   = as.matrix(w[, wind_cols]),
    Solar  = as.matrix(w[, solar_cols]),
    Gas    = w$gas_eur
  )
}

# Main loop ------------------------------------------------------------------
cat("Loading panels...\n")
supply <- load_supply(); weather <- load_weather()
cat(sprintf("  supply: %d rows | weather: %d days\n", nrow(supply), nrow(weather)))

all_results <- list()
all_curves <- list()
fit_id <- 0L

for (ms in MARKET_SESSIONS) for (hc_name in names(HOURS)) for (cfg in WINDOWS) {
  fit_id <- fit_id + 1L
  market <- ms$market; session <- ms$session
  hours  <- HOURS[[hc_name]]
  reform <- cfg$reform; side <- cfg$side
  pre_dates  <- seq(as.Date(cfg$pre[1]),  as.Date(cfg$pre[2]),  by = "day")
  post_dates <- seq(as.Date(cfg$post[1]), as.Date(cfg$post[2]), by = "day")

  tag <- sprintf("%s_%s_%s_S%s_%s",
                 reform, side, market,
                 ifelse(is.na(session), "NA", as.character(session)),
                 hc_name)
  cat(sprintf("[%3d] %s ... ", fit_id, tag))

  agg <- aggregate_hour_class(supply, market, session, hours)
  if (nrow(agg) == 0) { cat("skip (no aggregated rows)\n"); next }

  train <- build_data_list(agg, weather, pre_dates)
  test  <- build_data_list(agg, weather, post_dates)
  if (nrow(train$Supply) < 60 || nrow(test$Supply) < 5) {
    cat(sprintf("skip (n_pre=%d, n_post=%d)\n",
                nrow(train$Supply), nrow(test$Supply))); next
  }

  data_train <- list(Supply = train$Supply,
                     Wind   = train$Wind,
                     Solar  = train$Solar,
                     Gas    = train$Gas)

  # Pick K via fed (eigenvalue difference test) with default gamma
  k_est <- tryCatch(
    fed(Supply ~ Wind + Solar + Gas, data_train, gamma = 1, plot = FALSE),
    error = function(e) NULL)
  if (is.null(k_est)) { cat("fed err\n"); next }
  K_use <- pmax(k_est$K, 1)  # at least 1 factor per functional regressor
  cat(sprintf("K=%s ", paste(K_use, collapse = ",")))

  fit <- tryCatch(
    flm(Supply ~ Wind + Solar + Gas, data_train, K = K_use, inference = FALSE),
    error = function(e) { cat("flm err: ", conditionMessage(e), "\n"); NULL })
  if (is.null(fit)) next

  data_test <- list(Supply = test$Supply,
                    Wind   = test$Wind,
                    Solar  = test$Solar,
                    Gas    = test$Gas)
  pred <- tryCatch(predict(fit, newdata = data_test),
                   error = function(e) { cat("pred err\n"); NULL })
  if (is.null(pred)) next

  # Predictions: extract as matrix
  pred_mat <- if (is.list(pred) && !is.null(pred$prediction)) pred$prediction else pred
  if (!is.matrix(pred_mat)) {
    cat(sprintf("unexpected pred type: %s\n", class(pred_mat)[1])); next
  }

  actual <- test$Supply
  resid  <- actual - pred_mat
  rmse_per_grid <- sqrt(colMeans(resid^2))

  cat(sprintf("n_train=%d n_post=%d RMSE@MCP-region (50-100EUR)=%.0f MW\n",
              nrow(train$Supply), nrow(test$Supply),
              mean(rmse_per_grid[51:101])))

  # Per-curve summary row
  all_results[[length(all_results) + 1]] <- data.frame(
    reform = reform, side = side, market = market,
    session = ifelse(is.na(session), NA_integer_, session),
    hour_class = hc_name, n_train = nrow(train$Supply),
    n_post = nrow(test$Supply), K = paste(K_use, collapse = ","),
    rmse_lowprice  = mean(rmse_per_grid[1:51]),    # 0-50 EUR
    rmse_midprice  = mean(rmse_per_grid[52:101]),  # 51-100 EUR
    rmse_highprice = mean(rmse_per_grid[102:201]), # 101-200 EUR
    stringsAsFactors = FALSE
  )
  # Curve-level data for plotting: per (date, grid) actual + predicted MW
  for (i in seq_along(test$dates)) {
    all_curves[[length(all_curves) + 1]] <- data.frame(
      reform = reform, side = side, market = market,
      session = ifelse(is.na(session), NA_integer_, session),
      hour_class = hc_name, d = test$dates[i],
      price_eur = GRID_PRICES,
      actual_mw = actual[i, ],
      pred_mw   = pred_mat[i, ],
      stringsAsFactors = FALSE
    )
  }
}

# Save
if (length(all_results) > 0) {
  summary_df <- do.call(rbind, all_results)
  write.csv(summary_df, file.path(out_dir, "ffr_summary.csv"), row.names = FALSE)
  cat(sprintf("\nWrote summary: %d rows\n", nrow(summary_df)))
}
if (length(all_curves) > 0) {
  curves_df <- do.call(rbind, all_curves)
  write_parquet(curves_df, file.path(out_dir, "ffr_curves_predicted_actual.parquet"))
  cat(sprintf("Wrote curves parquet: %d rows\n", nrow(curves_df)))
}
cat("Done.\n")
