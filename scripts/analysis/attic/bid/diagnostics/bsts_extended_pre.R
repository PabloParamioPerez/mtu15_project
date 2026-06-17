# Extend the pre-window further back: include a year before the
# 6 -> 3 IDA-session reform (2024-06-14). The trade-off is better
# solar identification vs an IDA structural break inside the pre-period.

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

run_one <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  bsts_model <- imp$model$bsts.model
  burn <- bsts::SuggestBurn(0.1, bsts_model)
  coef_draws <- bsts_model$coefficients
  if (burn > 0) coef_draws <- coef_draws[-(1:burn), , drop = FALSE]
  cat(sprintf("\n=== %s ===\n", tag))
  cat(sprintf("  Effect: %+8.2f  CI=[%+8.2f, %+8.2f]  p=%.3f  (n_pre=%d, n_post=%d)\n",
              s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  for (cv in COVARS) {
    bd <- coef_draws[, cv]; nz <- bd[bd != 0]
    incl <- mean(bd != 0)
    pm <- if (length(nz)) mean(nz) else NA
    ps_ <- if (length(nz)) sd(nz)  else NA
    cat(sprintf("  %-10s  beta = %+8.4f (sd %.4f)  incl.prob = %.3f\n",
                cv, pm, ps_, incl))
  }
  pre_sub <- sub[sub$d < cutover, ]
  cat("  Pre-window solar range / sd:  ")
  v <- pre_sub$solar_gwh
  cat(sprintf("[%.1f, %.1f] sd=%.1f\n", min(v), max(v), sd(v)))
}

cat("Pre-window comparison: ID15 IDA price (post-cutover 2025-03-19 to 2025-04-27)\n")
run_one("ida_price_eur", "2024-06-14", "2025-03-19", "2025-04-27",
        "LONG     (post-IDA-reform, 2024-06-14)  n_pre=278")
run_one("ida_price_eur", "2023-06-14", "2025-03-19", "2025-04-27",
        "EXTENDED (pre-IDA-reform too, 2023-06-14)  n_pre~642")
run_one("ida_price_eur", "2022-01-01", "2025-03-19", "2025-04-27",
        "MAX      (full panel, 2022-01-01)  n_pre~1170")

cat("\nSame three windows applied to the DA price (cross-market spillover):\n")
run_one("da_price_eur", "2024-06-14", "2025-03-19", "2025-04-27",
        "LONG     DA (cross-market)")
run_one("da_price_eur", "2023-06-14", "2025-03-19", "2025-04-27",
        "EXTENDED DA (cross-market)")
run_one("da_price_eur", "2022-01-01", "2025-03-19", "2025-04-27",
        "MAX      DA (cross-market)")

cat("\nDone.\n")
