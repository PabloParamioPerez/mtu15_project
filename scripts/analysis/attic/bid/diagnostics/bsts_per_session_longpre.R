# Per-IDA-session BSTS with the LONG pre-window (post-IDA-reform,
# 2024-06-14 onwards), to test whether the per-session ~-80 magnitudes
# from the short post-ISP15 pre-window collapse to the pooled -38 once
# we use the same long pre-window the pooled BSTS uses.

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_per_session_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

run_one <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("%-40s SKIP (n=%d)\n", tag, nrow(sub))); return(invisible(NULL))
  }
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  bsts_model <- imp$model$bsts.model
  burn <- bsts::SuggestBurn(0.1, bsts_model)
  cdraws <- bsts_model$coefficients
  if (burn > 0) cdraws <- cdraws[-(1:burn), , drop = FALSE]
  sol_nz <- cdraws[, "solar_gwh"][cdraws[, "solar_gwh"] != 0]
  sol_pm  <- if (length(sol_nz)) mean(sol_nz) else NA
  sol_inc <- mean(cdraws[, "solar_gwh"] != 0)
  cat(sprintf("%-40s eff=%+7.2f CI=[%+7.2f,%+7.2f] p=%.3f  n=%d/%d  solar_b=%+.3f (incl %.2f)\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover),
              sol_pm, sol_inc))
}

cat("=== Per-IDA-session BSTS, LONG pre-window (matching pooled spec) ===\n\n")
cat("--- ID15 REAL: pre 2024-06-14 -> post 2025-03-19 to 2025-04-27 ---\n")
for (sess in 1:3) {
  run_one(sprintf("ida_price_eur_s%d", sess),
          "2024-06-14", "2025-03-19", "2025-04-27",
          sprintf("S%d REAL  LONG", sess))
}
cat("\n--- ID15 PLACEBO 2026 LONG: pre 2025-06-14 -> post 2026-03-19 to 2026-04-27 ---\n")
for (sess in 1:3) {
  run_one(sprintf("ida_price_eur_s%d", sess),
          "2025-06-14", "2026-03-19", "2026-04-27",
          sprintf("S%d PLB-26 LONG", sess))
}
cat("\n--- For comparison: pooled IDA price LONG (Table 7) ---\n")
run_one("ida_price_eur",
        "2024-06-14", "2025-03-19", "2025-04-27",
        "POOLED REAL  LONG")
run_one("ida_price_eur",
        "2025-06-14", "2026-03-19", "2026-04-27",
        "POOLED PLB-26 LONG")
cat("\nDone.\n")
