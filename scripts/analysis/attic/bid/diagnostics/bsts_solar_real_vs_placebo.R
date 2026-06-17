# Compare BSTS solar coefficients between REAL and PLACEBO windows
# under both LONG and SHORT pre-window specs, to test whether the
# real window absorbs the solar surge as well as the placebo does.

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
  cdraws <- bsts_model$coefficients
  if (burn > 0) cdraws <- cdraws[-(1:burn), , drop = FALSE]
  sol_bd <- cdraws[, "solar_gwh"]
  sol_nz <- sol_bd[sol_bd != 0]
  sol_inc <- mean(sol_bd != 0)
  sol_pm  <- if (length(sol_nz)) mean(sol_nz) else NA
  sol_psd <- if (length(sol_nz)) sd(sol_nz) else NA
  pre_sub  <- sub[sub$d <  cutover, ]
  post_sub <- sub[sub$d >= cutover, ]
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       sol_b   = sol_pm,
       sol_sd  = sol_psd,
       sol_inc = sol_inc,
       solar_pre_mean = mean(pre_sub$solar_gwh),
       solar_pre_sd   = sd(pre_sub$solar_gwh),
       solar_post_mean = mean(post_sub$solar_gwh),
       solar_delta_mean = mean(post_sub$solar_gwh) - mean(pre_sub$solar_gwh),
       tag = tag)
}

cat("Comparing real vs placebo solar coefficients (IDA price)\n\n")

specs <- list(
  list("LONG  REAL  2025",     "2024-06-14", "2025-03-19", "2025-04-27"),
  list("LONG  PLB  2024",      "2023-06-14", "2024-03-19", "2024-04-27"),
  list("LONG  PLB  2026",      "2025-06-14", "2026-03-19", "2026-04-27"),
  list("SHORT REAL  2025",     "2024-12-29", "2025-03-19", "2025-04-27"),
  list("SHORT PLB  2026",      "2025-12-29", "2026-03-19", "2026-04-27")
)

cat(sprintf("%-22s | %s %s %s | %s %s %s %s %s\n",
            "spec",
            "  eff", "  lo", "  hi",
            "  sol_b", "sol_sd", " incl",
            " sol_pre_mean(sd)", "sol_post_mean(delta)"))
cat(strrep("-", 130), "\n", sep = "")
for (sp in specs) {
  r <- run_one("ida_price_eur", sp[[2]], sp[[3]], sp[[4]], sp[[1]])
  cat(sprintf("%-22s | %+6.1f %+6.1f %+6.1f | %+7.3f %7.3f %5.2f | %5.1f (%.1f) -> %5.1f (delta %+.1f)\n",
              r$tag, r$eff, r$lo, r$hi, r$sol_b, r$sol_sd, r$sol_inc,
              r$solar_pre_mean, r$solar_pre_sd, r$solar_post_mean, r$solar_delta_mean))
}

cat("\n--- Same on the DA price (cross-market spillover) ---\n")
cat(strrep("-", 130), "\n", sep = "")
for (sp in specs) {
  r <- run_one("da_price_eur", sp[[2]], sp[[3]], sp[[4]], sp[[1]])
  cat(sprintf("%-22s | %+6.1f %+6.1f %+6.1f | %+7.3f %7.3f %5.2f | %5.1f (%.1f) -> %5.1f (delta %+.1f)\n",
              r$tag, r$eff, r$lo, r$hi, r$sol_b, r$sol_sd, r$sol_inc,
              r$solar_pre_mean, r$solar_pre_sd, r$solar_post_mean, r$solar_delta_mean))
}

cat("\nDone.\n")
