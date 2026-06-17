# STATUS: ALIVE
# LAST-AUDIT: 2026-05-31
# CLAIM: Per-zone BSTS on daily CCGT PHF-PDBF gap (restriction proxy),
#        with blackout cutover 2025-04-28. Tests the spatial hypothesis:
#        zones whose CCGT fleet had bigger within-hour production
#        swings under MTU15-IDA pre-blackout suffer bigger post-blackout
#        restriction jumps. Covariates: wind+solar+gas (same as
#        preliminary's Spec A).
#
# IN:  data/derived/panels/zonal_restriction_panel.parquet
#      data/derived/panels/zonal_volatility_score.csv
# OUT: results/regressions/regulatory/spatial_blackout/bsts_zonal_blackout.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/regulatory/bsts_zonal_restrictions_blackout.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo,
  "data/derived/panels/zonal_restriction_panel.parquet"))
vol <- read.csv(file.path(repo, "data/derived/panels/zonal_volatility_score.csv"),
                stringsAsFactors = FALSE)
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$zone, panel$d), ]

# Pre-window: 2024-06-14 (post-IDA reform) to 2025-04-27 (pre-blackout).
# Post-window: 2025-04-28 (blackout) to 2026-01-31 (~9 months).
# This isolates the BLACKOUT cutover; ID15 (2025-03-19) is in the pre
# window, so the BSTS counterfactual already absorbs the MTU15-IDA
# level shift before extrapolating to post.
PRE_START  <- as.Date("2024-06-14")
CUTOVER    <- as.Date("2025-04-28")
POST_END   <- as.Date("2026-01-31")

run_zone <- function(zone_name) {
  sub <- panel[panel$zone == zone_name & panel$d >= PRE_START & panel$d <= POST_END, ]
  sub <- sub[complete.cases(sub[, c("phf_pdbf_gap_gwh", "wind_gwh", "solar_gwh", "gas_eur")]), ]
  if (nrow(sub) < 30 || max(sub$d) < CUTOVER) return(NULL)
  data_mat <- as.matrix(sub[, c("phf_pdbf_gap_gwh", "wind_gwh", "solar_gwh", "gas_eur")])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(PRE_START, CUTOVER - 1), c(CUTOVER, POST_END),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       p   = s$p[1],
       n_pre = sum(sub$d < CUTOVER),
       n_post = sum(sub$d >= CUTOVER))
}

rows <- list()
for (z in vol$zone) {
  r <- run_zone(z)
  if (is.null(r)) next
  cat(sprintf("%-10s vol=%5.2f MW  eff=%+6.2f GWh/day  [%+6.2f,%+6.2f]  p=%.3f  n=%d/%d\n",
              z, vol$within_hour_sd_mw[vol$zone == z],
              r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
  rows[[length(rows)+1]] <- data.frame(
    zone = z,
    within_hour_sd_mw = vol$within_hour_sd_mw[vol$zone == z],
    within_hour_cv_pct = vol$within_hour_cv_pct[vol$zone == z],
    n_units = vol$n_units[vol$zone == z],
    bsts_eff_gwh_per_day = r$eff,
    bsts_lo = r$lo, bsts_hi = r$hi,
    bsts_p = r$p,
    n_pre = r$n_pre, n_post = r$n_post,
    stringsAsFactors = FALSE
  )
}
out_df <- do.call(rbind, rows)

# Spatial correlation: volatility score vs BSTS effect
if (nrow(out_df) > 3) {
  cor_lin <- cor(out_df$within_hour_sd_mw, out_df$bsts_eff_gwh_per_day,
                 use = "complete.obs", method = "pearson")
  cor_rnk <- cor(out_df$within_hour_sd_mw, out_df$bsts_eff_gwh_per_day,
                 use = "complete.obs", method = "spearman")
  cat(sprintf("\nSpatial correlation (volatility vs BSTS effect):  Pearson=%.2f  Spearman=%.2f\n",
              cor_lin, cor_rnk))
}

out_dir <- file.path(repo, "results/regressions/regulatory/spatial_blackout")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(out_df, file.path(out_dir, "bsts_zonal_blackout.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows.\n", nrow(out_df)))
