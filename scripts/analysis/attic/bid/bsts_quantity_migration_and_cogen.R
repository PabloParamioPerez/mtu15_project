# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix slide "Cleared quantities" of the June 2026 deck.
#
# Daily BSTS for:
#   (a) Cogen levels (Q_da, Q_ida) at ID15 + DA15
#   (b) Migration outcome Q_granular - Q_other per tech, both reforms
# Same long-pre Spec A design as bsts_daily_year_interactions.R: per-year
# (wind, solar) interactions; niter=10000; nseasons=7.
#
# Convention: migration coefficient is POSITIVE when volume moves toward the
# granular venue (IDA at ID15, DA at DA15).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_quantity_migration_and_cogen.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_daily_panel_w_cogen_mig.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

# DA15-signed mig columns (mig_da15 = Q_da - Q_ida = -mig_id15)
TECHS <- c("ccgt", "hydro", "hydro_pump", "nuclear", "solar", "wind", "cogen")
for (t in TECHS) {
  mig_col <- sprintf("q_%s_mig_gwh", t)
  if (mig_col %in% names(panel)) {
    panel[[sprintf("q_%s_mig_id15_gwh", t)]] <-  panel[[mig_col]]
    panel[[sprintf("q_%s_mig_da15_gwh", t)]] <- -panel[[mig_col]]
  } else {
    da_col  <- sprintf("q_%s_gwh_da", t)
    ida_col <- sprintf("q_%s_gwh_ida", t)
    panel[[sprintf("q_%s_mig_id15_gwh", t)]] <- panel[[ida_col]] - panel[[da_col]]
    panel[[sprintf("q_%s_mig_da15_gwh", t)]] <- panel[[da_col]] - panel[[ida_col]]
  }
}

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  yrs_kept <- yrs[-1]; new_cols <- c()
  for (y in yrs_kept) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 10000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  s <- imp$summary
  cat(sprintf("  %-30s eff=%+7.2f  CI=[%+6.2f,%+6.2f] p=%.3f n_pre=%d n_post=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  data.frame(tag=tag, eff=s["Average","AbsEffect"],
             lo=s["Average","AbsEffect.lower"], hi=s["Average","AbsEffect.upper"],
             p=s$p[1], n_pre=sum(sub$d < cutover), n_post=sum(sub$d >= cutover),
             stringsAsFactors=FALSE)
}

rows <- list()

# Cogen LEVELS at both reforms
cat("=== Cogen levels ===\n")
rows[[length(rows)+1]] <- run_bsts("q_cogen_gwh_da",  "2022-01-01", "2025-03-19", "2025-04-27", "ID15 DA (cogen)")
rows[[length(rows)+1]] <- run_bsts("q_cogen_gwh_ida", "2022-01-01", "2025-03-19", "2025-04-27", "ID15 IDA (cogen)")
rows[[length(rows)+1]] <- run_bsts("q_cogen_gwh_da",  "2022-01-01", "2025-10-01", "2025-12-31", "DA15 DA (cogen)")
rows[[length(rows)+1]] <- run_bsts("q_cogen_gwh_ida", "2022-01-01", "2025-10-01", "2025-12-31", "DA15 IDA (cogen)")

# MIGRATION per tech at both reforms (granular - other)
cat("\n=== Migration: granular - other (positive = toward granular) ===\n")
for (t in TECHS) {
  rows[[length(rows)+1]] <- run_bsts(sprintf("q_%s_mig_id15_gwh", t),
                                      "2022-01-01", "2025-03-19", "2025-04-27",
                                      sprintf("ID15 MIG (%s)", t))
  rows[[length(rows)+1]] <- run_bsts(sprintf("q_%s_mig_da15_gwh", t),
                                      "2022-01-01", "2025-10-01", "2025-12-31",
                                      sprintf("DA15 MIG (%s)", t))
}

out <- do.call(rbind, rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_quantity_migration_and_cogen.csv"),
          row.names=FALSE)
cat("\nWrote bsts_quantity_migration_and_cogen.csv\n")
