# STATUS: ALIVE
# LAST-AUDIT: 2026-06-09
# FEEDS: appendix slide "Cleared quantities" of the June 2026 deck.
#
# MINIMAL re-run: only Hydro_pump migration (BSTS). Daily-panel levels for
# hydro_pump_da / _ida are already in bsts_cross_market_q.csv from earlier
# (the daily panel always kept them separate); we don't re-run those.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_quantity_hydropump_mig.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })
repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_daily_panel_w_cogen_mig.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
panel$mig_id15 <-  panel$q_hydro_pump_mig_gwh
panel$mig_da15 <- -panel$q_hydro_pump_mig_gwh

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_int <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df=sub, cols=c()))
  cols <- c()
  for (y in yrs[-1]) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    cols <- c(cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df=sub, cols=cols)
}

run_bsts <- function(response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  yr <- add_year_int(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_ts <- zoo(as.matrix(sub[, c(response, cov_set)]), order.by=sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 10000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  s <- imp$summary
  cat(sprintf("  %-20s eff=%+7.2f  CI=[%+6.2f,%+6.2f] p=%.3f\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1]))
  data.frame(tag=tag, eff=s["Average","AbsEffect"],
             lo=s["Average","AbsEffect.lower"], hi=s["Average","AbsEffect.upper"],
             p=s$p[1], stringsAsFactors=FALSE)
}

rows <- list()
rows[[1]] <- run_bsts("mig_id15", "2022-01-01", "2025-03-19", "2025-04-27", "ID15 MIG (hp)")
rows[[2]] <- run_bsts("mig_da15", "2022-01-01", "2025-10-01", "2025-12-31", "DA15 MIG (hp)")
write.csv(do.call(rbind, rows),
  file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_quantity_hydropump_mig.csv"),
  row.names=FALSE)
cat("\nWrote bsts_quantity_hydropump_mig.csv\n")
