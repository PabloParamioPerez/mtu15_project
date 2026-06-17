# STATUS: ALIVE
# LAST-AUDIT: 2026-06-01
# CLAIM: Did MTU15-IDA (2025-03-19) raise the daily count of dual-direction
#        (buy + sell) unit-sessions in IDA auctions per tech group? Model
#        prediction: yes, because multiplying matched spot products per
#        clock-hour multiplies arbitrage opportunities. BSTS on the daily
#        count, per tech-group, with wind+solar+gas covariates and the
#        same pre-window cut as the preliminary results.
#
# IN:  data/derived/panels/arbitrage_intensity_daily.parquet
# OUT: results/regressions/firm/bsts_arbitrage_intensity.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/firm/bsts_arbitrage_intensity.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo,
  "data/derived/panels/arbitrage_intensity_daily.parquet"))
panel$d <- as.Date(panel$d)

PRE_START <- as.Date("2022-06-14")  # post-IDA-reform 6 -> 3
CUTOVER   <- as.Date("2025-03-19")  # MTU15-IDA
POST_END  <- as.Date("2025-04-27")  # pre-blackout, 40 days post

TECHS <- c("CCGT", "Hydro_pump", "Hydro", "Nuclear", "Solar PV", "Wind",
           "Cogen", "Biomass", "Hydro_RES", "Coal")

run_tech <- function(tech) {
  sub <- panel[panel$tech_group == tech & panel$d >= PRE_START &
               panel$d <= POST_END, ]
  sub <- sub[complete.cases(sub[, c("n_dual", "wind_gwh", "solar_gwh", "gas_eur")]), ]
  if (nrow(sub) < 60 || max(sub$d) < CUTOVER) return(NULL)
  mat <- as.matrix(sub[, c("n_dual", "wind_gwh", "solar_gwh", "gas_eur")])
  ts  <- zoo(mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(ts, c(PRE_START, CUTOVER - 1), c(CUTOVER, POST_END),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  data.frame(
    tech_group           = tech,
    pre_mean_dual        = s["Average", "Actual"]    - s["Average", "AbsEffect"],
    post_mean_dual       = s["Average", "Actual"],
    abs_effect           = s["Average", "AbsEffect"],
    abs_lower            = s["Average", "AbsEffect.lower"],
    abs_upper            = s["Average", "AbsEffect.upper"],
    rel_effect_pct       = 100 * s["Average", "RelEffect"],
    p_value              = s$p[1],
    n_pre                = sum(sub$d <  CUTOVER),
    n_post               = sum(sub$d >= CUTOVER),
    stringsAsFactors     = FALSE
  )
}

rows <- list()
for (t in TECHS) {
  r <- run_tech(t)
  if (is.null(r)) next
  cat(sprintf("%-12s pre=%7.1f  post=%7.1f  abs=%+7.1f [%+7.1f, %+7.1f]  rel=%+6.1f%%  p=%.3f  n=%d/%d\n",
              r$tech_group, r$pre_mean_dual, r$post_mean_dual,
              r$abs_effect, r$abs_lower, r$abs_upper,
              r$rel_effect_pct, r$p_value, r$n_pre, r$n_post))
  rows[[length(rows) + 1]] <- r
}
out_df <- do.call(rbind, rows)

out_dir <- file.path(repo, "results/regressions/firm")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(out_df, file.path(out_dir, "bsts_arbitrage_intensity.csv"),
          row.names = FALSE)
cat(sprintf("\nWrote %d rows.\n", nrow(out_df)))
