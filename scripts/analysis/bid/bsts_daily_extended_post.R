# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 4 -- supersedes the 40-day post-window BSTS
#        with the maximum available post-window (end of panel, 2026-04-27).
#        Same pre-windows as bsts_daily_longpre.R (regime-constant). For
#        DA15 the post goes from 2025-10-01 to 2026-04-27 (~7 months,
#        includes the full Oct-Apr winter cycle). For ID15 the post goes
#        from 2025-03-19 to 2026-04-27 (mixes pre- and post-blackout
#        regime; reported as a robustness check, not the headline).
#
# OUT: results/regressions/bid/mtu15_critical_flat/pointwise/
#      bsts_extpost_pointwise_*.csv (alongside the existing 40d versions)

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_extended_post.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")
pw_dir <- file.path(out_dir, "pointwise")
dir.create(pw_dir, recursive = TRUE, showWarnings = FALSE)

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

# Use demand as additional covariate for QUANTITY outcomes (it is exogenous
# w.r.t. CCGT auction-cleared MWh: load is set by weather and economic
# activity, not by who clears which slice of the merit order). We keep the
# original COVARS set for prices (where demand is endogenous via equilibrium).
COVARS_PRICE <- c("wind_gwh", "solar_gwh", "gas_eur")
COVARS_QTY   <- c("wind_gwh", "solar_gwh", "gas_eur", "demand_gwh")


run_bsts <- function(panel, response, pre_start, post_start, post_end,
                      covars, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, covars)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  %s: skip (n=%d)\n", tag, nrow(sub))); return(NULL)
  }
  data_mat <- as.matrix(sub[, c(response, covars)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  pre_period <- c(ps, cutover - 1)
  post_period <- c(cutover, pe)
  set.seed(42)
  imp <- CausalImpact(data_ts, pre_period, post_period,
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  s <- imp$summary
  pw <- as.data.frame(imp$series); pw$date <- as.Date(rownames(pw))
  write.csv(pw, file.path(pw_dir,
                            sprintf("bsts_extpost_pointwise_%s.csv",
                                     gsub(" ", "_", tag))),
             row.names = FALSE)
  cat(sprintf("  %-26s  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  rel=%+5.2f  p=%.3f  n_pre=%d  n_post=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s["Average","RelEffect"],
              s$p[1], sum(sub$d < cutover), sum(sub$d >= cutover)))
  list(eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], rel = s["Average","RelEffect"],
       p = s$p[1])
}

PANEL_END <- "2026-02-26"  # cleared-MW data endpoint; align all runs here

cat("=== DA15 EXTENDED-POST  (pre 2025-04-28..2025-09-30, post 2025-10-01..2026-04-27) ===\n")
DA_PRE <- "2025-04-28"; DA_REFORM <- "2025-10-01"; DA_POST_END <- PANEL_END

r <- run_bsts(panel, "da_price_eur", DA_PRE, DA_REFORM, DA_POST_END,
              COVARS_PRICE, "DA15 DA price")
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_da", tech),
                DA_PRE, DA_REFORM, DA_POST_END, COVARS_QTY,
                sprintf("DA15 q_%s_da", tech))
}

cat("\n=== ID15 EXTENDED-POST  (pre 2024-06-14..2025-03-18, post 2025-03-19..2026-04-27, MIXES regime) ===\n")
ID_PRE <- "2024-06-14"; ID_REFORM <- "2025-03-19"; ID_POST_END <- PANEL_END

r <- run_bsts(panel, "ida_price_eur", ID_PRE, ID_REFORM, ID_POST_END,
              COVARS_PRICE, "ID15 IDA price")
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_ida", tech),
                ID_PRE, ID_REFORM, ID_POST_END, COVARS_QTY,
                sprintf("ID15 q_%s_ida", tech))
}

cat("\n=== Placebo DA15-2024 (pre 2024-04-28..2024-09-30, post 2024-10-01..2025-04-27) ===\n")
P_DA_PRE <- "2024-04-28"; P_DA_REF <- "2024-10-01"; P_DA_END <- "2025-04-27"
r <- run_bsts(panel, "da_price_eur", P_DA_PRE, P_DA_REF, P_DA_END,
              COVARS_PRICE, "PLB-DA DA price")
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_da", tech),
                P_DA_PRE, P_DA_REF, P_DA_END, COVARS_QTY,
                sprintf("PLB-DA q_%s_da", tech))
}

cat("\n=== Placebo ID15-2024 (pre 2023-06-14..2024-03-18, post 2024-03-19..2025-04-27) ===\n")
P_ID_PRE <- "2023-06-14"; P_ID_REF <- "2024-03-19"; P_ID_END <- "2025-04-27"
r <- run_bsts(panel, "ida_price_eur", P_ID_PRE, P_ID_REF, P_ID_END,
              COVARS_PRICE, "PLB-ID IDA price")
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_ida", tech),
                P_ID_PRE, P_ID_REF, P_ID_END, COVARS_QTY,
                sprintf("PLB-ID q_%s_ida", tech))
}

cat("\nDone. Pointwise CSVs in:\n  ", pw_dir, "\n")
