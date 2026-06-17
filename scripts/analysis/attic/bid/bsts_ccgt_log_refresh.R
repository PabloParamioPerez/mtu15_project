# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 4 figures -- refreshes the CCGT-cleared
#        pointwise CSVs with log-transformed responses, so the BSTS
#        counterfactual never predicts negative quantities. Overwrites
#        the q_ccgt_* CSVs in the existing pointwise/ folder; price CSVs
#        are unchanged.
#
# Log spec: y' = log(max(y, 0) + 1); fit BSTS on y'; back-transform via
# (exp(y'_hat) - 1) clamped at 0. Both point predictions and 95% credible
# bounds are back-transformed (monotone), so the figure shows a strictly
# non-negative band.

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_ccgt_log_refresh.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
pw_dir <- file.path(repo,
  "results/regressions/bid/mtu15_critical_flat/pointwise")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

COVARS_QTY <- c("wind_gwh", "solar_gwh", "gas_eur", "demand_gwh")


run_log_bsts <- function(panel, response, pre_start, post_start, post_end,
                          covars, out_file) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, covars)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  skip (n=%d)\n", nrow(sub))); return(NULL)
  }
  # Reverted to level scale: log(1+y) back-transform makes the upper CI
  # bound explode through exp() (e.g. 700+ GWh/day for DA CCGT). Visual
  # clipping of the lower CI at zero is applied in the plotting layer.
  data_mat <- as.matrix(sub[, c(response, covars)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  pre_period <- c(ps, cutover - 1)
  post_period <- c(cutover, pe)
  set.seed(42)
  imp <- CausalImpact(data_ts, pre_period, post_period,
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  pw <- as.data.frame(imp$series); pw$date <- as.Date(rownames(pw))
  write.csv(pw, file.path(pw_dir, out_file), row.names = FALSE)

  post_mask <- pw$date >= cutover
  eff <- mean(pw$point.effect[post_mask], na.rm = TRUE)
  lo <- mean(pw$point.effect.lower[post_mask], na.rm = TRUE)
  hi <- mean(pw$point.effect.upper[post_mask], na.rm = TRUE)
  cat(sprintf("  %-50s  eff=%+8.3f  pw-CI=[%+8.3f,%+8.3f]  n_pre=%d  n_post=%d\n",
              out_file, eff, lo, hi,
              sum(sub$d < cutover), sum(sub$d >= cutover)))
}


PANEL_END <- "2026-02-26"  # cleared-MW data endpoint

cat("=== ID15 40-day post (pre-blackout): IDA CCGT and DA CCGT (cross) ===\n")
run_log_bsts(panel, "q_ccgt_gwh_ida", "2024-06-14", "2025-03-19",
             "2025-04-27", COVARS_QTY,
             "bsts_longpre_pointwise_ID15_q_ccgt_ida.csv")
run_log_bsts(panel, "q_ccgt_gwh_da", "2024-06-14", "2025-03-19",
             "2025-04-27", COVARS_QTY,
             "bsts_cross_pointwise_ID15_q_ccgt_da.csv")
# placebos
run_log_bsts(panel, "q_ccgt_gwh_ida", "2023-06-14", "2024-03-19",
             "2024-04-27", COVARS_QTY,
             "bsts_longpre_pointwise_PLB-ID_q_ccgt_ida.csv")
run_log_bsts(panel, "q_ccgt_gwh_da", "2023-06-14", "2024-03-19",
             "2024-04-27", COVARS_QTY,
             "bsts_cross_pointwise_PLB-ID_q_ccgt_da.csv")

cat("\n=== DA15 extended post (panel end): DA CCGT and IDA CCGT (cross) ===\n")
run_log_bsts(panel, "q_ccgt_gwh_da", "2025-04-28", "2025-10-01",
             PANEL_END, COVARS_QTY,
             "bsts_extpost_pointwise_DA15_q_ccgt_da.csv")
run_log_bsts(panel, "q_ccgt_gwh_ida", "2025-04-28", "2025-10-01",
             PANEL_END, COVARS_QTY,
             "bsts_cross_pointwise_DA15_q_ccgt_ida.csv")
# placebos
run_log_bsts(panel, "q_ccgt_gwh_da", "2024-04-28", "2024-10-01",
             "2025-04-27", COVARS_QTY,
             "bsts_extpost_pointwise_PLB-DA_q_ccgt_da.csv")
run_log_bsts(panel, "q_ccgt_gwh_ida", "2024-04-28", "2024-10-01",
             "2025-04-27", COVARS_QTY,
             "bsts_cross_pointwise_PLB-DA_q_ccgt_ida.csv")

cat("\n=== ID15 extended post (mixes regime): IDA CCGT (for long-view fig) ===\n")
run_log_bsts(panel, "q_ccgt_gwh_ida", "2024-06-14", "2025-03-19",
             PANEL_END, COVARS_QTY,
             "bsts_extpost_pointwise_ID15_q_ccgt_ida.csv")
run_log_bsts(panel, "q_ccgt_gwh_ida", "2023-06-14", "2024-03-19",
             "2025-04-27", COVARS_QTY,
             "bsts_extpost_pointwise_PLB-ID_q_ccgt_ida.csv")

cat("\nDone.\n")
