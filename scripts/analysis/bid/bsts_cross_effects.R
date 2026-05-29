# STATUS: ALIVE
# LAST-AUDIT: 2026-05-27
# FEEDS: advisor_memo.tex sec 4 -- per-reform 2x2 BSTS figures (price+CCGT
#        for BOTH markets, per reform). Adds the cross-market outcomes the
#        original bsts_daily_longpre.R / extended_post.R didn't run:
#          ID15 (40-day post, pre-blackout) -> DA price, DA CCGT cleared
#          DA15 (extended post)              -> IDA price, IDA CCGT cleared
#        And the matching 2024 same-calendar placebos.
#
# OUT: results/regressions/bid/mtu15_critical_flat/pointwise/
#      bsts_cross_pointwise_*.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_cross_effects.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
pw_dir <- file.path(repo,
  "results/regressions/bid/mtu15_critical_flat/pointwise")
dir.create(pw_dir, recursive = TRUE, showWarnings = FALSE)

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

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
                            sprintf("bsts_cross_pointwise_%s.csv",
                                     gsub(" ", "_", tag))),
             row.names = FALSE)
  cat(sprintf("  %-28s  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  p=%.3f  n_pre=%d  n_post=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s$p[1],
              sum(sub$d < cutover), sum(sub$d >= cutover)))
}


# ===========================================================
# ID15 (40-day post, pre-blackout)  ->  DA-side outcomes
# ===========================================================
cat("=== ID15 40d post  ->  DA-side outcomes ===\n")
ID_PRE <- "2024-06-14"; ID_REF <- "2025-03-19"; ID_END <- "2025-04-27"
run_bsts(panel, "da_price_eur",      ID_PRE, ID_REF, ID_END,
         COVARS_PRICE, "ID15 DA price")
run_bsts(panel, "q_ccgt_gwh_da",     ID_PRE, ID_REF, ID_END,
         COVARS_QTY, "ID15 q_ccgt_da")

cat("\n=== Placebo ID15-2024  ->  DA-side outcomes ===\n")
PID_PRE <- "2023-06-14"; PID_REF <- "2024-03-19"; PID_END <- "2024-04-27"
run_bsts(panel, "da_price_eur",      PID_PRE, PID_REF, PID_END,
         COVARS_PRICE, "PLB-ID DA price")
run_bsts(panel, "q_ccgt_gwh_da",     PID_PRE, PID_REF, PID_END,
         COVARS_QTY, "PLB-ID q_ccgt_da")

# ===========================================================
# DA15 (extended post, panel end)  ->  IDA-side outcomes
# ===========================================================
cat("\n=== DA15 extended post  ->  IDA-side outcomes ===\n")
PANEL_END <- "2026-02-26"  # cleared-MW data ends here; align price runs to match
DA_PRE <- "2025-04-28"; DA_REF <- "2025-10-01"; DA_END <- PANEL_END
run_bsts(panel, "ida_price_eur",     DA_PRE, DA_REF, DA_END,
         COVARS_PRICE, "DA15 IDA price")
run_bsts(panel, "q_ccgt_gwh_ida",    DA_PRE, DA_REF, DA_END,
         COVARS_QTY, "DA15 q_ccgt_ida")

cat("\n=== Placebo DA15-2024  ->  IDA-side outcomes ===\n")
PDA_PRE <- "2024-04-28"; PDA_REF <- "2024-10-01"; PDA_END <- "2025-04-27"
run_bsts(panel, "ida_price_eur",     PDA_PRE, PDA_REF, PDA_END,
         COVARS_PRICE, "PLB-DA IDA price")
run_bsts(panel, "q_ccgt_gwh_ida",    PDA_PRE, PDA_REF, PDA_END,
         COVARS_QTY, "PLB-DA q_ccgt_ida")

cat("\nDone.\n")
