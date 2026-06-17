# STATUS: ALIVE
# LAST-AUDIT: 2026-05-26
# FEEDS: advisor_memo.tex sec 4 -- supersedes the M-H 80-day BSTS with
#        regime-respecting long pre-windows. ID15 uses 278 days
#        (post-IDA-reform, 3-session regime constant). DA15 uses 156 days
#        (reforzada-constant). Both placebos use same-calendar 2024.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_longpre.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_longpre.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
SAVE_PW <- c("ID15 IDA price", "DA15 DA price",
              "DA15 q_ccgt_da", "ID15 q_ccgt_ida",
              "DA15 q_hydro_pump_da", "ID15 q_hydro_pump_ida",
              "PLB-DA q_ccgt_da", "PLB-ID q_ccgt_ida",
              "PLB-DA q_hydro_pump_da", "PLB-ID q_hydro_pump_ida",
              "PLB-DA DA price", "PLB-ID IDA price")


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
  if (tag %in% SAVE_PW) {
    pw <- as.data.frame(imp$series); pw$date <- as.Date(rownames(pw))
    pw_dir <- file.path(out_dir, "pointwise")
    dir.create(pw_dir, recursive = TRUE, showWarnings = FALSE)
    write.csv(pw, file.path(pw_dir,
                              sprintf("bsts_longpre_pointwise_%s.csv",
                                       gsub(" ", "_", tag))),
               row.names = FALSE)
  }
  cat(sprintf("  %-26s  eff=%+8.3f  CI=[%+8.3f,%+8.3f]  rel=%+5.2f  p=%.3f  n_pre=%d  n_post=%d\n",
              tag, s["Average","AbsEffect"], s["Average","AbsEffect.lower"],
              s["Average","AbsEffect.upper"], s["Average","RelEffect"],
              s$p[1], sum(sub$d < cutover), sum(sub$d >= cutover)))
  list(eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], rel = s["Average","RelEffect"],
       p = s$p[1],
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover))
}


rows <- list()
add <- function(reform, outcome, tech, r) {
  if (is.null(r)) return()
  rows[[length(rows)+1]] <<- data.frame(
    reform=reform, outcome=outcome, tech=tech,
    eff=r$eff, lo=r$lo, hi=r$hi, rel=r$rel, p=r$p,
    n_pre=r$n_pre, n_post=r$n_post, stringsAsFactors = FALSE)
}


# ===========================================================
# ID15 (278d post-IDA-reform pre, pre-blackout post)
# ===========================================================
cat("=== ID15 (278d post-IDA-reform pre + 40d post) ===\n")
ID_PRE <- "2024-06-14"; ID_REFORM <- "2025-03-19"; ID_POST_END <- "2025-04-27"
r <- run_bsts(panel, "ida_price_eur", ID_PRE, ID_REFORM, ID_POST_END, COVARS,
              "ID15 IDA price")
add("ID15", "price", NA, r)
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_ida", tech),
                ID_PRE, ID_REFORM, ID_POST_END, COVARS,
                sprintf("ID15 q_%s_ida", tech))
  add("ID15", "cleared_gwh", tech, r)
}

# ===========================================================
# DA15 (156d reforzada-constant pre + 40d post)
# ===========================================================
cat("\n=== DA15 (156d reforzada-constant pre + 40d post) ===\n")
DA_PRE <- "2025-04-28"; DA_REFORM <- "2025-10-01"; DA_POST_END <- "2025-11-09"
r <- run_bsts(panel, "da_price_eur", DA_PRE, DA_REFORM, DA_POST_END, COVARS,
              "DA15 DA price")
add("DA15", "price", NA, r)
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_da", tech),
                DA_PRE, DA_REFORM, DA_POST_END, COVARS,
                sprintf("DA15 q_%s_da", tech))
  add("DA15", "cleared_gwh", tech, r)
}

# ===========================================================
# Placebos (same windows, year prior)
# ===========================================================
cat("\n=== Placebo ID15-2024 (278d pre + 40d post) ===\n")
P_ID_PRE <- "2023-06-14"; P_ID_REF <- "2024-03-19"; P_ID_END <- "2024-04-27"
r <- run_bsts(panel, "ida_price_eur", P_ID_PRE, P_ID_REF, P_ID_END, COVARS,
              "PLB-ID IDA price")
add("PLB_ID15", "price", NA, r)
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_ida", tech),
                P_ID_PRE, P_ID_REF, P_ID_END, COVARS,
                sprintf("PLB-ID q_%s_ida", tech))
  add("PLB_ID15", "cleared_gwh", tech, r)
}

cat("\n=== Placebo DA15-2024 (156d pre + 40d post) ===\n")
P_DA_PRE <- "2024-04-28"; P_DA_REF <- "2024-10-01"; P_DA_END <- "2024-11-09"
r <- run_bsts(panel, "da_price_eur", P_DA_PRE, P_DA_REF, P_DA_END, COVARS,
              "PLB-DA DA price")
add("PLB_DA15", "price", NA, r)
for (tech in c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")) {
  r <- run_bsts(panel, sprintf("q_%s_gwh_da", tech),
                P_DA_PRE, P_DA_REF, P_DA_END, COVARS,
                sprintf("PLB-DA q_%s_da", tech))
  add("PLB_DA15", "cleared_gwh", tech, r)
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_daily_longpre.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_daily_longpre.csv\n", nrow(out_df)))
