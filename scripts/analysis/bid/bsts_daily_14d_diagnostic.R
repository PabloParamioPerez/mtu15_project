# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex Table 3 -- drift diagnostic.
#        Re-run the headline Spec A BSTS with a 14-day post-window instead
#        of the 40-day window so we can spot effects that emerge only in
#        the late post (the BSTS-extrapolation-drift signature). Same pre,
#        same covariates, same model.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_daily_14d_diagnostic.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_daily_14d_diagnostic.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]
COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
TECHS <- c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")

run_one <- function(response, pre_start, post_start, post_end) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) return(NULL)
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                                model.args = list(niter = 2000, nseasons = 7,
                                                   season.duration = 1)),
                  error = function(e) NULL)
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       p   = s$p[1])
}

# 14-day post-windows: same pre as bsts_daily_longpre.R, post truncated to
# day 1-14 after cutover.
# ID15: pre 2024-06-14 -> 2025-03-18; post 2025-03-19 -> 2025-04-01 (14d)
# DA15: pre 2025-04-28 -> 2025-09-30; post 2025-10-01 -> 2025-10-14 (14d)
PRE_ID  <- "2024-06-14"; POST_ID  <- "2025-03-19"; END_ID  <- "2025-04-01"
PRE_DA  <- "2025-04-28"; POST_DA  <- "2025-10-01"; END_DA  <- "2025-10-14"
PRE_PID <- "2023-06-14"; POST_PID <- "2024-03-19"; END_PID <- "2024-04-01"
PRE_PDA <- "2024-04-28"; POST_PDA <- "2024-10-01"; END_PDA <- "2024-10-14"

# Configs: (reform, side, response, pre, post_start, post_end)
CFGS <- list(
  list("ID15", "real",    "da_price_eur",  PRE_ID,  POST_ID,  END_ID),
  list("ID15", "real",    "ida_price_eur", PRE_ID,  POST_ID,  END_ID),
  list("ID15", "placebo", "da_price_eur",  PRE_PID, POST_PID, END_PID),
  list("ID15", "placebo", "ida_price_eur", PRE_PID, POST_PID, END_PID),
  list("DA15", "real",    "da_price_eur",  PRE_DA,  POST_DA,  END_DA),
  list("DA15", "real",    "ida_price_eur", PRE_DA,  POST_DA,  END_DA),
  list("DA15", "placebo", "da_price_eur",  PRE_PDA, POST_PDA, END_PDA),
  list("DA15", "placebo", "ida_price_eur", PRE_PDA, POST_PDA, END_PDA)
)
# Append per-tech cleared GWh (DA and IDA) for both reforms
for (cfg in list(list("ID15","real",PRE_ID,POST_ID,END_ID),
                 list("ID15","placebo",PRE_PID,POST_PID,END_PID),
                 list("DA15","real",PRE_DA,POST_DA,END_DA),
                 list("DA15","placebo",PRE_PDA,POST_PDA,END_PDA))) {
  for (tech in TECHS) for (mkt in c("da","ida")) {
    CFGS[[length(CFGS)+1]] <- list(cfg[[1]], cfg[[2]],
                                    sprintf("q_%s_gwh_%s", tech, mkt),
                                    cfg[[3]], cfg[[4]], cfg[[5]])
  }
}

rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]; resp <- cfg[[3]]
  pre_lo <- cfg[[4]]; post_lo <- cfg[[5]]; post_hi <- cfg[[6]]
  r <- run_one(resp, pre_lo, post_lo, post_hi)
  if (is.null(r)) next
  cat(sprintf("%s %-8s %-22s  eff=%+8.2f  [%+8.2f,%+8.2f]  p=%.3f\n",
              reform, side, resp, r$eff, r$lo, r$hi, r$p))
  rows[[length(rows)+1]] <- data.frame(
    reform=reform, side=side, response=resp,
    eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
    stringsAsFactors=FALSE)
}

out_df <- do.call(rbind, rows)
write.csv(out_df,
  file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_daily_14d_diagnostic.csv"),
  row.names = FALSE)
cat(sprintf("\nWrote %d rows\n", nrow(out_df)))
