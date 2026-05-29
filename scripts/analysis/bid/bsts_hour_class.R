# STATUS: ALIVE
# LAST-AUDIT: 2026-05-28
# FEEDS: advisor_memo.tex sec 4 -- per-(tech, market, hour_class) BSTS on
#        bid-level outcomes (in-band MW-weighted mean bid price; total in-band
#        quantity). For each reform (ID15, DA15) and 2024 placebo:
#          tech         in {ccgt, hydro, hydro_pump}
#          market       in {da, ida}
#          hour_class   in {critical, midday, flat}
#          outcome      in {p, q}
#        Same 156-/278-day pre + 40-day post windows as the daily BSTS in
#        bsts_daily_longpre.R; same wind+solar+gas covariates.
#
#        Logic: instead of applying BSTS to the (critical - flat) differential
#        (which would destroy the seasonal structure BSTS uses), run BSTS on
#        each hour-class series separately and then form the (critical - flat)
#        placebo-net contrast in post-processing. The joint 95% credible
#        interval propagates the four posterior variances (real_crit, real_flat,
#        plb_crit, plb_flat) treating them as independent.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_hour_class.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_hour_class.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

panel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_hour_class_panel.parquet"))
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

TECHS       <- c("ccgt", "hydro", "hydro_pump")
MARKETS     <- c("da", "ida")
HOUR_CLASSES <- c("critical", "midday", "flat")
OUTCOMES    <- c("p", "q")

ID_PRE    <- "2024-06-14"; ID_REFORM <- "2025-03-19"; ID_POST_END <- "2025-04-27"
DA_PRE    <- "2025-04-28"; DA_REFORM <- "2025-10-01"; DA_POST_END <- "2025-11-09"
P_ID_PRE  <- "2023-06-14"; P_ID_REF  <- "2024-03-19"; P_ID_END  <- "2024-04-27"
P_DA_PRE  <- "2024-04-28"; P_DA_REF  <- "2024-10-01"; P_DA_END  <- "2024-11-09"


run_bsts <- function(panel, response, pre_start, post_start, post_end,
                      covars, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end)
  cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, covars)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    return(NULL)
  }
  data_mat <- as.matrix(sub[, c(response, covars)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  pre_period  <- c(ps, cutover - 1)
  post_period <- c(cutover, pe)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, pre_period, post_period,
                  model.args = list(niter = 1500, nseasons = 7,
                                     season.duration = 1)),
    error = function(e) {
      message(sprintf("  [%s] BSTS error: %s", tag, conditionMessage(e)));
      NULL
    })
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  list(eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], rel = s["Average","RelEffect"],
       p = s$p[1],
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover))
}


rows <- list()
add <- function(reform, outcome, tech, market, hour_class, r) {
  if (is.null(r)) return()
  rows[[length(rows)+1]] <<- data.frame(
    reform = reform, outcome = outcome, tech = tech,
    market = market, hour_class = hour_class,
    eff = r$eff, lo = r$lo, hi = r$hi, rel = r$rel, p = r$p,
    n_pre = r$n_pre, n_post = r$n_post, stringsAsFactors = FALSE)
}


loop_one_reform <- function(label, pre, ref, end) {
  cat(sprintf("\n=== %s (%s -> %s -> %s) ===\n", label, pre, ref, end))
  for (outcome in OUTCOMES) {
    for (tech in TECHS) {
      for (market in MARKETS) {
        for (hc in HOUR_CLASSES) {
          col <- sprintf("%s_%s_%s_%s", outcome, tech, market, hc)
          if (!col %in% names(panel)) next
          tag <- sprintf("%s %s", label, col)
          r <- run_bsts(panel, col, pre, ref, end, COVARS, tag)
          add(label, outcome, tech, market, hc, r)
        }
      }
    }
  }
}


loop_one_reform("ID15",     ID_PRE,   ID_REFORM, ID_POST_END)
loop_one_reform("DA15",     DA_PRE,   DA_REFORM, DA_POST_END)
loop_one_reform("PLB_ID15", P_ID_PRE, P_ID_REF,  P_ID_END)
loop_one_reform("PLB_DA15", P_DA_PRE, P_DA_REF,  P_DA_END)

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_hour_class.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_hour_class.csv\n", nrow(out_df)))
