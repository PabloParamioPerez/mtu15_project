# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: Survives-the-renewable-coefficient-control diagnostic across
#        every Spec A outcome. Same year-by-year wind+solar interaction
#        spec as bsts_daily_year_interactions.R (just prices), now
#        applied to:
#          (a) per-tech cleared MWh (DA and IDA, ID15 + DA15)
#          (b) within-day wedge SD (overall + critical hour-class)
#          (c) IDA in-band sell-share per tech
#        Both real and same-calendar 2024 placebo, so the user can read
#        the net column with the same logic as Spec A.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_year_int_all.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_year_interactions_all_outcomes.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

# Panels
qpanel <- read_parquet(file.path(repo,
  "data/derived/panels/bsts_quantities_panel.parquet"))
qpanel$d <- as.Date(qpanel$d); qpanel <- qpanel[order(qpanel$d), ]

wpanel <- read_parquet(file.path(repo,
  "data/derived/panels/wedge_volatility_panel.parquet"))
wpanel$d <- as.Date(wpanel$d); wpanel <- wpanel[order(wpanel$d), ]

spanel <- read_parquet(file.path(repo,
  "data/derived/panels/ida_inband_sell_share_daily.parquet"))
spanel$d <- as.Date(spanel$d); spanel <- spanel[order(spanel$d), ]

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  new_cols <- c()
  for (y in yrs[-1]) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

na_result <- function() list(eff=NA_real_, lo=NA_real_, hi=NA_real_, p=NA_real_,
                              n_pre=0L, n_post=0L, n_cov=0L)

run_bsts <- function(panel, response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  %s: SKIP (n=%d)\n", tag, nrow(sub))); return(na_result())
  }
  # Guard against constant pre-window (BSTS aborts inference)
  pre_resp <- as.numeric(sub[[response]][sub$d < cutover])
  pre_resp <- pre_resp[!is.na(pre_resp)]
  if (length(pre_resp) < 5) {
    cat(sprintf("  %s: SKIP (pre-window too short, n_pre=%d)\n", tag, length(pre_resp)))
    return(na_result())
  }
  if (sd(pre_resp) < 1e-9) {
    cat(sprintf("  %s: SKIP (pre-window constant)\n", tag))
    return(na_result())
  }
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                  model.args = list(niter = 2000, nseasons = 7,
                                     season.duration = 1,
                                     prior.level.sd = 0.005)),
    error = function(e) { cat(sprintf("  %s: ERROR %s\n", tag, conditionMessage(e))); NULL },
    warning = function(w) {
      cat(sprintf("  %s: WARN %s\n", tag, conditionMessage(w)))
      tryCatch(suppressWarnings(
        CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                      model.args = list(niter = 2000, nseasons = 7,
                                         season.duration = 1,
                                         prior.level.sd = 0.005))),
               error = function(e) NULL)
    }
  )
  if (is.null(imp) || is.null(imp$summary) || nrow(imp$summary) == 0) return(na_result())
  s <- imp$summary
  eff <- tryCatch(as.numeric(s["Average", "AbsEffect"]), error = function(e) NA_real_)
  lo  <- tryCatch(as.numeric(s["Average", "AbsEffect.lower"]), error = function(e) NA_real_)
  hi  <- tryCatch(as.numeric(s["Average", "AbsEffect.upper"]), error = function(e) NA_real_)
  pv  <- tryCatch(as.numeric(s$p[1]), error = function(e) NA_real_)
  if (length(eff) == 0 || is.na(eff)) return(na_result())
  cat(sprintf("  %-40s  eff=%+9.3f  CI=[%+9.3f,%+9.3f]  p=%.3f  n_pre=%d  n_post=%d\n",
              tag, eff, lo, hi, pv,
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  list(eff = eff, lo = lo, hi = hi, p = pv,
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover),
       n_cov = length(cov_set))
}

rows <- list()
add <- function(reform, outcome_family, outcome, side, r) {
  rows[[length(rows)+1]] <<- data.frame(
    reform = reform, outcome_family = outcome_family,
    outcome = outcome, side = side,
    eff = r$eff, lo = r$lo, hi = r$hi, p = r$p,
    n_pre = r$n_pre, n_post = r$n_post, n_cov = r$n_cov,
    stringsAsFactors = FALSE)
}

# ===========================================================
# WINDOWS (long pre = panel start; post = same as Spec A)
# ===========================================================
PRE_LONG  <- "2022-01-01"
ID15_REF  <- "2025-03-19"; ID15_END <- "2025-04-27"
DA15_REF  <- "2025-10-01"; DA15_END <- "2025-11-09"
PLB_ID    <- "2024-03-19"; PLB_ID_END <- "2024-04-27"
PLB_DA    <- "2024-10-01"; PLB_DA_END <- "2024-11-09"

# ===========================================================
# (a) Per-tech cleared MWh, DA and IDA, both reforms + placebos
# ===========================================================
cat("\n=== Per-tech cleared MWh ===\n")
TECHS <- c("ccgt", "hydro", "hydro_pump", "wind", "solar", "nuclear")
for (mkt in c("da", "ida")) {
  for (tech in TECHS) {
    resp <- sprintf("q_%s_gwh_%s", tech, mkt)
    # ID15 real + placebo
    r <- run_bsts(qpanel, resp, PRE_LONG, ID15_REF, ID15_END,
                  sprintf("ID15 %s %s real", toupper(mkt), tech))
    add("ID15", "cleared_gwh", resp, "real", r)
    r <- run_bsts(qpanel, resp, PRE_LONG, PLB_ID, PLB_ID_END,
                  sprintf("ID15 %s %s plb24", toupper(mkt), tech))
    add("ID15", "cleared_gwh", resp, "placebo", r)
    # DA15 real + placebo
    r <- run_bsts(qpanel, resp, PRE_LONG, DA15_REF, DA15_END,
                  sprintf("DA15 %s %s real", toupper(mkt), tech))
    add("DA15", "cleared_gwh", resp, "real", r)
    r <- run_bsts(qpanel, resp, PRE_LONG, PLB_DA, PLB_DA_END,
                  sprintf("DA15 %s %s plb24", toupper(mkt), tech))
    add("DA15", "cleared_gwh", resp, "placebo", r)
  }
}

# ===========================================================
# (b) Wedge SD (overall + critical) under each reform
# ===========================================================
cat("\n=== Wedge SD ===\n")
for (resp in c("wedge_sd", "wedge_sd_critical")) {
  r <- run_bsts(wpanel, resp, PRE_LONG, ID15_REF, ID15_END,
                sprintf("ID15 %s real", resp))
  add("ID15", "wedge_sd", resp, "real", r)
  r <- run_bsts(wpanel, resp, PRE_LONG, PLB_ID, PLB_ID_END,
                sprintf("ID15 %s plb24", resp))
  add("ID15", "wedge_sd", resp, "placebo", r)
  r <- run_bsts(wpanel, resp, PRE_LONG, DA15_REF, DA15_END,
                sprintf("DA15 %s real", resp))
  add("DA15", "wedge_sd", resp, "real", r)
  r <- run_bsts(wpanel, resp, PRE_LONG, PLB_DA, PLB_DA_END,
                sprintf("DA15 %s plb24", resp))
  add("DA15", "wedge_sd", resp, "placebo", r)
}

# ===========================================================
# (c) IDA in-band sell-share per tech (long pre, real + placebo)
# Sell-share panel is tech-long, so we reshape per tech.
# ===========================================================
cat("\n=== IDA sell-share per tech ===\n")
SS_TECHS <- c("CCGT", "Hydro", "Hydro_pump", "Wind", "Solar PV", "Nuclear")
# Need to attach covariates (wind/solar/gas) from qpanel to spanel
covs <- qpanel[, c("d", BASE_COVARS)]
for (tech in SS_TECHS) {
  sub_t <- spanel[spanel$tech == tech, c("d", "sell_share")]
  sub_t <- merge(sub_t, covs, by = "d")
  resp <- "sell_share"
  tag_safe <- gsub(" ", "_", tech)
  r <- run_bsts(sub_t, resp, PRE_LONG, ID15_REF, ID15_END,
                sprintf("ID15 SS %s real", tag_safe))
  add("ID15", "sell_share", paste0("SS_", tag_safe), "real", r)
  # ID15 placebo unavailable (pre-reform sell-share is constant 1.00) -> still try
  r <- run_bsts(sub_t, resp, PRE_LONG, PLB_ID, PLB_ID_END,
                sprintf("ID15 SS %s plb24", tag_safe))
  add("ID15", "sell_share", paste0("SS_", tag_safe), "placebo", r)
  r <- run_bsts(sub_t, resp, PRE_LONG, DA15_REF, DA15_END,
                sprintf("DA15 SS %s real", tag_safe))
  add("DA15", "sell_share", paste0("SS_", tag_safe), "real", r)
  r <- run_bsts(sub_t, resp, PRE_LONG, PLB_DA, PLB_DA_END,
                sprintf("DA15 SS %s plb24", tag_safe))
  add("DA15", "sell_share", paste0("SS_", tag_safe), "placebo", r)
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_year_int_all.csv"), row.names = FALSE)
cat(sprintf("\nWrote %d rows to bsts_year_int_all.csv\n", nrow(out_df)))
