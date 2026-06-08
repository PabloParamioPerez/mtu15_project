# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex sec:results:granularity-vs-reforzada -- BSTS on
#        daily REE ajuste-cost channels under headline Spec A methodology
#        (long pre-window 2022-01-01 + year-by-year wind and solar interactions).
#        Tests whether MTU15-IDA and MTU15-DA actually reduce channel costs net
#        of seasonality, or whether the apparent reductions are substitution
#        between channels (e.g. TR up shrink offset by Fase I up rise).
#
# Outcomes (kEUR/day):
#   cost_f1_up cost_f1_dn cost_f2_up cost_f2_dn
#   cost_tr_up cost_tr_dn cost_afrr_up cost_afrr_dn
#
# Reform windows mirror nuclear/price BSTS:
#   ID15 real:    pre 2022-01-01 -> 2025-03-18; post 2025-03-19 -> 2025-04-27
#   ID15 placebo: pre 2022-01-01 -> 2024-03-18; post 2024-03-19 -> 2024-04-27
#   DA15 real:    pre 2022-01-01 -> 2025-09-30; post 2025-10-01 -> 2025-12-31
#   DA15 placebo: pre 2022-01-01 -> 2024-09-30; post 2024-10-01 -> 2024-12-31
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_ajuste_costs.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel_fp <- file.path(repo, "data/derived/panels/bsts_daily_panel_costs.parquet")
out_dir  <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
OUTS <- c("cost_f1_up", "cost_f1_dn",
          "cost_f2_up", "cost_f2_dn",
          "cost_tr_up", "cost_tr_dn",
          "cost_afrr_up", "cost_afrr_dn")

PRE_LONG <- "2022-01-01"
CFGS <- list(
  list("ID15", "real",    PRE_LONG, "2025-03-19", "2025-04-27"),
  list("ID15", "placebo", PRE_LONG, "2024-03-19", "2024-04-27"),
  list("DA15", "real",    PRE_LONG, "2025-10-01", "2025-12-31"),
  list("DA15", "placebo", PRE_LONG, "2024-10-01", "2024-12-31")
)

add_year_interactions <- function(sub) {
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  new_cols <- c()
  for (y in yrs[-1]) {
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d",  y)]] <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  list(df = sub, cols = new_cols)
}

na_result <- function() list(eff = NA_real_, lo = NA_real_, hi = NA_real_,
                              p = NA_real_, n_pre = 0L, n_post = 0L)

run_bsts <- function(panel, response, pre_start, post_start, post_end, tag) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  %-40s SKIP (n=%d)\n", tag, nrow(sub))); return(na_result())
  }
  yr <- add_year_interactions(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_ts <- zoo(as.matrix(sub[, c(response, cov_set)]), order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                  model.args = list(niter = 2000, nseasons = 7,
                                     season.duration = 1,
                                     prior.level.sd = 0.005)),
    error   = function(e) { cat(sprintf("  %-40s ERROR %s\n", tag, conditionMessage(e))); NULL },
    warning = function(w) {
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
  cat(sprintf("  %-40s eff=%+10.1f  CI=[%+10.1f,%+10.1f]  p=%.4f  n=%d/%d\n",
              tag, eff, lo, hi, pv,
              sum(sub$d < cutover), sum(sub$d >= cutover)))
  list(eff = eff, lo = lo, hi = hi, p = pv,
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover))
}

panel <- read_parquet(panel_fp)
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]
cat(sprintf("Panel: %d days, %s to %s\n", nrow(panel),
            min(panel$d), max(panel$d)))

rows <- list()
for (cfg in CFGS) {
  reform <- cfg[[1]]; side <- cfg[[2]]
  pre_start <- cfg[[3]]; post_start <- cfg[[4]]; post_end <- cfg[[5]]
  cat(sprintf("\n=== %s %s (pre %s -> post %s..%s) ===\n",
              reform, side, pre_start, post_start, post_end))
  for (outc in OUTS) {
    tag <- sprintf("%s/%s/%s", reform, side, outc)
    r <- run_bsts(panel, outc, pre_start, post_start, post_end, tag)
    rows[[length(rows) + 1]] <- data.frame(
      reform = reform, side = side, outcome = outc,
      eff = r$eff, lo = r$lo, hi = r$hi, p = r$p,
      n_pre = r$n_pre, n_post = r$n_post,
      stringsAsFactors = FALSE)
  }
}

out <- do.call(rbind, rows)
out_fp <- file.path(out_dir, "bsts_ajuste_costs.csv")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
write.csv(out, out_fp, row.names = FALSE)
cat(sprintf("\nWrote %s with %d rows.\n", out_fp, nrow(out)))

cat("\n=== Placebo-net summary (kEUR/day) ===\n")
for (reform in c("ID15", "DA15")) {
  cat(sprintf("\n  %s:\n", reform))
  for (outc in OUTS) {
    real <- out[out$reform == reform & out$side == "real"    & out$outcome == outc, "eff"][1]
    plac <- out[out$reform == reform & out$side == "placebo" & out$outcome == outc, "eff"][1]
    if (!is.na(real) && !is.na(plac)) {
      cat(sprintf("    %-15s real=%+9.1f  placebo=%+9.1f  net=%+9.1f\n",
                  outc, real, plac, real - plac))
    }
  }
}
