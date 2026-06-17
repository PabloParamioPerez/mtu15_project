# STATUS: ALIVE
# LAST-AUDIT: 2026-06-08
# FEEDS: thesis/paper/thesis.tex --- ID15 BSTS price effect at progressively
#        longer post-windows. The 40-day pre-blackout headline is unchanged;
#        90/180/196-day variants mix in the reforzada regime in the post.
#
# Year-by-renewable: 2024+2025 pooled (ID15 pre-window is winter-only 2025).
# Windows tested:
#   40d  : 2025-03-19 to 2025-04-27 (pre-blackout)
#   90d  : 2025-03-19 to 2025-06-16 (crosses blackout)
#   180d : 2025-03-19 to 2025-09-14 (deep into reforzada)
#   196d : 2025-03-19 to 2025-09-30 (up to DA15 cutover)
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_id15_longpost.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
panel <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); panel <- panel[order(panel$d), ]

BASE_COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")

add_year_interactions_pooled <- function(sub) {
  # ID15 spec: 2024+2025 pooled, 2023 separate, 2022 reference.
  yrs <- sort(unique(as.integer(format(sub$d, "%Y"))))
  if (length(yrs) <= 1) return(list(df = sub, cols = c()))
  ref_yr <- yrs[1]; combined_set <- c(2024L, 2025L); new_cols <- c()
  for (y in yrs) {
    if (y == ref_yr || y %in% combined_set) next
    is_y <- as.integer(format(sub$d, "%Y") == as.character(y))
    sub[[sprintf("wind_x_%d", y)]]  <- sub$wind_gwh  * is_y
    sub[[sprintf("solar_x_%d", y)]] <- sub$solar_gwh * is_y
    new_cols <- c(new_cols, sprintf("wind_x_%d", y), sprintf("solar_x_%d", y))
  }
  if (any(yrs %in% combined_set)) {
    is_2425 <- as.integer(as.integer(format(sub$d, "%Y")) %in% combined_set)
    sub[["wind_x_2024_25"]]  <- sub$wind_gwh  * is_2425
    sub[["solar_x_2024_25"]] <- sub$solar_gwh * is_2425
    new_cols <- c(new_cols, "wind_x_2024_25", "solar_x_2024_25")
  }
  list(df = sub, cols = new_cols)
}

run_bsts <- function(response, pre_start, post_start, post_end, tag, side) {
  ps <- as.Date(pre_start); pe <- as.Date(post_end); cutover <- as.Date(post_start)
  sub <- panel[panel$d >= ps & panel$d <= pe, ]
  sub <- sub[complete.cases(sub[, c(response, BASE_COVARS)]), ]
  if (nrow(sub) < 30 || max(sub$d) < cutover) {
    cat(sprintf("  %s: skip (n=%d)\n", tag, nrow(sub))); return(NULL)
  }
  yr <- add_year_interactions_pooled(sub); sub <- yr$df
  cov_set <- c(BASE_COVARS, yr$cols)
  data_mat <- as.matrix(sub[, c(response, cov_set)])
  data_ts <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                       model.args = list(niter = 10000, nseasons = 7,
                                          season.duration = 1,
                                          prior.level.sd = 0.005))
  s <- imp$summary
  list(side=side, tag=tag,
       eff = s["Average","AbsEffect"], lo = s["Average","AbsEffect.lower"],
       hi = s["Average","AbsEffect.upper"], p = s$p[1],
       n_pre = sum(sub$d < cutover), n_post = sum(sub$d >= cutover),
       n_cov = length(cov_set))
}

# Window-length sets — real reform = 2025-03-19 cutover; placebo = 2024-03-19 cutover
windows <- list(
  list(label="40d",  cutover_real="2025-03-19", post_hi_real="2025-04-27",
                     cutover_plb ="2024-03-19", post_hi_plb ="2024-04-27"),
  list(label="90d",  cutover_real="2025-03-19", post_hi_real="2025-06-16",
                     cutover_plb ="2024-03-19", post_hi_plb ="2024-06-16"),
  list(label="180d", cutover_real="2025-03-19", post_hi_real="2025-09-14",
                     cutover_plb ="2024-03-19", post_hi_plb ="2024-09-14"),
  list(label="196d", cutover_real="2025-03-19", post_hi_real="2025-09-30",
                     cutover_plb ="2024-03-19", post_hi_plb ="2024-09-30")
)
outcomes <- list(
  list(name="ida_price_eur", lbl="IDA price"),
  list(name="da_price_eur",  lbl="DA price")
)

rows <- list()
for (w in windows) {
  cat(sprintf("\n=== ID15 post-window length %s ===\n", w$label))
  for (o in outcomes) {
    r1 <- run_bsts(o$name, "2022-01-01", w$cutover_real, w$post_hi_real,
                    sprintf("ID15 %s post=%s", o$lbl, w$label), "real")
    if (!is.null(r1)) {
      cat(sprintf("  %s  REAL    eff=%+8.2f  CI=[%+7.2f,%+7.2f] p=%.3f n_pre=%d n_post=%d\n",
                  o$lbl, r1$eff, r1$lo, r1$hi, r1$p, r1$n_pre, r1$n_post))
      rows[[length(rows)+1]] <- data.frame(
        window=w$label, outcome=o$name, side="real", eff=r1$eff,
        lo=r1$lo, hi=r1$hi, p=r1$p, n_pre=r1$n_pre, n_post=r1$n_post)
    }
    r2 <- run_bsts(o$name, "2022-01-01", w$cutover_plb, w$post_hi_plb,
                    sprintf("ID15 %s post=%s PLB", o$lbl, w$label), "placebo")
    if (!is.null(r2)) {
      cat(sprintf("  %s  PLB     eff=%+8.2f  CI=[%+7.2f,%+7.2f] p=%.3f n_pre=%d n_post=%d\n",
                  o$lbl, r2$eff, r2$lo, r2$hi, r2$p, r2$n_pre, r2$n_post))
      rows[[length(rows)+1]] <- data.frame(
        window=w$label, outcome=o$name, side="placebo", eff=r2$eff,
        lo=r2$lo, hi=r2$hi, p=r2$p, n_pre=r2$n_pre, n_post=r2$n_post)
    }
  }
}
out <- do.call(rbind, rows)
write.csv(out, file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_id15_longpost.csv"),
          row.names=FALSE)
cat("\nWrote bsts_id15_longpost.csv\n")
