# STATUS: ALIVE
# LAST-AUDIT: 2026-06-04
# FEEDS: thesis/paper/thesis.tex - Spec A BSTS on per-tech IDA in-band
#        sell-share around ID15 and DA15 cutovers. Same windows / placebos
#        as Spec A for prices (Table 7).
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_ida_sell_share.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

.cmdargs <- commandArgs(trailingOnly = FALSE)
.thisfile <- sub("^--file=", "", .cmdargs[grep("^--file=", .cmdargs)])
repo <- normalizePath(file.path(dirname(.thisfile), "..", "..", ".."))
panel <- read_parquet(file.path(repo, "data/derived/panels/ida_inband_sell_share_daily.parquet"))
cov   <- read_parquet(file.path(repo, "data/derived/panels/bsts_quantities_panel.parquet"))
panel$d <- as.Date(panel$d); cov$d <- as.Date(cov$d)
COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
TECHS  <- c("CCGT","Hydro","Hydro_pump","Nuclear","Solar PV","Wind")

run_one <- function(tech, pre_lo, post_lo, post_hi, tag) {
  ps <- as.Date(pre_lo); pe <- as.Date(post_hi); cutover <- as.Date(post_lo)
  sub <- panel[panel$tech == tech & panel$d >= ps & panel$d <= pe, c("d","sell_share")]
  if (nrow(sub) < 30) { cat(sprintf("%-32s SKIP (n=%d)\n", tag, nrow(sub))); return(NULL) }
  sub <- merge(sub, cov[, c("d", COVARS)], by="d")
  sub <- sub[complete.cases(sub),]
  sub <- sub[order(sub$d), ]
  if (max(sub$d) < cutover || sum(sub$d >= cutover) < 5) {
    cat(sprintf("%-32s SKIP (insufficient post)\n", tag)); return(NULL)
  }
  if (sd(sub$sell_share[sub$d < cutover]) < 1e-6) {
    cat(sprintf("%-32s SKIP (constant pre)\n", tag)); return(NULL)
  }
  data_ts <- zoo(as.matrix(sub[, c("sell_share", COVARS)]), order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(data_ts, c(ps, cutover - 1), c(cutover, pe),
                  model.args = list(niter = 2000, nseasons = 7, season.duration = 1)),
    error = function(e) { cat(sprintf("%-32s ERROR %s\n", tag, conditionMessage(e))); NULL }
  )
  if (is.null(imp)) return(NULL)
  s <- imp$summary
  eff <- s["Average","AbsEffect"]; lo <- s["Average","AbsEffect.lower"]
  hi <- s["Average","AbsEffect.upper"]; pval <- s$p[1]
  npre <- sum(sub$d < cutover); npost <- sum(sub$d >= cutover)
  cat(sprintf("%-32s eff=%+7.4f  CI=[%+7.4f,%+7.4f]  p=%.3f  n=%d/%d\n",
              tag, eff, lo, hi, pval, npre, npost))
  data.frame(tech=tech, scenario=tag, effect=eff, lower=lo, upper=hi,
             p=pval, n_pre=npre, n_post=npost)
}

cat("\n=== ID15: real (long pre 2024-06-14, post 2025-03-19..2025-04-27) ===\n")
rows <- list()
for (tech in TECHS) {
  rows[[length(rows)+1]] <- run_one(tech, "2024-06-14", "2025-03-19",
                                     "2025-04-27", sprintf("REAL ID15 %s", tech))
}

cat("\n=== ID15: placebo 2024 (long pre 2023-06-14, post 2024-03-19..2024-04-27) ===\n")
for (tech in TECHS) {
  rows[[length(rows)+1]] <- run_one(tech, "2023-06-14", "2024-03-19",
                                     "2024-04-27", sprintf("PLB24 ID15 %s", tech))
}

cat("\n=== DA15: real (reforzada-constant pre 2025-04-28, post 2025-10-01..2025-11-09) ===\n")
for (tech in TECHS) {
  rows[[length(rows)+1]] <- run_one(tech, "2025-04-28", "2025-10-01",
                                     "2025-11-09", sprintf("REAL DA15 %s", tech))
}

cat("\n=== DA15: placebo 2024 (pre 2024-04-28, fake cutover 2024-10-01) ===\n")
for (tech in TECHS) {
  rows[[length(rows)+1]] <- run_one(tech, "2024-04-28", "2024-10-01",
                                     "2024-11-09", sprintf("PLB24 DA15 %s", tech))
}

out <- do.call(rbind, Filter(Negate(is.null), rows))
write.csv(out, file.path(repo,
  "results/regressions/bid/mtu15_critical_flat/bsts_ida_sell_share.csv"),
  row.names = FALSE)
cat("\nDone.\n")
