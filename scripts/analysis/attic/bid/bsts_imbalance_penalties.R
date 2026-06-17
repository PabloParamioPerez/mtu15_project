# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- BSTS counterfactual on imbalance
#        settlement penalty prices (`prdvsuqh` / `prdvbaqh`) at the
#        ID15 (March 2025) and DA15 (October 2025) cutovers.
#
# Outcomes (daily means of quarter-hourly settlement prices, EUR/MWh):
#   price_dev_up = average price for system-up deviation (long position)
#   price_dev_dn = average price for system-down deviation (short position)
#
# WINDOWS:
#   ID15: pre 2024-12-01 to 2025-03-18  / post 2025-03-19 to 2025-04-27
#   DA15: pre 2025-04-28 to 2025-09-30  / post 2025-10-01 to 2025-12-31
#
# Covariates: wind_gwh, solar_gwh, gas_eur (from bsts_daily_panel)
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_imbalance.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })

repo <- "/Users/pabloparamio/Desktop/CEMFI/2nd Year/Master Thesis/mtu15_project"
IMB   <- file.path(repo, "data/derived/panels/bsts_imbalance_daily.parquet")
RENEW <- file.path(repo, "data/derived/panels/bsts_daily_panel.parquet")
OUT   <- file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_imbalance.csv")

CONFIGS <- list(
  list(reform="ID15", outcome="price_dev_up_eur_mwh", pre_lo="2024-12-01",
        post_lo="2025-03-19", post_hi="2025-04-27"),
  list(reform="ID15", outcome="price_dev_dn_eur_mwh", pre_lo="2024-12-01",
        post_lo="2025-03-19", post_hi="2025-04-27"),
  list(reform="DA15", outcome="price_dev_up_eur_mwh", pre_lo="2025-04-28",
        post_lo="2025-10-01", post_hi="2025-12-31"),
  list(reform="DA15", outcome="price_dev_dn_eur_mwh", pre_lo="2025-04-28",
        post_lo="2025-10-01", post_hi="2025-12-31")
)

imb <- read_parquet(IMB); imb$d <- as.Date(imb$d)
ren <- read_parquet(RENEW); ren$d <- as.Date(ren$d)
m <- merge(imb, ren[, c("d","wind_gwh","solar_gwh","gas_eur")], by="d", all.x=TRUE)

run_bsts <- function(outcome_col, pre_lo, post_lo, post_hi, side="real") {
  if (side == "placebo") {
    p_lo <- as.Date(post_lo) - 365
    p_hi <- as.Date(post_hi) - 365
    pre_lo_eff <- pmax(as.Date(pre_lo) - 365, as.Date("2024-12-01"))
    pre_end <- p_lo - 1
  } else {
    p_lo <- as.Date(post_lo); p_hi <- as.Date(post_hi)
    pre_lo_eff <- as.Date(pre_lo); pre_end <- p_lo - 1
  }
  sub <- m[m$d >= pre_lo_eff & m$d <= p_hi, ]
  sub <- sub[complete.cases(sub[, c(outcome_col, "wind_gwh","solar_gwh","gas_eur")]), ]
  sub <- sub[order(sub$d), ]
  if (nrow(sub[sub$d < p_lo, ]) < 60) return(NULL)
  ts <- zoo(as.matrix(sub[, c(outcome_col, "wind_gwh","solar_gwh","gas_eur")]),
             order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(ts, c(pre_lo_eff, pre_end), c(p_lo, p_hi),
                  model.args = list(niter=2000, nseasons=7, season.duration=1,
                                     prior.level.sd=0.01)),
    error = function(e) { cat(sprintf("ERR %s\n", conditionMessage(e))); NULL })
  if (is.null(imp) || is.null(imp$summary)) return(NULL)
  s <- imp$summary
  list(eff=as.numeric(s["Average","AbsEffect"]),
       lo =as.numeric(s["Average","AbsEffect.lower"]),
       hi =as.numeric(s["Average","AbsEffect.upper"]),
       p  =as.numeric(s$p[1]),
       n_pre=sum(sub$d < p_lo), n_post=sum(sub$d >= p_lo))
}

rows <- list()
for (cfg in CONFIGS) {
  for (side in c("real","placebo")) {
    cat(sprintf("=== %s %s [%s] ===\n", cfg$reform, cfg$outcome, side))
    r <- run_bsts(cfg$outcome, cfg$pre_lo, cfg$post_lo, cfg$post_hi, side)
    if (is.null(r)) {
      rows[[length(rows)+1]] <- data.frame(
        reform=cfg$reform, outcome=cfg$outcome, side=side,
        eff=NA, lo=NA, hi=NA, p=NA, n_pre=0, n_post=0,
        stringsAsFactors=FALSE); next
    }
    cat(sprintf("  eff=%+8.3f  CI=[%+7.2f,%+7.2f]  p=%.3f  n=%d/%d\n",
                 r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
    rows[[length(rows)+1]] <- data.frame(
      reform=cfg$reform, outcome=cfg$outcome, side=side,
      eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
      n_pre=r$n_pre, n_post=r$n_post, stringsAsFactors=FALSE)
  }
}
out <- do.call(rbind, rows)
dir.create(dirname(OUT), recursive=TRUE, showWarnings=FALSE)
write.csv(out, OUT, row.names=FALSE)
cat(sprintf("\nWrote %s with %d rows.\n", OUT, nrow(out)))
