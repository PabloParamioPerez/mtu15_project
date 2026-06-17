# STATUS: ALIVE
# LAST-AUDIT: 2026-06-06
# FEEDS: thesis/paper/thesis.tex --- BSTS counterfactual on
#        (1) total daily |imbalance| volume (sum of |endcurqh|, system-aggregate)
#        (2) BS3 daily = imresecqh / (endvBRPqh + enrepscqh) -- regulatory
#            penalty per |MWh| of deviation (secondary-band channel)
#        (3) RAD3 daily = imrad / (endvBRPqh + enrepscqh) -- regulatory
#            penalty per |MWh| of deviation (active-demand-response channel)
#
# These are the TRULY REGULATORY components of the imbalance penalty, as
# defined in the expert webinar (27.02.2025 slides "Calculo BS3 y RAD3"
# and "Situacion DSV y BS3DV+RAD3DV"). They are administratively set by
# formula from the liquicomun common fund, NOT market clearing prices.
# By contrast, prdvsuqh/prdvbaqh are market-clearing balancing prices.
#
# WINDOWS:
#   ID15: pre 2024-12-01 to 2025-03-18 / post 2025-03-19 to 2025-04-27
#   DA15: pre 2025-04-28 to 2025-09-30 / post 2025-10-01 to 2025-12-31
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_imbalance_penalty.csv

suppressPackageStartupMessages({ library(arrow); library(CausalImpact); library(zoo) })

.cmdargs <- commandArgs(trailingOnly = FALSE)
.thisfile <- sub("^--file=", "", .cmdargs[grep("^--file=", .cmdargs)])
repo <- normalizePath(file.path(dirname(.thisfile), "..", "..", ".."))
PANEL <- file.path(repo, "data/derived/panels/bsts_imbalance_penalty_daily.parquet")
RENEW <- file.path(repo, "data/derived/panels/bsts_daily_panel.parquet")
OUT   <- file.path(repo, "results/regressions/bid/mtu15_critical_flat/bsts_imbalance_penalty.csv")

CONFIGS <- list(
  list(reform="ID15", outcome="abs_imbalance_mwh", pre_lo="2024-12-01",
        post_lo="2025-03-19", post_hi="2025-04-27"),
  list(reform="ID15", outcome="bs3_eur_mwh", pre_lo="2024-12-01",
        post_lo="2025-03-19", post_hi="2025-04-27"),
  list(reform="ID15", outcome="rad3_eur_mwh", pre_lo="2024-12-01",
        post_lo="2025-03-19", post_hi="2025-04-27"),
  list(reform="DA15", outcome="abs_imbalance_mwh", pre_lo="2025-04-28",
        post_lo="2025-10-01", post_hi="2025-12-31"),
  list(reform="DA15", outcome="bs3_eur_mwh", pre_lo="2025-04-28",
        post_lo="2025-10-01", post_hi="2025-12-31"),
  list(reform="DA15", outcome="rad3_eur_mwh", pre_lo="2025-04-28",
        post_lo="2025-10-01", post_hi="2025-12-31")
)

panel <- read_parquet(PANEL); panel$d <- as.Date(panel$d)
ren   <- read_parquet(RENEW); ren$d   <- as.Date(ren$d)
m <- merge(panel, ren[, c("d","wind_gwh","solar_gwh","gas_eur")], by="d", all.x=TRUE)

run_bsts <- function(outcome_col, pre_lo, post_lo, post_hi) {
  p_lo <- as.Date(post_lo); p_hi <- as.Date(post_hi); pre_end <- p_lo - 1
  sub <- m[m$d >= as.Date(pre_lo) & m$d <= p_hi, ]
  sub <- sub[complete.cases(sub[, c(outcome_col, "wind_gwh","solar_gwh","gas_eur")]), ]
  sub <- sub[order(sub$d), ]
  if (nrow(sub[sub$d < p_lo, ]) < 60) return(NULL)
  ts <- zoo(as.matrix(sub[, c(outcome_col, "wind_gwh","solar_gwh","gas_eur")]),
             order.by = sub$d)
  set.seed(42)
  imp <- tryCatch(
    CausalImpact(ts, c(as.Date(pre_lo), pre_end), c(p_lo, p_hi),
                  model.args = list(niter=2000, nseasons=7, season.duration=1,
                                     prior.level.sd=0.01)),
    error = function(e) { cat(sprintf("ERR %s\n", conditionMessage(e))); NULL })
  if (is.null(imp) || is.null(imp$summary)) return(NULL)
  s <- imp$summary
  pre_mean  <- mean(as.numeric(sub[sub$d < p_lo, outcome_col]), na.rm=TRUE)
  list(eff=as.numeric(s["Average","AbsEffect"]),
       lo =as.numeric(s["Average","AbsEffect.lower"]),
       hi =as.numeric(s["Average","AbsEffect.upper"]),
       rel=as.numeric(s["Average","RelEffect"]),
       p  =as.numeric(s$p[1]),
       pre_mean=pre_mean,
       n_pre=sum(sub$d < p_lo), n_post=sum(sub$d >= p_lo))
}

rows <- list()
for (cfg in CONFIGS) {
  cat(sprintf("=== %s %s ===\n", cfg$reform, cfg$outcome))
  r <- run_bsts(cfg$outcome, cfg$pre_lo, cfg$post_lo, cfg$post_hi)
  if (is.null(r)) {
    rows[[length(rows)+1]] <- data.frame(
      reform=cfg$reform, outcome=cfg$outcome,
      eff=NA, lo=NA, hi=NA, rel=NA, p=NA, pre_mean=NA, n_pre=0, n_post=0,
      stringsAsFactors=FALSE); next
  }
  cat(sprintf("  eff=%+9.3f  [%+8.2f,%+8.2f]  rel=%+5.1f%%  p=%.3f  pre_mean=%8.2f  n=%d/%d\n",
               r$eff, r$lo, r$hi, 100*r$rel, r$p, r$pre_mean, r$n_pre, r$n_post))
  rows[[length(rows)+1]] <- data.frame(
    reform=cfg$reform, outcome=cfg$outcome,
    eff=r$eff, lo=r$lo, hi=r$hi, rel=r$rel, p=r$p, pre_mean=r$pre_mean,
    n_pre=r$n_pre, n_post=r$n_post, stringsAsFactors=FALSE)
}
out <- do.call(rbind, rows)
dir.create(dirname(OUT), recursive=TRUE, showWarnings=FALSE)
write.csv(out, OUT, row.names=FALSE)
cat(sprintf("\nWrote %s with %d rows.\n", OUT, nrow(out)))
