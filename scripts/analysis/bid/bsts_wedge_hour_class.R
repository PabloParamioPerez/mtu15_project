# STATUS: ALIVE
# LAST-AUDIT: 2026-05-29
# FEEDS: thesis/provisional/advisor_memo.tex sec 4(ii) -- single BSTS on the
#        DA - IDA wedge SEPARATELY by hour-class (critical / midday / flat).
#        Mechanism check: if granularity-asymmetry is the wedge channel, the
#        wedge effect should concentrate in critical hours where within-hour
#        residual demand actually varies. Same long-history pre-window as
#        the daily wedge BSTS: pre 2022-01-01 -> 2025-03-18, post 2025-03-19
#        -> 2026-04-27.
#
# OUT: results/regressions/bid/mtu15_critical_flat/bsts_wedge_hour_class.csv
#      + pointwise/bsts_wedge_hour_class_pointwise_{critical,midday,flat}.csv

suppressPackageStartupMessages({
  library(arrow); library(CausalImpact); library(zoo)
})

args <- commandArgs(trailingOnly = FALSE)
sa <- args[grep("^--file=", args)]
sp <- if (length(sa)) sub("^--file=", "", sa) else
  "scripts/analysis/bid/bsts_wedge_hour_class.R"
repo <- normalizePath(file.path(dirname(sp), "..", "..", ".."))
panel_fp <- file.path(repo, "data/derived/panels/wedge_hour_class_panel.parquet")
out_dir <- file.path(repo, "results/regressions/bid/mtu15_critical_flat")

COVARS <- c("wind_gwh", "solar_gwh", "gas_eur")
OUTCOMES <- c("wedge_critical", "wedge_midday", "wedge_flat")
PRE_LO  <- as.Date("2022-01-01")
PRE_HI  <- as.Date("2025-03-18")
POST_LO <- as.Date("2025-03-19")
POST_HI <- as.Date("2026-04-27")


panel <- read_parquet(panel_fp)
panel$d <- as.Date(panel$d)
panel <- panel[order(panel$d), ]
cat(sprintf("Panel: %d days, %s to %s\n", nrow(panel),
            min(panel$d), max(panel$d)))


run_one <- function(response) {
  sub <- panel[panel$d >= PRE_LO & panel$d <= POST_HI, ]
  sub <- sub[complete.cases(sub[, c(response, COVARS)]), ]
  data_mat <- as.matrix(sub[, c(response, COVARS)])
  data_ts  <- zoo(data_mat, order.by = sub$d)
  set.seed(42)
  imp <- CausalImpact(data_ts, c(PRE_LO, PRE_HI), c(POST_LO, POST_HI),
                       model.args = list(niter = 2000, nseasons = 7,
                                          season.duration = 1))
  pw <- as.data.frame(imp$series); pw$date <- as.Date(rownames(pw))
  pw_dir <- file.path(out_dir, "pointwise")
  dir.create(pw_dir, recursive = TRUE, showWarnings = FALSE)
  write.csv(pw,
             file.path(pw_dir,
                        sprintf("bsts_wedge_hour_class_pointwise_%s.csv",
                                 sub("^wedge_", "", response))),
             row.names = FALSE)
  s <- imp$summary
  list(eff = s["Average","AbsEffect"],
       lo  = s["Average","AbsEffect.lower"],
       hi  = s["Average","AbsEffect.upper"],
       p   = s$p[1],
       n_pre  = sum(sub$d < POST_LO),
       n_post = sum(sub$d >= POST_LO))
}


rows <- list()
for (outcome in OUTCOMES) {
  cat(sprintf("\n=== %s ===\n", outcome))
  r <- run_one(outcome)
  cat(sprintf("  eff=%+7.2f  [%+6.2f, %+6.2f]  p=%5.3f  n=%d/%d\n",
              r$eff, r$lo, r$hi, r$p, r$n_pre, r$n_post))
  rows[[length(rows)+1]] <- data.frame(
    outcome=outcome, eff=r$eff, lo=r$lo, hi=r$hi, p=r$p,
    n_pre=r$n_pre, n_post=r$n_post,
    stringsAsFactors=FALSE)
}

out_df <- do.call(rbind, rows)
write.csv(out_df, file.path(out_dir, "bsts_wedge_hour_class.csv"),
           row.names=FALSE)
cat(sprintf("\nWrote %d rows to bsts_wedge_hour_class.csv\n", nrow(out_df)))
